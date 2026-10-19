package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const parserResponseLimit = 2 << 20

type Parser interface {
	Parse(context.Context, domain.DocumentVersion, []byte) (string, error)
}

type documentParser struct {
	endpoint string
	client   *http.Client
}

// NewParser accepts an operator-owned service address, never a document URL.
// An empty address still permits local UTF-8 text and Markdown parsing.
func NewParser(tikaURL string) (Parser, error) {
	p := &documentParser{}
	if tikaURL == "" {
		return p, nil
	}
	u, err := url.Parse(tikaURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.IndexFunc(tikaURL, unicode.IsSpace) >= 0 {
		return nil, &Failure{Code: "parser_unavailable"}
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/tika/text"
	u.RawPath = ""
	p.endpoint = u.String()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.MaxResponseHeaderBytes = 64 << 10
	p.client = &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return p, nil
}

func (p *documentParser) Parse(ctx context.Context, version domain.DocumentVersion, content []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(content) > domain.MaxStoredDocumentBytes {
		return "", &Failure{Code: "extraction_limit"}
	}
	var expectedMedia string
	switch strings.ToLower(filepath.Ext(version.Filename)) {
	case ".txt", ".md", ".markdown":
		return normalizeParserText(content)
	case ".pdf":
		if !bytes.HasPrefix(content, []byte("%PDF-")) {
			return "", &Failure{Code: "unsupported_format"}
		}
		expectedMedia = "application/pdf"
	case ".docx":
		if !bytes.HasPrefix(content, []byte{'P', 'K', 3, 4}) {
			return "", &Failure{Code: "unsupported_format"}
		}
		expectedMedia = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	default:
		return "", &Failure{Code: "unsupported_format"}
	}
	if p.endpoint == "" {
		return "", &Failure{Code: "parser_unavailable"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, p.endpoint, bytes.NewReader(content))
	if err != nil {
		return "", &Failure{Code: "parser_unavailable"}
	}
	request.Header.Set("Accept", "application/json")
	// Let Tika inspect the actual bytes, rather than trusting the uploaded MIME.
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": version.Filename}))
	request.Header.Set("writeLimit", "1048576") // Tika counts characters; we also bound UTF-8 bytes below.
	request.Header.Set("throwOnWriteLimitReached", "true")
	request.Header.Set("X-Tika-Skip-Embedded", "true")
	request.Header.Set("X-Tika-OCRskipOcr", "true")
	request.Header.Set("X-Tika-PDFOcrStrategy", "no_ocr")
	response, err := p.client.Do(request)
	if err != nil {
		return "", parserTransportError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
			return "", &Failure{Code: "parser_unavailable"}
		}
		return "", &Failure{Code: "extraction_failed"}
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return "", &Failure{Code: "extraction_failed"}
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, parserResponseLimit+1))
	if err != nil {
		return "", parserTransportError(ctx, err)
	}
	if len(raw) > parserResponseLimit {
		return "", &Failure{Code: "extraction_limit"}
	}
	metadata, err := decodeParserMetadata(raw)
	if err != nil {
		return "", err
	}
	for key, value := range metadata {
		lower := strings.ToLower(key)
		// Tika's StandardWriteFilter excludes X-TIKA:content from metadata
		// truncation. This exact warning concerns fields we do not index;
		// body write limits, parser exceptions and unknown truncation remain fatal.
		if lower == "x-tika:warn:truncated_metadata" {
			continue
		}
		if strings.HasPrefix(lower, "x-tika:") && (strings.Contains(lower, "write_limit") || strings.Contains(lower, "truncated") || strings.Contains(lower, "embedded_resource_limit")) {
			if string(value) != `"false"` && string(value) != `["false"]` {
				return "", &Failure{Code: "extraction_limit"}
			}
		} else if strings.HasPrefix(lower, "x-tika:") && strings.Contains(lower, "exception") {
			return "", &Failure{Code: "extraction_failed"}
		}
	}
	var detectedMedia, text string
	if json.Unmarshal(metadata["Content-Type"], &detectedMedia) != nil || json.Unmarshal(metadata["X-TIKA:content"], &text) != nil {
		return "", &Failure{Code: "extraction_failed"}
	}
	detectedMedia, _, err = mime.ParseMediaType(detectedMedia)
	if err != nil || detectedMedia != expectedMedia {
		return "", &Failure{Code: "unsupported_format"}
	}
	return normalizeParserText([]byte(text))
}

func parserTransportError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return &Failure{Code: "parser_unavailable"}
}

// Decode one metadata object, rejecting duplicate root keys and trailing JSON.
// Otherwise a later duplicate could conceal a parser exception or truncation.
func decodeParserMetadata(raw []byte) (map[string]json.RawMessage, error) {
	invalid := &Failure{Code: "extraction_failed"}
	if !utf8.Valid(raw) {
		return nil, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalid
	}
	metadata := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, invalid
		}
		if _, exists := metadata[key]; exists {
			return nil, invalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, invalid
		}
		metadata[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, invalid
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, invalid
	}
	return metadata, nil
}

func normalizeParserText(content []byte) (string, error) {
	if !utf8.Valid(content) {
		return "", &Failure{Code: "invalid_text"}
	}
	text := strings.TrimPrefix(string(content), "\ufeff")
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	if len(text) > domain.MaxExtractedBytes {
		return "", &Failure{Code: "extraction_limit"}
	}
	if !validParserText(text) {
		return "", &Failure{Code: "invalid_text"}
	}
	if strings.TrimSpace(text) == "" {
		return "", &Failure{Code: "empty_document"}
	}
	return text, nil
}

func validParserText(text string) bool {
	if !utf8.ValidString(text) {
		return false
	}
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' && r != '\f' {
			return false
		}
	}
	return true
}
