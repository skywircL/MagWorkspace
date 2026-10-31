package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxExtractionQueryBytes    = 16000
	maxExtractionResponseBytes = 64 << 10
	maxExtractionErrorBytes    = 1024
)

// ErrExtractionBusy means the authenticated service declined admission before
// starting a model call. Other failures do not establish that no call occurred.
var ErrExtractionBusy = errors.New("memory extraction service is busy")

// ExtractionItem is an untrusted candidate, not authorization to save a memory.
// Evidence must occur literally in the authenticated user's original query.
type ExtractionItem struct {
	Kind       string `json:"kind"`
	Content    string `json:"content"`
	Topic      string `json:"topic"`
	Importance int    `json:"importance"`
	Evidence   string `json:"evidence"`
}

type MemoryExtractor interface {
	ExtractMemories(context.Context, string) ([]ExtractionItem, error)
}

var _ MemoryExtractor = (*HTTPClient)(nil)

// ExtractMemories sends only this user's current query, with no history, tool
// results, identities or model credentials. It never retries a model request.
func (c *HTTPClient) ExtractMemories(ctx context.Context, query string) ([]ExtractionItem, error) {
	if !utf8.ValidString(query) || strings.TrimSpace(query) == "" || len(query) > MaxExtractionQueryBytes {
		return nil, fmt.Errorf("%w: invalid extraction query", ErrFailed)
	}
	if c.ctx.Err() != nil {
		return nil, fmt.Errorf("%w: HTTP client is closed", ErrUnavailable)
	}
	timeout := min(c.config.Timeout, 30*time.Second)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	payload, err := json.Marshal(struct {
		Query string `json:"query"`
	}{query})
	if err != nil {
		return nil, fmt.Errorf("%w: invalid extraction request", ErrFailed)
	}
	req, err := http.NewRequestWithContext(runCtx, http.MethodPost, strings.TrimRight(c.config.URL, "/")+"/v1/memories/extract", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid extraction request", ErrFailed)
	}
	req.GetBody = nil
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Magent-Protocol", "1")
	deadline, _ := runCtx.Deadline()
	milliseconds := (time.Until(deadline) + time.Millisecond - 1) / time.Millisecond
	if milliseconds <= 0 {
		return nil, fmt.Errorf("%w: %w", ErrFailed, context.DeadlineExceeded)
	}
	req.Header.Set("X-Magent-Timeout-Ms", strconv.FormatInt(int64(milliseconds), 10))
	response, err := c.client.Do(req)
	if err != nil {
		return nil, c.requestError(runCtx, "extraction service unavailable", ErrUnavailable)
	}
	defer func() { cancel(); _ = response.Body.Close() }()
	contentType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	validProtocol := len(response.Header.Values("X-Magent-Protocol")) == 1 && response.Header.Get("X-Magent-Protocol") == "1" &&
		len(response.Header.Values("Content-Type")) == 1 && mediaErr == nil && contentType == "application/json" &&
		len(response.Header.Values("Content-Encoding")) <= 1 && response.Header.Get("Content-Encoding") == ""
	if response.StatusCode != http.StatusOK {
		// Only a bounded, authenticated protocol receipt proves admission failed.
		// An intermediary's 429 or a malformed body must consume the ordinary
		// failure budget because model execution is unknown.
		if validProtocol && (response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusBadGateway) {
			raw, err := io.ReadAll(io.LimitReader(response.Body, maxExtractionErrorBytes+1))
			if err != nil {
				return nil, c.requestError(runCtx, "extraction error response interrupted", ErrUnavailable)
			}
			switch code := extractionErrorCode(raw); {
			case response.StatusCode == http.StatusTooManyRequests && code == "agent_busy":
				return nil, ErrExtractionBusy
			case response.StatusCode == http.StatusBadGateway && code == "agent_failed":
				return nil, fmt.Errorf("%w: extraction service failed", ErrFailed)
			}
		}
		return nil, fmt.Errorf("%w: extraction service rejected request", ErrUnavailable)
	}
	if !validProtocol {
		return nil, fmt.Errorf("%w: invalid extraction protocol", ErrFailed)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxExtractionResponseBytes+1))
	if err != nil {
		return nil, c.requestError(runCtx, "extraction response interrupted", ErrFailed)
	}
	return decodeExtraction(raw, query)
}

func extractionErrorCode(raw []byte) string {
	if len(raw) > maxExtractionErrorBytes || !utf8.Valid(raw) || !uniqueJSON(raw) || !validExtractionUnicode(raw) {
		return ""
	}
	var value map[string]string
	if json.Unmarshal(raw, &value) != nil || len(value) != 1 {
		return ""
	}
	return value["code"]
}

func decodeExtraction(raw []byte, query string) ([]ExtractionItem, error) {
	fail := func() ([]ExtractionItem, error) { return nil, fmt.Errorf("%w: invalid extraction response", ErrFailed) }
	if len(raw) > maxExtractionResponseBytes || !utf8.Valid(raw) || !uniqueJSON(raw) || !validExtractionUnicode(raw) {
		return fail()
	}
	// Require every field explicitly. A missing numeric/string field must not
	// silently turn into a Go zero value accepted by later persistence code.
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || len(envelope) != 1 || envelope["items"] == nil || bytes.Equal(bytes.TrimSpace(envelope["items"]), []byte("null")) {
		return fail()
	}
	var values []map[string]json.RawMessage
	if json.Unmarshal(envelope["items"], &values) != nil || len(values) > 5 {
		return fail()
	}
	items := make([]ExtractionItem, 0, len(values))
	for _, fields := range values {
		if len(fields) != 5 {
			return fail()
		}
		for _, field := range []string{"kind", "content", "topic", "importance", "evidence"} {
			value := bytes.TrimSpace(fields[field])
			if len(value) == 0 || bytes.Equal(value, []byte("null")) {
				return fail()
			}
		}
		var item ExtractionItem
		encoded, _ := json.Marshal(fields)
		if json.Unmarshal(encoded, &item) != nil || !validExtractionItem(item, query) {
			return fail()
		}
		items = append(items, item)
	}
	return items, nil
}

// encoding/json silently replaces unmatched UTF-16 escapes. Reject them at the
// transport boundary rather than turning malformed model content into facts.
func validExtractionUnicode(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) || raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		value, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func validExtractionItem(item ExtractionItem, query string) bool {
	switch item.Kind {
	case "profile", "preference", "fact", "task":
	default:
		return false
	}
	return utf8.ValidString(item.Content) && strings.TrimSpace(item.Content) != "" && utf8.RuneCountInString(item.Content) <= 500 &&
		utf8.ValidString(item.Topic) && utf8.RuneCountInString(item.Topic) <= 100 && item.Importance >= 1 && item.Importance <= 5 &&
		utf8.ValidString(item.Evidence) && strings.TrimSpace(item.Evidence) != "" && len(item.Evidence) <= MaxExtractionQueryBytes && strings.Contains(query, item.Evidence)
}
