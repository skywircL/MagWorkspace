package indexing_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
)

func parserForTest(t *testing.T, base string) indexing.Parser {
	t.Helper()
	parser, err := indexing.NewParser(base)
	if err != nil {
		t.Fatal(err)
	}
	return parser
}

func parserPDF() []byte {
	return parserPDFDocument("Parser fixture", nil)
}

// Test text is restricted to PDF string-safe ASCII; the embedded PDF exercises
// the same supported parser when attachment extraction is accidentally enabled.
func parserPDFDocument(body string, embedded []byte) []byte {
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
	text := "BT /F1 12 Tf 20 200 Td (" + body + ") Tj ET\n"
	objects = append(objects, fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(text), text))
	if embedded != nil {
		objects[0] = "<< /Type /Catalog /Pages 2 0 R /Names << /EmbeddedFiles << /Names [(attachment.pdf) 7 0 R] >> >> >>"
		objects = append(objects,
			fmt.Sprintf("<< /Type /EmbeddedFile /Subtype /application#2Fpdf /Length %d >>\nstream\n%sendstream", len(embedded), embedded),
			"<< /Type /Filespec /F (attachment.pdf) /EF << /F 6 0 R >> >>",
		)
	}
	offsets := []int{0}
	for i, object := range objects {
		offsets = append(offsets, out.Len())
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xref)
	return out.Bytes()
}

func parserDOCX(t *testing.T) []byte {
	return parserDOCXText(t, "Parser fixture")
}

func parserDOCXText(t *testing.T, text string) []byte {
	t.Helper()
	var escaped bytes.Buffer
	if err := xml.EscapeText(&escaped, []byte(text)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, entry := range []struct{ path, text string }{
		{"[Content_Types].xml", `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>`},
		{"_rels/.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>`},
		{"word/document.xml", `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>` + escaped.String() + `</w:t></w:r></w:p></w:body></w:document>`},
	} {
		item, err := writer.Create(entry.path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(item, entry.text); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestParserTextNormalizationAndValidation(t *testing.T) {
	parser := parserForTest(t, "")
	for _, test := range []struct {
		name, filename string
		input          []byte
		want, code     string
	}{
		{"text", "notes.txt", []byte("\xef\xbb\xbf第一行\r\nsecond\rlast\tline\n"), "第一行\nsecond\nlast\tline\n", ""},
		{"markdown", "NOTES.MD", []byte("# Heading\n\n正文"), "# Heading\n\n正文", ""},
		{"markdown extension", "notes.markdown", []byte("plain"), "plain", ""},
		{"preserve spaces", "notes.txt", []byte("  literal   text  \n"), "  literal   text  \n", ""},
		{"empty", "notes.txt", nil, "", "empty_document"},
		{"whitespace", "notes.txt", []byte("\xef\xbb\xbf \t\r\n　"), "", "empty_document"},
		{"NUL", "notes.txt", []byte{'a', 0, 'b'}, "", "invalid_text"},
		{"binary control", "notes.txt", []byte{'a', 1, 'b'}, "", "invalid_text"},
		{"invalid UTF8", "notes.txt", []byte{0xff, 0xfe, 'a'}, "", "invalid_text"},
		{"extracted too long", "notes.txt", bytes.Repeat([]byte{'a'}, domain.MaxExtractedBytes+1), "", "extraction_limit"},
		{"unsupported", "notes.html", []byte("<p>text</p>"), "", "unsupported_format"},
		{"bad PDF magic", "notes.pdf", []byte("not a PDF"), "", "unsupported_format"},
		{"bad DOCX magic", "notes.docx", []byte("not a ZIP"), "", "unsupported_format"},
		{"unconfigured PDF", "notes.pdf", parserPDF(), "", "parser_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := append([]byte(nil), test.input...)
			text, err := parser.Parse(context.Background(), domain.DocumentVersion{Filename: test.filename}, test.input)
			if test.code == "" {
				if err != nil || text != test.want {
					t.Fatalf("text=%q error=%v, want %q", text, err, test.want)
				}
			} else if err == nil || indexing.ErrorCode(err) != test.code || text != "" {
				t.Fatalf("text length=%d error=%v code=%s, want %s", len(text), err, indexing.ErrorCode(err), test.code)
			}
			if !bytes.Equal(test.input, original) {
				t.Fatal("parser mutated immutable version bytes")
			}
		})
	}
	content := strings.Repeat("x", domain.MaxExtractedBytes)
	text, err := parser.Parse(context.Background(), domain.DocumentVersion{Filename: "limit.txt"}, []byte(content))
	if err != nil || text != content {
		t.Fatalf("exact extracted-byte boundary rejected: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := parser.Parse(ctx, domain.DocumentVersion{Filename: "notes.txt"}, []byte("text")); indexing.ErrorCode(err) != "processing_canceled" {
		t.Fatalf("canceled parse ignored context: %v", err)
	}
}

func TestParserTikaReceivesOnlyBytesAndExtractsPDFAndDOCX(t *testing.T) {
	for _, test := range []struct {
		filename, media string
		content         []byte
	}{
		{"sample.pdf", "application/pdf", parserPDF()},
		{"sample.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", parserDOCX(t)},
	} {
		t.Run(test.filename, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPut || r.URL.Path != "/parser/tika/text" || !bytes.Equal(body, test.content) {
					t.Error("request did not contain exact version bytes at the fixed endpoint")
				}
				for name, value := range map[string]string{"Accept": "application/json", "Content-Type": "application/octet-stream", "writeLimit": "1048576", "throwOnWriteLimitReached": "true", "X-Tika-Skip-Embedded": "true", "X-Tika-OCRskipOcr": "true", "X-Tika-PDFOcrStrategy": "no_ocr"} {
					if r.Header.Get(name) != value {
						t.Errorf("missing controlled parser header %s", name)
					}
				}
				disposition, params, err := mime.ParseMediaType(r.Header.Get("Content-Disposition"))
				if err != nil || disposition != "attachment" || params["filename"] != test.filename {
					t.Error("filename detection hint was not encoded safely")
				}
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				_ = json.NewEncoder(w).Encode(map[string]any{"Content-Type": test.media, "X-TIKA:content": "\ufeffFirst\r\n中文\r", "dc:title": "untrusted metadata is not正文"})
			}))
			defer server.Close()
			text, err := parserForTest(t, server.URL+"/parser/").Parse(context.Background(), domain.DocumentVersion{Filename: test.filename, MediaType: "text/html"}, test.content)
			if err != nil || text != "First\n中文\n" || calls.Load() != 1 {
				t.Fatalf("unexpected parse text=%q calls=%d error=%v", text, calls.Load(), err)
			}
		})
	}
}

func TestParserTikaRejectsPartialMalformedAndFailedResponses(t *testing.T) {
	responseJSON := func(fields map[string]any) string {
		fields["Content-Type"] = "application/pdf"
		fields["X-TIKA:WARN:truncated_metadata"] = "true"
		if _, ok := fields["X-TIKA:content"]; !ok {
			fields["X-TIKA:content"] = "private-partial-output"
		}
		raw, _ := json.Marshal(fields)
		return string(raw)
	}
	for _, test := range []struct {
		name, body, code string
		status           int
		media            string
	}{
		{"write limit", responseJSON(map[string]any{"X-TIKA:EXCEPTION:write_limit_reached": "true"}), "extraction_limit", 200, "application/json"},
		{"truncated content", responseJSON(map[string]any{"X-TIKA:WARN:truncated_content": "true"}), "extraction_limit", 200, "application/json"},
		{"embedded limit", responseJSON(map[string]any{"X-TIKA:EXCEPTION:embedded_resource_limit_reached": []string{"true"}}), "extraction_limit", 200, "application/json"},
		{"parser exception", responseJSON(map[string]any{"X-TIKA:EXCEPTION:container_exception": "private parser stacktrace"}), "extraction_failed", 200, "application/json"},
		{"empty text", responseJSON(map[string]any{"X-TIKA:content": "\r\n "}), "empty_document", 200, "application/json"},
		{"NUL text", responseJSON(map[string]any{"X-TIKA:content": "binary\x00text"}), "invalid_text", 200, "application/json"},
		{"UTF8 byte limit", responseJSON(map[string]any{"X-TIKA:content": strings.Repeat("中", domain.MaxExtractedBytes/3+1)}), "extraction_limit", 200, "application/json"},
		{"response limit", strings.Repeat(" ", (2<<20)+1), "extraction_limit", 200, "application/json"},
		{"incomplete JSON", `{"Content-Type":"application/pdf","X-TIKA:content":"private-partial-output"`, "extraction_failed", 200, "application/json"},
		{"trailing JSON", responseJSON(map[string]any{}) + `{}`, "extraction_failed", 200, "application/json"},
		{"duplicate field", `{"Content-Type":"application/pdf","X-TIKA:content":"first","X-TIKA:content":"second"}`, "extraction_failed", 200, "application/json"},
		{"wrong shape", `[{"X-TIKA:content":"text"}]`, "extraction_failed", 200, "application/json"},
		{"missing text", `{"Content-Type":"application/pdf"}`, "extraction_failed", 200, "application/json"},
		{"wrong detected format", `{"Content-Type":"text/html","X-TIKA:content":"text"}`, "unsupported_format", 200, "application/json"},
		{"invalid JSON UTF8", "{\"X-TIKA:content\":\"\xff\"}", "extraction_failed", 200, "application/json"},
		{"wrong response MIME", "private upstream HTML", "extraction_failed", 200, "text/html"},
		{"encrypted unsupported", "private upstream diagnostic", "extraction_failed", 422, "application/json"},
		{"temporarily unavailable", "private upstream diagnostic", "parser_unavailable", 503, "application/json"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.media)
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			text, err := parserForTest(t, server.URL).Parse(context.Background(), domain.DocumentVersion{Filename: "sample.pdf"}, parserPDF())
			if err == nil || indexing.ErrorCode(err) != test.code || text != "" || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("unsafe failed extraction: text bytes=%d error=%v expected code=%s", len(text), err, test.code)
			}
		})
	}
}

func TestParserDoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Store(true) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	text, err := parserForTest(t, redirect.URL).Parse(context.Background(), domain.DocumentVersion{Filename: "sample.pdf"}, parserPDF())
	if err == nil || indexing.ErrorCode(err) != "extraction_failed" || text != "" || reached.Load() {
		t.Fatalf("parser followed redirect or returned text: reached=%v error=%v", reached.Load(), err)
	}
}

func TestParserResponseLimitClosesUnfinishedBody(t *testing.T) {
	closed, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, strings.Repeat(" ", (2<<20)+1))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(closed)
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	text, err := parserForTest(t, server.URL).Parse(ctx, domain.DocumentVersion{Filename: "sample.pdf"}, parserPDF())
	if err == nil || indexing.ErrorCode(err) != "extraction_limit" || text != "" {
		t.Fatalf("parser waited for EOF or accepted an oversized body: text bytes=%d error=%v", len(text), err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("response limit did not close the unfinished upstream body")
	}
}

func TestParserCancellationAndDeadlineCloseUpstream(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			started, closed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-r.Context().Done():
					close(closed)
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			parser := parserForTest(t, server.URL)
			go func() {
				_, err := parser.Parse(ctx, domain.DocumentVersion{Filename: "sample.pdf"}, parserPDF())
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("parse did not reach upstream")
			}
			if canceled {
				cancel()
			}
			want := "processing_timeout"
			if canceled {
				want = "processing_canceled"
			}
			select {
			case err := <-done:
				if err == nil || indexing.ErrorCode(err) != want {
					t.Fatalf("context not preserved safely: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("parse did not stop after deadline/cancellation")
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled parser left upstream request running")
			}
		})
	}
}

func TestParserRejectsInvalidServiceConfiguration(t *testing.T) {
	for _, address := range []string{"relative", "file:///tmp/tika", "http://", "http://user:secret@localhost:9998", "http://localhost:9998?url=http://private", "http://localhost:9998#fragment", " http://localhost:9998", "http://localhost:invalid"} {
		if parser, err := indexing.NewParser(address); err == nil || parser != nil || indexing.ErrorCode(err) != "parser_unavailable" || strings.Contains(err.Error(), "secret") {
			t.Errorf("accepted invalid service configuration or leaked it: %v", err)
		}
	}
}

// This test is opt-in because the ordinary unit suite has no Tika dependency.
// The integration runner supplies the URL of its isolated Tika container.
func TestParserRealTika(t *testing.T) {
	address := os.Getenv("MAGWORKSPACE_TEST_TIKA_URL")
	if address == "" {
		t.Skip("MAGWORKSPACE_TEST_TIKA_URL is not set")
	}
	parser := parserForTest(t, address)
	for _, test := range []struct {
		filename string
		content  []byte
		want     string
	}{
		{"fixture.pdf", parserPDF(), "Parser fixture"},
		{"fixture.docx", parserDOCXText(t, "真实 DOCX 正文 Parser fixture"), "真实 DOCX 正文 Parser fixture"},
		{"attachment.pdf", parserPDFDocument("Visible root text", parserPDFDocument("PRIVATE ATTACHMENT TEXT", nil)), "Visible root text"},
	} {
		t.Run(test.filename, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
			defer cancel()
			original := append([]byte(nil), test.content...)
			text, err := parser.Parse(ctx, domain.DocumentVersion{Filename: test.filename}, test.content)
			if err != nil || !strings.Contains(text, test.want) || strings.Contains(text, "PRIVATE ATTACHMENT TEXT") || strings.Contains(text, "\r") || !bytes.Equal(original, test.content) {
				t.Fatalf("real parser contract failed: text=%q error=%v", text, err)
			}
		})
	}
	t.Run("reject oversized extraction", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
		defer cancel()
		text, err := parser.Parse(ctx, domain.DocumentVersion{Filename: "oversized.docx"}, parserDOCXText(t, strings.Repeat("oversized body ", domain.MaxExtractedBytes/15+2)))
		// Tika can reject this compressed input before writeLimit via its ZIP
		// expansion guard. Neither path may return a partial successful result.
		if err == nil || text != "" || (indexing.ErrorCode(err) != "extraction_limit" && indexing.ErrorCode(err) != "extraction_failed") {
			t.Fatalf("real parser accepted or partially returned oversized text: bytes=%d error=%v", len(text), err)
		}
	})
}

// Metadata truncation must neither discard complete body text nor hide a real
// parser failure (the rejection table above also carries this warning).
func TestParserTikaMetadataWarningPreservesCompleteBody(t *testing.T) {
	for _, warning := range []any{"true", []string{"true"}} {
		t.Run(fmt.Sprint(warning), func(t *testing.T) {
			want := "第一章\n" + strings.Repeat("云计算实验正文。", 12000) + "\n最后一章"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Content-Type": "application/pdf", "X-TIKA:content": want, "X-TIKA:WARN:truncated_metadata": warning})
			}))
			defer server.Close()
			got, err := parserForTest(t, server.URL).Parse(context.Background(), domain.DocumentVersion{Filename: "metadata.pdf"}, parserPDF())
			if err != nil || got != want {
				t.Fatalf("metadata warning discarded body: bytes=%d err=%v", len(got), err)
			}
			chunks, err := indexing.Split(got)
			if err != nil || len(chunks) == 0 || !strings.HasSuffix(chunks[len(chunks)-1].Content, "最后一章") {
				t.Fatalf("body tail lost during splitting: %v", err)
			}
		})
	}
}
