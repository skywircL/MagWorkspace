package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

func (s *server) updateDocument(w http.ResponseWriter, r *http.Request) {
	values, ok := readDocumentStringFields(w, r, 1<<20, "title")
	if !ok {
		return
	}
	document, err := s.db.UpdateDocument(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), values["title"])
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, http.StatusOK, document)
}

func (s *server) updateDocumentContent(w http.ResponseWriter, r *http.Request) {
	base, content, ok := readDocumentTextUpdate(w, r)
	if !ok {
		return
	}
	write, err := s.db.UpdateDocumentText(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), base, content)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, http.StatusOK, write)
}

// Bound the decoded text and its worst-case JSON escaping separately. Content
// must decode without Unicode replacement before becoming immutable bytes.
func readDocumentTextUpdate(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	values, ok := readDocumentStringFields(w, r, int64(6*domain.MaxEditableDocumentBytes+(64<<10)), "base_version_id", "content")
	if !ok {
		return "", "", false
	}
	if len(values) != 2 || values["base_version_id"] == "" {
		problem(w, 400, "invalid_request", "base_version_id and content strings are required")
		return "", "", false
	}
	if len(values["content"]) > domain.MaxEditableDocumentBytes {
		problem(w, 413, "document_too_large", "inline text must not exceed 1 MiB")
		return "", "", false
	}
	return values["base_version_id"], values["content"], true
}

func readDocumentStringFields(w http.ResponseWriter, r *http.Request, limit int64, fields ...string) (map[string]string, bool) {
	invalid := func() (map[string]string, bool) {
		problem(w, 400, "invalid_request", "a JSON object with permitted UTF-8 string fields is required")
		return nil, false
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "unsupported_media_type", "Content-Type must be application/json")
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			problem(w, 413, "document_too_large", "document update request exceeds size limit")
			return nil, false
		}
		return invalid()
	}
	if !utf8.Valid(raw) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return invalid()
	}
	allowed := make(map[string]bool, len(fields))
	for _, field := range fields {
		allowed[field] = true
	}
	values := make(map[string]string, len(fields))
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || !allowed[key] {
			return invalid()
		}
		if _, duplicate := values[key]; duplicate {
			return invalid()
		}
		var literal json.RawMessage
		if decoder.Decode(&literal) != nil || len(literal) == 0 || literal[0] != '"' {
			return invalid()
		}
		var value string
		if json.Unmarshal(literal, &value) != nil || !validJSONStringUnicode(literal) {
			return invalid()
		}
		values[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return invalid()
	}
	if decoder.Decode(new(any)) != io.EOF {
		return invalid()
	}
	return values, true
}

// encoding/json replaces unmatched UTF-16 surrogate escapes with U+FFFD.
// Reject those escapes rather than silently changing an edited document.
// The caller has already checked JSON string syntax and raw UTF-8 validity.
func validJSONStringUnicode(literal []byte) bool {
	for i := 1; i < len(literal)-1; i++ {
		if literal[i] != '\\' {
			continue
		}
		i++
		if literal[i] != 'u' {
			continue
		}
		value, _ := strconv.ParseUint(string(literal[i+1:i+5]), 16, 16)
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value >= 0xd800 && value <= 0xdbff {
			if i+6 >= len(literal) || literal[i+1] != '\\' || literal[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(literal[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
