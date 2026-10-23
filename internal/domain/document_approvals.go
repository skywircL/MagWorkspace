package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxAgentDocumentBytes = 32 << 10

type DocumentMutation struct {
	KnowledgeBaseID string `json:"knowledge_base_id,omitempty"`
	Title           string `json:"title,omitempty"`
	Filename        string `json:"filename,omitempty"`
	DocumentID      string `json:"document_id,omitempty"`
	BaseVersionID   string `json:"base_version_id,omitempty"`
	Content         string `json:"content"`
}

type DocumentApproval struct {
	KnowledgeBaseID string          `json:"knowledge_base_id,omitempty"`
	Tool            string          `json:"tool"`
	Risk            string          `json:"risk"`
	Description     string          `json:"description"`
	Reason          string          `json:"reason"`
	Arguments       json.RawMessage `json:"arguments,omitempty"`
	ArgumentsSHA256 string          `json:"arguments_sha256"`
	DocumentID      string          `json:"document_id,omitempty"`
	BaseVersionID   string          `json:"base_version_id,omitempty"`
	Title           string          `json:"title"`
	Filename        string          `json:"filename"`
	BaseSHA256      string          `json:"base_sha256,omitempty"`
	ContentSHA256   string          `json:"content_sha256"`
	ContentBytes    int             `json:"content_bytes"`
	Result          *DocumentWrite  `json:"result,omitempty"`
}

// ParseDocumentMutation is shared by the transport and the final store gate.
// Values are only strings, keys are unique, and no field grants authority.
func ParseDocumentMutation(tool string, raw []byte) (DocumentMutation, string, error) {
	var out DocumentMutation
	if !utf8.Valid(raw) {
		return out, "", ErrInvalid
	}
	wanted := map[string]bool{"content": true}
	optional := map[string]bool{}
	switch tool {
	case "create_document":
		wanted["title"], wanted["filename"] = true, true
		optional["knowledge_base_id"] = true
	case "update_document":
		wanted["document_id"], wanted["base_version_id"] = true, true
	default:
		return out, "", ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return out, "", ErrInvalid
	}
	fields := map[string]string{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return out, "", ErrInvalid
		}
		k, ok := key.(string)
		if !ok || (!wanted[k] && !optional[k]) {
			return out, "", ErrInvalid
		}
		if _, exists := fields[k]; exists {
			return out, "", ErrInvalid
		}
		var value any
		if d.Decode(&value) != nil {
			return out, "", ErrInvalid
		}
		v, ok := value.(string)
		if !ok || !utf8.ValidString(v) {
			return out, "", ErrInvalid
		}
		fields[k] = v
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') || d.Decode(new(any)) != io.EOF {
		return out, "", ErrInvalid
	}
	for key := range wanted {
		if _, ok := fields[key]; !ok {
			return out, "", ErrInvalid
		}
	}
	if id, ok := fields["knowledge_base_id"]; ok {
		if err := ValidateKnowledgeBaseIDs([]string{id}); err != nil {
			return out, "", err
		}
	}
	out = DocumentMutation{KnowledgeBaseID: fields["knowledge_base_id"], Title: fields["title"], Filename: fields["filename"], DocumentID: fields["document_id"], BaseVersionID: fields["base_version_id"], Content: fields["content"]}
	if len(out.Content) > MaxAgentDocumentBytes {
		return out, "", ErrInvalid
	}
	for _, r := range out.Content {
		if unicode.IsControl(r) && !strings.ContainsRune("\n\r\t\f", r) {
			return out, "", ErrInvalid
		}
	}
	if tool == "create_document" {
		for _, v := range []string{out.Title, out.Filename} {
			if strings.TrimSpace(v) == "" || len(v) > 255 || strings.IndexFunc(v, unicode.IsControl) >= 0 {
				return out, "", ErrInvalid
			}
		}
		if strings.ContainsAny(out.Filename, `/\`) {
			return out, "", ErrInvalid
		}
		switch strings.ToLower(filepath.Ext(out.Filename)) {
		case ".txt", ".md", ".markdown":
		default:
			return out, "", ErrInvalid
		}
	} else {
		for _, v := range []string{out.DocumentID, out.BaseVersionID} {
			if len(v) != 32 || strings.Trim(v, "0123456789abcdef") != "" {
				return out, "", ErrInvalid
			}
		}
	}
	canonical, _ := json.Marshal(fields)
	sum := sha256.Sum256(canonical)
	return out, hex.EncodeToString(sum[:]), nil
}
