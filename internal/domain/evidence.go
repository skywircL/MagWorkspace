package domain

import (
	"regexp"
	"strings"
	"time"
)

// Evidence identifies a verified passage acquired by one private agent run.
// Content is hydrated from the immutable published MySQL chunk generation.
type Evidence struct {
	ID           string    `json:"evidence_id"`
	RunID        string    `json:"run_id"`
	WorkspaceID  string    `json:"workspace_id"`
	DocumentID   string    `json:"document_id"`
	VersionID    string    `json:"version_id"`
	IndexID      string    `json:"index_id"`
	Generation   string    `json:"generation"`
	ChunkID      string    `json:"chunk_id"`
	SourceSHA256 string    `json:"source_sha256"`
	SHA256       string    `json:"sha256"`
	Title        string    `json:"title"`
	StartByte    int       `json:"start_byte"`
	EndByte      int       `json:"end_byte"`
	Content      string    `json:"content,omitempty"`
	Cited        bool      `json:"cited"`
	CreatedAt    time.Time `json:"created_at"`
}

var citationMarker = regexp.MustCompile(`\[E:([a-f0-9]{32})\]`)

// CitationIDs accepts only canonical, complete evidence markers. Their IDs must
// still be authorized against the run's acquired evidence in the store.
func CitationIDs(answer string) ([]string, error) {
	ids := []string{}
	seen := map[string]bool{}
	for _, match := range citationMarker.FindAllStringSubmatch(answer, -1) {
		if !seen[match[1]] {
			ids = append(ids, match[1])
			seen[match[1]] = true
		}
	}
	if strings.Contains(citationMarker.ReplaceAllString(answer, ""), "[E:") {
		return nil, ErrInvalid
	}
	return ids, nil
}
