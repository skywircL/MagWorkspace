package httpapi

import (
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/store"
)

type readDocumentToolInput struct {
	DocumentID string `json:"document_id"`
	EvidenceID string `json:"evidence_id"`
	VersionID  string `json:"version_id"`
	IndexID    string `json:"index_id"`
	Offset     int    `json:"offset"`
	Limit      *int   `json:"limit"`
	Context    int    `json:"context"`
	Query      string `json:"query"`
}

type readDocumentToolResult struct {
	store.ToolDocument
	ContentStatus string            `json:"content_status"`
	Evidence      []domain.Evidence `json:"evidence"`
	Offset        int               `json:"offset"`
	HasMore       bool              `json:"has_more"`
	NextOffset    *int              `json:"next_offset,omitempty"`
	Query         string            `json:"query,omitempty"`
}

func toolLimit(value *int, defaultValue, maximum int) (int, error) {
	if value == nil {
		return defaultValue, nil
	}
	if *value < 1 || *value > maximum {
		return 0, domain.ErrInvalid
	}
	return *value, nil
}

func (s *server) knowledgeTool(ctx context.Context, uid, wid, sid, rid, name string, raw json.RawMessage) (any, error) {
	profileID := ""
	if s.cfg.Indexing != nil {
		profileID = s.cfg.Indexing.IndexProfile().ID
	}
	switch name {
	case "list_documents":
		var in struct {
			Keyword string `json:"keyword"`
			Offset  int    `json:"offset"`
			Limit   *int   `json:"limit"`
		}
		if strictToolArgs(raw, &in) != nil {
			return nil, domain.ErrInvalid
		}
		limit, err := toolLimit(in.Limit, 10, 20)
		if err != nil {
			return nil, err
		}
		return s.db.RunToolDocuments(ctx, uid, wid, sid, rid, in.Keyword, profileID, in.Offset, limit)
	case "search_knowledge":
		var in struct {
			Query string `json:"query"`
			Mode  string `json:"mode"`
			Limit *int   `json:"limit"`
		}
		if strictToolArgs(raw, &in) != nil {
			return nil, domain.ErrInvalid
		}
		limit, err := toolLimit(in.Limit, 5, 10)
		if err != nil {
			return nil, err
		}
		mode := "hybrid"
		switch in.Mode {
		case "", "hybrid":
		case "semantic":
			mode = "vector"
		case "keyword":
			mode = "lexical"
		default:
			return nil, domain.ErrInvalid
		}
		scope, err := s.db.RunDocumentScope(ctx, uid, wid, sid, rid)
		if err != nil {
			return nil, err
		}
		if s.cfg.Indexing == nil {
			return nil, agent.ErrUnavailable
		}
		hits, err := s.cfg.Indexing.SearchScoped(ctx, uid, wid, in.Query, mode, limit, scope)
		if err != nil {
			return nil, err
		}
		evidence, err := s.db.RecordRunEvidence(ctx, uid, wid, sid, rid, hits)
		if err != nil {
			return nil, err
		}
		if err = attachKnowledgeContent(evidence, hits); err != nil {
			return nil, err
		}
		return map[string]any{"evidence": evidence}, nil
	case "read_document":
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return nil, domain.ErrInvalid
		}
		if _, exists := fields["source"]; exists {
			var in struct {
				DocumentID string `json:"document_id"`
				VersionID  string `json:"version_id"`
				Source     bool   `json:"source"`
				Offset     int    `json:"source_offset"`
			}
			if strictToolArgs(raw, &in) != nil || !in.Source {
				return nil, domain.ErrInvalid
			}
			return s.db.RunDocumentSource(ctx, uid, wid, sid, rid, in.DocumentID, in.VersionID, in.Offset)
		}
		var in readDocumentToolInput
		if strictToolArgs(raw, &in) != nil {
			return nil, domain.ErrInvalid
		}
		return s.readDocumentTool(ctx, uid, wid, sid, rid, profileID, in)
	default:
		return nil, domain.ErrForbidden
	}
}

func attachKnowledgeContent(evidence []domain.Evidence, hits []domain.SearchHit) error {
	chunks := make(map[string]domain.IndexChunk, len(hits))
	for _, hit := range hits {
		chunks[hit.Chunk.ID] = hit.Chunk
	}
	for i := range evidence {
		c, ok := chunks[evidence[i].ChunkID]
		if !ok || !matchesEvidence(evidence[i], c) {
			return domain.ErrConflict
		}
		evidence[i].Content = c.Content
	}
	return nil
}

func (s *server) readDocumentTool(ctx context.Context, uid, wid, sid, rid, profileID string, in readDocumentToolInput) (readDocumentToolResult, error) {
	out := readDocumentToolResult{Evidence: []domain.Evidence{}, Offset: in.Offset}
	limit, err := toolLimit(in.Limit, 5, 10)
	in.Query = strings.TrimSpace(in.Query)
	if err != nil || (in.DocumentID == "") == (in.EvidenceID == "") || in.Offset < 0 || in.Offset > domain.MaxIndexChunks || in.Context < 0 || in.Context > 3 || len(in.Query) > 256 || !utf8.ValidString(in.Query) || (in.VersionID == "") != (in.IndexID == "") {
		return out, domain.ErrInvalid
	}
	if in.EvidenceID != "" && (in.VersionID != "" || in.Offset != 0 || in.Query != "") {
		return out, domain.ErrInvalid
	}
	if in.EvidenceID == "" && in.Query == "" && in.Context != 0 {
		return out, domain.ErrInvalid
	}
	if in.Offset > 0 && in.VersionID == "" {
		return out, domain.ErrInvalid
	}
	focus := ""
	if in.EvidenceID != "" {
		// Existing evidence is a private Run capability; do not resolve arbitrary
		// document/chunk IDs supplied by the model into a wider authorization scope.
		e, err := s.db.RunEvidence(ctx, uid, wid, sid, rid, in.EvidenceID)
		if err != nil {
			return out, err
		}
		in.DocumentID, in.VersionID, in.IndexID = e.DocumentID, e.VersionID, e.IndexID
		focus = e.ChunkID
	}
	out.ToolDocument, err = s.db.RunToolDocument(ctx, uid, wid, sid, rid, in.DocumentID, in.VersionID, in.IndexID, profileID)
	if err != nil {
		return out, err
	}
	out.Query = in.Query
	switch {
	case s.cfg.Indexing == nil:
		out.ContentStatus = "indexing_disabled"
	case out.Index == nil:
		out.ContentStatus = "index_missing"
	default:
		out.ContentStatus = out.Index.Status
	}
	if out.ContentStatus != "ready" {
		return out, nil
	}
	chunks, err := s.cfg.Indexing.Chunks(ctx, uid, wid, in.DocumentID, out.Version.ID, out.Index.ID)
	if err != nil {
		return out, err
	}
	selected, next, err := selectKnowledgeChunks(chunks, in.Offset, limit, in.Context, in.Query, focus)
	if err != nil {
		return out, err
	}
	hits := make([]domain.SearchHit, 0, len(selected))
	for _, chunk := range selected {
		hits = append(hits, domain.SearchHit{Chunk: chunk, Title: out.Document.Title})
	}
	out.Evidence, err = s.db.RecordRunDocumentEvidence(ctx, uid, wid, sid, rid, hits)
	if err != nil {
		return out, err
	}
	if err = attachKnowledgeContent(out.Evidence, hits); err != nil {
		return out, err
	}
	// RecordRunDocumentEvidence rechecks live authorization and source metadata
	// after MinIO I/O, including an empty search/page. Metadata is re-read too.
	final, err := s.db.RunToolDocument(ctx, uid, wid, sid, rid, in.DocumentID, out.Version.ID, out.Index.ID, profileID)
	if err != nil {
		return out, err
	}
	if final.Index.Generation != out.Index.Generation {
		return out, domain.ErrConflict
	}
	out.ToolDocument = final
	out.NextOffset = next
	out.HasMore = next != nil
	return out, nil
}

// Query pagination counts chunk positions. Limit bounds all returned chunks,
// including context, and its cursor resumes at the next matching position.
func selectKnowledgeChunks(chunks []domain.IndexChunk, offset, limit, neighbors int, query, focus string) ([]domain.IndexChunk, *int, error) {
	if focus != "" {
		for i, c := range chunks {
			if c.ID == focus {
				start, end := knowledgeContextWindow(len(chunks), i, neighbors, limit)
				return chunks[start:end], nil, nil
			}
		}
		return nil, nil, domain.ErrNotFound
	}
	if offset >= len(chunks) {
		return []domain.IndexChunk{}, nil, nil
	}
	if query == "" {
		end := min(len(chunks), offset+limit)
		if end < len(chunks) {
			return chunks[offset:end], &end, nil
		}
		return chunks[offset:end], nil, nil
	}
	terms := strings.Fields(strings.ToLower(query))
	result := []domain.IndexChunk{}
	seen := make(map[int]bool)
	for i := offset; i < len(chunks); i++ {
		text := strings.ToLower(chunks[i].Content)
		match := true
		for _, term := range terms {
			if !strings.Contains(text, term) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		if len(result) >= limit {
			next := i
			return result, &next, nil
		}
		start, end := knowledgeContextWindow(len(chunks), i, neighbors, limit-len(result))
		for j := start; j < end; j++ {
			if !seen[j] {
				result = append(result, chunks[j])
				seen[j] = true
			}
		}
	}
	return result, nil, nil
}

// Prefer a balanced window that always contains the focus. Near document edges
// the available side fills the budget, without exceeding the context radius.
func knowledgeContextWindow(length, focus, neighbors, limit int) (int, int) {
	start, end := max(0, focus-neighbors), min(length, focus+neighbors+1)
	if end-start <= limit {
		return start, end
	}
	start = max(start, focus-(limit-1)/2)
	end = min(end, start+limit)
	start = max(max(0, focus-neighbors), end-limit)
	return start, end
}
