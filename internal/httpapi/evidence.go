package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
)

var errInvalidCitation = errors.New("invalid answer citation")

type evidenceView struct {
	Evidence   domain.Evidence `json:"evidence"`
	Citation   string          `json:"citation"`
	SourceURL  string          `json:"source_url"`
	PassageURL string          `json:"passage_url"`
	Locator    evidenceLocator `json:"locator"`
}

// The extractor currently supplies text offsets, not PDF page coordinates.
// Name that coordinate system explicitly so clients cannot mistake it for raw
// file bytes (or invent a page number for Tika's normalized text).
type evidenceLocator struct {
	Kind      string `json:"kind"`
	StartByte int    `json:"start_byte"`
	EndByte   int    `json:"end_byte"`
}

func publicEvidence(e domain.Evidence, sid string) evidenceView {
	base := "/api/v1/workspaces/" + e.WorkspaceID
	return evidenceView{
		Evidence: e, Citation: "[E:" + e.ID + "]",
		SourceURL:  base + "/documents/" + e.DocumentID + "/versions/" + e.VersionID + "/content",
		PassageURL: base + "/sessions/" + sid + "/runs/" + e.RunID + "/evidence/" + e.ID,
		Locator:    evidenceLocator{Kind: "extracted_text_utf8_bytes", StartByte: e.StartByte, EndByte: e.EndByte},
	}
}

func strictToolArgs(raw json.RawMessage, out any) error {
	if len(raw) == 0 || len(raw) > 4096 || !utf8.Valid(raw) {
		return domain.ErrInvalid
	}
	check := json.NewDecoder(bytes.NewReader(raw))
	token, err := check.Token()
	if err != nil || token != json.Delim('{') {
		return domain.ErrInvalid
	}
	seen := map[string]bool{}
	for check.More() {
		key, err := check.Token()
		if err != nil {
			return domain.ErrInvalid
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return domain.ErrInvalid
		}
		seen[name] = true
		var value json.RawMessage
		if check.Decode(&value) != nil || bytes.Equal(value, []byte("null")) {
			return domain.ErrInvalid
		}
		if len(value) > 0 && value[0] == '"' && !validJSONStringUnicode(value) {
			return domain.ErrInvalid
		}
	}
	if token, err = check.Token(); err != nil || token != json.Delim('}') || check.Decode(new(any)) != io.EOF {
		return domain.ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return domain.ErrInvalid
	}
	if dec.Decode(new(any)) != io.EOF {
		return domain.ErrInvalid
	}
	return nil
}

func (s *server) retrievalTools(uid, wid, sid, rid string) agent.ToolHandler {
	return func(ctx context.Context, name string, raw json.RawMessage) (value any, err error) {
		ctx, finish := s.cfg.Telemetry.Start(ctx, "tool."+name)
		defer func() { finish(err) }()
		// This closure is the only identity source. Model arguments cannot select
		// a caller, workspace, session or run.
		scope, err := s.db.RunResourceScope(ctx, uid, wid, sid, rid, nil)
		if err != nil {
			return nil, err
		}
		switch name {
		case "create_document", "update_document":
			result, e := s.db.ExecuteDocumentWrite(ctx, uid, wid, sid, rid, agent.ToolCallID(ctx), name, raw)
			if e != nil {
				code := "tool_unavailable"
				switch {
				case errors.Is(e, domain.ErrConflict):
					code = "write_conflict"
				case errors.Is(e, domain.ErrForbidden):
					code = "forbidden"
				case errors.Is(e, domain.ErrNotFound):
					code = "not_found"
				case errors.Is(e, domain.ErrInvalid):
					code = "invalid_arguments"
				}
				return nil, &agent.ToolError{Code: code}
			}
			return result, nil
		case "tool_capabilities", "list_knowledge_bases", "search_knowledge", "read_document", "list_documents":
			return s.knowledgeTool(ctx, uid, wid, sid, rid, name, raw)
		case "search_conversations":
			var in struct {
				Query  string `json:"query"`
				Limit  *int   `json:"limit"`
				Offset *int   `json:"offset"`
			}
			if strictToolArgs(raw, &in) != nil {
				return nil, domain.ErrInvalid
			}
			limit, offset := 5, 0
			if in.Limit != nil {
				limit = *in.Limit
			}
			if in.Offset != nil {
				offset = *in.Offset
			}
			return s.db.SearchRunConversations(ctx, uid, wid, sid, rid, in.Query, limit, offset)
		case "resolve_citation_aliases":
			var in struct {
				IDs []string `json:"evidence_ids"`
			}
			if strictToolArgs(raw, &in) != nil {
				return nil, domain.ErrInvalid
			}
			aliases, e := s.db.ResolveRunCitationAliases(ctx, uid, wid, sid, rid, in.IDs)
			if e != nil {
				return nil, e
			}
			return map[string]any{"aliases": aliases}, nil
		case "restore_evidence":
			if strictToolArgs(raw, &struct{}{}) != nil {
				return nil, domain.ErrInvalid
			}
			stored, err := s.db.EvidenceForRun(ctx, uid, wid, sid, rid, false)
			if err != nil {
				return nil, err
			}
			evidence := make([]domain.Evidence, 0, len(stored))
			for _, e := range stored {
				hydrated, err := s.hydrateEvidence(ctx, uid, wid, sid, rid, e.ID)
				if err != nil {
					return nil, err
				}
				evidence = append(evidence, hydrated)
			}
			if err := s.db.ValidateRunSources(ctx, uid, wid, sid, rid); err != nil {
				return nil, err
			}
			return map[string]any{"evidence": evidence}, nil
		case "search_documents":
			if s.cfg.Indexing == nil {
				return nil, agent.ErrUnavailable
			}
			var in struct {
				Query string `json:"query"`
				Mode  string `json:"mode"`
				Limit int    `json:"limit"`
			}
			if strictToolArgs(raw, &in) != nil || in.Limit < 0 || in.Limit > 10 {
				return nil, domain.ErrInvalid
			}
			if in.Mode == "" {
				in.Mode = "hybrid"
			}
			if in.Limit == 0 {
				in.Limit = 5
			}
			hits, err := s.cfg.Indexing.SearchResourcesScoped(ctx, uid, wid, in.Query, in.Mode, in.Limit, scope)
			if err != nil {
				return nil, err
			}
			evidence, err := s.db.RecordRunEvidence(ctx, uid, wid, sid, rid, hits)
			if err != nil {
				return nil, err
			}
			byChunk := make(map[string]domain.IndexChunk, len(hits))
			for _, hit := range hits {
				byChunk[hit.Chunk.ID] = hit.Chunk
			}
			for i := range evidence {
				chunk, ok := byChunk[evidence[i].ChunkID]
				if !ok || !matchesEvidence(evidence[i], chunk) {
					return nil, domain.ErrConflict
				}
				evidence[i].Content = chunk.Content
			}
			if evidence == nil {
				evidence = []domain.Evidence{}
			}
			return map[string]any{"evidence": evidence}, nil
		case "read_evidence":
			if s.cfg.Indexing == nil {
				return nil, agent.ErrUnavailable
			}
			var in struct {
				ID string `json:"evidence_id"`
			}
			if strictToolArgs(raw, &in) != nil {
				return nil, domain.ErrInvalid
			}
			return s.hydrateEvidence(ctx, uid, wid, sid, rid, in.ID)
		default:
			return nil, domain.ErrForbidden
		}
	}
}

func matchesEvidence(e domain.Evidence, c domain.IndexChunk) bool {
	return e.IndexID == c.IndexID && e.DocumentID == c.DocumentID && e.VersionID == c.VersionID &&
		e.SourceSHA256 == c.SourceSHA256 && e.ChunkID == c.ID && e.SHA256 == c.SHA256 &&
		e.StartByte == c.StartByte && e.EndByte == c.EndByte
}

func (s *server) hydrateEvidence(ctx context.Context, uid, wid, sid, rid, eid string) (domain.Evidence, error) {
	e, err := s.db.RunEvidence(ctx, uid, wid, sid, rid, eid)
	if err != nil {
		return domain.Evidence{}, err
	}
	if s.cfg.Indexing == nil {
		return domain.Evidence{}, agent.ErrUnavailable
	}
	chunk, err := s.cfg.Indexing.Chunk(ctx, uid, wid, e.DocumentID, e.VersionID, e.IndexID, e.ChunkID)
	if err != nil {
		return domain.Evidence{}, err
	}
	if !matchesEvidence(e, chunk) {
		return domain.Evidence{}, domain.ErrConflict
	}
	// Recheck after the fixed SQL chunk read. A changed head is allowed for a
	// historical citation; deletion/revocation or generation changes aren't.
	if _, err = s.db.RunEvidence(ctx, uid, wid, sid, rid, eid); err != nil {
		return domain.Evidence{}, err
	}
	e.Content = chunk.Content
	return e, nil
}

func (s *server) answerCitations(ctx context.Context, request agent.Request, answer string) ([]evidenceView, error) {
	ids, err := domain.CitationIDs(answer)
	if err != nil {
		return nil, errInvalidCitation
	}
	result := make([]evidenceView, 0, len(ids))
	for _, id := range ids {
		e, err := s.hydrateEvidence(ctx, request.UserID, request.WorkspaceID, request.SessionID, request.RunID, id)
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalid) {
			return nil, errInvalidCitation
		}
		if err != nil {
			return nil, err
		}
		e.Cited = true
		result = append(result, publicEvidence(e, request.SessionID))
	}
	return result, nil
}

func (s *server) runEvidence(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	e, err := s.hydrateEvidence(ctx, userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"), r.PathValue("evidenceID"))
	if err != nil {
		indexFailure(w, err)
		return
	}
	respond(w, 200, publicEvidence(e, r.PathValue("sessionID")))
}

func (s *server) runCitations(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	uid, wid, sid, rid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID")
	evidence, err := s.db.EvidenceForRun(ctx, uid, wid, sid, rid, true)
	if err != nil {
		fail(w, err)
		return
	}
	views := make([]evidenceView, 0, len(evidence))
	for _, stored := range evidence {
		e, err := s.hydrateEvidence(ctx, uid, wid, sid, rid, stored.ID)
		if err != nil {
			indexFailure(w, err)
			return
		}
		views = append(views, publicEvidence(e, sid))
	}
	// Reading later manifests may race with deleting an earlier source. Recheck
	// the whole assembled batch after all remote I/O before exposing any text.
	current, err := s.db.EvidenceForRun(ctx, uid, wid, sid, rid, true)
	if err != nil {
		fail(w, err)
		return
	}
	allowed := make(map[string]bool, len(current))
	for _, e := range current {
		allowed[e.ID] = true
	}
	filtered := make([]evidenceView, 0, len(views))
	for _, view := range views {
		if allowed[view.Evidence.ID] {
			filtered = append(filtered, view)
		}
	}
	respond(w, 200, map[string]any{"citations": filtered})
}

// A previous cited answer is source-derived data too. Do not feed it back to
// the model when its evidence is deleted, revoked or outside this turn's scope.
// Durable user-visible history remains intact; only model hydration is reduced.
func (s *server) retrievalHistory(ctx context.Context, uid, wid, sid, targetRunID string, history []domain.Message) ([]agent.Message, []string, error) {
	// BeginRun hydrates at most 100 completed turns. Recheck every candidate's
	// provenance before the model window is selected, without a hidden 20-turn cut.
	filtered := make([]domain.Message, 0, len(history))
	for i := 0; i+1 < len(history); i += 2 {
		question, answer := history[i], history[i+1]
		if question.Role != "user" || answer.Role != "assistant" || question.RunID != answer.RunID {
			continue
		}
		ids, err := domain.CitationIDs(answer.Content)
		if err != nil {
			continue
		}
		valid, err := s.db.CanHydrateRunHistoryForRun(ctx, uid, wid, sid, answer.RunID, targetRunID)
		if err != nil {
			return nil, nil, err
		}
		if !valid {
			continue
		}
		for _, id := range ids {
			_, err := s.db.RunEvidence(ctx, uid, wid, sid, answer.RunID, id)
			if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrConflict) {
				valid = false
				break
			}
			if err != nil {
				return nil, nil, err
			}
		}
		if valid {
			question.Content, err = s.db.ClarifiedQuery(ctx, uid, wid, sid, question.RunID, question.Content)
			if errors.Is(err, domain.ErrInvalid) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			filtered = append(filtered, question, answer)
		}
	}
	selectedHistory := boundedHistory(filtered)
	window := make([]agent.Message, 0, len(selectedHistory))
	sourceRuns := []string{}
	for i, message := range selectedHistory {
		window = append(window, agent.Message{Role: message.Role, Content: message.Content})
		if i%2 == 0 {
			sourceRuns = append(sourceRuns, message.RunID)
		}
	}
	return window, sourceRuns, nil
}
