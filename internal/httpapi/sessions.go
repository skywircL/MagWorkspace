package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/store"
)

func (s *server) sessions(w http.ResponseWriter, r *http.Request) {
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	data, total, err := s.db.Sessions(r.Context(), userID(r), r.PathValue("workspaceID"), n, (p-1)*n)
	if err != nil {
		fail(w, err)
		return
	}
	page(w, data, total, p, n)
}
func (s *server) session(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Session(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) createSession(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	if !decode(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.Title) == "" {
		in.Title = "新会话"
	}
	data, err := s.db.CreateSession(r.Context(), userID(r), r.PathValue("workspaceID"), strings.TrimSpace(in.Title))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}
func (s *server) updateSession(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	if !decode(w, r, &in) {
		return
	}
	data, err := s.db.UpdateSession(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), strings.TrimSpace(in.Title))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) deleteSession(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteSession(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *server) messages(w http.ResponseWriter, r *http.Request) {
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	data, total, err := s.db.Messages(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), n, (p-1)*n)
	if err != nil {
		fail(w, err)
		return
	}
	page(w, data, total, p, n)
}
func (s *server) run(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Run(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) chat(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Content     string    `json:"content"`
		DocumentIDs *[]string `json:"document_ids"`
	}
	if !decode(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.Content) == "" || len(in.Content) > 64000 {
		problem(w, 400, "invalid_request", "content must contain 1..64000 bytes")
		return
	}
	uid, wid, sid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID")
	// Authorization precedes capability disclosure; an unavailable provider never creates a turn.
	if _, err := s.db.Session(r.Context(), uid, wid, sid); err != nil {
		fail(w, err)
		return
	}
	if s.agent == nil {
		problem(w, 503, "agent_unavailable", "configure MAGENT_MODE to enable conversations")
		return
	}
	var streaming agent.StreamingClient
	if wantsEventStream(r) {
		if r.Header.Get("Last-Event-ID") != "" {
			problem(w, 400, "replay_not_supported", "event replay is not supported; inspect the existing run before sending a new message")
			return
		}
		var ok bool
		streaming, ok = s.agent.(agent.StreamingClient)
		if !ok || !canFlush(w) {
			problem(w, 503, "stream_unavailable", "streaming is unavailable")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RunTimeout)
	defer cancel()
	var selected []string
	if in.DocumentIDs != nil {
		selected = append([]string{}, (*in.DocumentIDs)...)
	}
	run, history, role, err := s.db.BeginRun(ctx, uid, wid, sid, in.Content, s.cfg.RunTimeout+5*time.Second, selected)
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("X-Run-ID", run.ID)
	request := agent.Request{RunID: run.ID, SessionID: sid, WorkspaceID: wid, UserID: uid, Role: role, HistoryWindowed: true, Query: in.Content}
	var historyRuns []string
	request.History, historyRuns, err = s.retrievalHistory(ctx, uid, wid, sid, history, selected)
	if err == nil {
		err = s.db.BindRunHistory(ctx, uid, wid, sid, run.ID, historyRuns)
	}
	if err != nil {
		s.failRun(uid, wid, sid, run.ID, "history_unavailable")
		fail(w, err)
		return
	}
	request.DocumentIDs = in.DocumentIDs
	request.ToolHandler = s.retrievalTools(uid, wid, sid, run.ID)
	ctx, err = s.bindRunJournal(ctx, &request)
	if err != nil {
		s.failRun(uid, wid, sid, run.ID, "journal_unavailable", request.Generation)
		fail(w, err)
		return
	}
	if streaming != nil {
		s.streamChat(w, r, ctx, cancel, run, request, streaming)
		return
	}
	s.jsonChat(w, r, ctx, run, request)
}

func (s *server) jsonChat(w http.ResponseWriter, r *http.Request, ctx context.Context, run domain.Run, request agent.Request) {
	uid, wid, sid := request.UserID, request.WorkspaceID, request.SessionID
	done := false
	defer func() {
		if !done {
			s.failRun(uid, wid, sid, run.ID, "interrupted", request.Generation)
		}
	}()
	result, err := s.agent.Run(ctx, request)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err == nil && !validResult(result, run.ID) {
		err = agent.ErrFailed
	}
	if err != nil {
		status, code := agentFailure(err)
		done = s.failRun(uid, wid, sid, run.ID, code, request.Generation)
		problem(w, status, code, "agent run did not complete; use X-Run-ID to inspect its status")
		return
	}
	if result.Status == "suspended" {
		prompt, err := s.saveQuestion(ctx, request, result)
		if err != nil {
			fail(w, err)
			return
		}
		done = true
		s.cfg.Telemetry.ObserveCount("run.finish", "suspended", 1)
		run.Status = "suspended"
		run.Mode = result.Mode
		respond(w, 202, map[string]any{"run": run, "prompt": prompt})
		return
	}
	citations, err := s.answerCitations(ctx, request, result.Answer)
	if err != nil {
		code := "evidence_unavailable"
		if errors.Is(err, errInvalidCitation) {
			code = "invalid_citation"
		}
		done = s.failRun(uid, wid, sid, run.ID, code, request.Generation)
		problem(w, 502, code, "answer evidence could not be verified")
		return
	}
	if err = s.db.FinishRun(ctx, uid, wid, sid, run.ID, result.Answer, result.Mode); err != nil {
		if errors.Is(err, domain.ErrInvalid) {
			done = s.failRun(uid, wid, sid, run.ID, "invalid_citation", request.Generation)
			problem(w, 502, "invalid_citation", "answer evidence could not be verified")
			return
		}
		fail(w, err)
		return
	}
	done = true
	s.cfg.Telemetry.ObserveCount("run.finish", "success", 1)
	completed, err := s.db.Run(r.Context(), uid, wid, sid, run.ID)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, map[string]any{"run": completed, "answer": result.Answer, "citations": citations})
}

func validResult(result agent.Result, runID string) bool {
	if result.RunID != runID || (result.Mode != "demo" && result.Mode != "live") {
		return false
	}
	if result.Status == "suspended" {
		return result.Prompt != nil && result.Answer == ""
	}
	return (result.Status == "" || result.Status == "completed") && result.Prompt == nil && strings.TrimSpace(result.Answer) != "" && len(result.Answer) <= 64000
}

func agentFailure(err error) (int, string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return 504, "agent_timeout"
	case errors.Is(err, context.Canceled):
		return 408, "agent_canceled"
	case errors.Is(err, agent.ErrUnavailable):
		return 503, "agent_unavailable"
	default:
		return 502, "agent_failed"
	}
}

// Failure recording survives client disconnects, but never blocks shutdown indefinitely.
func (s *server) failRun(uid, wid, sid, rid, code string, generations ...uint64) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if len(generations) != 0 && generations[0] != 0 {
		ctx = store.WithRunExecution(ctx, rid, generations[0])
	}
	if err := s.db.FailRun(ctx, uid, wid, sid, rid, code); err != nil {
		slog.Error("record agent failure", "run_id", rid)
		return false
	}
	s.cfg.Telemetry.ObserveCount("run.finish", "error", 1)
	return true
}

const maxHydratedMessages = 200
const maxHydratedBytes = 512 << 10
const maxHydratedMessageBytes = 64 << 10

// Select complete, recent turns without truncating either message. The storage
// layer supplies at most 100 completed turns; durable history remains intact.
func boundedHistory(history []domain.Message) []domain.Message {
	selected := make([]domain.Message, 0, min(len(history), maxHydratedMessages))
	size := 2 // JSON array brackets; count the actual Go JSON escaping.
	for end := len(history); end >= 2 && len(selected) < maxHydratedMessages; end -= 2 {
		a, b := history[end-2], history[end-1]
		if a.Role != "user" || b.Role != "assistant" || a.RunID != b.RunID {
			continue
		}
		if len(a.Content) > maxHydratedMessageBytes || len(b.Content) > maxHydratedMessageBytes {
			continue
		}
		encoded, err := json.Marshal([]agent.Message{{Role: a.Role, Content: a.Content}, {Role: b.Role, Content: b.Content}})
		if err != nil {
			continue
		}
		pairBytes := len(encoded) - 2
		if len(selected) > 0 {
			pairBytes++
		}
		if size+pairBytes > maxHydratedBytes {
			continue
		}
		size += pairBytes
		selected = append(selected, b, a)
	}
	for left, right := 0, len(selected)-1; left < right; left, right = left+1, right-1 {
		selected[left], selected[right] = selected[right], selected[left]
	}
	return selected
}

func historyWindow(history []domain.Message) []agent.Message {
	selected := boundedHistory(history)
	out := make([]agent.Message, 0, len(selected))
	for _, message := range selected {
		out = append(out, agent.Message{Role: message.Role, Content: message.Content})
	}
	return out
}
