package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/store"
)

// bindRunJournal gives the child an execution-scoped capability, never storage
// paths or database credentials. Every callback rechecks the current generation.
func (s *server) bindRunJournal(ctx context.Context, request *agent.Request) (context.Context, error) {
	generation := request.Generation
	if generation == 0 {
		var err error
		generation, err = s.db.BeginRunJournal(ctx, request.UserID, request.WorkspaceID, request.SessionID, request.RunID)
		if err != nil {
			return ctx, err
		}
	}
	request.Generation = generation
	request.MemoryHandler = func(callbackCtx context.Context, operation string, arguments json.RawMessage) (any, error) {
		return s.db.RunMemory(callbackCtx, request.UserID, request.WorkspaceID, request.SessionID, request.RunID, generation, operation, arguments)
	}
	if handler := request.ToolHandler; handler != nil {
		request.ToolHandler = func(callbackCtx context.Context, name string, arguments json.RawMessage) (any, error) {
			return handler(store.WithRunExecution(callbackCtx, request.RunID, generation), name, arguments)
		}
	}
	request.JournalHandler = func(callbackCtx context.Context, operation string, arguments json.RawMessage) (any, error) {
		return s.db.RunJournal(callbackCtx, request.UserID, request.WorkspaceID, request.SessionID, request.RunID, generation, operation, arguments)
	}
	return store.WithRunExecution(ctx, request.RunID, generation), nil
}

// Recovery is explicit. It never creates a new user message, changes the
// original scope, or treats an HTTP reconnect as permission to execute again.
func (s *server) recoverRun(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	uid, wid, sid, rid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID")
	if _, err := s.db.Run(r.Context(), uid, wid, sid, rid); err != nil {
		fail(w, err)
		return
	}
	if s.agent == nil {
		problem(w, 503, "recovery_unavailable", "persistent agent recovery is unavailable")
		return
	}
	var streaming agent.StreamingClient
	if wantsEventStream(r) {
		if r.Header.Get("Last-Event-ID") != "" {
			problem(w, 400, "replay_not_supported", "inspect the existing run before retrying")
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
	resumed, err := s.db.RecoverRun(ctx, uid, wid, sid, rid, s.cfg.RunTimeout+5*time.Second)
	if err != nil {
		fail(w, err)
		return
	}
	request := agent.Request{
		RunID: rid, SessionID: sid, WorkspaceID: wid, UserID: uid,
		Role: resumed.Role, Query: resumed.Query, Recover: true,
		Generation: resumed.Generation, ToolHandler: s.retrievalTools(uid, wid, sid, rid),
	}
	if resumed.DocumentIDs != nil {
		request.DocumentIDs = &resumed.DocumentIDs
	}
	if len(resumed.Prompt) != 0 {
		request.Resume = &agent.ResumeRequest{Prompt: resumed.Prompt}
	}
	ctx, err = s.bindRunJournal(ctx, &request)
	if err != nil {
		s.failRun(uid, wid, sid, rid, "journal_unavailable", request.Generation)
		fail(w, err)
		return
	}
	w.Header().Set("X-Run-ID", rid)
	if streaming != nil {
		s.streamChat(w, r, ctx, cancel, resumed.Run, request, streaming)
		return
	}
	s.jsonChat(w, r, ctx, resumed.Run, request)
}
