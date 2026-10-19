package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"mime"
	"net/http"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
)

func (s *server) saveQuestion(ctx context.Context, request agent.Request, result agent.Result) (out domain.RunPrompt, err error) {
	ctx, finish := s.cfg.Telemetry.Start(ctx, "prompt.suspend")
	defer func() { finish(err) }()
	p := result.Prompt
	if p == nil || math.IsNaN(p.ExpiresAt) || math.IsInf(p.ExpiresAt, 0) || p.ExpiresAt <= 0 || p.ExpiresAt > float64(time.Now().Add(8*24*time.Hour).Unix()) {
		return domain.RunPrompt{}, domain.ErrInvalid
	}
	var approval *domain.DocumentApproval
	if p.Kind == "tool_approval" {
		approval = &domain.DocumentApproval{}
		if json.Unmarshal(p.Request, approval) != nil {
			return domain.RunPrompt{}, domain.ErrInvalid
		}
	} else if p.Kind != "" && p.Kind != "ask_user" {
		return domain.RunPrompt{}, domain.ErrInvalid
	}
	return s.db.SuspendRun(ctx, request.UserID, request.WorkspaceID, request.SessionID, request.RunID, result.Mode, domain.SuspendedPrompt{
		Approval: approval, ID: p.ID, ToolCallID: p.ToolCallID, Question: p.Question, Options: p.Options, Snapshot: p.Snapshot, ExpiresAt: time.UnixMicro(int64(p.ExpiresAt * 1e6)),
	})
}

func (s *server) runPrompt(w http.ResponseWriter, r *http.Request) {
	prompt, err := s.db.RunPrompt(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"), r.PathValue("promptID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, prompt)
}

func (s *server) answerPrompt(w http.ResponseWriter, r *http.Request)  { s.resolvePrompt(w, r, false) }
func (s *server) approvePrompt(w http.ResponseWriter, r *http.Request) { s.resolvePrompt(w, r, true) }
func (s *server) resolvePrompt(w http.ResponseWriter, r *http.Request, approval bool) {
	answer := ""
	var approved bool
	if approval {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			problem(w, 415, "unsupported_media_type", "Content-Type must be application/json")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		var in struct {
			Approved *bool `json:"approved"`
		}
		if err != nil || strictToolArgs(raw, &in) != nil || in.Approved == nil {
			fail(w, domain.ErrInvalid)
			return
		}
		approved = *in.Approved
	} else {
		fields, ok := readDocumentStringFields(w, r, 32<<10, "answer")
		if !ok {
			return
		}
		answer, ok = fields["answer"]
		if !ok {
			fail(w, domain.ErrInvalid)
			return
		}
	}
	uid, wid, sid, rid, pid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"), r.PathValue("promptID")
	// Check ownership before disclosing whether the runtime is configured.
	if _, err := s.db.RunPrompt(r.Context(), uid, wid, sid, rid, pid); err != nil {
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
			problem(w, 400, "replay_not_supported", "inspect the existing run before retrying")
			return
		}
		var supported bool
		streaming, supported = s.agent.(agent.StreamingClient)
		if !supported || !canFlush(w) {
			problem(w, 503, "stream_unavailable", "streaming is unavailable")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RunTimeout)
	defer cancel()
	resumeCtx, finishResume := s.cfg.Telemetry.Start(ctx, "prompt.resume")
	var resumed domain.ResumedRun
	var err error
	if approval {
		resumed, err = s.db.ResumeApproval(resumeCtx, uid, wid, sid, rid, pid, approved, s.cfg.RunTimeout+5*time.Second)
	} else {
		resumed, err = s.db.ResumePrompt(resumeCtx, uid, wid, sid, rid, pid, answer, s.cfg.RunTimeout+5*time.Second)
	}
	finishResume(err)
	if err != nil {
		fail(w, err)
		return
	}
	request := agent.Request{RunID: rid, SessionID: sid, WorkspaceID: wid, UserID: uid, Role: resumed.Role, Query: resumed.Query, Resume: &agent.ResumeRequest{Prompt: resumed.Prompt}, ToolHandler: s.retrievalTools(uid, wid, sid, rid)}
	request.Generation = resumed.Generation
	if resumed.DocumentIDs != nil {
		request.DocumentIDs = &resumed.DocumentIDs
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

func (s *server) cancelPrompt(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	ctx, finish := s.cfg.Telemetry.Start(r.Context(), "prompt.cancel")
	err := s.db.CancelPrompt(ctx, userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"), r.PathValue("promptID"))
	finish(err)
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
