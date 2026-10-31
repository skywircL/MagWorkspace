package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/stream"
)

type Handler struct {
	http.Handler
	mu      sync.Mutex
	stopped bool
	cancel  context.CancelFunc
	ctx     context.Context
	workers sync.WaitGroup
}

func (h *Handler) start(work func()) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped || h.ctx.Err() != nil {
		return false
	}
	h.workers.Add(1)
	go func() { defer h.workers.Done(); work() }()
	return true
}

// Shutdown cancels detached runs and waits for their failure records and cleanup.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.stopped = true
	h.cancel()
	h.mu.Unlock()
	done := make(chan struct{})
	go func() { h.workers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type streamOutcome struct {
	result agent.Result
	err    error
}

func (s *server) streamChat(w http.ResponseWriter, r *http.Request, base context.Context, _ context.CancelFunc, run domain.Run, request agent.Request, client agent.StreamingClient) {
	// Retain tracing and execution fencing, but not the browser's cancellation.
	deadline, ok := base.Deadline()
	if !ok {
		deadline = time.Now().Add(s.cfg.RunTimeout)
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(base), deadline)
	stopShutdown := context.AfterFunc(s.runtime.ctx, cancel)
	key := stream.Key{SessionID: request.SessionID, RunID: run.ID, Generation: request.Generation}
	marker := stream.LiveRun{RunID: run.ID, Generation: request.Generation, RequestID: run.ID}
	// Server-generated run/generation uniquely identifies this execution.
	failStart := func() {
		cancel()
		stopShutdown()
		s.failRun(request.UserID, request.WorkspaceID, request.SessionID, run.ID, "stream_unavailable", request.Generation)
		problem(w, 503, "events_unavailable", "stream storage unavailable; inspect the run")
	}
	old, err := s.cfg.Streams.GetLiveRun(ctx, key.SessionID)
	if err != nil {
		failStart()
		return
	}
	current, gen, err := s.db.RunEventState(ctx, request.UserID, request.WorkspaceID, request.SessionID, run.ID)
	if err != nil || gen != key.Generation || current.Status != "running" {
		cancel()
		stopShutdown()
		s.failRun(request.UserID, request.WorkspaceID, request.SessionID, run.ID, "interrupted", request.Generation)
		fail(w, domain.ErrConflict)
		return
	}
	if old == (stream.LiveRun{}) {
		err = s.cfg.Streams.SetLiveRun(ctx, key.SessionID, marker)
	} else {
		err = s.cfg.Streams.ClaimLiveRun(ctx, key.SessionID, old, marker)
	}
	if err != nil {
		failStart()
		return
	}
	raw, _ := json.Marshal(map[string]any{"run_id": run.ID, "session_id": request.SessionID, "run": run})
	if err = s.cfg.Streams.AppendEvent(ctx, key, stream.Event{Sequence: 1, Type: "run.started", Data: raw}); err != nil {
		_ = s.cfg.Streams.ClearLiveRun(ctx, key.SessionID, marker)
		failStart()
		return
	}
	if !s.runtime.start(func() {
		defer cancel()
		defer stopShutdown()
		s.produceStream(ctx, cancel, key, marker, run, request, client)
	}) {
		_ = s.cfg.Streams.ClearLiveRun(ctx, key.SessionID, marker)
		failStart()
		return
	}
	s.subscribeEvents(w, r, request.UserID, request.WorkspaceID, key, 0)
}

func (s *server) produceStream(ctx context.Context, cancel context.CancelFunc, key stream.Key, marker stream.LiveRun, run domain.Run, request agent.Request, client agent.StreamingClient) {
	uid, wid, sid := request.UserID, request.WorkspaceID, request.SessionID
	finalized := false
	defer func() {
		if recover() != nil {
			slog.Error("stream producer panic", "run_id", run.ID)
		}
		cancel()
		if !finalized {
			s.failRun(uid, wid, sid, run.ID, "interrupted", request.Generation)
		}
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if s.cfg.Streams.ClearLiveRun(cleanup, sid, marker) != nil {
			s.cfg.Telemetry.ObserveCount("stream.cleanup", "error", 1)
		}
	}()
	seq := uint64(1)
	bufferFailed := false
	publish := func(name string, data any) error {
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		// Cancellation failures still need a terminal event after SQL records them.
		writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer stop()
		err = s.cfg.Streams.AppendEvent(writeCtx, key, stream.Event{Sequence: seq + 1, Type: name, Data: raw})
		if err == nil {
			seq++
		} else {
			bufferFailed = true
			s.cfg.Telemetry.ObserveCount("stream.events", "error", 1)
		}
		return err
	}
	failStream := func(code string) {
		finalized = s.failRun(uid, wid, sid, run.ID, code, request.Generation)
		if !finalized {
			code = "persistence_failed"
		}
		_ = publish("run.failed", map[string]any{"run_id": run.ID, "session_id": sid, "error": map[string]string{"code": code, "message": "run did not complete; inspect the run before retrying"}})
	}
	// Stop polling remains alive with no HTTP subscribers. Only the producer
	// writes output, so terminal publication cannot race a late callback.
	stopCtx, stopPolling := context.WithCancel(ctx)
	stopPollingDone := make(chan struct{})
	stopPollFailed := false
	go func() {
		defer close(stopPollingDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				requested, err := s.cfg.Streams.StopRequested(stopCtx, key)
				if err != nil {
					if stopCtx.Err() == nil {
						stopPollFailed = true
					}
					cancel()
					return
				}
				if requested {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { stopPolling(); <-stopPollingDone }()
	outcome := streamOutcome{}
	func() {
		defer func() {
			if recover() != nil {
				outcome.err = agent.ErrFailed
			}
		}()
		round, roundBytes := 0, 0
		discarded := false
		outcome.result, outcome.err = client.Stream(ctx, request, func(event agent.Event) error {
			if event.RunID != run.ID || event.SessionID != sid {
				return agent.ErrFailed
			}
			switch event.Type {
			case "message.start":
				if event.Round != round+1 || event.Round > 32 || event.Delta != "" {
					return agent.ErrFailed
				}
				round, roundBytes, discarded = event.Round, 0, false
			case "message.delta":
				if round == 0 || discarded || event.Round != round || event.Delta == "" || len(event.Delta) > 64000-roundBytes {
					return agent.ErrFailed
				}
				roundBytes += len(event.Delta)
			case "message.discard":
				if round == 0 || discarded || event.Round != round || event.Delta != "" {
					return agent.ErrFailed
				}
				discarded = true
			default:
				return agent.ErrFailed
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return publish(event.Type, event)
		})
	}()
	stopPolling()
	<-stopPollingDone

	if outcome.err == nil && ctx.Err() != nil {
		outcome.err = ctx.Err()
	}
	if outcome.err == nil && !validResult(outcome.result, run.ID) {
		outcome.err = agent.ErrFailed
	}
	if bufferFailed || stopPollFailed {
		failStream("stream_unavailable")
		return
	}
	if outcome.err != nil {
		_, code := agentFailure(outcome.err)
		failStream(code)
		return
	}
	if outcome.result.Status == "suspended" {
		prompt, err := s.saveQuestion(ctx, request, outcome.result)
		if err != nil {
			failStream("persistence_failed")
			return
		}
		finalized = true
		s.cfg.Telemetry.ObserveCount("run.finish", "suspended", 1)
		run.Status = "suspended"
		run.Mode = outcome.result.Mode
		_ = publish("run.suspended", map[string]any{"run_id": run.ID, "session_id": sid, "run": run, "prompt": prompt})
		return
	}
	citations, err := s.answerCitations(ctx, request, outcome.result.Answer)
	if err != nil {
		code := "evidence_unavailable"
		if errors.Is(err, errInvalidCitation) {
			code = "invalid_citation"
		}
		failStream(code)
		return
	}
	if err := s.db.FinishRun(ctx, uid, wid, sid, run.ID, outcome.result.Answer, outcome.result.Mode); err != nil {
		if ctx.Err() != nil {
			_, code := agentFailure(ctx.Err())
			failStream(code)
		} else if errors.Is(err, domain.ErrInvalid) {
			failStream("invalid_citation")
		} else {
			failStream("persistence_failed")
		}
		return
	}
	// The completed state is durable even if the client disconnects now.
	finalized = true
	s.cfg.Telemetry.ObserveCount("run.finish", "success", 1)
	completed, err := s.db.Run(ctx, uid, wid, sid, run.ID)
	if err != nil {
		// An ambiguous delivery is resolved by polling, never by claiming a
		// committed run failed or replaying it automatically.
		slog.Error("read committed stream result", "run_id", run.ID)
		return
	}
	_ = publish("run.completed", map[string]any{"run_id": run.ID, "session_id": sid, "run": completed, "answer": outcome.result.Answer, "citations": citations})
	return
}
