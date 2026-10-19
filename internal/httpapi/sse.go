package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
)

// Streaming is opt-in. Wildcards retain the existing JSON response contract.
func wantsEventStream(r *http.Request) bool {
	streamQuality, jsonQuality := 0.0, 0.0
	for _, field := range r.Header.Values("Accept") {
		for _, item := range strings.Split(field, ",") {
			media, params, err := mime.ParseMediaType(strings.TrimSpace(item))
			if err != nil {
				continue
			}
			quality := 1.0
			if value, exists := params["q"]; exists {
				quality, err = strconv.ParseFloat(value, 64)
				if err != nil || quality < 0 || quality > 1 {
					continue
				}
			}
			switch media {
			case "text/event-stream":
				streamQuality = max(streamQuality, quality)
			case "application/json":
				jsonQuality = max(jsonQuality, quality)
			}
		}
	}
	return streamQuality > 0 && streamQuality >= jsonQuality
}

func canFlush(w http.ResponseWriter) bool {
	for {
		switch writer := w.(type) {
		case interface{ FlushError() error }:
			return true
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = writer.Unwrap()
		default:
			return false
		}
	}
}

type eventWriter struct {
	w          http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
	record     func(string, []byte) string
}

func (s *eventWriter) write(frame string) error {
	if err := s.controller.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	// An idle connection must not retain an expired per-write deadline between heartbeats.
	defer s.controller.SetWriteDeadline(time.Time{})
	if _, err := fmt.Fprint(s.w, frame); err != nil {
		return err
	}
	return s.controller.Flush()
}

func (s *eventWriter) event(name string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	id := ""
	if s.record != nil {
		id = s.record(name, encoded)
	}
	return s.write(id + "event: " + name + "\ndata: " + string(encoded) + "\n\n")
}

type streamOutcome struct {
	result agent.Result
	err    error
}

// streamChat is the sole HTTP writer. The worker only delivers bounded events;
// cancellation unblocks its callback and terminates the subprocess it owns.
func (s *server) streamChat(w http.ResponseWriter, r *http.Request, ctx context.Context, cancel context.CancelFunc, run domain.Run, request agent.Request, client agent.StreamingClient) {
	uid, wid, sid := request.UserID, request.WorkspaceID, request.SessionID
	finalized := false
	failureCode := "interrupted"
	writeFailure := func() {
		failureCode = "agent_canceled"
		if ctx.Err() != nil {
			_, failureCode = agentFailure(ctx.Err())
		}
	}
	defer func() {
		cancel()
		if !finalized {
			s.failRun(uid, wid, sid, run.ID, failureCode, request.Generation)
		}
	}()
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	writer := &eventWriter{w: w, controller: http.NewResponseController(w), timeout: s.cfg.SSEWriteTimeout}
	if s.cfg.Redis != nil && request.Generation > 0 {
		var seq uint64
		healthy := true
		writer.record = func(name string, data []byte) string {
			seq++
			if !healthy {
				return ""
			}
			if err := s.cfg.Redis.AppendEvent(ctx, run.ID, request.Generation, seq, name, data); err != nil {
				healthy = false
				s.cfg.Telemetry.ObserveCount("redis.events", "error", 1)
				return ""
			}
			return fmt.Sprintf("id: %d:%d\n", request.Generation, seq)
		}
	}

	if err := writer.event("run.started", map[string]any{"run_id": run.ID, "session_id": sid, "run": run}); err != nil {
		writeFailure()
		return
	}
	events := make(chan agent.Event)
	outcomes := make(chan streamOutcome, 1)
	go func() {
		outcome := streamOutcome{}
		defer func() {
			if recover() != nil {
				outcome.err = agent.ErrFailed
			}
			outcomes <- outcome
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
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	workerFinished := false
	defer func() {
		cancel()
		if !workerFinished {
			// ProcessClient reaps its child on cancellation, with a one-second pipe deadline.
			select {
			case <-outcomes:
			case <-time.After(3 * time.Second):
				slog.Error("stream worker did not stop", "run_id", run.ID)
			}
		}
	}()
	heartbeat := time.NewTicker(s.cfg.SSEHeartbeat)
	defer heartbeat.Stop()

	failStream := func(code string) {
		failureCode = code
		finalized = s.failRun(uid, wid, sid, run.ID, code, request.Generation)
		if !finalized {
			code = "persistence_failed"
		}
		// Once headers are flushed, failures use SSE; HTTP status stays 200.
		_ = writer.event("run.failed", map[string]any{
			"run_id": run.ID, "session_id": sid,
			"error": map[string]string{"code": code, "message": "run did not complete; inspect the run before retrying"},
		})
	}
	for {
		select {
		case <-ctx.Done():
			_, code := agentFailure(ctx.Err())
			failStream(code)
			return
		case <-heartbeat.C:
			if err := writer.write(": heartbeat\n\n"); err != nil {
				writeFailure()
				return
			}
		case event := <-events:
			if err := writer.event(event.Type, event); err != nil {
				writeFailure()
				return
			}
		case outcome := <-outcomes:
			workerFinished = true
			if outcome.err == nil && ctx.Err() != nil {
				outcome.err = ctx.Err()
			}
			if outcome.err == nil && !validResult(outcome.result, run.ID) {
				outcome.err = agent.ErrFailed
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
				_ = writer.event("run.suspended", map[string]any{"run_id": run.ID, "session_id": sid, "run": run, "prompt": prompt})
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
			_ = writer.event("run.completed", map[string]any{"run_id": run.ID, "session_id": sid, "run": completed, "answer": outcome.result.Answer, "citations": citations})
			return
		}
	}
}
