package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"magworkspace/internal/domain"
	"magworkspace/internal/redisstate"
)

// GET is read-only: reconnecting never claims a run or executes the Agent.
func (s *server) runEvents(w http.ResponseWriter, r *http.Request) {
	uid, wid, sid, rid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID")
	run, generation, err := s.db.RunEventState(r.Context(), uid, wid, sid, rid)
	if err != nil {
		fail(w, err)
		return
	}
	if s.cfg.Redis == nil || !canFlush(w) {
		problem(w, 503, "events_unavailable", "event replay is unavailable; inspect the run")
		return
	}
	var after uint64
	cursor := r.Header.Get("Last-Event-ID")
	if cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 {
			fail(w, domain.ErrInvalid)
			return
		}
		gen, e := strconv.ParseUint(parts[0], 10, 64)
		seq, e2 := strconv.ParseUint(parts[1], 10, 64)
		if e != nil || e2 != nil {
			fail(w, domain.ErrInvalid)
			return
		}
		if gen != generation {
			problem(w, 409, "event_generation_changed", "execution changed; inspect the run")
			return
		}
		after = seq
	}
	writer := &eventWriter{w: w, controller: http.NewResponseController(w), timeout: s.cfg.SSEWriteTimeout}
	started := false
	deadline := time.NewTimer(s.cfg.RunTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	heartbeat := time.Now()
	for {
		events, e := s.cfg.Redis.Events(r.Context(), rid, generation, after)
		if e != nil {
			if started {
				_ = writer.event("stream.unavailable", map[string]string{"code": "events_unavailable"})
			} else if errors.Is(e, redisstate.ErrHistory) {
				problem(w, 409, "event_history_expired", "event history is incomplete; inspect the run")
			} else {
				problem(w, 503, "events_unavailable", "event replay is unavailable; inspect the run")
			}
			return
		}
		// Recheck after Redis I/O; no cached identity, role or source grants access.
		current, gen, e := s.db.RunEventState(r.Context(), uid, wid, sid, rid)
		if e != nil || gen != generation {
			if started {
				_ = writer.event("stream.unavailable", map[string]string{"code": "access_or_execution_changed"})
			} else if e != nil {
				fail(w, e)
			} else {
				fail(w, domain.ErrConflict)
			}
			return
		}
		run = current
		if !started {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store")
			w.Header().Set("X-Accel-Buffering", "no")
			started = true
		}
		for _, event := range events {
			if writer.write(fmt.Sprintf("id: %d:%d\nevent: %s\ndata: %s\n\n", generation, event.Sequence, event.Name, event.Data)) != nil {
				return
			}
			after = event.Sequence
			if event.Name == "run.completed" || event.Name == "run.failed" || event.Name == "run.suspended" {
				return
			}
		}
		if len(events) == 128 {
			continue
		}
		// Terminal DB state can precede cache publication. A snapshot is explicit;
		// it never manufactures a cached answer or declares missing deltas complete.
		if run.Status != "running" && len(events) == 0 {
			_ = writer.event("stream.closed", map[string]any{"run": run})
			return
		}
		if time.Since(heartbeat) >= s.cfg.SSEHeartbeat {
			if writer.write(": heartbeat\n\n") != nil {
				return
			}
			heartbeat = time.Now()
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}
