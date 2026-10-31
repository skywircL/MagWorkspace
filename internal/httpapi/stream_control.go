package httpapi

import (
	"magworkspace/internal/domain"
	"magworkspace/internal/stream"
	"net/http"
)

// Stop targets the authorized current generation, never a caller supplied identity.
// The execution owner polls this generation-specific stop signal, including across replicas.
func (s *server) stopRun(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	uid, wid, sid, rid := userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID")
	run, generation, err := s.db.RunEventState(r.Context(), uid, wid, sid, rid)
	if err != nil {
		fail(w, err)
		return
	}
	if run.Status != "running" {
		fail(w, domain.ErrConflict)
		return
	}
	live, err := s.cfg.Streams.GetLiveRun(r.Context(), sid)
	if err != nil {
		problem(w, 503, "events_unavailable", "stream storage unavailable; inspect the run")
		return
	}
	if live.RunID != rid || live.Generation != generation {
		fail(w, domain.ErrConflict)
		return
	}
	err = s.cfg.Streams.RequestStop(r.Context(), stream.Key{SessionID: sid, RunID: rid, Generation: generation})
	if err != nil {
		problem(w, 503, "events_unavailable", "stop could not be requested; inspect the run")
		return
	}
	respond(w, http.StatusAccepted, map[string]string{"run_id": rid, "status": "stop_requested"})
}
