package httpapi

import (
	"magworkspace/internal/domain"
	"net/http"
)

func (s *server) memories(w http.ResponseWriter, r *http.Request) {
	for key, values := range r.URL.Query() {
		if key != "status" || len(values) != 1 {
			problem(w, 400, "invalid_request", "only status is supported")
			return
		}
	}
	out, err := s.db.Memories(r.Context(), userID(r), r.PathValue("workspaceID"), r.URL.Query().Get("status"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, out)
}
func (s *server) createMemory(w http.ResponseWriter, r *http.Request) { s.saveMemory(w, r, false) }
func (s *server) updateMemory(w http.ResponseWriter, r *http.Request) { s.saveMemory(w, r, true) }
func (s *server) saveMemory(w http.ResponseWriter, r *http.Request, update bool) {
	var in struct {
		Kind            string   `json:"kind"`
		Content         string   `json:"content"`
		Topic           string   `json:"topic"`
		Importance      *int     `json:"importance"`
		ExpiresAt       *float64 `json:"expires_at"`
		SourceSessionID string   `json:"source_session_id"`
	}
	if !decode(w, r, &in) {
		return
	}
	importance := 3
	if in.Importance != nil {
		importance = *in.Importance
	}
	input := domain.CreateMemory{Kind: in.Kind, Content: in.Content, Topic: in.Topic, Importance: importance, ExpiresAt: in.ExpiresAt, SourceSessionID: in.SourceSessionID}
	var out domain.Memory
	var err error
	if update {
		out, err = s.db.UpdateMemory(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("memoryID"), input)
	} else {
		out, err = s.db.CreateMemory(r.Context(), userID(r), r.PathValue("workspaceID"), input)
	}

	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusCreated
	if update {
		status = http.StatusOK
	}
	respond(w, status, out)
}
func (s *server) forgetMemory(w http.ResponseWriter, r *http.Request) {
	if err := s.db.ForgetMemory(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("memoryID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) memorySettings(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Enabled *bool `json:"enabled"`
	}
	if r.Method == http.MethodPut {
		if !decode(w, r, &in) {
			return
		}
		if in.Enabled == nil {
			fail(w, domain.ErrInvalid)
			return
		}
	}
	if len(r.URL.Query()) != 0 {
		fail(w, domain.ErrInvalid)
		return
	}
	out, err := s.db.MemorySettings(r.Context(), userID(r), r.PathValue("workspaceID"), in.Enabled)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, http.StatusOK, out)
}
func (s *server) exportMemories(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 0 {
		fail(w, domain.ErrInvalid)
		return
	}
	out, err := s.db.ExportMemories(r.Context(), userID(r), r.PathValue("workspaceID"))
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="personal-memories.json"`)
	respond(w, http.StatusOK, out)
}
func (s *server) clearMemories(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Confirm *bool `json:"confirm"`
	}
	if !decode(w, r, &in) {
		return
	}
	if len(r.URL.Query()) != 0 || in.Confirm == nil || !*in.Confirm {
		fail(w, domain.ErrInvalid)
		return
	}
	if err := s.db.ClearMemories(r.Context(), userID(r), r.PathValue("workspaceID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
