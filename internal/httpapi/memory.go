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
func (s *server) createMemory(w http.ResponseWriter, r *http.Request) {
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
	out, err := s.db.CreateMemory(r.Context(), userID(r), r.PathValue("workspaceID"), domain.CreateMemory{Kind: in.Kind, Content: in.Content, Topic: in.Topic, Importance: importance, ExpiresAt: in.ExpiresAt, SourceSessionID: in.SourceSessionID})
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, out)
}
func (s *server) forgetMemory(w http.ResponseWriter, r *http.Request) {
	if err := s.db.ForgetMemory(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("memoryID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
