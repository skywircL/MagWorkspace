package httpapi

import (
	"net/http"
	"strings"
)

type workspaceInput struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (s *server) workspaces(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Workspaces(r.Context(), userID(r))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) workspace(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Workspace(r.Context(), userID(r), r.PathValue("workspaceID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) createWorkspace(w http.ResponseWriter, r *http.Request) {
	var in workspaceInput
	if !decode(w, r, &in) {
		return
	}
	data, err := s.db.CreateWorkspace(r.Context(), userID(r), strings.TrimSpace(in.Name), in.Description)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}
func (s *server) updateWorkspace(w http.ResponseWriter, r *http.Request) {
	// Merge omitted fields only after locking the workspace in the storage layer.
	var in struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.Name == nil && in.Description == nil {
		problem(w, 400, "invalid_request", "name or description required")
		return
	}
	if in.Name != nil {
		value := strings.TrimSpace(*in.Name)
		in.Name = &value
	}
	data, err := s.db.PatchWorkspace(r.Context(), userID(r), r.PathValue("workspaceID"), in.Name, in.Description)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) deleteWorkspace(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteWorkspace(r.Context(), userID(r), r.PathValue("workspaceID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *server) members(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Members(r.Context(), userID(r), r.PathValue("workspaceID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}
func (s *server) addMember(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if !decode(w, r, &in) {
		return
	}
	data, err := s.db.AddMember(r.Context(), userID(r), r.PathValue("workspaceID"), in.Email, in.Role)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}
func (s *server) updateMember(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Role string `json:"role"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.db.UpdateMember(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("userID"), in.Role); err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, map[string]string{"user_id": r.PathValue("userID"), "role": in.Role})
}
func (s *server) deleteMember(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteMember(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("userID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
