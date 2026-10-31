package httpapi

import (
	"magworkspace/internal/domain"
	"net/http"
)

func (s *server) memoryCaptureStatus(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 0 {
		fail(w, domain.ErrInvalid)
		return
	}
	out, err := s.db.MemoryCaptureStatus(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("sessionID"), r.PathValue("runID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, http.StatusOK, out)
}
