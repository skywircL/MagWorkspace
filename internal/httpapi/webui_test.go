package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPWebUIAndAPIBoundary(t *testing.T) {
	handler := New(nil, nil, Config{})
	for _, tc := range []struct {
		path    string
		status  int
		content string
	}{
		{"/api/v1/config", 200, `"max_file_size_bytes":52428800`},
		{"/", 200, "MagWorkspace"}, {"/app.js", 200, "consumeEvents"},
		{"/api/v1/workspaces", 401, "unauthorized"}, {"/api/v1/not-found", 404, "404"}, {"/.env", 404, "404"},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.content) {
			t.Fatalf("%s: status %d", tc.path, w.Code)
		}
	}
}
