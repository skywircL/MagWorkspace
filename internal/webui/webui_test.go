package webui

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedClient(t *testing.T) {
	for _, path := range []string{"/", "/app.js", "/core.js", "/style.css"} {
		r := httptest.NewRecorder()
		Handler().ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		if r.Code != 200 || r.Body.Len() == 0 {
			t.Fatalf("%s: %d", path, r.Code)
		}
		if !strings.Contains(r.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("missing browser protection")
		}
	}
	for _, path := range []string{"/.env", "/assets/../webui.go", "/api/v1/missing", "/missing.js"} {
		r := httptest.NewRecorder()
		Handler().ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		if r.Code != 404 {
			t.Fatalf("unexpected fallback for %s", path)
		}
	}
	r := httptest.NewRecorder()
	Handler().ServeHTTP(r, httptest.NewRequest("POST", "/", nil))
	if r.Code != 405 {
		t.Fatal("unexpected POST")
	}
}
