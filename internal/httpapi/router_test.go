package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestGinRouteParametersAndStaticTelemetry(t *testing.T) {
	router := newRouter()
	registerHTTP(router, "GET", "/workspaces/:workspaceID/documents/:documentID", func(w http.ResponseWriter, r *http.Request) {
		if r.Pattern != "GET /workspaces/{workspaceID}/documents/{documentID}" {
			t.Errorf("non-template pattern %q", r.Pattern)
		}
		if r.PathValue("workspaceID") != "workspace" || r.PathValue("documentID") != "document" {
			t.Error("missing decoded parameters")
		}
		w.WriteHeader(204)
	})
	for _, method := range []string{"GET", "HEAD"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(method, "/workspaces/workspace/documents/%64ocument", nil))
		if w.Code != 204 {
			t.Fatal(method, w.Code)
		}
	}
	if routes := router.Routes(); len(routes) != 2 {
		t.Fatal("not registered in Gin", routes)
	}
	router.GET("/peer", func(c *gin.Context) {
		if c.ClientIP() != "192.0.2.1" {
			t.Errorf("trusted spoofed peer %q", c.ClientIP())
		}
	})
	r := httptest.NewRequest("GET", "/peer", nil)
	r.Header.Set("X-Forwarded-For", "203.0.113.1")
	router.ServeHTTP(httptest.NewRecorder(), r)
}

// Only the real writer's streaming support may be exposed through Gin.
type plainGinTestWriter struct{ header http.Header }

func (w *plainGinTestWriter) Header() http.Header         { return w.header }
func (w *plainGinTestWriter) WriteHeader(int)             {}
func (w *plainGinTestWriter) Write(b []byte) (int, error) { return len(b), nil }
func TestGinSSEWriterCapabilitiesAndDeadlines(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		router := newRouter()
		registerHTTP(router, "GET", "/stream", func(w http.ResponseWriter, r *http.Request) {
			if canFlush(w) {
				t.Error("Gin advertised unsupported flushing")
			}
		})
		router.ServeHTTP(&plainGinTestWriter{header: make(http.Header)}, httptest.NewRequest("GET", "/stream", nil))
	})
	for _, mode := range []string{"success", "flush_error", "write_error"} {
		t.Run(mode, func(t *testing.T) {
			raw := &controlledWriter{header: make(http.Header)}
			transport := errors.New("transport failed")
			if mode == "flush_error" {
				raw.flushErr = transport
			}
			if mode == "write_error" {
				raw.writeErr = transport
			}
			router := newRouter()
			registerHTTP(router, "GET", "/stream", func(w http.ResponseWriter, r *http.Request) {
				writer := &eventWriter{w: w, controller: http.NewResponseController(w), timeout: time.Second}
				err := writer.event("message.delta", map[string]string{"delta": "test"})
				if mode == "success" && err != nil {
					t.Fatal(err)
				}
				if mode != "success" && !errors.Is(err, transport) {
					t.Fatal("transport error swallowed", err)
				}
			})
			router.ServeHTTP(raw, httptest.NewRequest("GET", "/stream", nil))
			if len(raw.deadlines) != 2 || raw.deadlines[0].IsZero() || !raw.deadlines[1].IsZero() {
				t.Fatal("deadlines not preserved", raw.deadlines)
			}
		})
	}
}
func TestGinPublicBoundary(t *testing.T) {
	handler := New(nil, nil, Config{})
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/healthz", 200}, {"HEAD", "/healthz", 200},
		{"GET", "/", 200}, {"HEAD", "/core.js", 200}, {"POST", "/", 405},
		{"GET", "/missing/private-value", 404}, {"GET", "/api/v1/unknown", 404}, {"GET", "/api/v1/auth/register", 404},
		{"GET", "/api/v1/workspaces", 401}, {"GET", "/api/v1/workspaces/private-id/documents/private-document", 401},
		{"POST", "/api/v1/workspaces/private-id/sessions/private-session/messages", 401},
		{"GET", "/api/v1/workspaces/", 404}, {"POST", "/api/v1/auth/register/", 404},
		{"GET", "/api//v1/workspaces", 404}, {"GET", "/api/v1/../v1/workspaces", 404},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status {
				t.Fatalf("status %d body %s", w.Code, w.Body)
			}
			if w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("X-Request-ID") == "" {
				t.Fatal("middleware lost")
			}
			if tc.status >= 400 && strings.Contains(w.Body.String(), "<!doctype") {
				t.Fatal("API fell back to HTML success")
			}
			if w.Header().Get("Location") != "" {
				t.Fatal("unexpected redirect")
			}
		})
	}
}
