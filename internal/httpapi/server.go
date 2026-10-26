// Package httpapi exposes the authenticated workspace and private session API.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"magworkspace/internal/agent"
	"magworkspace/internal/auth"
	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
	"magworkspace/internal/observability"
	"magworkspace/internal/redisstate"
	"magworkspace/internal/store"
	"magworkspace/internal/stream"
	"magworkspace/internal/webui"
)

type Config struct {
	Redis             *redisstate.Client
	Streams           stream.Manager
	BackgroundContext context.Context
	AllowRegistration bool
	AccessTTL         time.Duration
	RefreshTTL        time.Duration
	RunTimeout        time.Duration
	SSEHeartbeat      time.Duration
	SSEWriteTimeout   time.Duration
	Indexing          *indexing.Service
	Telemetry         *observability.Telemetry
}

type server struct {
	runtime   *Handler
	db        *store.Store
	agent     agent.Client
	cfg       Config
	loginMu   sync.Mutex
	attempts  map[string]attempt
	authSlots chan struct{}
}
type attempt struct {
	count int
	until time.Time
}
type identityKey struct{}

func New(db *store.Store, client agent.Client, cfg Config) *Handler {
	if cfg.AccessTTL <= 0 {
		cfg.AccessTTL = time.Hour
	}
	if cfg.RefreshTTL <= 0 {
		cfg.RefreshTTL = 30 * 24 * time.Hour
	}
	if cfg.RunTimeout <= 0 {
		cfg.RunTimeout = 120 * time.Second
	}
	if cfg.SSEHeartbeat <= 0 {
		cfg.SSEHeartbeat = 15 * time.Second
	}
	if cfg.SSEWriteTimeout <= 0 {
		cfg.SSEWriteTimeout = 5 * time.Second
	}
	if cfg.Streams == nil {
		cfg.Streams = stream.NewMemory(stream.DefaultTTL)
	}
	if cfg.BackgroundContext == nil {
		cfg.BackgroundContext = context.Background()
	}
	background, stop := context.WithCancel(cfg.BackgroundContext)
	runtime := &Handler{ctx: background, cancel: stop}
	s := &server{runtime: runtime, db: db, agent: observeAgent(client, cfg.Telemetry), cfg: cfg, attempts: make(map[string]attempt), authSlots: make(chan struct{}, 8)}
	router := newRouter()
	route := func(method, path string, handler http.HandlerFunc) { registerHTTP(router, method, path, handler) }
	ui := webui.Handler()
	for _, path := range []string{"/", "/app.js", "/core.js", "/style.css"} {
		route(http.MethodGet, path, ui.ServeHTTP)
	}
	router.NoRoute(func(c *gin.Context) { ui.ServeHTTP(ginHTTPWriter(c.Writer), c.Request) })
	route("GET", "/api/v1/config", func(w http.ResponseWriter, r *http.Request) {
		maximum := s.db.UploadLimitBytes()
		respond(w, 200, map[string]int{"max_file_size_mb": maximum / (1 << 20), "max_file_size_bytes": maximum})
	})
	route("GET", "/healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	route("GET", "/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			problem(w, 503, "database_unavailable", "database unavailable")
			return
		}
		state := map[string]string{"status": "ready", "redis": "disabled"}
		if s.cfg.Redis != nil {
			state["redis"] = "ready"
			if s.cfg.Redis.Ping(ctx) != nil {
				state["redis"] = "degraded"
			}
		}
		respond(w, 200, state)
	})
	route("POST", "/api/v1/auth/browser/register", s.browserAuth(s.limitAuth(s.register)))
	route("POST", "/api/v1/auth/browser/login", s.browserAuth(s.limitAuth(s.login)))
	route("POST", "/api/v1/auth/browser/refresh", s.browserAuth(s.limitAuth(s.browserRefresh)))
	route("POST", "/api/v1/auth/browser/logout", s.browserAuth(s.limitAuth(s.browserLogout)))
	route("POST", "/api/v1/auth/register", s.limitAuth(s.register))
	route("POST", "/api/v1/auth/login", s.limitAuth(s.login))
	route("POST", "/api/v1/auth/refresh", s.limitAuth(s.refresh))
	route("POST", "/api/v1/auth/logout", s.protected(s.logout))
	route("GET", "/api/v1/auth/me", s.protected(s.me))
	route("GET", "/api/v1/workspaces", s.protected(s.workspaces))
	route("POST", "/api/v1/workspaces", s.protected(s.createWorkspace))
	route("GET", "/api/v1/workspaces/:workspaceID", s.protected(s.workspace))
	route("PATCH", "/api/v1/workspaces/:workspaceID", s.protected(s.updateWorkspace))
	route("DELETE", "/api/v1/workspaces/:workspaceID", s.protected(s.deleteWorkspace))
	route("GET", "/api/v1/workspaces/:workspaceID/knowledge-bases", s.protected(s.knowledgeBases))
	route("POST", "/api/v1/workspaces/:workspaceID/knowledge-bases", s.protected(s.createKnowledgeBase))
	route("GET", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID", s.protected(s.knowledgeBase))
	route("PATCH", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID", s.protected(s.updateKnowledgeBase))
	route("DELETE", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID", s.protected(s.deleteKnowledgeBase))
	route("GET", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID/documents", s.protected(s.knowledgeBaseDocuments))
	route("POST", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID/documents", s.protected(s.createKnowledgeBaseDocument))
	route("POST", "/api/v1/workspaces/:workspaceID/knowledge-bases/:knowledgeBaseID/search", s.protected(s.searchDocuments))
	route("GET", "/api/v1/workspaces/:workspaceID/members", s.protected(s.members))
	route("POST", "/api/v1/workspaces/:workspaceID/members", s.protected(s.addMember))
	route("PATCH", "/api/v1/workspaces/:workspaceID/members/:userID", s.protected(s.updateMember))
	route("DELETE", "/api/v1/workspaces/:workspaceID/members/:userID", s.protected(s.deleteMember))
	route("GET", "/api/v1/workspaces/:workspaceID/documents", s.protected(s.documents))
	route("POST", "/api/v1/workspaces/:workspaceID/documents", s.protected(s.createDocument))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID", s.protected(s.document))
	route("PATCH", "/api/v1/workspaces/:workspaceID/documents/:documentID", s.protected(s.updateDocument))
	route("PUT", "/api/v1/workspaces/:workspaceID/documents/:documentID/content", s.protected(s.updateDocumentContent))
	route("DELETE", "/api/v1/workspaces/:workspaceID/documents/:documentID", s.protected(s.deleteDocument))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions", s.protected(s.documentVersions))
	route("POST", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions", s.protected(s.appendDocumentVersion))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID", s.protected(s.documentVersion))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID/content", s.protected(s.documentContent))
	route("POST", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID/index", s.protected(s.enqueueDocumentIndex))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID/index", s.protected(s.documentIndex))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID/indexes/:indexID/chunks", s.protected(s.indexChunks))
	route("GET", "/api/v1/workspaces/:workspaceID/documents/:documentID/versions/:versionID/indexes/:indexID/chunks/:chunkID", s.protected(s.indexChunk))
	route("POST", "/api/v1/workspaces/:workspaceID/search", s.protected(s.searchDocuments))
	route("GET", "/api/v1/workspaces/:workspaceID/memories", s.protected(s.memories))
	route("POST", "/api/v1/workspaces/:workspaceID/memories", s.protected(s.createMemory))
	route("PUT", "/api/v1/workspaces/:workspaceID/memories/:memoryID", s.protected(s.updateMemory))
	route("GET", "/api/v1/workspaces/:workspaceID/memory-settings", s.protected(s.memorySettings))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/memory", s.protected(s.memoryCaptureStatus))
	route("PUT", "/api/v1/workspaces/:workspaceID/memory-settings", s.protected(s.memorySettings))
	route("GET", "/api/v1/workspaces/:workspaceID/memories/export", s.protected(s.exportMemories))
	route("DELETE", "/api/v1/workspaces/:workspaceID/memories", s.protected(s.clearMemories))
	route("DELETE", "/api/v1/workspaces/:workspaceID/memories/:memoryID", s.protected(s.forgetMemory))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions", s.protected(s.sessions))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions", s.protected(s.createSession))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID", s.protected(s.session))
	route("PATCH", "/api/v1/workspaces/:workspaceID/sessions/:sessionID", s.protected(s.updateSession))
	route("DELETE", "/api/v1/workspaces/:workspaceID/sessions/:sessionID", s.protected(s.deleteSession))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/messages", s.protected(s.messages))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/messages", s.protected(s.chat))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID", s.protected(s.run))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/events", s.protected(s.runEvents))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/stop", s.protected(s.stopRun))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/recover", s.protected(s.recoverRun))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/prompt", s.protected(s.runPrompt))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/prompts/:promptID", s.protected(s.runPrompt))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/prompts/:promptID/answer", s.protected(s.answerPrompt))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/prompts/:promptID/approve", s.protected(s.approvePrompt))
	route("POST", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/prompts/:promptID/cancel", s.protected(s.cancelPrompt))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/citations", s.protected(s.runCitations))
	route("GET", "/api/v1/workspaces/:workspaceID/sessions/:sessionID/runs/:runID/evidence/:evidenceID", s.protected(s.runEvidence))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		defer func() {
			if v := recover(); v != nil {
				observability.Logger(r.Context()).Error("request panic", "request_id", w.Header().Get("X-Request-ID"))
				if strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
					// Do not append a JSON error to an already-started event stream.
					panic(http.ErrAbortHandler)
				}
				problem(w, 500, "internal_error", "internal server error")
			}
		}()
		router.ServeHTTP(w, r)
	})
	runtime.Handler = cfg.Telemetry.Handler(handler)
	return runtime
}

func (s *server) protected(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(value, "Bearer ")
		if !ok || !auth.ValidToken(token, "at") {
			problem(w, 401, "unauthorized", "valid access token required")
			return
		}
		a, err := s.db.Authenticate(r.Context(), auth.HashToken(token))
		if err != nil {
			fail(w, err)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, a)))
	}
}
func identity(r *http.Request) domain.AuthSession {
	return r.Context().Value(identityKey{}).(domain.AuthSession)
}
func userID(r *http.Request) string { return identity(r).UserID }

// Bound unauthenticated password work. Proxy headers never define the rate-limit identity.
func (s *server) limitAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		now := time.Now()
		s.loginMu.Lock()
		for ip, a := range s.attempts {
			if !a.until.After(now) {
				delete(s.attempts, ip)
			}
		}
		a, exists := s.attempts[host]
		if a.count >= 60 || (!exists && len(s.attempts) >= 4096) {
			s.loginMu.Unlock()
			w.Header().Set("Retry-After", "60")
			problem(w, 429, "rate_limited", "try again later")
			return
		}
		if !exists {
			a.until = now.Add(time.Minute)
		}
		a.count++
		s.attempts[host] = a
		s.loginMu.Unlock()
		select {
		case s.authSlots <- struct{}{}:
			defer func() { <-s.authSlots }()
		default:
			problem(w, 429, "rate_limited", "try again later")
			return
		}
		next(w, r)
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err = dec.Decode(dst); err != nil {
		problem(w, 400, "invalid_request", "invalid JSON or unknown field")
		return false
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		problem(w, 400, "invalid_request", "one JSON object required")
		return false
	}
	return true
}
func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
}
func page(w http.ResponseWriter, data any, total, p, n int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data, "total": total, "page": p, "page_size": n})
}
func problem(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": map[string]string{"code": code, "message": message}})
}
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		problem(w, 400, "invalid_request", "invalid request values")
	case errors.Is(err, domain.ErrUnauthorized):
		problem(w, 401, "unauthorized", "invalid or expired credential")
	case errors.Is(err, domain.ErrForbidden):
		problem(w, 403, "forbidden", "insufficient workspace role")
	case errors.Is(err, domain.ErrNotFound):
		problem(w, 404, "not_found", "resource not found")
	case errors.Is(err, domain.ErrConflict):
		problem(w, 409, "conflict", "resource already exists, is busy, or operation violates ownership rules")
	case errors.Is(err, blob.ErrUnavailable):
		problem(w, 503, "object_storage_unavailable", "document storage unavailable")
	case errors.Is(err, blob.ErrIntegrity):
		problem(w, 503, "object_integrity_failed", "document integrity verification failed")
	case errors.Is(err, context.DeadlineExceeded):
		problem(w, 504, "timeout", "operation timed out")
	case errors.Is(err, context.Canceled):
		problem(w, 408, "canceled", "request canceled")
	default:
		slog.Error("request failed", "request_id", w.Header().Get("X-Request-ID"))
		problem(w, 500, "internal_error", "internal server error")
	}
}
func pagination(w http.ResponseWriter, r *http.Request) (p, n int, ok bool) {
	p, n = 1, 50
	for key, dst := range map[string]*int{"page": &p, "page_size": &n} {
		if raw := r.URL.Query().Get(key); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil {
				problem(w, 400, "invalid_request", "invalid pagination")
				return 0, 0, false
			}
			*dst = v
		}
	}
	if p < 1 || p > 10000 || n < 1 || n > 100 {
		problem(w, 400, "invalid_request", "page must be 1..10000; page_size must be 1..100")
		return 0, 0, false
	}
	return p, n, true
}
