package httpapi

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Release mode is process-wide and set once during package initialization.
// Request logging/recovery belong to the existing redacted HTTP middleware.
func init() { gin.SetMode(gin.ReleaseMode) }

func newRouter() *gin.Engine {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = false
	router.UseRawPath = true
	router.UnescapePathValues = true
	// Forwarded headers must never redefine an authentication/rate-limit peer.
	_ = router.SetTrustedProxies(nil)
	return router
}

// registerHTTP mounts each endpoint in Gin's tree. Existing handlers retain
// net/http contracts so business validation and asynchronous work cannot retain
// a pooled gin.Context. GET also supports HEAD, matching the old HTTP API.
func registerHTTP(router *gin.Engine, method, path string, next http.HandlerFunc) {
	handler := func(c *gin.Context) {
		parts := strings.Split(c.FullPath(), "/")
		for i, part := range parts {
			if strings.HasPrefix(part, ":") {
				parts[i] = "{" + part[1:] + "}"
			}
		}
		c.Request.Pattern = method + " " + strings.Join(parts, "/")
		for _, param := range c.Params {
			c.Request.SetPathValue(param.Key, param.Value)
		}
		next(ginHTTPWriter(c.Writer), c.Request)
	}
	router.Handle(method, path, handler)
	if method == http.MethodGet {
		router.HEAD(path, handler)
	}
}

// Gin's Flush method cannot return transport errors and always advertises
// http.Flusher. Expose only the underlying capabilities to SSE; commit through
// Gin before flushing so its status/size bookkeeping stays consistent.
type ginWriter struct {
	writer gin.ResponseWriter
	raw    http.ResponseWriter
}

func (w *ginWriter) Header() http.Header            { return w.writer.Header() }
func (w *ginWriter) WriteHeader(status int)         { w.writer.WriteHeader(status) }
func (w *ginWriter) Write(data []byte) (int, error) { return w.writer.Write(data) }
func (w *ginWriter) Unwrap() http.ResponseWriter    { return w.raw }

type ginFlushingWriter struct{ *ginWriter }

func (w *ginFlushingWriter) FlushError() error {
	w.writer.WriteHeaderNow()
	return http.NewResponseController(w.raw).Flush()
}
func ginHTTPWriter(writer gin.ResponseWriter) http.ResponseWriter {
	raw := writer.(interface{ Unwrap() http.ResponseWriter }).Unwrap()
	bridge := &ginWriter{writer: writer, raw: raw}
	if canFlush(raw) {
		return &ginFlushingWriter{bridge}
	}
	return bridge
}
