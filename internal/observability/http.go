package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type requestIDKey struct{}
type loggerKey struct{}

// RequestID returns the server-generated correlation ID. Client headers and
// traceparent never supply this value or establish an authenticated identity.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// Logger returns the request/stage logger. It adds correlation fields only;
// callers must continue to exclude contents and credentials from their fields.
func Logger(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}

func withLogger(ctx context.Context, fallback *slog.Logger) context.Context {
	logger, ok := ctx.Value(loggerKey{}).(*slog.Logger)
	if !ok {
		logger = fallback
		if id := RequestID(ctx); id != "" {
			logger = logger.With("request_id", id)
		}
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			logger = logger.With("trace_id", sc.TraceID().String())
		}
	}
	return context.WithValue(ctx, loggerKey{}, logger)
}

// Handler records the entire HTTP/SSE lifetime. Wrap the application's router; it must populate Request.Pattern
// with a registered static route template, never URL.Path.
// Unknown routes collapse to "unmatched". Bodies, headers and queries are never
// inspected. The middleware intentionally does not recover application panics.
func (t *Telemetry) Handler(next http.Handler) http.Handler {
	if t == nil {
		// Correlation remains available to the default HTTP configuration even
		// when application instrumentation has not been configured.
		t = &Telemetry{logger: slog.Default(), tracer: noop.NewTracerProvider().Tracer("magworkspace/internal/observability")}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		var randomID [16]byte
		_, _ = rand.Read(randomID[:])
		requestID := hex.EncodeToString(randomID[:])
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		// An untrusted upstream trace is a link, not a parent. This keeps local
		// IDs and sampling independent from client-controlled traceparent.
		remote := propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(r.Header))
		opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer), trace.WithNewRoot()}
		if sc := trace.SpanContextFromContext(remote); sc.IsValid() {
			// tracestate is arbitrary vendor text from an untrusted header; keep
			// only the fixed-width upstream correlation identifiers in the link.
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc.WithTraceState(trace.TraceState{})}))
		}
		tracer := t.tracer
		if t.closed.Load() {
			tracer = noop.NewTracerProvider().Tracer("magworkspace/internal/observability")
		}
		method := normalizeMethod(r.Method)
		ctx, span := tracer.Start(ctx, "HTTP "+method, opts...)
		ctx = withLogger(ctx, t.logger)
		request := r.WithContext(ctx)
		writer := &responseWriter{ResponseWriter: w}
		var wrapped http.ResponseWriter = writer
		if supportsFlush(w) {
			wrapped = &flushingResponseWriter{responseWriter: writer}
		}
		if t.registry != nil {
			t.httpActive.WithLabelValues(method).Inc()
		}
		returned := false
		defer func() {
			if t.registry != nil {
				t.httpActive.WithLabelValues(method).Dec()
			}
			route := routePattern(request.Pattern)
			status := writer.status
			if status == 0 {
				status = http.StatusOK
			}
			outcome := errorOutcome(writer.err, ctx.Err())
			if !returned {
				outcome = "error"
				if writer.status == 0 {
					status = http.StatusInternalServerError
				}
			} else if status >= 500 && outcome == "success" {
				outcome = "error"
			}
			duration := time.Since(started).Seconds()
			if t.registry != nil {
				t.httpRequests.WithLabelValues(method, route, statusLabel(status), outcome).Inc()
				t.httpDuration.WithLabelValues(method, route).Observe(duration)
				t.httpBytes.WithLabelValues(method, route).Observe(float64(writer.bytes))
			}
			span.SetName(method + " " + route)
			span.SetAttributes(attribute.String("http.request.method", method), attribute.String("http.route", route),
				attribute.Int("http.response.status_code", status), attribute.Int64("http.response.body.size", writer.bytes),
				attribute.String("operation.outcome", outcome), attribute.String("request.id", requestID))
			if outcome != "success" {
				span.SetStatus(codes.Error, outcome)
			}
			span.End()
			Logger(ctx).InfoContext(ctx, "http request", "method", method, "route", route,
				"status", status, "duration_seconds", duration, "response_bytes", writer.bytes, "outcome", outcome)
		}()
		next.ServeHTTP(wrapped, request)
		returned = true
	})
}

func normalizeMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return method
	default:
		return "OTHER"
	}
}

func routePattern(pattern string) string {
	// Request.Pattern is populated by the router, not by request bytes. It may
	// include a method or host, but neither is needed in this label.
	if _, path, ok := strings.Cut(pattern, " "); ok {
		pattern = path
	}
	if pattern == "" || !strings.HasPrefix(pattern, "/") || len(pattern) > 256 || strings.ContainsAny(pattern, "\r\n\x00?#") {
		return "unmatched"
	}
	return pattern
}

type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
	err    error
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	// Informational responses do not commit the final HTTP status.
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.ResponseWriter.WriteHeader(status)
	w.status = status
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	w.bytes += int64(n)
	if err != nil && w.err == nil {
		w.err = err
	}
	return n, err
}

type flushingResponseWriter struct{ *responseWriter }

func (w *flushingResponseWriter) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err != nil && w.err == nil {
		w.err = err
	}
	return err
}

func (w *flushingResponseWriter) Flush() { _ = w.FlushError() }

func supportsFlush(w http.ResponseWriter) bool {
	for range 32 {
		switch writer := w.(type) {
		case interface{ FlushError() error }:
			return true
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = writer.Unwrap()
		default:
			return false
		}
	}
	return false
}
