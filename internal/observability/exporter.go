package observability

import (
	"context"
	"errors"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// safeSpanExporter is an instance boundary, not a global OTel error handler.
// The SDK sends asynchronous exporter errors to otel.Handle; the HTTP exporter
// can include the collector URL and response body in those errors. Replace the
// error before it reaches any SDK processor, logger or other error consumer.
type safeSpanExporter struct{ inner sdktrace.SpanExporter }

var (
	errTraceExport   = errors.New("observability: trace export failed")
	errTraceShutdown = errors.New("observability: trace exporter shutdown failed")
)

func (e safeSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return sanitizeExporterError(e.inner.ExportSpans(ctx, spans), errTraceExport)
}

func (e safeSpanExporter) Shutdown(ctx context.Context) error {
	return sanitizeExporterError(e.inner.Shutdown(ctx), errTraceShutdown)
}

func sanitizeExporterError(err, fallback error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		return context.Canceled
	default:
		return fallback
	}
}

var _ sdktrace.SpanExporter = safeSpanExporter{}
