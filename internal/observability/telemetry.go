// Package observability owns local metrics, structured request logs and optional
// OTLP traces. It never installs a global OpenTelemetry provider or propagator.
package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config is explicit: tracing is disabled unless TracesEnabled is true. The
// endpoint is a complete OTLP/HTTP traces URL, normally ending in /v1/traces.
// No resource contents, user identities or secrets belong in these fields.
type Config struct {
	ServiceName      string
	Environment      string
	TracesEnabled    bool
	OTLPHTTPEndpoint string
	SampleRatio      float64
	ExportTimeout    time.Duration
	Logger           *slog.Logger
}

// Telemetry is owned by one application instance. Separate instances do not
// share metric registrations or alter another library's OpenTelemetry state.
// A nil *Telemetry is safe to use and behaves as disabled instrumentation.
type Telemetry struct {
	registry *prometheus.Registry
	tracer   trace.Tracer
	provider *sdktrace.TracerProvider
	logger   *slog.Logger
	closed   atomic.Bool
	shutdown sync.Once
	stopErr  error

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	httpBytes    *prometheus.HistogramVec
	httpActive   *prometheus.GaugeVec
	operations   *prometheus.CounterVec
	durations    *prometheus.HistogramVec
	items        *prometheus.CounterVec
}

// Init creates local metrics even with tracing disabled. Disabled tracing does
// not construct an exporter, inspect OTEL_* environment variables or send data.
func Init(ctx context.Context, cfg Config) (*Telemetry, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	t := &Telemetry{
		registry: prometheus.NewRegistry(),
		logger:   cfg.Logger,
		tracer:   noop.NewTracerProvider().Tracer("magworkspace/internal/observability"),
	}
	t.initMetrics()
	if !cfg.TracesEnabled {
		return t, nil
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "magworkspace"
	}
	if cfg.ExportTimeout == 0 {
		cfg.ExportTimeout = 5 * time.Second
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.OTLPHTTPEndpoint),
		otlptracehttp.WithHeaders(map[string]string{}),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
	)
	if err != nil {
		// Exporter errors may include an endpoint or HTTP credential. Return a
		// fixed diagnostic rather than forwarding an external error message.
		return nil, errors.New("observability: initialize OTLP exporter")
	}
	attrs := []attribute.KeyValue{attribute.String("service.name", cfg.ServiceName)}
	if cfg.Environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment.name", cfg.Environment))
	}
	t.provider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(attrs...)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithBatcher(safeSpanExporter{inner: exporter},
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(256),
			sdktrace.WithBatchTimeout(time.Second),
			sdktrace.WithExportTimeout(cfg.ExportTimeout),
		),
	)
	t.tracer = t.provider.Tracer("magworkspace/internal/observability")
	return t, nil
}

func validateConfig(cfg Config) error {
	endpoint, err := url.Parse(cfg.OTLPHTTPEndpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return errors.New("observability: OTLP HTTP endpoint must be an http(s) URL without credentials, query or fragment")
	}
	if math.IsNaN(cfg.SampleRatio) || math.IsInf(cfg.SampleRatio, 0) || cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return errors.New("observability: sample ratio must be between 0 and 1")
	}
	if cfg.ExportTimeout < 0 || cfg.ExportTimeout > time.Minute {
		return errors.New("observability: export timeout must be between 0 and 1 minute")
	}
	if len(cfg.ServiceName) > 128 || len(cfg.Environment) > 64 || strings.ContainsAny(cfg.ServiceName+cfg.Environment, "\r\n\x00") {
		return errors.New("observability: invalid service or environment name")
	}
	return nil
}

func (t *Telemetry) initMetrics() {
	t.httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "magworkspace_http_requests_total", Help: "Completed HTTP requests by static route and status.",
	}, []string{"method", "route", "status", "outcome"})
	t.httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "magworkspace_http_request_duration_seconds", Help: "HTTP request duration including the lifetime of SSE streams.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"method", "route"})
	t.httpBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "magworkspace_http_response_size_bytes", Help: "Bytes written to HTTP response bodies.",
		Buckets: prometheus.ExponentialBuckets(64, 4, 9),
	}, []string{"method", "route"})
	t.httpActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "magworkspace_http_requests_active", Help: "Currently active HTTP requests, including SSE streams.",
	}, []string{"method"})
	t.operations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "magworkspace_operations_total", Help: "Completed application operations by bounded operation and outcome.",
	}, []string{"operation", "outcome"})
	t.durations = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "magworkspace_operation_duration_seconds", Help: "Application operation duration.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"operation", "outcome"})
	t.items = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "magworkspace_observed_items_total", Help: "Application items processed, including candidates and returned results.",
	}, []string{"operation", "outcome"})
	t.registry.MustRegister(t.httpRequests, t.httpDuration, t.httpBytes, t.httpActive, t.operations, t.durations, t.items,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// MetricsHandler exposes only this instance's registry. The caller owns its
// network listener and access policy; do not expose operational data publicly.
func (t *Telemetry) MetricsHandler() http.Handler {
	if t == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(t.registry, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}

// Shutdown drains trace exports at most once. Metrics remain available locally.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.shutdown.Do(func() {
		t.closed.Store(true)
		if t.provider != nil {
			if err := t.provider.Shutdown(ctx); err != nil {
				t.stopErr = errors.New("observability: trace shutdown failed")
			}
		}
	})
	return t.stopErr
}

// Start observes one application stage. Operation names are selected from a
// closed set; unknown names are collapsed to "other" so resource data cannot
// accidentally become telemetry labels. finish is concurrency-safe and once.
func (t *Telemetry) Start(ctx context.Context, operation string) (context.Context, func(error)) {
	if t == nil {
		return ctx, func(error) {}
	}
	operation = normalizeOperation(operation)
	started := time.Now()
	tracer := t.tracer
	if t.closed.Load() {
		tracer = noop.NewTracerProvider().Tracer("magworkspace/internal/observability")
	}
	ctx, span := tracer.Start(ctx, operation)
	ctx = withLogger(ctx, t.logger)
	var once sync.Once
	return ctx, func(err error) {
		once.Do(func() {
			outcome := errorOutcome(err, ctx.Err())
			t.operations.WithLabelValues(operation, outcome).Inc()
			t.durations.WithLabelValues(operation, outcome).Observe(time.Since(started).Seconds())
			if outcome != "success" {
				span.SetStatus(codes.Error, outcome)
				span.SetAttributes(attribute.String("error.type", outcome))
			}
			span.SetAttributes(attribute.String("operation.outcome", outcome))
			span.End()
		})
	}
}

// ObserveCount counts nonnegative items using bounded label values. It does not
// start a span, export document contents or retain an individual observation.
func (t *Telemetry) ObserveCount(operation, outcome string, n int) {
	if t != nil && n > 0 {
		t.items.WithLabelValues(normalizeOperation(operation), normalizeOutcome(outcome)).Add(float64(n))
	}
}

func normalizeOperation(operation string) string {
	switch operation {
	case "memory.dispatch", "memory.capture", "memory.extract", "redis.dispatch", "redis.events", "retrieval.search", "retrieval.lexical", "retrieval.vector", "retrieval.merge", "retrieval.filter", "retrieval.hydrate", "retrieval.embed", "retrieval.scope", "retrieval.resolve",
		"index.process", "index.parse", "index.chunk", "index.embed", "index.write", "index.publish", "agent.run", "agent.json", "agent.sse", "run.finish",
		"tool.create_document", "tool.update_document", "tool.search_knowledge", "tool.read_document", "tool.list_documents", "tool.search_conversations", "tool.ask_user", "tool.restore_evidence", "tool.resolve_citation_aliases",
		"prompt.suspend", "prompt.resume", "prompt.cancel", "object.cleanup", "object.get", "object.put", "object.delete":
		return operation
	default:
		return "other"
	}
}

func normalizeOutcome(outcome string) string {
	switch outcome {
	case "success", "error", "canceled", "deadline_exceeded", "skipped", "rejected", "empty", "suspended":
		return outcome
	default:
		return "other"
	}
}

func errorOutcome(err, contextErr error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
		return "canceled"
	}
	if err != nil {
		return "error"
	}
	return "success"
}

func statusLabel(status int) string {
	if status < 100 || status > 599 {
		return "other"
	}
	return fmt.Sprintf("%d", status)
}
