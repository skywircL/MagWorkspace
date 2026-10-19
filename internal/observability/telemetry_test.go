package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	collecttrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func newTestTelemetry(t *testing.T) (*Telemetry, *bytes.Buffer, *tracetest.InMemoryExporter) {
	t.Helper()
	var logs bytes.Buffer
	telemetry, err := Init(context.Background(), Config{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	exporter := tracetest.NewInMemoryExporter()
	telemetry.provider = sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	telemetry.tracer = telemetry.provider.Tracer("observability-test")
	t.Cleanup(func() {
		if err := telemetry.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return telemetry, &logs, exporter
}

func scrape(t *testing.T, telemetry *Telemetry) string {
	t.Helper()
	w := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("metrics status: %d", w.Code)
	}
	return w.Body.String()
}

func TestDisabledTelemetryDoesNotExportOrReadOTELEnvironment(t *testing.T) {
	var calls atomic.Int64
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=secret-never-export")
	globalProvider := otel.GetTracerProvider()
	globalPropagator := otel.GetTextMapPropagator()
	telemetry, err := Init(context.Background(), Config{OTLPHTTPEndpoint: "not-a-valid-endpoint", SampleRatio: math.NaN()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, finish := telemetry.Start(context.Background(), "retrieval.search")
	finish(errors.New("secret body should stay private"))
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("disabled instrumentation supplied an exporting span")
	}
	if err := telemetry.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 || telemetry.provider != nil || globalProvider != otel.GetTracerProvider() || globalPropagator != otel.GetTextMapPropagator() {
		t.Fatal("disabled telemetry exported data or changed global OpenTelemetry state")
	}
	if !strings.Contains(scrape(t, telemetry), `magworkspace_operations_total{operation="retrieval.search",outcome="error"} 1`) {
		t.Fatal("disabled traces should retain local metrics")
	}
}

func TestTelemetryConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Config)
	}{
		{"empty endpoint", func(c *Config) { c.OTLPHTTPEndpoint = "" }},
		{"credentials", func(c *Config) { c.OTLPHTTPEndpoint = "https://user:do-not-echo@example.test/v1/traces" }},
		{"query", func(c *Config) { c.OTLPHTTPEndpoint += "?token=do-not-echo" }},
		{"fragment", func(c *Config) { c.OTLPHTTPEndpoint += "#do-not-echo" }},
		{"protocol", func(c *Config) { c.OTLPHTTPEndpoint = "file:///tmp/trace" }},
		{"nan", func(c *Config) { c.SampleRatio = math.NaN() }},
		{"infinity", func(c *Config) { c.SampleRatio = math.Inf(1) }},
		{"negative ratio", func(c *Config) { c.SampleRatio = -.1 }},
		{"large ratio", func(c *Config) { c.SampleRatio = 1.1 }},
		{"negative timeout", func(c *Config) { c.ExportTimeout = -time.Second }},
		{"large timeout", func(c *Config) { c.ExportTimeout = time.Hour }},
		{"service newline", func(c *Config) { c.ServiceName = "service\nprivate" }},
		{"environment length", func(c *Config) { c.Environment = strings.Repeat("e", 65) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{TracesEnabled: true, OTLPHTTPEndpoint: "http://127.0.0.1:4318/v1/traces", SampleRatio: 1}
			tc.edit(&cfg)
			telemetry, err := Init(context.Background(), cfg)
			if err == nil || telemetry != nil {
				t.Fatalf("invalid config accepted: %s", tc.name)
			}
			if strings.Contains(err.Error(), "do-not-echo") {
				t.Fatal("config error exposed credential")
			}
		})
	}
}

func TestTelemetryOperationsBoundedPrivateAndFinishedOnce(t *testing.T) {
	telemetry, logs, exporter := newTestTelemetry(t)
	_, finish := telemetry.Start(context.Background(), "document-id-private")
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { finish(errors.New("api-key-private and user document contents")) })
	}
	wg.Wait()
	if got := testutil.ToFloat64(telemetry.operations.WithLabelValues("other", "error")); got != 1 {
		t.Fatalf("finished %v times", got)
	}
	telemetry.ObserveCount("document-id-private", "api-key-private", 3)
	telemetry.ObserveCount("document-id-private", "api-key-private", -2)
	if got := testutil.ToFloat64(telemetry.items.WithLabelValues("other", "other")); got != 3 {
		t.Fatalf("count: %v", got)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "other" || spans[0].Status.Description != "error" || len(spans[0].Events) != 0 {
		t.Fatalf("unsafe span: %#v", spans)
	}
	all := scrape(t, telemetry) + logs.String()
	for _, private := range []string{"document-id-private", "api-key-private", "user document contents"} {
		if strings.Contains(all, private) {
			t.Fatalf("telemetry contains private value %q", private)
		}
	}
}

func TestTelemetryOperationCancellation(t *testing.T) {
	telemetry, _, _ := newTestTelemetry(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		ctx     context.Context
		err     error
		outcome string
	}{
		{context.Background(), nil, "success"},
		{context.Background(), errors.New("private"), "error"},
		{canceled, nil, "canceled"},
		{expired, nil, "deadline_exceeded"},
	} {
		_, finish := telemetry.Start(tc.ctx, "agent.run")
		finish(tc.err)
		if got := testutil.ToFloat64(telemetry.operations.WithLabelValues("agent.run", tc.outcome)); got != 1 {
			t.Fatalf("outcome %s: %v", tc.outcome, got)
		}
	}
}

func TestTelemetryRegistriesAndNilReceivers(t *testing.T) {
	first, _, _ := newTestTelemetry(t)
	second, _, _ := newTestTelemetry(t)
	first.ObserveCount("retrieval.search", "success", 2)
	if strings.Contains(scrape(t, second), `operation="retrieval.search"`) {
		t.Fatal("metrics leaked between registry instances")
	}
	var disabled *Telemetry
	ctx := context.Background()
	result, finish := disabled.Start(ctx, "agent.run")
	finish(nil)
	disabled.ObserveCount("agent.run", "success", 1)
	if result != ctx || disabled.Shutdown(ctx) != nil {
		t.Fatal("nil instrumentation changed context or failed")
	}
	w := httptest.NewRecorder()
	disabled.MetricsHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("nil metrics status: %d", w.Code)
	}
	var seen string
	handler := disabled.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestID(r.Context())
		if Logger(r.Context()) == nil {
			t.Error("nil context logger")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	w = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nil", nil)
	req.Header.Set("X-Request-ID", "untrusted-client-value")
	handler.ServeHTTP(w, req)
	if len(seen) != 32 || seen != w.Header().Get("X-Request-ID") || seen == "untrusted-client-value" {
		t.Fatalf("nil middleware request ID: %q", seen)
	}
}

func TestOTLPHTTPExportAndShutdown(t *testing.T) {
	var calls atomic.Int64
	var received collecttrace.ExportTraceServiceRequest
	var receiveMu sync.Mutex
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("invalid OTLP request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("exporter inherited a credential from OTEL env")
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			t.Error(err)
		}
		receiveMu.Lock()
		err = proto.Unmarshal(body, &received)
		receiveMu.Unlock()
		if err != nil {
			t.Error(err)
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=secret-from-env")
	telemetry, err := Init(context.Background(), Config{
		TracesEnabled: true, OTLPHTTPEndpoint: collector.URL + "/v1/traces", SampleRatio: 1,
		ServiceName: "magworkspace-test", Environment: "test", ExportTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, finish := telemetry.Start(context.Background(), "retrieval.search")
	finish(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := telemetry.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := telemetry.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("export count: %d", calls.Load())
	}
	receiveMu.Lock()
	defer receiveMu.Unlock()
	if len(received.ResourceSpans) != 1 || len(received.ResourceSpans[0].ScopeSpans) != 1 ||
		len(received.ResourceSpans[0].ScopeSpans[0].Spans) != 1 || received.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "retrieval.search" {
		t.Fatal("missing completed span in OTLP request")
	}
}
