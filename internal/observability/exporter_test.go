package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type failingSpanExporter struct{ err error }

func (e failingSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return e.err
}
func (e failingSpanExporter) Shutdown(context.Context) error { return e.err }

func TestSafeExporterSanitizesAllReturnedErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"success", nil, nil},
		{"private failure", errors.New("https://collector/private: secret-body"), errTraceExport},
		{"canceled", fmt.Errorf("secret-body: %w", context.Canceled), context.Canceled},
		{"deadline", fmt.Errorf("secret-body: %w", context.DeadlineExceeded), context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := safeSpanExporter{inner: failingSpanExporter{err: tc.err}}
			if err := exporter.ExportSpans(context.Background(), nil); err != tc.want {
				t.Fatalf("unsafe export error: %v", err)
			}
			wantShutdown := tc.want
			if wantShutdown == errTraceExport {
				wantShutdown = errTraceShutdown
			}
			if err := exporter.Shutdown(context.Background()); err != wantShutdown {
				t.Fatalf("unsafe shutdown error: %v", err)
			}
		})
	}
}

func TestOTLPCollectorFailureCannotLeakThroughSDKErrorHandler(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var recorded []error
			var errorsMu sync.Mutex
			previous := otel.GetErrorHandler()
			capture := otel.ErrorHandlerFunc(func(err error) {
				errorsMu.Lock()
				recorded = append(recorded, err)
				errorsMu.Unlock()
			})
			otel.SetErrorHandler(capture)
			defer otel.SetErrorHandler(previous)
			requestStarted := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case requestStarted <- struct{}{}:
				default:
				}
				<-release
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private-collector-secret-body")
			}))
			defer collector.Close()
			telemetry, err := Init(context.Background(), Config{
				TracesEnabled: true, OTLPHTTPEndpoint: collector.URL + "/private-collector-path/v1/traces",
				SampleRatio: 1, ExportTimeout: 3 * time.Second,
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			// Fill a batch and hold its HTTP export open. A later application
			// operation must finish independently of this blocked collector.
			for range 256 {
				_, finish := telemetry.Start(context.Background(), "retrieval.search")
				finish(nil)
			}
			select {
			case <-requestStarted:
			case <-time.After(2 * time.Second):
				unblock()
				_ = telemetry.Shutdown(context.Background())
				t.Fatal("OTLP exporter did not contact collector")
			}
			finished := make(chan struct{})
			go func() {
				_, finish := telemetry.Start(context.Background(), "agent.run")
				finish(nil)
				close(finished)
			}()
			select {
			case <-finished:
			case <-time.After(time.Second):
				unblock()
				_ = telemetry.Shutdown(context.Background())
				t.Fatal("collector failure blocked application operation")
			}
			unblock()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			if err := telemetry.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			errorsMu.Lock()
			defer errorsMu.Unlock()
			if len(recorded) == 0 {
				t.Fatal("SDK error handler did not observe failing HTTP export")
			}
			for _, err := range recorded {
				if err != errTraceExport || strings.Contains(err.Error(), "private-") || strings.Contains(err.Error(), collector.URL) {
					t.Fatalf("collector details reached SDK error handler: %v", err)
				}
			}
		})
	}
}
