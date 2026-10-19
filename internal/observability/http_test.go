package observability

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPObservabilityPrivacyAndRemoteTraceIsolation(t *testing.T) {
	telemetry, logs, exporter := newTestTelemetry(t)
	mux := http.NewServeMux()
	const remoteTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const privateDocumentID = "private-document-id"
	var seenRequestID string
	mux.HandleFunc("POST /documents/{documentID}", func(w http.ResponseWriter, r *http.Request) {
		seenRequestID = RequestID(r.Context())
		if seenRequestID == "" || seenRequestID != w.Header().Get("X-Request-ID") {
			t.Error("missing trusted request ID")
		}
		if Logger(r.Context()) == nil {
			t.Error("missing context logger")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "private body untouched" {
			t.Errorf("middleware consumed request body: %s", body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "private response untouched")
	})
	req := httptest.NewRequest(http.MethodPost, "/documents/"+privateDocumentID+"?token=private-query-secret", strings.NewReader("private body untouched"))
	req.Header.Set("Authorization", "Bearer private-api-secret")
	req.Header.Set("X-Request-ID", "private-client-request-id")
	req.Header.Set("Traceparent", "00-"+remoteTraceID+"-00f067aa0ba902b7-01")
	req.Header.Set("Tracestate", "vendor=private-tracestate")
	w := httptest.NewRecorder()
	telemetry.Handler(mux).ServeHTTP(w, req)
	if len(seenRequestID) != 32 || seenRequestID == "private-client-request-id" || seenRequestID == remoteTraceID {
		t.Fatalf("untrusted request ID: %q", seenRequestID)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "POST /documents/{documentID}" || spans[0].Parent.IsValid() ||
		spans[0].SpanContext.TraceID().String() == remoteTraceID || len(spans[0].Links) != 1 || spans[0].Links[0].SpanContext.TraceID().String() != remoteTraceID {
		t.Fatalf("remote trace was adopted as trusted parent: %#v", spans)
	}
	if spans[0].Links[0].SpanContext.TraceState().Len() != 0 {
		t.Fatal("untrusted vendor tracestate was exported")
	}
	if !strings.Contains(logs.String(), `"trace_id":"`+spans[0].SpanContext.TraceID().String()+`"`) ||
		!strings.Contains(logs.String(), `"request_id":"`+seenRequestID+`"`) {
		t.Fatalf("missing correlation: %s", logs.String())
	}
	all := scrape(t, telemetry) + logs.String()
	for _, private := range []string{privateDocumentID, "private-query-secret", "private-api-secret", "private-client-request-id", "private body", "private response"} {
		if strings.Contains(all, private) {
			t.Fatalf("private request value reached metrics or logs: %q", private)
		}
		for _, attr := range spans[0].Attributes {
			if strings.Contains(attr.Value.Emit(), private) {
				t.Fatalf("private request value reached trace: %q", private)
			}
		}
	}
	if w.Code != http.StatusCreated || w.Body.String() != "private response untouched" {
		t.Fatal("middleware changed response")
	}
}

func TestHTTPObservabilityUnmatchedRoutesAndMethods(t *testing.T) {
	telemetry, logs, exporter := newTestTelemetry(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /documents/{documentID}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := telemetry.Handler(mux)
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/not-found/private-one", http.StatusNotFound},
		{http.MethodGet, "/not-found/private-two", http.StatusNotFound},
		{http.MethodPost, "/documents/private-three", http.StatusMethodNotAllowed},
		{"PRIVATE-METHOD", "/documents/private-four", http.StatusMethodNotAllowed},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("status %s %s = %d", tc.method, tc.path, w.Code)
		}
	}
	all := scrape(t, telemetry) + logs.String()
	if strings.Contains(all, "private-") || strings.Contains(all, "PRIVATE-METHOD") || !strings.Contains(all, `route="unmatched"`) {
		t.Fatalf("unbounded routes/methods: %s", all)
	}
	for _, span := range exporter.GetSpans() {
		if !strings.HasSuffix(span.Name, " unmatched") {
			t.Fatalf("unmatched route span: %s", span.Name)
		}
	}
}

func TestHTTPObservabilitySSEFlushAndLifetime(t *testing.T) {
	telemetry, _, _ := newTestTelemetry(t)
	finish := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Error("SSE Flusher capability lost")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: first\ndata: hello\n\n")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
		<-finish
		_, _ = io.WriteString(w, "event: last\ndata: done\n\n")
	})
	server := httptest.NewServer(telemetry.Handler(mux))
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(server.URL + "/events")
	if err != nil {
		close(finish)
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	active := testutil.ToFloat64(telemetry.httpActive.WithLabelValues(http.MethodGet))
	close(finish)
	if err != nil || line != "event: first\n" || active != 1 {
		t.Fatalf("first SSE event was buffered: line=%q err=%v active=%v", line, err, active)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	if active := testutil.ToFloat64(telemetry.httpActive.WithLabelValues(http.MethodGet)); active != 0 {
		t.Fatalf("SSE remained active after completion: %v", active)
	}
	if got := testutil.ToFloat64(telemetry.httpRequests.WithLabelValues("GET", "/events", "200", "success")); got != 1 {
		t.Fatalf("SSE completion count: %v", got)
	}
}

type flushFailureWriter struct {
	header   http.Header
	deadline time.Time
}

func (w *flushFailureWriter) Header() http.Header { return w.header }
func (w *flushFailureWriter) WriteHeader(int)     {}
func (w *flushFailureWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
func (w *flushFailureWriter) FlushError() error {
	return errors.New("private downstream connection error")
}
func (w *flushFailureWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestHTTPObservabilitySSEFlushFailureAndUnwrap(t *testing.T) {
	telemetry, logs, exporter := newTestTelemetry(t)
	mux := http.NewServeMux()
	deadline := time.Now().Add(time.Second)
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).SetWriteDeadline(deadline); err != nil {
			t.Error(err)
		}
		if err := http.NewResponseController(w).Flush(); err == nil {
			t.Error("FlushError was swallowed")
		}
	})
	w := &flushFailureWriter{header: make(http.Header)}
	telemetry.Handler(mux).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events", nil))
	if !w.deadline.Equal(deadline) || testutil.ToFloat64(telemetry.httpRequests.WithLabelValues("GET", "/events", "200", "error")) != 1 {
		t.Fatal("flush error or Unwrap deadline was lost")
	}
	if strings.Contains(logs.String()+scrape(t, telemetry), "private downstream") || exporter.GetSpans()[0].Status.Description != "error" {
		t.Fatal("connection error text leaked")
	}
}

type noFlushWriter struct{ header http.Header }

func (w *noFlushWriter) Header() http.Header         { return w.header }
func (w *noFlushWriter) WriteHeader(int)             {}
func (w *noFlushWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestHTTPObservabilityDoesNotInventFlushCapability(t *testing.T) {
	telemetry, _, _ := newTestTelemetry(t)
	handler := telemetry.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); ok {
			t.Error("non-flusher was advertised as Flusher")
		}
		if err := http.NewResponseController(w).Flush(); !errors.Is(err, http.ErrNotSupported) {
			t.Errorf("Flush returned %v", err)
		}
	}))
	handler.ServeHTTP(&noFlushWriter{header: make(http.Header)}, httptest.NewRequest(http.MethodGet, "/events", nil))
}

func TestHTTPObservabilityCancellationAndPanic(t *testing.T) {
	telemetry, logs, _ := newTestTelemetry(t)
	ctx, cancel := context.WithCancel(context.Background())
	handler := telemetry.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cancel() }))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/private-cancel", nil).WithContext(ctx))
	if testutil.ToFloat64(telemetry.httpRequests.WithLabelValues("GET", "unmatched", "200", "canceled")) != 1 {
		t.Fatal("canceled HTTP request not classified")
	}
	func() {
		defer func() {
			if recover() != "private panic value" {
				t.Error("middleware changed application panic")
			}
		}()
		telemetry.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("private panic value") })).ServeHTTP(
			httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/private-panic", nil))
	}()
	if testutil.ToFloat64(telemetry.httpRequests.WithLabelValues("GET", "unmatched", "500", "error")) != 1 || strings.Contains(logs.String(), "private panic") {
		t.Fatal("panic classification exposed contents or failed")
	}
	if trace.SpanContextFromContext(context.Background()).IsValid() {
		t.Fatal("trace context leaked outside request")
	}
}
