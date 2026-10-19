package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSSEAcceptNegotiation(t *testing.T) {
	for _, tc := range []struct {
		accept string
		want   bool
	}{
		{"", false}, {"*/*", false}, {"application/json", false},
		{"text/event-stream", true}, {"text/event-stream; q=0", false},
		{"text/event-stream; q=0.5, application/json", false},
		{"application/json; q=0.5, text/event-stream", true},
		{"text/event-stream;q=garbage", false}, {"text/event-stream;q=NaN", false},
	} {
		t.Run(tc.accept, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("Accept", tc.accept)
			if got := wantsEventStream(r); got != tc.want {
				t.Fatalf("stream=%v, want %v", got, tc.want)
			}
		})
	}
}

type controlledWriter struct {
	header    http.Header
	content   strings.Builder
	deadlines []time.Time
	writeErr  error
	flushErr  error
}

func (w *controlledWriter) Header() http.Header { return w.header }
func (w *controlledWriter) WriteHeader(int)     {}
func (w *controlledWriter) Write(b []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.content.Write(b)
}
func (w *controlledWriter) FlushError() error { return w.flushErr }
func (w *controlledWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestSSEWriterEncodingAndDeadlines(t *testing.T) {
	w := &controlledWriter{header: make(http.Header)}
	writer := eventWriter{w: w, controller: http.NewResponseController(w), timeout: time.Second}
	if err := writer.event("run.completed", map[string]string{"answer": "你好\n\nevent: injected\r\n"}); err != nil {
		t.Fatal(err)
	}
	text := w.content.String()
	if strings.Count(text, "\nevent:") != 0 || !strings.HasSuffix(text, "\n\n") || strings.Count(text, "\n") != 3 {
		t.Fatalf("invalid framing: %q", text)
	}
	if len(w.deadlines) != 2 || w.deadlines[0].IsZero() || !w.deadlines[1].IsZero() {
		t.Fatal("per-write deadline was not cleared")
	}
	if !canFlush(w) {
		t.Fatal("FlushError writer not recognized")
	}
	for _, target := range []*error{&w.writeErr, &w.flushErr} {
		*target = errors.New("broken connection")
		if err := writer.write(": heartbeat\n\n"); !errors.Is(err, *target) {
			t.Fatalf("lost writer error: %v", err)
		}
		*target = nil
	}
}
