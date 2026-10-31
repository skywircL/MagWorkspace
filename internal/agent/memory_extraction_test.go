package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const extractionQuery = "我喜欢中文回答。"

func extractionFixture() string {
	return `{"items":[{"kind":"preference","content":"喜欢中文回答","topic":"language","importance":3,"evidence":"我喜欢中文回答"}]}`
}

func TestHTTPMemoryExtractionContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/memories/extract" || r.Header.Get("Authorization") != "Bearer "+testServiceKey || r.Header.Get("X-Magent-Protocol") != "1" || r.Header.Get("Accept") != "application/json" {
			t.Error("unexpected extraction request")
		}
		deadline, err := strconv.Atoi(r.Header.Get("X-Magent-Timeout-Ms"))
		if err != nil || deadline < 1 || deadline > 30000 {
			t.Error("extraction deadline not bounded")
		}
		var request map[string]any
		if json.NewDecoder(r.Body).Decode(&request) != nil || len(request) != 1 || request["query"] != extractionQuery {
			t.Error("unexpected extraction identity/history")
		}
		writeHTTPProtocol(w, "application/json")
		_, _ = io.WriteString(w, extractionFixture())
	}))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	client.config.Timeout = time.Minute
	items, err := client.ExtractMemories(context.Background(), extractionQuery)
	if err != nil || len(items) != 1 || items[0].Evidence != "我喜欢中文回答" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestHTTPMemoryExtractionStrictOutput(t *testing.T) {
	for name, value := range map[string]string{
		"unknown envelope":       `{"items":[],"query":"private"}`,
		"null":                   `{"items":null}`,
		"missing":                `{}`,
		"object instead of list": `{"items":{}}`,
		"duplicate":              `{"items":[],"items":[]}`,
		"unknown kind":           strings.Replace(extractionFixture(), "preference", "secret", 1),
		"unsupported evidence":   strings.Replace(extractionFixture(), "我喜欢中文回答", "我在北京", 1),
		"empty evidence":         strings.Replace(extractionFixture(), "我喜欢中文回答", "", 1),
		"missing topic":          strings.Replace(extractionFixture(), `"topic":"language",`, "", 1),
		"null topic":             strings.Replace(extractionFixture(), `"topic":"language"`, `"topic":null`, 1),
		"unknown item":           strings.Replace(extractionFixture(), `"kind":`, `"scope":"admin","kind":`, 1),
		"fractional importance":  strings.Replace(extractionFixture(), `"importance":3`, `"importance":1.5`, 1),
		"zero importance":        strings.Replace(extractionFixture(), `"importance":3`, `"importance":0`, 1),
		"invalid surrogate":      strings.Replace(extractionFixture(), "喜欢中文回答", `\ud800`, 1),
		"oversize content":       strings.Replace(extractionFixture(), "喜欢中文回答", strings.Repeat("文", 501), 1),
		"oversize topic":         strings.Replace(extractionFixture(), "language", strings.Repeat("文", 101), 1),
		"too many":               `{"items":[` + strings.TrimSuffix(strings.Repeat(strings.TrimSuffix(strings.TrimPrefix(extractionFixture(), `{"items":[`), `]}`)+",", 6), ",") + `]}`,
		"trailing":               extractionFixture() + `{}`,
		"oversize body":          strings.Repeat(" ", maxExtractionResponseBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeExtraction([]byte(value), extractionQuery); !errors.Is(err, ErrFailed) {
				t.Fatalf("invalid output accepted: %v", err)
			}
		})
	}
	if items, err := decodeExtraction([]byte(`{"items":[]}`), extractionQuery); err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty result=%+v error=%v", items, err)
	}
}

func TestHTTPMemoryExtractionRejectsInvalidInputAndRedirect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Redirect(w, r, "/private", 307) }))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	for _, query := range []string{"", "  ", strings.Repeat("x", 16001), string([]byte{255})} {
		if _, err := client.ExtractMemories(context.Background(), query); !errors.Is(err, ErrFailed) {
			t.Fatal("invalid input accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached service")
	}
	if _, err := client.ExtractMemories(context.Background(), extractionQuery); !errors.Is(err, ErrUnavailable) {
		t.Fatal("redirect accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("redirect followed or POST retried")
	}
}

func TestHTTPMemoryExtractionCancellationAndPrivateErrors(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.ExtractMemories(ctx, extractionQuery); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("extraction did not cancel")
	}
	_ = client.Close()
	if _, err := client.ExtractMemories(context.Background(), extractionQuery); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed client accepted extraction")
	}
}

func TestHTTPMemoryExtractionRejectsProtocolAndPrivateFailure(t *testing.T) {
	for _, scenario := range []string{"unauthorized", "missing protocol", "encoding", "oversize", "private failure"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeHTTPProtocol(w, "application/json")
				switch scenario {
				case "unauthorized":
					w.WriteHeader(401)
				case "missing protocol":
					w.Header().Del("X-Magent-Protocol")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "private failure":
					w.WriteHeader(502)
				case "oversize":
					_, _ = io.WriteString(w, strings.Repeat("x", maxExtractionResponseBytes+1))
					return
				}
				_, _ = io.WriteString(w, `{"private":"secret-provider-key"}`)
			}))
			defer server.Close()
			_, err := newTestHTTPClient(t, server.URL).ExtractMemories(context.Background(), extractionQuery)
			if err == nil || strings.Contains(err.Error(), "secret-provider-key") {
				t.Fatalf("unsafe failure: %v", err)
			}
		})
	}
}

func TestHTTPMemoryExtractionAdmissionReceipt(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
		body   string
		header func(http.Header)
		want   error
	}{
		{name: "busy", status: 429, body: `{"code":"agent_busy"}`, want: ErrExtractionBusy},
		{name: "model failure", status: 502, body: `{"code":"agent_failed"}`, want: ErrFailed},
		{name: "unavailable", status: 503, body: `{"code":"agent_unavailable"}`, want: ErrUnavailable},
		{name: "unauthorized busy", status: 401, body: `{"code":"agent_busy"}`, want: ErrUnavailable},
		{name: "unavailable busy", status: 503, body: `{"code":"agent_busy"}`, want: ErrUnavailable},
		{name: "failed busy", status: 502, body: `{"code":"agent_busy"}`, want: ErrUnavailable},
		{name: "busy failed", status: 429, body: `{"code":"agent_failed"}`, want: ErrUnavailable},
		{name: "missing protocol", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Del("X-Magent-Protocol") }, want: ErrUnavailable},
		{name: "duplicate protocol", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Add("X-Magent-Protocol", "1") }, want: ErrUnavailable},
		{name: "wrong protocol", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Set("X-Magent-Protocol", "2") }, want: ErrUnavailable},
		{name: "wrong media", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Set("Content-Type", "text/plain") }, want: ErrUnavailable},
		{name: "duplicate media", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Add("Content-Type", "application/json") }, want: ErrUnavailable},
		{name: "encoded", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Set("Content-Encoding", "gzip") }, want: ErrUnavailable},
		{name: "duplicate encoding", status: 429, body: `{"code":"agent_busy"}`, header: func(h http.Header) { h.Add("Content-Encoding", ""); h.Add("Content-Encoding", "gzip") }, want: ErrUnavailable},
		{name: "extra fields", status: 429, body: `{"code":"agent_busy","private":"secret-provider-key"}`, want: ErrUnavailable},
		{name: "duplicate fields", status: 429, body: `{"code":"agent_busy","code":"agent_busy"}`, want: ErrUnavailable},
		{name: "trailing object", status: 429, body: `{"code":"agent_busy"}{}`, want: ErrUnavailable},
		{name: "null code", status: 429, body: `{"code":null}`, want: ErrUnavailable},
		{name: "unknown code", status: 429, body: `{"code":"secret-provider-key"}`, want: ErrUnavailable},
		{name: "invalid unicode", status: 429, body: `{"code":"\ud800"}`, want: ErrUnavailable},
		{name: "invalid utf8", status: 429, body: "{\"code\":\"agent_busy\"}\xff", want: ErrUnavailable},
		{name: "oversize", status: 429, body: `{"code":"agent_busy"}` + strings.Repeat(" ", maxExtractionErrorBytes), want: ErrUnavailable},
		{name: "private model failure", status: 502, body: `{"code":"agent_failed","private":"secret-provider-key"}`, want: ErrUnavailable},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				writeHTTPProtocol(w, "application/json")
				if scenario.header != nil {
					scenario.header(w.Header())
				}
				w.WriteHeader(scenario.status)
				_, _ = io.WriteString(w, scenario.body)
			}))
			defer server.Close()
			items, err := newTestHTTPClient(t, server.URL).ExtractMemories(context.Background(), extractionQuery)
			if items != nil || !errors.Is(err, scenario.want) || (scenario.want != ErrExtractionBusy && errors.Is(err, ErrExtractionBusy)) {
				t.Fatalf("items=%+v err=%v want=%v", items, err, scenario.want)
			}
			if strings.Contains(err.Error(), "secret-provider-key") || calls.Load() != 1 {
				t.Fatalf("unsafe failure or retry: err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestHTTPMemoryExtractionIncompleteBusyReceiptPreservesDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeHTTPProtocol(w, "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"agent_busy"}`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := newTestHTTPClient(t, server.URL).ExtractMemories(ctx, extractionQuery)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrExtractionBusy) {
		t.Fatalf("incomplete receipt confirmed admission: %v", err)
	}
}
