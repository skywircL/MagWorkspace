package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testServiceKey = "test-only-harness-service-key-0123456789"

func newTestHTTPClient(t *testing.T, url string) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient(HTTPConfig{URL: url, APIKey: testServiceKey, Mode: "demo", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func resultHTTPJSON(request Request) string {
	data, _ := json.Marshal(map[string]any{
		"run_id": request.RunID, "session_id": request.SessionID, "mode": "demo", "status": "completed", "answer": "完成",
		"messages": []Message{{Role: "user", Content: request.Query}, {Role: "assistant", Content: "完成"}},
	})
	return string(data)
}

func sseHTTPEvent(kind string, value any) string {
	data, _ := json.Marshal(value)
	return "event: " + kind + "\ndata: " + string(data) + "\n\n"
}

func sseHTTPStart(request Request) string {
	return sseHTTPEvent("message.start", Event{Type: "message.start", RunID: request.RunID, SessionID: request.SessionID, Round: 1})
}

func writeHTTPProtocol(w http.ResponseWriter, contentType string) {
	w.Header().Set("X-Magent-Protocol", "1")
	w.Header().Set("Content-Type", contentType)
}

func TestHTTPHarnessJSONRequestContractAndStrictScope(t *testing.T) {
	for _, restricted := range []bool{false, true} {
		t.Run(fmt.Sprint(restricted), func(t *testing.T) {
			request := requestForTest()
			if restricted {
				var ids []string
				request.DocumentIDs = &ids
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" || r.Header.Get("Authorization") != "Bearer "+testServiceKey || r.Header.Get("X-Magent-Protocol") != "1" || r.Header.Get("Accept") != "application/json" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("unexpected service request headers")
				}
				timeout, err := strconv.Atoi(r.Header.Get("X-Magent-Timeout-Ms"))
				if err != nil || timeout <= 0 || timeout > 1000 {
					t.Error("run deadline was not propagated")
				}
				var payload map[string]json.RawMessage
				if json.NewDecoder(r.Body).Decode(&payload) != nil || string(payload["run_id"]) != `"run-1"` || string(payload["user_id"]) != `"user-1"` {
					t.Error("authenticated request did not round-trip")
				}
				want := "null"
				if restricted {
					want = "[]"
				}
				if string(payload["document_ids"]) != want {
					t.Errorf("scope = %s, want %s", payload["document_ids"], want)
				}
				for _, key := range []string{"api_key", "model", "base_url", "generation"} {
					if payload[key] != nil {
						t.Errorf("unexpected authority/config field %s", key)
					}
				}
				writeHTTPProtocol(w, "application/json")
				_, _ = io.WriteString(w, resultHTTPJSON(request))
			}))
			defer server.Close()
			client := newTestHTTPClient(t, server.URL)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := client.Run(ctx, request)
			if err != nil || result.Answer != "完成" {
				t.Fatalf("result = %+v, error = %v", result, err)
			}
		})
	}
}

func TestHTTPHarnessRejectsInvalidDeploymentConfiguration(t *testing.T) {
	valid := HTTPConfig{URL: "http://harness:8090", APIKey: testServiceKey, Mode: "demo"}
	for name, mutate := range map[string]func(*HTTPConfig){
		"public HTTP":               func(c *HTTPConfig) { c.URL = "http://example.com" },
		"userinfo":                  func(c *HTTPConfig) { c.URL = "https://user:secret@example.com" },
		"query":                     func(c *HTTPConfig) { c.URL += "?key=secret" },
		"fragment":                  func(c *HTTPConfig) { c.URL += "#secret" },
		"invalid port":              func(c *HTTPConfig) { c.URL = "http://harness:99999" },
		"mode":                      func(c *HTTPConfig) { c.Mode = "" },
		"short key":                 func(c *HTTPConfig) { c.APIKey = "secret" },
		"header injection":          func(c *HTTPConfig) { c.APIKey += "\r\nsecret" },
		"unicode key":               func(c *HTTPConfig) { c.APIKey += "密钥" },
		"key with spaces":           func(c *HTTPConfig) { c.APIKey += " key" },
		"callback path":             func(c *HTTPConfig) { c.CallbackBaseURL = "http://backend:8081/api" },
		"callback trailing slash":   func(c *HTTPConfig) { c.CallbackBaseURL = "http://backend:8081/" },
		"callback implicit port":    func(c *HTTPConfig) { c.CallbackBaseURL = "http://backend" },
		"callback IPv6":             func(c *HTTPConfig) { c.CallbackBaseURL = "http://[::1]:8081" },
		"callback uppercase host":   func(c *HTTPConfig) { c.CallbackBaseURL = "http://BACKEND:8081" },
		"callback zero padded port": func(c *HTTPConfig) { c.CallbackBaseURL = "http://backend:08081" },
		"unadvertised wildcard":     func(c *HTTPConfig) { c.CallbackListenAddr = "0.0.0.0:0" },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			client, err := NewHTTPClient(config)
			if client != nil {
				_ = client.Close()
			}
			if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe configuration error: %v", err)
			}
		})
	}
	for _, origin := range []string{"https://harness.example.com/api", "http://harness:8090", "http://127.0.0.1:8090", "http://10.0.0.2:8090", "http://harness.svc.cluster.local"} {
		if _, err := parseServiceURL(origin); err != nil {
			t.Errorf("configured origin rejected: %s", origin)
		}
	}
}

func TestHTTPHarnessRejectsMalformedJSONAndProtocol(t *testing.T) {
	request := requestForTest()
	valid := resultHTTPJSON(request)
	for name, test := range map[string]struct{ body, contentType, protocol string }{
		"missing protocol":   {valid, "application/json", ""},
		"wrong protocol":     {valid, "application/json", "2"},
		"wrong content type": {valid, "text/plain", "1"},
		"trailing JSON":      {valid + "{}", "application/json", "1"},
		"duplicate JSON":     {strings.Replace(valid, `"mode":"demo"`, `"mode":"demo","mode":"demo"`, 1), "application/json", "1"},
		"different run":      {strings.ReplaceAll(valid, "run-1", "another-run"), "application/json", "1"},
		"different session":  {strings.ReplaceAll(valid, "session-1", "another-session"), "application/json", "1"},
		"different mode":     {strings.ReplaceAll(valid, `"demo"`, `"live"`), "application/json", "1"},
		"invalid UTF8":       {strings.ReplaceAll(valid, "完成", string([]byte{0xff})), "application/json", "1"},
		"oversized JSON":     {strings.Repeat(" ", maxOutputBytes+1), "application/json", "1"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				if test.protocol != "" {
					w.Header().Set("X-Magent-Protocol", test.protocol)
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			_, err := newTestHTTPClient(t, server.URL).Run(context.Background(), request)
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestHTTPHarnessServiceErrorsAreRedactedAndRedirectsAreNotFollowed(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	for _, status := range []int{301, 302, 307, 308, 400, 401, 403, 404, 429, 500, 502, 503, 504} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target.URL)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private-document provider-secret credential="+testServiceKey)
			}))
			defer server.Close()
			_, err := newTestHTTPClient(t, server.URL).Run(context.Background(), requestForTest())
			want := ErrFailed
			if status == 401 || status == 403 || status == 404 || status == 429 || status == 502 || status == 503 || status == 504 {
				want = ErrUnavailable
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "private-document") || strings.Contains(err.Error(), testServiceKey) {
				t.Fatalf("unexpected service error %v", err)
			}
		})
	}
	if forwarded.Load() != 0 {
		t.Fatal("service credentials were forwarded to a redirect")
	}
}

func TestHTTPHarnessFragmentedSSEAndProvisionalAttempts(t *testing.T) {
	request := requestForTest()
	stream := ": keepalive\n\n" + sseHTTPStart(request)
	stream += sseHTTPEvent("message.delta", Event{Type: "message.delta", RunID: request.RunID, SessionID: request.SessionID, Round: 1, Delta: "草稿"})
	stream += sseHTTPEvent("message.discard", Event{Type: "message.discard", RunID: request.RunID, SessionID: request.SessionID, Round: 1})
	stream += sseHTTPEvent("message.start", Event{Type: "message.start", RunID: request.RunID, SessionID: request.SessionID, Round: 2})
	stream += sseHTTPEvent("message.delta", Event{Type: "message.delta", RunID: request.RunID, SessionID: request.SessionID, Round: 2, Delta: "完成"})
	stream += "event: result\ndata: " + resultHTTPJSON(request) + "\n\n"
	firstDelivered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Error("SSE not negotiated")
		}
		writeHTTPProtocol(w, "text/event-stream; charset=utf-8")
		prefix := ": keepalive\n\n" + sseHTTPStart(request)
		_, _ = io.WriteString(w, prefix)
		w.(http.Flusher).Flush()
		select {
		case <-firstDelivered:
		case <-time.After(time.Second):
			t.Error("first event buffered until result")
		}
		for _, b := range []byte(strings.TrimPrefix(stream, prefix)) {
			_, _ = w.Write([]byte{b})
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()
	var events []Event
	result, err := newTestHTTPClient(t, server.URL).Stream(context.Background(), request, func(event Event) error {
		events = append(events, event)
		if len(events) == 1 {
			close(firstDelivered)
		}
		return nil
	})
	if err != nil || result.Answer != "完成" || len(events) != 5 || events[1].Delta != "草稿" || events[4].Round != 2 {
		t.Fatalf("result=%+v events=%+v err=%v", result, events, err)
	}
}

func TestHTTPHarnessRejectsMalformedSSE(t *testing.T) {
	request := requestForTest()
	start := sseHTTPStart(request)
	result := "event: result\ndata: " + resultHTTPJSON(request) + "\n\n"
	for name, stream := range map[string]string{
		"missing terminal":        start,
		"incomplete terminal":     start + strings.TrimSuffix(result, "\n\n"),
		"duplicate terminal":      start + result + result,
		"trailing comment":        start + result + ": after terminal\n",
		"terminal before message": result,
		"cross-run event":         strings.ReplaceAll(start, "run-1", "run-2") + result,
		"cross-run result":        start + strings.ReplaceAll(result, "run-1", "run-2"),
		"cross-session event":     strings.ReplaceAll(start, "session-1", "session-2") + result,
		"duplicate key":           strings.Replace(start, `"round":1`, `"round":1,"round":1`, 1) + result,
		"event mismatch":          strings.Replace(start, "event: message.start", "event: message.delta", 1) + result,
		"invalid UTF8":            start + "event: message.delta\ndata: " + string([]byte{0xff}) + "\n\n",
		"unknown field":           "retry: 100\n" + start + result,
		"unknown event":           "event: private-error\ndata: {}\n\n",
		"unknown error code":      "event: error\ndata: {\"code\":\"provider-private-error\"}\n\n",
		"error extra details":     "event: error\ndata: {\"code\":\"agent_failed\",\"details\":\"secret\"}\n\n",
		"oversized record":        "event: message.delta\ndata: " + strings.Repeat("x", maxOutputBytes+1) + "\n\n",
		"oversized total":         strings.Repeat(":"+strings.Repeat(" ", 1021)+"\n", maxStreamBytes/1024+2),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeHTTPProtocol(w, "text/event-stream")
				_, _ = io.WriteString(w, stream)
			}))
			defer server.Close()
			_, err := newTestHTTPClient(t, server.URL).Stream(context.Background(), request, func(Event) error { return nil })
			if !errors.Is(err, ErrFailed) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unexpected protocol error %v", err)
			}
		})
	}
}

func TestHTTPHarnessSSESafeTerminalErrors(t *testing.T) {
	for code, expected := range map[string]error{"agent_failed": ErrFailed, "agent_unavailable": ErrUnavailable} {
		t.Run(code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeHTTPProtocol(w, "text/event-stream")
				_, _ = io.WriteString(w, sseHTTPEvent("error", map[string]string{"code": code}))
			}))
			defer server.Close()
			_, err := newTestHTTPClient(t, server.URL).Stream(context.Background(), requestForTest(), func(Event) error { return nil })
			if !errors.Is(err, expected) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestHTTPHarnessBodyDeadlineCancellationAndClientClose(t *testing.T) {
	for _, operation := range []string{"timeout JSON", "timeout SSE", "cancel JSON", "cancel SSE", "close JSON"} {
		t.Run(operation, func(t *testing.T) {
			started, disconnected := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				contentType := "application/json"
				if strings.HasSuffix(operation, "SSE") {
					contentType = "text/event-stream"
				}
				writeHTTPProtocol(w, contentType)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
				close(disconnected)
			}))
			defer server.Close()
			client := newTestHTTPClient(t, server.URL)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if strings.HasPrefix(operation, "timeout") {
				client.config.Timeout = 80 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() {
				var err error
				if strings.HasSuffix(operation, "SSE") {
					_, err = client.Stream(ctx, requestForTest(), func(Event) error { return nil })
				} else {
					_, err = client.Run(ctx, requestForTest())
				}
				done <- err
			}()
			<-started
			if strings.HasPrefix(operation, "cancel") {
				cancel()
			}
			if strings.HasPrefix(operation, "close") {
				_ = client.Close()
			}
			select {
			case err := <-done:
				want := context.Canceled
				if strings.HasPrefix(operation, "timeout") {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, ErrFailed) || !errors.Is(err, want) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("run outlived cancellation")
			}
			select {
			case <-disconnected:
			case <-time.After(time.Second):
				t.Fatal("response body was not canceled")
			}
		})
	}
}

func TestHTTPHarnessCallbackFailureCancelsStreamAndRedacts(t *testing.T) {
	for _, panicCallback := range []bool{false, true} {
		t.Run(fmt.Sprint(panicCallback), func(t *testing.T) {
			disconnected := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeHTTPProtocol(w, "text/event-stream")
				_, _ = io.WriteString(w, sseHTTPStart(requestForTest()))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(disconnected)
			}))
			defer server.Close()
			sentinel := errors.New("private callback details")
			_, err := newTestHTTPClient(t, server.URL).Stream(context.Background(), requestForTest(), func(Event) error {
				if panicCallback {
					panic("private callback details")
				}
				return sentinel
			})
			if !errors.Is(err, ErrFailed) || (!panicCallback && !errors.Is(err, sentinel)) || strings.Contains(err.Error(), "private callback") {
				t.Fatalf("unsafe callback error: %v", err)
			}
			select {
			case <-disconnected:
			case <-time.After(time.Second):
				t.Fatal("callback failure did not cancel service")
			}
		})
	}
}

func TestHTTPHarnessRejectsOversizedRequestAndClosedClient(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	request := requestForTest()
	for index := 0; index < 40; index++ {
		request.History = append(request.History, Message{Role: "user", Content: strings.Repeat("x", maxMessageBytes)})
	}
	if _, err := client.Run(context.Background(), request); !errors.Is(err, ErrFailed) {
		t.Fatalf("oversized request: %v", err)
	}
	_ = client.Close()
	if _, err := client.Run(context.Background(), requestForTest()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed client: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached service")
	}
}

func TestHTTPHarnessNeverReplaysARequestAfterLostResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			writeHTTPProtocol(w, "application/json")
			_, _ = io.WriteString(w, resultHTTPJSON(requestForTest()))
			return
		}
		// The run has already been received and could have committed a tool
		// mutation. A lost response must not cause the same POST to execute again.
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	if _, err := client.Run(context.Background(), requestForTest()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Run(context.Background(), requestForTest()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lost response: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("request was replayed: received %d requests", calls.Load())
	}
}
