package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type callbackEndpoints struct {
	Tools   *retrievalEndpoint `json:"retrieval"`
	Journal *retrievalEndpoint `json:"persistence"`
	Memory  *retrievalEndpoint `json:"memory"`
}

func callbackRequest(t *testing.T, endpoint retrievalEndpoint, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, endpoint.URL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+endpoint.Token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(data)
}

func newTestCallbackGateway(t *testing.T) *callbackGateway {
	t.Helper()
	gateway, err := newCallbackGateway("127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	return gateway
}

func registerTestCallbacks(t *testing.T, gateway *callbackGateway, ctx context.Context, request Request) (callbackEndpoints, func()) {
	t.Helper()
	data, cleanup, err := gateway.payload(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints callbackEndpoints
	if json.Unmarshal(data, &endpoints) != nil {
		t.Fatal("invalid callback payload")
	}
	t.Cleanup(cleanup)
	return endpoints, cleanup
}

func TestHTTPCallbackCapabilitiesStayBoundToAuthenticatedRun(t *testing.T) {
	gateway := newTestCallbackGateway(t)
	var tools, journals, memories atomic.Int32
	var endpoints []callbackEndpoints
	var cleanups []func()
	type boundContextKey struct{}
	for _, name := range []string{"alice-workspace", "bob-workspace"} {
		request := requestForTest()
		request.ToolHandler = func(ctx context.Context, tool string, arguments json.RawMessage) (any, error) {
			tools.Add(1)
			if ctx.Value(boundContextKey{}) != name {
				return nil, errors.New("wrong caller context")
			}
			return map[string]string{"workspace": name}, nil
		}
		request.JournalHandler = func(context.Context, string, json.RawMessage) (any, error) {
			journals.Add(1)
			return map[string]any{"records": []any{}}, nil
		}
		request.MemoryHandler = func(context.Context, string, json.RawMessage) (any, error) {
			memories.Add(1)
			return map[string]any{"items": []any{}}, nil
		}
		endpoint, cleanup := registerTestCallbacks(t, gateway, context.WithValue(context.Background(), boundContextKey{}, name), request)
		endpoints, cleanups = append(endpoints, endpoint), append(cleanups, cleanup)
	}
	seen := map[string]bool{}
	for index, group := range endpoints {
		for _, endpoint := range []*retrievalEndpoint{group.Tools, group.Journal, group.Memory} {
			if endpoint == nil || seen[endpoint.Token] || len(endpoint.Token) != 64 {
				t.Fatal("capabilities were reused")
			}
			seen[endpoint.Token] = true
		}
		status, data := callbackRequest(t, *group.Tools, `{"tool":"list_documents","arguments":{}}`)
		want := []string{"alice-workspace", "bob-workspace"}[index]
		if status != 200 || !strings.Contains(data, want) {
			t.Fatalf("wrong bound identity: status=%d data=%s", status, data)
		}
		status, _ = callbackRequest(t, *group.Journal, `{"operation":"load","arguments":{}}`)
		if status != 200 {
			t.Fatalf("journal=%d", status)
		}
		status, _ = callbackRequest(t, *group.Memory, `{"operation":"list","arguments":{}}`)
		if status != 200 {
			t.Fatalf("memory=%d", status)
		}
		for _, path := range []string{"/journal", "/memory"} {
			status, _ = callbackRequest(t, retrievalEndpoint{URL: gateway.baseURL + path, Token: group.Tools.Token}, `{"operation":"load","arguments":{}}`)
			if status != 401 {
				t.Fatalf("tool capability crossed to %s: %d", path, status)
			}
		}
		status, _ = callbackRequest(t, *group.Tools, `{"tool":"list_documents","arguments":{"workspace_id":"another-workspace"}}`)
		if status != 400 {
			t.Fatalf("model-selected scope accepted: %d", status)
		}
	}
	cleanups[0]()
	for _, endpoint := range []*retrievalEndpoint{endpoints[0].Tools, endpoints[0].Journal, endpoints[0].Memory} {
		if status, _ := callbackRequest(t, *endpoint, `{}`); status != 401 {
			t.Fatalf("revoked capability accepted: %d", status)
		}
	}
	if status, _ := callbackRequest(t, *endpoints[1].Tools, `{"tool":"list_documents","arguments":{}}`); status != 200 {
		t.Fatal("revoking one run revoked another run")
	}
	if tools.Load() != 3 || journals.Load() != 2 || memories.Load() != 2 {
		t.Fatal("denied request reached an authenticated handler")
	}
}

func TestHTTPCallbackCancellationRevokesAndCancelsInflightWork(t *testing.T) {
	for _, closeGateway := range []bool{false, true} {
		t.Run(fmt.Sprint(closeGateway), func(t *testing.T) {
			gateway := newTestCallbackGateway(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, canceled := make(chan struct{}), make(chan struct{})
			request := requestForTest()
			request.ToolHandler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}
			endpoints, _ := registerTestCallbacks(t, gateway, ctx, request)
			done := make(chan struct{})
			go func() {
				defer close(done)
				req, _ := http.NewRequest(http.MethodPost, endpoints.Tools.URL, strings.NewReader(`{"tool":"list_documents","arguments":{}}`))
				req.Header.Set("Authorization", "Bearer "+endpoints.Tools.Token)
				req.Header.Set("Content-Type", "application/json")
				client := &http.Client{Timeout: 2 * time.Second}
				response, err := client.Do(req)
				if err == nil {
					_ = response.Body.Close()
				}
			}()
			<-started
			if closeGateway {
				_ = gateway.Close()
			} else {
				cancel()
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("in-flight handler retained authority after cancellation")
			}
			<-done
			if !closeGateway {
				if status, _ := callbackRequest(t, *endpoints.Tools, `{}`); status != 401 {
					t.Fatal("canceled capability still accepted")
				}
			}
		})
	}
}

func TestHTTPCallbackRejectsDuplicateAuthenticationAndRevokesOnMarshalFailure(t *testing.T) {
	gateway := newTestCallbackGateway(t)
	var calls atomic.Int32
	request := requestForTest()
	request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) { calls.Add(1); return nil, nil }
	endpoints, cleanup := registerTestCallbacks(t, gateway, context.Background(), request)
	for _, invalid := range []string{"missing", "duplicate", "forged", "unknown path"} {
		t.Run(invalid, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/tools", strings.NewReader(`{"tool":"list_documents","arguments":{}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+endpoints.Tools.Token)
			switch invalid {
			case "missing":
				req.Header.Del("Authorization")
			case "duplicate":
				req.Header.Add("Authorization", "Bearer "+endpoints.Tools.Token)
			case "forged":
				req.Header.Set("Authorization", "Bearer "+strings.Repeat("0", 64))
			case "unknown path":
				req.URL.Path = "/api/v1/workspaces"
			}
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, req)
			if response.Code != 401 && response.Code != 404 {
				t.Fatalf("status=%d", response.Code)
			}
		})
	}
	cleanup()
	request.Resume = &ResumeRequest{Prompt: json.RawMessage(`invalid`)}
	if _, _, err := gateway.payload(context.Background(), request); !errors.Is(err, ErrFailed) {
		t.Fatalf("marshal failure=%v", err)
	}
	gateway.mu.Lock()
	count := len(gateway.entries)
	gateway.mu.Unlock()
	if calls.Load() != 0 || count != 0 {
		t.Fatal("invalid payload leaked a capability or reached handler")
	}
}

func TestHTTPCallbackConcurrentRunsAndRevocation(t *testing.T) {
	gateway := newTestCallbackGateway(t)
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := requestForTest()
			request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) { return map[string]bool{"ok": true}, nil }
			payload, cleanup, err := gateway.payload(ctx, request)
			if err != nil {
				t.Error(err)
				return
			}
			var endpoints callbackEndpoints
			_ = json.Unmarshal(payload, &endpoints)
			status, _ := callbackRequest(t, *endpoints.Tools, `{"tool":"list_documents","arguments":{}}`)
			if status != 200 {
				t.Errorf("status=%d", status)
			}
			cancel()
			cleanup()
		}()
	}
	group.Wait()
	gateway.mu.Lock()
	remaining := len(gateway.entries)
	gateway.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d capabilities", remaining)
	}
}

func TestHTTPHarnessCompletionRevokesAllCallbackCapabilities(t *testing.T) {
	var endpoints callbackEndpoints
	request := requestForTest()
	request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) { return map[string]bool{"ok": true}, nil }
	request.JournalHandler = func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"records": []any{}}, nil
	}
	request.MemoryHandler = func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"items": []any{}}, nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&endpoints); err != nil {
			t.Error(err)
			return
		}
		if status, _ := callbackRequest(t, *endpoints.Tools, `{"tool":"list_documents","arguments":{}}`); status != 200 {
			t.Error("live callback unavailable")
		}
		writeHTTPProtocol(w, "application/json")
		_, _ = io.WriteString(w, resultHTTPJSON(request))
	}))
	defer server.Close()
	client := newTestHTTPClient(t, server.URL)
	if _, err := client.Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []*retrievalEndpoint{endpoints.Tools, endpoints.Journal, endpoints.Memory} {
		if status, _ := callbackRequest(t, *endpoint, `{}`); status != 401 {
			t.Fatal("completed run retained callback authority")
		}
	}
}
