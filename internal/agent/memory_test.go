package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"magworkspace/internal/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMemoryTransportScopeSchemaAndSanitizedErrors(t *testing.T) {
	calls := 0
	secret := strings.Repeat("a", 64)
	handler := memoryHTTPHandler(secret, func(context.Context, string, json.RawMessage) (any, error) { calls++; return []any{}, nil })
	for _, tc := range []struct {
		body, auth string
		status     int
	}{
		{`{"operation":"list","arguments":{}}`, "", 401},
		{`{"operation":"list","arguments":{"user_id":"forged"}}`, secret, 400},
		{`{"operation":"get_items","arguments":{"item_ids":[],"workspace_id":"forged"}}`, secret, 400},
		{`{"operation":"forget","arguments":{"item_ids":[]}}`, secret, 400},
		{`{"operation":"list","arguments":{},"caller":"forged"}`, secret, 400},
		{`{"operation":"list","operation":"touch","arguments":{}}`, secret, 400},
		{`{"operation":"get_items","arguments":{"item_ids":null}}`, secret, 400},
		{`{"operation":"touch","arguments":{"item_ids":[],"now":null}}`, secret, 400},
		{`{"operation":"list","arguments":{}}`, secret, 200},
		{`{"operation":"get_items","arguments":{"item_ids":[]}}`, secret, 200},
		{`{"operation":"touch","arguments":{"item_ids":[],"now":100}}`, secret, 200},
	} {
		r := httptest.NewRequest("POST", "/memory", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", "application/json")
		if tc.auth != "" {
			r.Header.Set("Authorization", "Bearer "+tc.auth)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d want %d for %s: %s", w.Code, tc.status, tc.body, w.Body.String())
		}
	}
	if calls != 3 {
		t.Fatal(calls)
	}
	failing := memoryHTTPHandler(secret, func(context.Context, string, json.RawMessage) (any, error) {
		return nil, errors.New("private credential")
	})
	r := httptest.NewRequest("POST", "/memory", strings.NewReader(`{"operation":"list","arguments":{}}`))
	r.Header.Set("Authorization", "Bearer "+secret)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	failing.ServeHTTP(w, r)
	if w.Code != 403 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("failure leaked")
	}
}

func TestHarnessMemoryOnlyLiveJSONAndSSE(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			request := requestForTest()
			request.Query = "Markdown preference"
			var mu sync.Mutex
			calls := 0
			operations := map[string]int{}
			item := domain.Memory{ID: strings.Repeat("a", 32), Kind: "preference", Content: "Use concise Markdown.", Status: "active", CreatedAt: float64(time.Now().Unix()), Importance: 3}
			request.MemoryHandler = func(ctx context.Context, op string, raw json.RawMessage) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				operations[op]++
				if op == "touch" {
					return map[string]any{}, nil
				}
				return []domain.Memory{item}, nil
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Stream   bool                             `json:"stream"`
					Messages []struct{ Role, Content string } `json:"messages"`
					Tools    []struct {
						Function struct{ Name string } `json:"function"`
					} `json:"tools"`
				}
				if json.NewDecoder(r.Body).Decode(&payload) != nil {
					http.Error(w, "invalid", 400)
					return
				}
				mu.Lock()
				calls++
				step := calls
				mu.Unlock()
				if len(payload.Tools) != 1 || payload.Tools[0].Function.Name != "search_memory" {
					t.Errorf("memory-only tool set: %+v", payload.Tools)
					http.Error(w, "unexpected tools", 400)
					return
				}
				found := false
				for _, m := range payload.Messages {
					if m.Role == "system" && strings.Contains(m.Content, item.Content) {
						found = true
					}
				}
				if !found {
					t.Error("memory-only injection absent")
				}
				finish := "stop"
				message := map[string]any{"role": "assistant", "content": "Memory-only answer"}
				delta := map[string]any{"content": "Memory-only answer"}
				if step == 1 {
					finish = "tool_calls"
					function := map[string]any{"name": "search_memory", "arguments": `{"query":"Markdown"}`}
					message["content"] = nil
					message["tool_calls"] = []any{map[string]any{"id": "memory-tool", "type": "function", "function": function}}
					delta = map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "memory-tool", "type": "function", "function": function}}}
				}
				if payload.Stream {
					w.Header().Set("Content-Type", "text/event-stream")
					raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}}})
					fmt.Fprintf(w, "data: %s\n\n", raw)
					raw, _ = json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}})
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}})
			}))
			defer provider.Close()
			config := harnessConfig(t)
			config.Mode = "live"
			config.BaseURL = provider.URL
			config.APIKey = "only-local-provider-key"
			config.Model = "local-model"
			client := mustClient(t, config)
			var result Result
			var err error
			if stream {
				result, err = client.Stream(context.Background(), request, func(Event) error { return nil })
			} else {
				result, err = client.Run(context.Background(), request)
			}
			if err != nil || result.Answer != "Memory-only answer" || len(result.Messages) != 2 {
				t.Fatalf("memory-only result: %+v %v", result, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 2 || operations["list"] == 0 || operations["get_items"] == 0 || operations["touch"] == 0 {
				t.Fatalf("memory-only operations: %d %+v", calls, operations)
			}
		})
	}
}
