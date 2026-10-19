package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func memoryPath(wid string) string { return workspacePath(wid) + "/memories" }
func TestHTTPMemoryManagementIsolationRestartAndRevocation(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "memoryowner")
	viewer := f.register(t, "memoryviewer")
	wid := owner.Workspaces[0].ID
	f.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
	f.request(t, "POST", memoryPath(wid), owner.Token, map[string]any{"kind": "preference", "content": "secret owner memory", "user_id": viewer.User.ID}, 400)
	item := integrationData[domain.Memory](t, f.request(t, "POST", memoryPath(wid), owner.Token, map[string]any{"kind": "preference", "content": "私有记忆 Markdown", "topic": "格式"}, 201))
	f.request(t, "DELETE", memoryPath(wid)+"/"+item.ID, viewer.Token, nil, 404)
	list := integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid), viewer.Token, nil, 200))
	if len(list) != 0 {
		t.Fatal("viewer saw owner's personal memories")
	}
	own := integrationData[domain.Memory](t, f.request(t, "POST", memoryPath(wid), viewer.Token, map[string]any{"kind": "profile", "content": "viewer personal preference"}, 201))
	f.restart(t)
	list = integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(list) != 1 || list[0].ID != item.ID {
		t.Fatal("restart lost memory")
	}
	f.request(t, "DELETE", memoryPath(wid)+"/"+item.ID, owner.Token, nil, 204)
	f.request(t, "DELETE", memoryPath(wid)+"/"+item.ID, owner.Token, nil, 204)
	f.request(t, "POST", memoryPath(wid), owner.Token, map[string]any{"kind": "preference", "content": " 私有记忆\nMarkdown ", "topic": "different new topic"}, 409)
	list = integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid)+"?status=archived", owner.Token, nil, 200))
	if len(list) != 1 || list[0].Status != "archived" {
		t.Fatal("missing tombstone status")
	}
	f.request(t, "DELETE", workspacePath(wid)+"/members/"+viewer.User.ID, owner.Token, nil, 204)
	f.request(t, "GET", memoryPath(wid), viewer.Token, nil, 404)
	f.request(t, "DELETE", memoryPath(wid)+"/"+own.ID, viewer.Token, nil, 404)
	f.request(t, "POST", memoryPath(wid), viewer.Token, map[string]any{"kind": "fact", "content": "denied"}, 404)
	f.request(t, "GET", memoryPath(wid)+"?status=invalid", owner.Token, nil, 400)
	f.request(t, "GET", memoryPath(wid)+"?user_id="+viewer.User.ID, owner.Token, nil, 400)
}

// A controlled local provider verifies the actual model input. Go HTTP, MySQL,
// ProcessClient and fresh Python Harness processes remain real.
func TestHTTPMemoryRealHarnessCrossSessionRestartForgetAndSSE(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true, RunTimeout: 20 * time.Second})
	owner := f.register(t, "memoryreal")
	wid := owner.Workspaces[0].ID
	source := f.session(t, owner, wid, "audit source")
	remembered := "Remembered preference: concise Markdown responses."
	item := integrationData[domain.Memory](t, f.request(t, "POST", memoryPath(wid), owner.Token, map[string]any{"kind": "preference", "content": remembered, "topic": "format", "source_session_id": source.ID}, 201))
	var mu sync.Mutex
	expectPresent := true
	calls := 0
	toolCalls := 0
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var payload struct {
			Stream   bool `json:"stream"`
			Messages []struct {
				Role, Content string
				ToolCallID    string `json:"tool_call_id"`
			} `json:"messages"`
			Tools []struct {
				Function struct{ Name string } `json:"function"`
			} `json:"tools"`
		}
		if json.NewDecoder(r.Body).Decode(&payload) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		calls++
		hasMemory := false
		hasTool := false
		hasResult := false
		for _, tool := range payload.Tools {
			if tool.Function.Name == "search_memory" {
				hasTool = true
			}
		}
		for _, m := range payload.Messages {
			if strings.Contains(m.Content, "127.0.0.1") || strings.Contains(m.Content, "local-test-key") || strings.Contains(m.Content, "memory_endpoint") {
				t.Error("private capability entered model input")
			}
			if m.Role == "system" && strings.Contains(m.Content, remembered) {
				hasMemory = true
			}
			if m.Role == "tool" && m.ToolCallID == "personal-memory" {
				hasResult = true
				if strings.Contains(m.Content, remembered) != expectPresent {
					t.Errorf("search_memory returned stale or missing content: %s", m.Content)
				}
			}
		}
		if hasMemory != expectPresent || !hasTool {
			t.Errorf("authoritative injection present=%v want=%v tool=%v", hasMemory, expectPresent, hasTool)
			http.Error(w, "invalid context", 400)
			return
		}
		message := map[string]any{"role": "assistant", "content": "Memory contract checked."}
		finish := "stop"
		if !hasResult {
			toolCalls++
			message["content"] = nil
			message["tool_calls"] = []any{map[string]any{"id": "personal-memory", "type": "function", "function": map[string]any{"name": "search_memory", "arguments": `{"query":"Markdown"}`}}}
			finish = "tool_calls"
		}
		if payload.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			delta := map[string]any{"role": "assistant"}
			if hasResult {
				delta["content"] = "Memory contract checked."
			} else {
				delta["tool_calls"] = []any{map[string]any{"index": 0, "id": "personal-memory", "type": "function", "function": map[string]any{"name": "search_memory", "arguments": `{"query":"Markdown"}`}}}
			}
			raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}}})
			fmt.Fprintf(w, "data: %s\n\n", raw)
			raw, _ = json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}})
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}, "usage": map[string]int{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}})
	}))
	defer provider.Close()
	harness := os.Getenv("MAGENT_TEST_HARNESS_PATH")
	if harness == "" {
		harness = filepath.Join("..", "..", "..", "Magent Harness")
	}
	python := os.Getenv("MAGENT_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	start := func() {
		t.Helper()
		client, err := agent.NewProcessClient(agent.Config{Python: python, Script: filepath.Join("..", "..", "bridges", "magent", "bridge.py"), HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "local-provider", Timeout: 15 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		f.server.Close()
		f.server = httptest.NewServer(httpapi.New(f.store, client, f.config))
	}
	start()
	for index := 0; index < 2; index++ {
		session := f.session(t, owner, wid, fmt.Sprintf("cross-session %d", index))
		f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "Check my Markdown preference"}, 201)
		if index == 0 {
			f.restart(t)
			start()
		}
	}
	// A new process after forgetting may not inject old resident memory or return
	// it through the explicit tool. The SSE path shares that same verification.
	f.request(t, "DELETE", memoryPath(wid)+"/"+item.ID, owner.Token, nil, 204)
	mu.Lock()
	expectPresent = false
	mu.Unlock()
	session := f.session(t, owner, wid, "after forgetting")
	req, err := http.NewRequest("POST", f.server.URL+sessionPath(session)+"/messages", strings.NewReader(`{"content":"Check my Markdown preference"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+owner.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var stream strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, readErr := response.Body.Read(buffer)
		stream.Write(buffer[:n])
		if readErr != nil {
			break
		}
	}
	if response.StatusCode != 200 || !strings.Contains(stream.String(), "event: run.completed") || !strings.Contains(stream.String(), "event: message.delta") || strings.Contains(stream.String(), remembered) {
		t.Fatalf("SSE did not complete safely: %d %s", response.StatusCode, stream.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 6 || toolCalls != 3 {
		t.Fatalf("actual provider/tool calls: %d/%d", calls, toolCalls)
	}
	active, err := f.store.Memories(context.Background(), owner.User.ID, wid, "active")
	if err != nil || len(active) != 0 {
		t.Fatal("forgotten memory active")
	}
}
