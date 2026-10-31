package httpapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"magworkspace/internal/domain"
)

// A real FastAPI/Harness stream reaches its final synthesis after all 12 tool
// rounds. The provider is local; no paid model or application data is used.
func TestHTTPHarnessServiceFinalSynthesisAfterToolLimit(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "synthesis")
	session := x.session(t, owner, owner.Workspaces[0].ID, "工具上限后汇总")
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Stream   bool  `json:"stream"`
			Tools    []any `json:"tools"`
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			return
		}
		step := calls.Add(1)
		if !input.Stream {
			t.Error("expected native model stream")
		}
		toolResults := 0
		for _, message := range input.Messages {
			if message.Role == "tool" {
				toolResults++
			}
		}
		if step <= 12 {
			if len(input.Tools) == 0 || toolResults != int(step)-1 {
				t.Error("missing tool round facts")
			}
			call := map[string]any{"index": 0, "id": fmt.Sprintf("read-%d", step), "type": "function", "function": map[string]string{"name": "list_documents", "arguments": `{}`}}
			chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
			return
		}
		if step != 13 || len(input.Tools) != 0 || toolResults != 12 {
			t.Error("invalid final synthesis context")
		}
		harnessHTTPModelReply(w, harnessHTTPModelRequest{Stream: true}, "已完成十二轮资料核对并汇总。", "", "")
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	installHarnessHTTPClient(x, harnessHTTPClient(t, service.url, callback))
	completed := harnessHTTPCompletedSSE(t, x, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "核对资料后汇总", "document_ids": []string{}})
	if calls.Load() != 13 || completed.Answer != "已完成十二轮资料核对并汇总。" {
		t.Fatal(calls.Load(), completed)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 2 || messages[1].Content != completed.Answer || messages[1].RunID != completed.Run.ID {
		t.Fatal("final answer was not durably saved", messages)
	}
}
