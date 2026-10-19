package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

// Actual HTTP/MySQL/ProcessClient/native Harness, with only the model and object
// service controlled locally. Each approval resumes in a new Python process.
func TestHTTPDocumentApprovalRealHarnessRestartJSONAndSSE(t *testing.T) {
	for _, scenario := range []string{"create-json", "update-sse", "reject-json", "completed-recovery", "before-write-crash", "after-write-crash", "two-writes-json"} {
		t.Run(scenario, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "writeharness")
			other := x.register(t, "writeother")
			session := x.session(t, owner, owner.Workspaces[0].ID, "审批")
			args := map[string]string{"title": "已审批文档", "filename": "approved.md", "content": "审批正文：精确保存\n第二行"}
			tool := "create_document"
			before := 0
			var original domain.DocumentWrite
			if scenario == "update-sse" {
				original = x.upload(t, owner, "原文：保留历史版本")
				before = 1
				tool = "update_document"
				args = map[string]string{"document_id": original.Document.ID, "base_version_id": original.Version.ID, "content": "更新正文：精确保存\n第二行"}
			}
			rawArgs, _ := json.Marshal(args)
			var mu sync.Mutex
			calls := 0
			var receipt domain.DocumentWrite
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				var payload struct {
					Stream   bool `json:"stream"`
					Messages []struct {
						Role, Content string
						ToolCallID    string `json:"tool_call_id"`
					} `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					http.Error(w, "bad request", 400)
					return
				}
				visible, _ := json.Marshal(payload.Messages)
				if strings.Contains(string(visible), owner.User.ID) || strings.Contains(string(visible), owner.Workspaces[0].ID) {
					t.Error("private authority fields entered model transcript")
				}
				step := calls
				calls++
				if scenario == "update-sse" && step == 0 {
					call := map[string]any{"id": "read-original", "type": "function", "function": map[string]string{"name": "read_document", "arguments": `{"document_id":"` + original.Document.ID + `","source":true}`}}
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
					return
				}
				if scenario == "update-sse" && step == 1 {
					found := false
					for _, m := range payload.Messages {
						if m.Role == "tool" && m.ToolCallID == "read-original" {
							var v struct {
								Source struct {
									Content string `json:"content"`
								} `json:"source"`
							}
							if json.Unmarshal([]byte(m.Content), &v) != nil || v.Source.Content != "原文：保留历史版本" {
								t.Errorf("original text unavailable: %s", m.Content)
							}
							found = true
						}
					}
					if !found {
						t.Error("original source result missing")
					}
				}
				if step == 0 || (scenario == "update-sse" && step == 1) {
					call := map[string]any{"id": "approved-write", "type": "function", "function": map[string]string{"name": tool, "arguments": string(rawArgs)}}
					toolCalls := []any{call}
					if scenario == "two-writes-json" {
						toolCalls = append(toolCalls, map[string]any{"id": "approved-write-2", "type": "function", "function": map[string]string{"name": tool, "arguments": string(rawArgs)}})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": toolCalls}, "finish_reason": "tool_calls"}}})
					return
				}
				found := false
				for _, m := range payload.Messages {
					if m.Role == "tool" && strings.HasPrefix(m.ToolCallID, "approved-write") {
						found = true
						if scenario == "reject-json" {
							if !strings.HasPrefix(m.Content, "Error:") {
								t.Errorf("rejected call succeeded: %s", m.Content)
							}
						} else if err := json.Unmarshal([]byte(m.Content), &receipt); err != nil || receipt.Version.ID == "" {
							t.Errorf("missing committed write receipt: %s", m.Content)
						}
					}
				}
				if !found {
					t.Error("write result absent from native history")
				}
				answer := "审批处理完成"
				if payload.Stream {
					chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": answer}, "finish_reason": "stop"}}})
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"role": "assistant", "content": answer}, "finish_reason": "stop"}}})
				}
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
			script := filepath.Join("..", "..", "bridges", "magent", "bridge.py")
			if strings.HasSuffix(scenario, "write-crash") {
				bridgeDir, e := filepath.Abs(filepath.Dir(script))
				if e != nil {
					t.Fatal(e)
				}
				pathJSON, _ := json.Marshal(bridgeDir)
				crashBefore := "False"
				if scenario == "before-write-crash" {
					crashBefore = "True"
				}
				code := fmt.Sprintf(`import sys,os
sys.path.insert(0,%s)
import bridge,retrieval
original_call=retrieval.Backend.call
async def crashing_call(self,tool,args,**kwargs):
    if tool in {"create_document","update_document"} and %s:
        os._exit(73)
    result=await original_call(self,tool,args,**kwargs)
    if tool in {"create_document","update_document"}:
        os._exit(74)
    return result
retrieval.Backend.call=crashing_call
sys.exit(bridge.main())
`, pathJSON, crashBefore)
				script = filepath.Join(t.TempDir(), "write_crash.py")
				if e = os.WriteFile(script, []byte(code), 0600); e != nil {
					t.Fatal(e)
				}
			}
			recovering := false
			start := func() {
				client, err := agent.NewProcessClient(agent.Config{Python: python, Script: script, HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "local-controlled-provider", Timeout: 20 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				x.server.Close()
				var runtime agent.Client = client
				if scenario == "completed-recovery" && !recovering {
					runtime = &lostHarnessResult{client: client}
				}
				x.server = httptest.NewServer(httpapi.New(x.store, runtime, x.config))
			}
			start()
			request := map[string]any{"content": "请提出文档修改并等待我批准"}
			if scenario == "update-sse" {
				request["document_ids"] = []string{original.Document.ID}
			}
			response := x.call("POST", sessionPath(session)+"/messages", owner.Token, request)
			pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
			promptRequirePrivate(t, response.body)
			if pending.Prompt.Kind != "tool_approval" || pending.Prompt.Approval == nil || pending.Prompt.Approval.Tool != tool || pending.Prompt.Approval.ContentBytes != len(args["content"]) {
				t.Fatalf("missing exact approval proposal: %+v", pending)
			}
			_, total, err := x.store.Documents(context.Background(), owner.User.ID, session.WorkspaceID, 20, 0)
			if err != nil || total != before {
				t.Fatal("native tool wrote before approval", err, total)
			}
			path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
			for _, malformed := range []struct {
				body, media string
				status      int
			}{
				{`{"approved":true,"approved":false}`, "application/json", 400},
				{`{"approved":true}`, "text/plain", 415},
			} {
				response, _ := documentHTTP(x.integrationFixture, "POST", path+"/approve", owner.Token, malformed.media, []byte(malformed.body))
				checkIntegrationResponse(t, response, malformed.status)
			}

			x.request(t, "GET", path, other.Token, nil, 404)
			x.request(t, "POST", path+"/approve", other.Token, map[string]bool{"approved": true}, 404)
			x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "true"}, 400)
			for _, body := range []any{map[string]any{}, map[string]any{"approved": nil}, map[string]any{"approved": "true"}, map[string]any{"approved": true, "modified_args": args}} {
				x.request(t, "POST", path+"/approve", owner.Token, body, 400)
			}
			start()
			proposal := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
			if proposal.Approval.Arguments == nil {
				t.Fatal("restart lost reviewable contents")
			}
			if strings.HasSuffix(scenario, "write-crash") || scenario == "completed-recovery" {
				x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": true}, 502)
				interim := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
				committed := scenario != "before-write-crash"
				if (interim.Approval.Result != nil) != committed {
					t.Fatalf("wrong durable receipt at interruption: %+v", interim)
				}
				script = filepath.Join("..", "..", "bridges", "magent", "bridge.py")
				recovering = true
				start()
				if scenario == "completed-recovery" {
					out := integrationData[recoveryHTTPResult](t, x.request(t, "POST", recoveryPath(session, pending.Run.ID), owner.Token, map[string]any{}, 201))
					if out.Run.Status != "completed" {
						t.Fatal("known completed write did not recover")
					}
				} else {
					x.request(t, "POST", recoveryPath(session, pending.Run.ID), owner.Token, map[string]any{}, 502)
					_, total, e := x.store.Documents(context.Background(), owner.User.ID, session.WorkspaceID, 20, 0)
					want := 0
					if committed {
						want = 1
					}
					if e != nil || total != want {
						t.Fatal("unknown write replayed", e, total)
					}
					mu.Lock()
					cnt := calls
					mu.Unlock()
					if cnt != 1 {
						t.Fatal("unsafe recovery reached provider")
					}
					return
				}
			} else if scenario == "update-sse" {
				res, _, cancel := sseTestRequest(t, x.integrationFixture, path+"/approve", owner.Token, map[string]bool{"approved": true}, "")
				body, e := io.ReadAll(res.Body)
				cancel()
				res.Body.Close()
				if e != nil || res.StatusCode != 200 || !strings.Contains(string(body), "event: run.completed") || strings.Contains(string(body), "event: run.failed") {
					t.Fatalf("SSE approval failed: %d %v %s", res.StatusCode, e, body)
				}
				promptRequirePrivate(t, body)
			} else {
				if scenario == "two-writes-json" {
					next := integrationData[promptHTTPResult](t, x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": true}, 202))
					if next.Run.ID != pending.Run.ID || next.Prompt.Kind != "tool_approval" || next.Prompt.ID == pending.Prompt.ID {
						t.Fatal("next approval missing")
					}
					first := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
					if first.Approval.Result == nil || first.Status != "done" {
						t.Fatal("first write receipt not committed before next approval")
					}
					x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": true}, 409)
					path = promptPath(session, next.Run.ID, next.Prompt.ID)
					start()
				}
				out := integrationData[promptHTTPResult](t, x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": scenario != "reject-json"}, 201))
				if out.Run.Status != "completed" {
					t.Fatalf("approval did not complete: %+v", out)
				}
			}
			x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": true}, 409)
			final := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
			mu.Lock()
			got, cnt := receipt, calls
			mu.Unlock()
			expectedCalls := 2
			if scenario == "update-sse" {
				expectedCalls = 3
			}
			if cnt != expectedCalls {
				t.Fatalf("provider replayed: %d", cnt)
			}
			if scenario == "reject-json" {
				if final.Approval.Result != nil {
					t.Fatal("rejected receipt")
				}
				_, total, err = x.store.Documents(context.Background(), owner.User.ID, session.WorkspaceID, 20, 0)
				if err != nil || total != 0 {
					t.Fatal("rejected write published")
				}
				return
			}
			if final.Approval.Result == nil || final.Approval.Result.Version.ID != got.Version.ID {
				t.Fatalf("approval receipt absent: %+v", final)
			}
			_, content, err := x.store.DocumentContent(context.Background(), owner.User.ID, session.WorkspaceID, got.Document.ID, got.Version.ID)
			if err != nil || string(content) != args["content"] {
				t.Fatalf("written bytes differ: %v", err)
			}
			if scenario == "update-sse" {
				_, old, e := x.store.DocumentContent(context.Background(), owner.User.ID, session.WorkspaceID, original.Document.ID, original.Version.ID)
				if e != nil || string(old) != "原文：保留历史版本" || got.Version.Number != 2 {
					t.Fatal("update did not preserve history", e)
				}
			}
			x.process(t)
		})
	}
}
