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
	"sync/atomic"
	"testing"
	"time"
)

func answerPrompt(t *testing.T, req Request, prompt *UserPrompt, answer string) *ResumeRequest {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id": prompt.ID, "kind": "ask_user", "workspace_id": req.WorkspaceID, "user_id": req.UserID, "session_id": req.SessionID, "run_id": req.RunID,
		"tool_call_id": prompt.ToolCallID, "request": map[string]any{"question": prompt.Question, "options": prompt.Options}, "status": "answered", "snapshot": prompt.Snapshot,
		"response": map[string]string{"answer": answer}, "created_at": float64(time.Now().Unix()), "expires_at": prompt.ExpiresAt, "resolved_at": float64(time.Now().Unix()), "resolved_by": req.UserID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &ResumeRequest{Prompt: raw}
}

func TestHarnessWorkspaceQuestionsSuspendResumeAndRepeat(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			var restored atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, args json.RawMessage) (any, error) {
				if name != "restore_evidence" || string(args) != "{}" {
					t.Errorf("unexpected callback %s %s", name, args)
				}
				restored.Add(1)
				return map[string]any{"evidence": []any{}}, nil
			}
			var steps atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var payload struct {
					Tools []struct {
						Function struct {
							Name string `json:"name"`
						} `json:"function"`
					} `json:"tools"`
				}
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Error(err)
				}
				got := map[string]bool{}
				for _, tool := range payload.Tools {
					got[tool.Function.Name] = true
				}
				if len(got) != 7 || !got["create_document"] || !got["update_document"] || !got["ask_user"] || !got["search_knowledge"] || !got["read_document"] || !got["list_documents"] || !got["search_conversations"] {
					t.Errorf("unexpected model tools %v", got)
				}
				n := steps.Add(1)
				if n < 3 {
					if n == 2 && !strings.Contains(string(body), "国内") {
						t.Error("native resume did not insert answer")
					}
					args := `{"question":"国内还是海外差旅？","options":["国内","海外"]}`
					if n == 2 {
						args = `{"question":"哪个城市？","options":["北京","上海"]}`
					}
					call := map[string]any{"id": fmt.Sprintf("ask-%d", n), "type": "function", "function": map[string]any{"name": "ask_user", "arguments": args}}
					if stream {
						call["index"] = 0
						sendStream(w, providerDelta(map[string]any{"tool_calls": []any{call}}, "tool_calls")+"data: [DONE]\n\n")
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
					}
					return
				}
				if !strings.Contains(string(body), "北京") {
					t.Error("second answer not restored")
				}
				if stream {
					sendStream(w, providerChunk("已明确国内北京差旅。", "stop")+"data: [DONE]\n\n")
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "已明确国内北京差旅。"}, "finish_reason": "stop"}}})
				}
			}))
			defer server.Close()
			client := mustClient(t, liveStreamConfig(t, server.URL))
			run := func() (Result, error) {
				if stream {
					return client.Stream(context.Background(), req, func(e Event) error {
						if strings.Contains(e.Delta, "snapshot") || strings.Contains(e.Delta, "ask-") {
							t.Error("private prompt escaped as text")
						}
						return nil
					})
				}
				return client.Run(context.Background(), req)
			}
			first, err := run()
			if err != nil || first.Status != "suspended" || first.Prompt == nil || first.Answer != "" {
				t.Fatalf("first question %+v %v", first, err)
			}
			req.Resume = answerPrompt(t, req, first.Prompt, "国内")
			second, err := run()
			if err != nil || second.Status != "suspended" || second.Prompt == nil || second.Prompt.ID == first.Prompt.ID || second.Prompt.Question != "哪个城市？" {
				t.Fatalf("second question %+v %v", second, err)
			}
			req.Resume = answerPrompt(t, req, second.Prompt, "北京")
			final, err := run()
			if err != nil || final.Status != "completed" || final.Prompt != nil || final.Answer != "已明确国内北京差旅。" || restored.Load() != 2 || steps.Load() != 3 {
				t.Fatalf("resume final %+v %v restores=%d calls=%d", final, err, restored.Load(), steps.Load())
			}
		})
	}
}

func TestHarnessWorkspaceResumeRejectsScopeIdentityAndWritePolicy(t *testing.T) {
	req := retrievalRequest()
	req.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"evidence": []any{}}, nil
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		call := map[string]any{"id": "question", "type": "function", "function": map[string]any{"name": "ask_user", "arguments": `{"question":"Which?"}`}}
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
	}))
	defer server.Close()
	client := mustClient(t, liveStreamConfig(t, server.URL))
	first, err := client.Run(context.Background(), req)
	if err != nil || first.Prompt == nil {
		t.Fatalf("suspend: %+v %v", first, err)
	}
	for _, kind := range []string{"scope", "workspace", "run", "write", "tool", "question", "expired"} {
		t.Run(kind, func(t *testing.T) {
			prompt := *first.Prompt
			var snapshot map[string]any
			if err := json.Unmarshal([]byte(prompt.Snapshot), &snapshot); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "scope":
				snapshot["scope"].(map[string]any)["document_ids"] = []string{strings.Repeat("6", 32)}
			case "workspace":
				snapshot["caller"].(map[string]any)["workspace_id"] = strings.Repeat("f", 32)
			case "run":
				snapshot["run_id"] = strings.Repeat("f", 32)
			case "write":
				snapshot["policy"].(map[string]any)["access"] = "auto_write"
			case "tool":
				snapshot["policy"].(map[string]any)["allowed_tools"] = []string{"write_document"}
			case "question":
				prompt.Question = "Changed question"
			case "expired":
				prompt.ExpiresAt = float64(time.Now().Add(-time.Hour).Unix())
			}
			raw, _ := json.Marshal(snapshot)
			prompt.Snapshot = string(raw)
			resumed := req
			resumed.Resume = answerPrompt(t, resumed, &prompt, "yes")
			_, err := client.Run(context.Background(), resumed)
			if !errors.Is(err, ErrFailed) || calls.Load() != 1 {
				t.Fatalf("unsafe resume reached model %v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestHarnessWorkspaceReadToolsRegisterVerifiedEvidence(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			evidence := evidenceForTest(req)
			metadata := func() map[string]any {
				return map[string]any{
					"document": map[string]any{"id": evidence["document_id"], "workspace_id": req.WorkspaceID, "title": "差旅制度", "current_version_id": evidence["version_id"], "created_by": req.UserID},
					"version":  map[string]any{"id": evidence["version_id"], "document_id": evidence["document_id"], "filename": "rules.md"},
					"index":    map[string]any{"id": evidence["index_id"], "document_id": evidence["document_id"], "version_id": evidence["version_id"], "status": "ready"},
				}
			}
			var toolCount atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, args json.RawMessage) (any, error) {
				toolCount.Add(1)
				switch name {
				case "search_knowledge":
					return map[string]any{"evidence": []any{evidence}}, nil
				case "read_document":
					result := metadata()
					result["evidence"] = []any{evidence}
					result["offset"] = 0
					result["has_more"] = false
					return result, nil
				case "list_documents":
					return map[string]any{"documents": []any{metadata()}, "offset": 0, "limit": 10, "has_more": false}, nil
				case "search_conversations":
					return map[string]any{"items": []any{map[string]any{"question": "以前的配置", "answer": "历史[E:" + strings.Repeat("c", 32) + "]"}}, "has_more": false}, nil
				default:
					t.Errorf("unexpected tool %s", name)
					return nil, errors.New("bad tool")
				}
			}
			var steps atomic.Int32
			names := []string{"search_knowledge", "read_document", "list_documents", "search_conversations"}
			args := []string{`{"query":"住宿标准","mode":"keyword"}`, `{"evidence_id":"` + strings.Repeat("5", 32) + `","context":1}`, `{"keyword":"制度","limit":10}`, `{"query":"配置"}`}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if strings.Contains(string(body), req.UserID) {
					t.Error("metadata leaked caller identity into model context")
				}
				n := int(steps.Add(1)) - 1
				if n < 4 {
					if n > 0 && !strings.Contains(string(body), "[E:"+strings.Repeat("5", 32)+"]") {
						t.Error("verified evidence citation missing from tool output")
					}
					call := map[string]any{"id": fmt.Sprintf("read-%d", n), "type": "function", "function": map[string]any{"name": names[n], "arguments": args[n]}}
					if stream {
						call["index"] = 0
						sendStream(w, providerDelta(map[string]any{"tool_calls": []any{call}}, "tool_calls")+"data: [DONE]\n\n")
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
					}
					return
				}
				if !strings.Contains(string(body), "historical source; retrieve again") || strings.Contains(string(body), strings.Repeat("c", 32)) {
					t.Error("history boundary lost its source notice or exposed the old evidence ID")
				}
				answer := "每晚500元。[E:" + strings.Repeat("5", 32) + "]"
				if stream {
					sendStream(w, providerChunk(answer, "stop")+"data: [DONE]\n\n")
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": answer}, "finish_reason": "stop"}}})
				}
			}))
			defer server.Close()
			client := mustClient(t, liveStreamConfig(t, server.URL))
			var result Result
			var err error
			if stream {
				result, err = client.Stream(context.Background(), req, func(Event) error { return nil })
			} else {
				result, err = client.Run(context.Background(), req)
			}
			if err != nil || result.Status != "completed" || toolCount.Load() != 4 || steps.Load() != 5 {
				t.Fatalf("new tools failed %+v %v tools=%d model=%d", result, err, toolCount.Load(), steps.Load())
			}
		})
	}
}

func TestHarnessWorkspaceResumeRestoresCitationRegistry(t *testing.T) {
	req := retrievalRequest()
	var restored atomic.Int32
	req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
		if name == "restore_evidence" {
			restored.Add(1)
		} else if name != "search_knowledge" {
			t.Errorf("unexpected tool %s", name)
		}
		return map[string]any{"evidence": []any{evidenceForTest(req)}}, nil
	}
	var steps atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := steps.Add(1)
		if n < 3 {
			name, args := "search_knowledge", `{"query":"住宿标准"}`
			if n == 2 {
				name, args = "ask_user", `{"question":"国内还是海外？"}`
			}
			call := map[string]any{"id": fmt.Sprintf("step-%d", n), "type": "function", "function": map[string]any{"name": name, "arguments": args}}
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
			return
		}
		answer := "500元。[E:" + strings.Repeat("5", 32) + "]"
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": answer}, "finish_reason": "stop"}}})
	}))
	defer server.Close()
	client := mustClient(t, liveStreamConfig(t, server.URL))
	first, err := client.Run(context.Background(), req)
	if err != nil || first.Prompt == nil {
		t.Fatalf("suspend after retrieval %+v %v", first, err)
	}
	req.Resume = answerPrompt(t, req, first.Prompt, "国内")
	final, err := client.Run(context.Background(), req)
	if err != nil || restored.Load() != 1 || !strings.Contains(final.Answer, "[E:") {
		t.Fatalf("restored citation rejected %+v %v count=%d", final, err, restored.Load())
	}
}

func TestHarnessWorkspaceStreamCanSuspendAgainBeforeModelCall(t *testing.T) {
	req := retrievalRequest()
	req.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"evidence": []any{}}, nil
	}
	var steps atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if steps.Add(1) == 1 {
			calls := []any{}
			for i, question := range []string{"第一项？", "第二项？"} {
				args, _ := json.Marshal(map[string]string{"question": question})
				calls = append(calls, map[string]any{"index": i, "id": fmt.Sprintf("ask-%d", i), "type": "function", "function": map[string]any{"name": "ask_user", "arguments": string(args)}})
			}
			sendStream(w, providerDelta(map[string]any{"tool_calls": calls}, "tool_calls")+"data: [DONE]\n\n")
			return
		}
		sendStream(w, providerChunk("两项已确认。", "stop")+"data: [DONE]\n\n")
	}))
	defer server.Close()
	client := mustClient(t, liveStreamConfig(t, server.URL))
	first, err := client.Stream(context.Background(), req, func(Event) error { return nil })
	if err != nil || first.Prompt == nil {
		t.Fatalf("first question %+v %v", first, err)
	}
	req.Resume = answerPrompt(t, req, first.Prompt, "第一项答案")
	second, err := client.Stream(context.Background(), req, func(Event) error { t.Error("pending second question must not produce model text"); return nil })
	if err != nil || second.Prompt == nil || second.Prompt.Question != "第二项？" || steps.Load() != 1 {
		t.Fatalf("immediate re-suspension %+v %v calls=%d", second, err, steps.Load())
	}
	req.Resume = answerPrompt(t, req, second.Prompt, "第二项答案")
	final, err := client.Stream(context.Background(), req, func(Event) error { return nil })
	if err != nil || final.Status != "completed" || steps.Load() != 2 {
		t.Fatalf("resume after pending questions %+v %v calls=%d", final, err, steps.Load())
	}
}

func TestToolTransportWorkspaceScopesAndPaginationAreClosed(t *testing.T) {
	script := `import json,sys,urllib.request,urllib.error
r=json.load(sys.stdin); ep=r["retrieval"]
invalid=[
 ("search_knowledge",{"query":"查询","mode":"lexical"}),
 ("search_knowledge",{"query":"查询","workspace_id":"forged"}),
 ("list_documents",{"limit":21}),
 ("list_documents",{"user_id":"forged"}),
 ("read_document",{"document_id":"d","offset":1}),
 ("read_document",{"document_id":"d","version_id":"v"}),
 ("read_document",{"document_id":"d","evidence_id":"e"}),
 ("read_document",{"evidence_id":"e","query":"query"}),
 ("read_document",{"document_id":"d","context":4}),
 ("search_conversations",{"query":"query","session_id":"forged"}),
 ("search_conversations",{"query":"query","limit":9}),
 ("restore_evidence",{"run_id":"forged"}),
 ("resolve_citation_aliases",{"evidence_ids":[]}),
 ("resolve_citation_aliases",{"evidence_ids":["a"*32],"run_id":"forged"}),
 ("resolve_citation_aliases",{"evidence_ids":["a"*32,"a"*32]}),
 ("resolve_citation_aliases",{"evidence_ids":["bad"]}),
 ("resolve_citation_aliases",{"evidence_ids":[format(i,"032x") for i in range(65)]}),
 ("ask_user",{"question":"cannot call native prompt over callback"}),
]
for name,args in invalid:
 body=json.dumps({"tool":name,"arguments":args}).encode()
 request=urllib.request.Request(ep["url"],data=body,headers={"Authorization":"Bearer "+ep["token"],"Content-Type":"application/json"})
 try:
  urllib.request.urlopen(request,timeout=2).close()
 except urllib.error.HTTPError as error:
  assert error.code==400
  error.close()
 else: raise Exception("invalid arguments accepted")
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`
	req := retrievalRequest()
	req.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
		t.Error("unsafe input reached authenticated handler")
		return nil, nil
	}
	result, err := mustClient(t, fakeScriptConfig(t, script)).Run(context.Background(), req)
	if err != nil || result.Answer != "ok" {
		t.Fatalf("closed tool boundary %+v %v", result, err)
	}
}
