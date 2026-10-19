package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func retrievalRequest() Request {
	request := requestForTest()
	request.RunID = strings.Repeat("1", 32)
	request.SessionID = strings.Repeat("2", 32)
	request.WorkspaceID = strings.Repeat("3", 32)
	request.UserID = strings.Repeat("4", 32)
	return request
}

func evidenceForTest(request Request) map[string]any {
	content := "住宿报销上限为每晚 500 元。"
	hash := sha256.Sum256([]byte(content))
	return map[string]any{
		"evidence_id": strings.Repeat("5", 32), "run_id": request.RunID, "workspace_id": request.WorkspaceID,
		"document_id": strings.Repeat("6", 32), "version_id": strings.Repeat("7", 32), "index_id": strings.Repeat("8", 32),
		"generation": strings.Repeat("9", 32), "chunk_id": strings.Repeat("a", 32), "source_sha256": strings.Repeat("b", 64),
		"sha256": hex.EncodeToString(hash[:]), "title": "差旅制度", "content": content, "start_byte": 10, "end_byte": 10 + len(content),
	}
}

func TestHarnessRetrievalDemoAndStreamUseBackendEvidence(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			request := retrievalRequest()
			request.History = []Message{{Role: "user", Content: "previous"}, {Role: "assistant", Content: "previous reply"}}
			var calls atomic.Int32
			request.ToolHandler = func(ctx context.Context, name string, raw json.RawMessage) (any, error) {
				calls.Add(1)
				if name != "search_knowledge" || ctx.Err() != nil || strings.Contains(string(raw), "user_id") || strings.Contains(string(raw), "workspace_id") {
					t.Errorf("invalid callback %s %s", name, raw)
				}
				return map[string]any{"evidence": []any{evidenceForTest(request)}}, nil
			}
			client := mustClient(t, harnessConfig(t))
			var result Result
			var err error
			var events []Event
			if stream {
				result, err = client.Stream(context.Background(), request, func(event Event) error { events = append(events, event); return nil })
			} else {
				result, err = client.Run(context.Background(), request)
			}
			if err != nil || calls.Load() != 1 || !strings.Contains(result.Answer, "[E:"+strings.Repeat("5", 32)+"]") || !strings.Contains(result.Answer, "500") {
				t.Fatalf("real Harness retrieval failed: %+v calls=%d err=%v", result, calls.Load(), err)
			}
			if len(result.Messages) != 4 || result.Messages[0] != request.History[0] || result.Messages[2].Content != request.Query || result.Messages[3].Content != result.Answer {
				t.Fatalf("tool transcript escaped into persisted history: %+v", result.Messages)
			}
			if stream && (len(events) != 4 || events[0].Type != "message.start" || events[1].Type != "message.discard" || events[2].Type != "message.start" || events[3].Type != "message.delta") {
				t.Fatalf("tool round was not discarded: %+v", events)
			}
		})
	}
}

func TestHarnessRetrievalRejectsForeignScopeAndForgedContent(t *testing.T) {
	for _, kind := range []string{"workspace", "document", "run", "hash"} {
		t.Run(kind, func(t *testing.T) {
			request := retrievalRequest()
			ids := []string{strings.Repeat("6", 32)}
			request.DocumentIDs = &ids
			request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
				evidence := evidenceForTest(request)
				switch kind {
				case "workspace":
					evidence["workspace_id"] = strings.Repeat("c", 32)
				case "document":
					evidence["document_id"] = strings.Repeat("c", 32)
				case "run":
					evidence["run_id"] = strings.Repeat("c", 32)
				case "hash":
					evidence["content"] = "forged private content"
				}
				return map[string]any{"evidence": []any{evidence}}, nil
			}
			result, err := mustClient(t, harnessConfig(t)).Run(context.Background(), request)
			// Failed tool results may become an honest no-evidence response. They
			// must never become a cited or leaked passage in the final transcript.
			if err == nil && (strings.Contains(result.Answer, "[E:") || strings.Contains(result.Answer, "forged private content") || strings.Contains(result.Answer, "500")) {
				t.Fatalf("untrusted evidence escaped validation: %+v", result)
			}
		})
	}
}

func TestHarnessRetrievalEmptyScopeDoesNotExpand(t *testing.T) {
	request := retrievalRequest()
	ids := []string{}
	request.DocumentIDs = &ids
	request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
		// Even a buggy backend returning a valid document cannot broaden scope.
		return map[string]any{"evidence": []any{evidenceForTest(request)}}, nil
	}
	result, err := mustClient(t, harnessConfig(t)).Run(context.Background(), request)
	if err != nil || strings.Contains(result.Answer, "[E:") || strings.Contains(result.Answer, "500") {
		t.Fatalf("explicit empty scope expanded: %+v %v", result, err)
	}
}

// The following tests go through the actual ProcessClient private pipe, listener
// and Python process without relying on model behavior for denial coverage.
func TestToolTransportRejectsUntrustedInputsAndReapsListener(t *testing.T) {
	script := `import json,sys,urllib.request,urllib.error
req=json.load(sys.stdin)
ep=req["retrieval"]
assert req["document_ids"] is None
cases=[
 ({"tool":"search_documents","arguments":{"query":"xx"}},"wrong",401),
 ({"tool":"search_documents","arguments":{"query":"xx","user_id":"forged"}},ep["token"],400),
 ({"tool":"write_document","arguments":{}},ep["token"],400),
 ('{"tool":"search_documents","tool":"read_evidence","arguments":{"evidence_id":"x"}}',ep["token"],400),
 ('{"tool":"search_documents","arguments":{"query":"xx","query":"y"}}',ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"xx"*9000}},ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"valid","mode":"keyword"}},ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"valid","limit":11}},ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"好"}},ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"hello\nworld"}},ep["token"],400),
 ({"tool":"search_documents","arguments":{"query":"valid","mode":"lexical","limit":10}},ep["token"],200),
]
for body, token, expected in cases:
 raw=(body if isinstance(body,str) else json.dumps(body)).encode()
 r=urllib.request.Request(ep["url"],data=raw,headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
 try:
  with urllib.request.urlopen(r,timeout=2) as response:
   assert response.status==expected
 except urllib.error.HTTPError as response:
  assert response.code==expected
answer=ep["url"]
print(json.dumps({"run_id":req["run_id"],"session_id":req["session_id"],"status":"completed","answer":answer,"mode":"demo","messages":[{"role":"user","content":req["query"]},{"role":"assistant","content":answer}]}))
`
	request := requestForTest()
	var calls atomic.Int32
	request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
		calls.Add(1)
		return map[string]any{"evidence": []any{}}, nil
	}
	result, err := mustClient(t, fakeScriptConfig(t, script)).Run(context.Background(), request)
	if err != nil || calls.Load() != 1 {
		t.Fatalf("invalid callbacks reached handler: calls=%d error=%v", calls.Load(), err)
	}
	client := &http.Client{Timeout: 300 * time.Millisecond}
	response, err := client.Get(result.Answer)
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatal("per-run listener still accepts requests after process exit")
	}
}

func TestToolTransportScopeIsExplicitAndCapabilityPrivate(t *testing.T) {
	for _, ids := range []*[]string{nil, {}, new([]string)} {
		script := `import json,sys
r=json.load(sys.stdin)
assert "document_ids" in r
assert "ToolHandler" not in r and "tool_handler" not in r
answer=json.dumps(r["document_ids"])
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":answer,"mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":answer}]}))
`
		request := requestForTest()
		request.DocumentIDs = ids
		request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
			t.Fatal("unexpected callback")
			return nil, nil
		}
		result, err := mustClient(t, fakeScriptConfig(t, script)).Run(context.Background(), request)
		want := "null"
		if ids != nil {
			want = "[]"
		}
		if err != nil || result.Answer != want {
			t.Fatalf("scope changed: %s want %s error=%v", result.Answer, want, err)
		}
	}
}

func TestToolTransportCancellationClosesHandlerAndChild(t *testing.T) {
	script := `import json,sys,urllib.request
r=json.load(sys.stdin)
e=r["retrieval"]
req=urllib.request.Request(e["url"],data=b'{"tool":"search_documents","arguments":{"query":"xx"}}',headers={"Authorization":"Bearer "+e["token"],"Content-Type":"application/json"})
urllib.request.urlopen(req,timeout=60).read()
`
	request := requestForTest()
	started := make(chan struct{})
	ended := make(chan struct{})
	request.ToolHandler = func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(ended)
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := mustClient(t, fakeScriptConfig(t, script)).Run(ctx, request); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("retrieval did not reach handler")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation not propagated: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not reap process")
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("callback context outlived run")
	}
}

func TestToolTransportSanitizesErrorsAndBoundsResponses(t *testing.T) {
	for _, kind := range []string{"panic", "error", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			script := `import json,sys,urllib.request,urllib.error
r=json.load(sys.stdin); e=r["retrieval"]
req=urllib.request.Request(e["url"],data=b'{"tool":"search_documents","arguments":{"query":"xx"}}',headers={"Authorization":"Bearer "+e["token"],"Content-Type":"application/json"})
try: urllib.request.urlopen(req,timeout=2)
except urllib.error.HTTPError as response:
 body=response.read()
 assert response.code in (403,502) and b"PRIVATE" not in body and len(body)<1024
else: raise Exception("unexpected success")
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`
			request := requestForTest()
			request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
				switch kind {
				case "panic":
					panic("PRIVATE panic")
				case "error":
					return nil, errors.New("PRIVATE secret")
				default:
					return strings.Repeat("x", maxToolResponseBytes+1), nil
				}
			}
			result, err := mustClient(t, fakeScriptConfig(t, script)).Run(context.Background(), request)
			if err != nil || result.Answer != "ok" {
				t.Fatalf("callback failure escaped bounds: %+v %v", result, err)
			}
		})
	}
}

func TestRetrievalPythonAdapterRejectsNonLoopback(t *testing.T) {
	config := harnessConfig(t)
	bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`import sys,json
sys.path.insert(0,%q)
from retrieval import validate_endpoint
for url in ["https://example.com/tools", "http://localhost:123/tools", "http://127.0.0.1:123/tools?x=1", "http://127.0.0.1:123/other", "http://127.0.0.1:123@evil.com/tools"]:
 try: validate_endpoint({"url":url,"token":"a"*64})
 except ValueError: pass
 else: raise Exception("unsafe endpoint")
r=json.load(sys.stdin)
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`, bridgeDir)
	config.Script = filepath.Join(t.TempDir(), "adapter-test.py")
	if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := mustClient(t, config).Run(context.Background(), requestForTest())
	if err != nil || result.Answer != "ok" {
		t.Fatalf("unsafe endpoint accepted: %v", err)
	}
}

func TestHarnessLiveRetrievalToolsAndCitationValidation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scenario := range []string{"valid", "forged citation", "identity injection", "write tool"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, scenario), func(t *testing.T) {
				request := retrievalRequest()
				var toolCalls atomic.Int32
				request.ToolHandler = func(context.Context, string, json.RawMessage) (any, error) {
					toolCalls.Add(1)
					return map[string]any{"evidence": []any{evidenceForTest(request)}}, nil
				}
				var providerCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					if strings.Contains(string(raw), request.UserID) || strings.Contains(string(raw), `"retrieval"`) || strings.Contains(string(raw), `"token"`) {
						t.Error("private capability or caller entered model context")
					}
					var payload struct {
						Tools []any `json:"tools"`
					}
					if err := json.Unmarshal(raw, &payload); err != nil || len(payload.Tools) != 7 {
						t.Errorf("native provider did not receive the four read tools, ask_user and two approval-gated write tools: %s %v", raw, err)
					}
					if providerCalls.Add(1) == 1 {
						tool, args := "search_documents", `{"query":"住宿标准"}`
						if scenario == "identity injection" {
							args = `{"query":"住宿标准","workspace_id":"forged"}`
						}
						if scenario == "write tool" {
							tool, args = "write_document", `{"document_id":"forged","content":"replace"}`
						}
						call := map[string]any{"id": "read-call", "type": "function", "function": map[string]any{"name": tool, "arguments": args}}
						if stream {
							call["index"] = 0
							sendStream(w, providerDelta(map[string]any{"content": "planning draft", "tool_calls": []any{call}}, "tool_calls")+"data: [DONE]\n\n")
						} else {
							_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "planning draft", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
						}
						return
					}
					id := strings.Repeat("5", 32)
					if scenario == "forged citation" {
						id = strings.Repeat("c", 32)
					}
					answer := "住宿上限为 500 元。[E:" + id + "]"
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
				var events []Event
				if stream {
					result, err = client.Stream(context.Background(), request, func(event Event) error { events = append(events, event); return nil })
				} else {
					result, err = client.Run(context.Background(), request)
				}
				if scenario == "valid" {
					if err != nil || toolCalls.Load() != 1 || providerCalls.Load() != 2 || len(result.Messages) != 2 || !strings.Contains(result.Answer, "[E:") {
						t.Fatalf("native live retrieval failed: %+v toolcalls=%d providercalls=%d err=%v", result, toolCalls.Load(), providerCalls.Load(), err)
					}
					if stream && (len(events) != 5 || events[2].Type != "message.discard") {
						t.Fatalf("tool step planning was not discarded: %+v", events)
					}
				} else {
					if !errors.Is(err, ErrFailed) || result.Answer != "" {
						t.Fatalf("forged citation was committed: %+v %v", result, err)
					}
					if (scenario == "identity injection" || scenario == "write tool") && toolCalls.Load() != 0 {
						t.Fatal("untrusted tool crossed backend boundary")
					}
				}
			})
		}
	}
}

func TestRetrievalPythonAdapterBoundsResponseAndDeniesRedirect(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
	}))
	defer target.Close()
	for _, scenario := range []string{"redirect", "oversized", "duplicate", "timeout", "error"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch scenario {
				case "redirect":
					w.Header().Set("Location", target.URL+"/tools")
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", maxToolResponseBytes+1))
				case "duplicate":
					_, _ = io.WriteString(w, `{"evidence":[],"evidence":[]}`)
				case "timeout":
					select {
					case <-r.Context().Done():
					case <-time.After(500 * time.Millisecond):
					}
				case "error":
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, "PRIVATE backend error with sensitive content")
				}
			}))
			defer server.Close()
			config := harnessConfig(t)
			bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
			if err != nil {
				t.Fatal(err)
			}
			script := fmt.Sprintf(`import sys,json,asyncio
sys.path.insert(0,%q)
from retrieval import Backend,RetrievalError
backend=Backend({"url":%q,"token":"a"*64},timeout=0.1)
async def check():
 async with backend:
  try: await backend.call("search_documents",{"query":"query"})
  except RetrievalError as error:
   assert "PRIVATE" not in str(error) and "127.0.0.1" not in str(error) and "a"*64 not in str(error)
  else: raise Exception("invalid response accepted")
asyncio.run(check())
r=json.load(sys.stdin)
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`, bridgeDir, server.URL+"/tools")
			config.Script = filepath.Join(t.TempDir(), "adapter-test.py")
			if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
				t.Fatal(err)
			}
			result, err := mustClient(t, config).Run(context.Background(), requestForTest())
			if err != nil || result.Answer != "ok" {
				t.Fatalf("unsafe response accepted: %v", err)
			}
		})
	}
	if redirected.Load() {
		t.Fatal("private callback bearer followed a redirect")
	}
}

func TestHarnessLiveReadEvidenceRequiresPriorAcquisition(t *testing.T) {
	for _, acquired := range []bool{true, false} {
		t.Run(fmt.Sprint(acquired), func(t *testing.T) {
			request := retrievalRequest()
			var searches, reads atomic.Int32
			request.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				if name == "search_documents" {
					searches.Add(1)
					return map[string]any{"evidence": []any{evidenceForTest(request)}}, nil
				}
				reads.Add(1)
				return evidenceForTest(request), nil
			}
			var step atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := step.Add(1)
				name, args := "read_evidence", `{"evidence_id":"`+strings.Repeat("5", 32)+`"}`
				last := int32(2)
				if acquired {
					last = 3
					if n == 1 {
						name, args = "search_documents", `{"query":"住宿标准"}`
					}
				}
				message := map[string]any{"role": "assistant", "content": "500 元。[E:" + strings.Repeat("5", 32) + "]"}
				finish := "stop"
				if n < last {
					message["content"] = ""
					message["tool_calls"] = []any{map[string]any{"id": fmt.Sprint("call", n), "type": "function", "function": map[string]any{"name": name, "arguments": args}}}
					finish = "tool_calls"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}})
			}))
			defer server.Close()
			result, err := mustClient(t, liveStreamConfig(t, server.URL)).Run(context.Background(), request)
			if acquired {
				if err != nil || searches.Load() != 1 || reads.Load() != 1 || !strings.Contains(result.Answer, "[E:") {
					t.Fatalf("acquired evidence could not be re-read: %+v %v search=%d read=%d", result, err, searches.Load(), reads.Load())
				}
			} else if !errors.Is(err, ErrFailed) || searches.Load() != 0 || reads.Load() != 0 {
				t.Fatalf("unacquired ID reached backend or final answer: %+v %v reads=%d", result, err, reads.Load())
			}
		})
	}
}
