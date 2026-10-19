package agent

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
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the actual bridge, not a Python-only scheduler assembled by the test.
func TestHarnessToolPipelineParallelOrdering(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var active, peak, started, completed, models atomic.Int32
			eight := make(chan struct{})
			others := make(chan struct{})
			req := retrievalRequest()
			req.ToolHandler = func(ctx context.Context, name string, raw json.RawMessage) (any, error) {
				if name != "list_documents" {
					return nil, fmt.Errorf("unexpected tool")
				}
				var args struct {
					Offset int `json:"offset"`
				}
				if err := json.Unmarshal(raw, &args); err != nil {
					return nil, err
				}
				n := active.Add(1)
				defer active.Add(-1)
				for p := peak.Load(); n > p; p = peak.Load() {
					if peak.CompareAndSwap(p, n) {
						break
					}
				}
				if started.Add(1) == 8 {
					close(eight)
				}
				select {
				case <-eight:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if args.Offset == 0 {
					select {
					case <-others:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				} else if completed.Add(1) == 11 {
					close(others)
				}
				return map[string]any{"documents": []any{}, "offset": args.Offset}, nil
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Messages []struct {
						Role    string `json:"role"`
						ID      string `json:"tool_call_id"`
						Content string `json:"content"`
					} `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				if models.Add(1) == 1 {
					calls := make([]any, 0, 12)
					for i := range 12 {
						call := map[string]any{"id": fmt.Sprintf("c%d", i), "type": "function", "function": map[string]any{"name": "list_documents", "arguments": fmt.Sprintf(`{"offset":%d}`, i)}}
						if stream {
							call["index"] = i
						}
						calls = append(calls, call)
					}
					if stream {
						sendStream(w, providerDelta(map[string]any{"tool_calls": calls}, "tool_calls")+"data: [DONE]\n\n")
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": calls}, "finish_reason": "tool_calls"}}})
					}
					return
				}
				count := 0
				for _, message := range payload.Messages {
					if message.Role != "tool" {
						continue
					}
					var result struct {
						Offset int `json:"offset"`
					}
					if err := json.Unmarshal([]byte(message.Content), &result); err != nil {
						t.Error(err)
					}
					if message.ID != fmt.Sprintf("c%d", count) || result.Offset != count {
						t.Errorf("tool result ordering mismatch at %d", count)
					}
					count++
				}
				if count != 12 {
					t.Errorf("expected 12 complete results, got %d", count)
				}
				if stream {
					sendStream(w, providerChunk("done", "stop")+"data: [DONE]\n\n")
				} else {
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`)
				}
			}))
			defer provider.Close()
			client := mustClient(t, liveStreamConfig(t, provider.URL))
			var result Result
			var err error
			if stream {
				result, err = client.Stream(context.Background(), req, func(Event) error { return nil })
			} else {
				result, err = client.Run(context.Background(), req)
			}
			if err != nil || result.Answer != "done" || peak.Load() != 8 || active.Load() != 0 || started.Load() != 12 || models.Load() != 2 {
				t.Fatalf("pipeline failed: err=%v active=%d peak=%d started=%d models=%d", err, active.Load(), peak.Load(), started.Load(), models.Load())
			}
		})
	}
}

// Cancel inside a still-running Python process. Killing the process externally
// would also close urllib sockets and cannot detect the original to_thread bug.
func TestToolTransportCancellationClosesSocketBeforeProcessExit(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/started" {
			select {
			case <-started:
				fmt.Fprint(w, "started")
			case <-r.Context().Done():
			}
			return
		}
		if r.URL.Path == "/probe" {
			select {
			case <-canceled:
				fmt.Fprint(w, "closed")
			case <-r.Context().Done():
			}
			return
		}
		close(started)
		select {
		case <-r.Context().Done():
			canceled <- struct{}{}
		case <-time.After(5 * time.Second):
		}
	}))
	defer server.Close()
	config := harnessConfig(t)
	bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`import asyncio,json,sys
sys.path.insert(0,%q)
from retrieval import Backend
async def check():
 async with Backend({"url":%q,"token":"a"*64}) as backend:
  task=asyncio.create_task(backend.call("list_documents",{}))
  import httpx
  async with httpx.AsyncClient(trust_env=False,timeout=2) as probe:
   assert (await probe.get(%q)).text=="started"
   task.cancel()
   try: await task
   except asyncio.CancelledError: pass
   else: raise AssertionError("cancellation swallowed")
   assert (await probe.get(%q)).text=="closed"
 assert backend._client is None
asyncio.run(check())
r=json.load(sys.stdin)
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`, bridgeDir, server.URL+"/tools", server.URL+"/started", server.URL+"/probe")
	config.Script = filepath.Join(t.TempDir(), "cancel-transport.py")
	if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := mustClient(t, config).Run(context.Background(), requestForTest())
	if err != nil || result.Answer != "ok" {
		t.Fatalf("cancel did not close socket during process lifetime: %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("tool callback never started")
	}
}

func TestToolTransportDeadlineBoundsTricklingBody(t *testing.T) {
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				canceled <- struct{}{}
				return
			case <-ticker.C:
				fmt.Fprint(w, " ")
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer server.Close()
	config := harnessConfig(t)
	bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`import asyncio,json,sys,time
sys.path.insert(0,%q)
from retrieval import Backend,RetrievalError
async def check():
 async with Backend({"url":%q,"token":"a"*64},timeout=0.15) as backend:
  started=time.monotonic()
  try: await backend.call("list_documents",{})
  except RetrievalError as error: assert str(error)=="retrieval unavailable"
  else: raise AssertionError("unbounded trickling response")
  assert time.monotonic()-started<2
asyncio.run(check())
r=json.load(sys.stdin)
print(json.dumps({"run_id":r["run_id"],"session_id":r["session_id"],"status":"completed","answer":"ok","mode":"demo","messages":[{"role":"user","content":r["query"]},{"role":"assistant","content":"ok"}]}))
`, bridgeDir, server.URL+"/tools")
	config.Script = filepath.Join(t.TempDir(), "deadline-transport.py")
	if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := mustClient(t, config).Run(context.Background(), requestForTest())
	if err != nil || result.Answer != "ok" || strings.Contains(result.Answer, "Bearer") {
		t.Fatalf("deadline failed: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("deadline left socket open")
	}
}
