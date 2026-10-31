package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

// Kill the actual Python worker while the controlled provider still holds its
// first response open. SQL has the run creation but no reliable model result.
func TestHTTPModelInflightCrashRecovery(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "inflightmodel")
			session := x.session(t, owner, owner.Workspaces[0].ID, "模型调用中断")
			const query = "继续当前请求，保留原始问题。"
			const draft = "UNCOMMITTED_MODEL_DRAFT"
			const answer = "恢复后重新调用模型完成。"
			var calls atomic.Int32
			entered, release := make(chan struct{}), make(chan struct{})
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Stream   bool            `json:"stream"`
					Messages []agent.Message `json:"messages"`
				}
				if r.Method != http.MethodPost || r.URL.Path != "/chat/completions" || json.NewDecoder(r.Body).Decode(&payload) != nil {
					t.Error("invalid controlled provider request")
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				if payload.Stream != streaming || len(payload.Messages) == 0 || payload.Messages[len(payload.Messages)-1].Content != query {
					t.Error("provider lost the requested transport or original query")
				}
				for _, message := range payload.Messages {
					if strings.Contains(message.Content, draft) {
						t.Error("uncommitted model draft entered the recovered context")
					}
				}
				step := calls.Add(1)
				if step == 1 {
					if streaming {
						w.Header().Set("Content-Type", "text/event-stream")
						chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": draft}, "finish_reason": nil}}})
						fmt.Fprintf(w, "data: %s\n\n", chunk)
						w.(http.Flusher).Flush()
					}
					close(entered)
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				if step != 2 {
					t.Errorf("unexpected model invocation after recovery: %d", step)
				}
				if streaming {
					w.Header().Set("Content-Type", "text/event-stream")
					chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": answer}, "finish_reason": "stop"}}})
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
				} else {
					fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}]}`, answer)
				}
			}))
			defer provider.Close()
			defer close(release) // Always unblock the provider, including failed assertions.

			bridgePath, err := filepath.Abs(filepath.Join("..", "..", "bridges", "magent", "bridge.py"))
			if err != nil {
				t.Fatal(err)
			}
			python, harness := os.Getenv("MAGENT_TEST_PYTHON"), os.Getenv("MAGENT_TEST_HARNESS_PATH")
			if python == "" {
				python = "python3"
			}
			if harness == "" {
				harness = filepath.Join("..", "..", "..", "Magent Harness")
			}
			directory := t.TempDir()
			pidPath, shim := filepath.Join(directory, "worker.pid"), filepath.Join(directory, "inflight_bridge.py")
			bridgeJSON, _ := json.Marshal(filepath.Dir(bridgePath))
			pidJSON, _ := json.Marshal(pidPath)
			code := fmt.Sprintf(`import sys, os
sys.path.insert(0, %s)
import bridge
from magent_harness import AgentEngine
original_think = AgentEngine._think
async def identify_worker(self, run):
    with open(%s, "w", encoding="ascii") as marker:
        marker.write(str(os.getpid()))
    return await original_think(self, run)
AgentEngine._think = identify_worker
sys.exit(bridge.main())
`, bridgeJSON, pidJSON)
			if err := os.WriteFile(shim, []byte(code), 0600); err != nil {
				t.Fatal(err)
			}
			var handlers []*httpapi.Handler
			defer func() {
				for _, handler := range handlers {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := handler.Shutdown(ctx); err != nil {
						t.Errorf("stop inflight test worker: %v", err)
					}
					cancel()
				}
			}()
			install := func(script string) *httpapi.Handler {
				t.Helper()
				client, err := agent.NewProcessClient(agent.Config{Python: python, Script: script, HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "controlled-inflight-model", Timeout: 20 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				x.server.Close()
				handler := httpapi.New(x.store, client, x.config)
				handlers = append(handlers, handler)
				x.server = httptest.NewServer(handler)
				return handler
			}
			initialHandler := install(shim)
			body, _ := json.Marshal(map[string]any{"content": query})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, x.server.URL+sessionPath(session)+"/messages", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+owner.Token)
			if streaming {
				request.Header.Set("Accept", "text/event-stream")
			}
			finished, draftSeen := make(chan integrationResponse, 1), make(chan struct{}, 1)
			go func() {
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					finished <- integrationResponse{err: err}
					return
				}
				defer response.Body.Close()
				var data bytes.Buffer
				scanner := bufio.NewScanner(response.Body)
				for scanner.Scan() {
					line := scanner.Text()
					data.WriteString(line + "\n")
					if strings.Contains(line, draft) {
						select {
						case draftSeen <- struct{}{}:
						default:
						}
					}
				}
				finished <- integrationResponse{status: response.StatusCode, body: data.Bytes(), err: scanner.Err()}
			}()
			select {
			case <-entered:
			case early := <-finished:
				t.Fatalf("request ended before the provider entered: %+v", early)
			case <-ctx.Done():
				t.Fatal("provider did not receive the first request")
			}
			if streaming {
				select {
				case <-draftSeen:
				case <-ctx.Done():
					t.Fatal("uncommitted provider draft did not reach the public stream")
				}
			}
			pidBytes, err := os.ReadFile(pidPath)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(string(pidBytes))
			if err != nil || pid <= 1 || pid == os.Getpid() {
				t.Fatal("invalid PID from the test worker")
			}
			worker, err := os.FindProcess(pid)
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.Kill(); err != nil {
				t.Fatalf("kill the worker while its model request is in flight: %v", err)
			}
			_ = worker.Release()
			var failed integrationResponse
			select {
			case failed = <-finished:
			case <-ctx.Done():
				t.Fatal("killed worker did not finish the HTTP request")
			}
			if streaming {
				if failed.err != nil || failed.status != 200 || strings.Count(string(failed.body), "event: run.failed\n") != 1 || strings.Contains(string(failed.body), "event: run.completed\n") {
					t.Fatalf("inflight SSE crash did not fail exactly once: %+v", failed)
				}
			} else if envelope := checkIntegrationResponse(t, failed, 502); envelope.Error.Code != "agent_failed" {
				t.Fatalf("inflight JSON crash has wrong error: %s", failed.body)
			}
			assertJournalResponsePrivate(t, failed.body)
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 1 || messages[0].Role != "user" || messages[0].Content != query || calls.Load() != 1 {
				t.Fatalf("crash saved a draft, duplicated a message, or repeated a model request: messages=%+v calls=%d", messages, calls.Load())
			}
			runID := messages[0].RunID
			failedRun := waitStreamRun(t, x.integrationFixture, owner.Token, sessionPath(session)+"/runs/"+runID, "failed")
			if failedRun.ErrorCode != "agent_failed" {
				t.Fatalf("inflight worker death has wrong durable status: %+v", failedRun)
			}
			assertInflightJournal(t, x, runID, draft, false)
			stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			err = initialHandler.Shutdown(stopCtx)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			recoveryRestart(t, x)
			install(bridgePath)
			var completed recoveryHTTPResult
			if streaming {
				response, _, stop := sseTestRequest(t, x.integrationFixture, recoveryPath(session, runID), owner.Token, map[string]any{}, "")
				raw, err := io.ReadAll(response.Body)
				stop()
				response.Body.Close()
				if err != nil || response.StatusCode != 200 || strings.Count(string(raw), "event: run.completed\n") != 1 || strings.Contains(string(raw), "event: run.failed\n") || strings.Contains(string(raw), draft) {
					t.Fatalf("inflight recovery SSE failed or repeated the draft: %v %s", err, raw)
				}
				for _, block := range strings.Split(string(raw), "\n\n") {
					if _, data, ok := strings.Cut(block, "event: run.completed\ndata: "); ok {
						if err := json.Unmarshal([]byte(data), &completed); err != nil {
							t.Fatal(err)
						}
					}
				}
				assertJournalResponsePrivate(t, raw)
			} else {
				response := x.call("POST", recoveryPath(session, runID), owner.Token, map[string]any{})
				completed = integrationData[recoveryHTTPResult](t, checkIntegrationResponse(t, response, 201))
				assertJournalResponsePrivate(t, response.body)
			}
			if completed.Run.ID != runID || completed.Run.Status != "completed" || completed.Answer != answer || calls.Load() != 2 {
				t.Fatalf("recovery did not repeat the unresolved model request exactly once: %+v calls=%d", completed, calls.Load())
			}
			messages = integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 2 || messages[0].Content != query || messages[0].RunID != runID || messages[1].RunID != runID || messages[1].Content != answer {
				t.Fatalf("inflight recovery changed the conversation or duplicated its turn: %+v", messages)
			}
			assertInflightJournal(t, x, runID, draft, true)
			x.request(t, "POST", recoveryPath(session, runID), owner.Token, map[string]any{}, 409)
			if calls.Load() != 2 {
				t.Fatal("completed run recovery repeated the provider call")
			}
		})
	}
}

func assertInflightJournal(t *testing.T, x *indexFixture, runID, draft string, completed bool) {
	t.Helper()
	counts := map[string]int{}
	for _, raw := range storedJournalFacts(t, x, runID) {
		var record struct{ Kind string }
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		counts[record.Kind]++
		if strings.Contains(string(raw), draft) {
			t.Fatal("incomplete provider response was persisted as a recoverable fact")
		}
	}
	wantCompleted := 0
	if completed {
		wantCompleted = 1
	}
	if counts["created"] != 1 || counts["completed"] != wantCompleted || counts["planned"] != 0 || counts["tool_started"] != 0 || counts["tool_finished"] != 0 || counts["failed"] != 0 || counts["canceled"] != 0 {
		t.Fatalf("inflight crash did not preserve the intended durable boundary: %+v", counts)
	}
}
