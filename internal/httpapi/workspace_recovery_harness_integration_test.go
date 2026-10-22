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
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

// The complete ProcessClient invocation succeeds, including remote journal
// commits, but its result is lost before the Go service can commit its terminal.
type lostHarnessResult struct {
	client *agent.ProcessClient
	lost   atomic.Bool
}

func (c *lostHarnessResult) Run(ctx context.Context, request agent.Request) (agent.Result, error) {
	result, err := c.client.Run(ctx, request)
	if err == nil && result.Status == "completed" && c.lost.CompareAndSwap(false, true) {
		return agent.Result{}, agent.ErrFailed
	}
	return result, err
}

// Actual HTTP, MySQL, database-backed journal, ProcessClient and Harness run here.
// Only the model provider and external index/object services are local fixtures.
func TestHTTPWorkspaceRealHarnessJournalRecovery(t *testing.T) {
	for _, crashPoint := range []string{"after_tool_round", "after_completed_commit"} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", crashPoint, streaming), func(t *testing.T) {
				x := newIndexFixture(t)
				owner := x.register(t, "journalharness")
				write := x.upload(t, owner, "用于实际目录工具恢复验收的合成文档。")
				x.process(t)
				session := x.session(t, owner, owner.Workspaces[0].ID, "普通运行持久化恢复")
				const callID = "journal-list-documents"
				const answer = "目录读取已恢复。"
				var calls atomic.Int32
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost || r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer local-test-key" {
						t.Error("unexpected controlled model endpoint or credential")
						http.Error(w, "invalid controlled request", http.StatusBadRequest)
						return
					}
					var payload struct {
						Stream   bool `json:"stream"`
						Messages []struct {
							Role, Content string
							ToolCallID    string `json:"tool_call_id"`
						} `json:"messages"`
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
						return
					}
					step := calls.Add(1)
					if step == 1 {
						if payload.Stream {
							t.Error("initial JSON run unexpectedly streamed")
						}
						call := map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": "list_documents", "arguments": `{}`}}
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
						return
					}
					if step != 2 {
						t.Errorf("unexpected model replay: %d calls", step)
					}
					found := false
					for _, message := range payload.Messages {
						if message.Role != "tool" || message.ToolCallID != callID {
							continue
						}
						var result struct {
							Documents []struct {
								Document domain.Document        `json:"document"`
								Version  domain.DocumentVersion `json:"version"`
							} `json:"documents"`
						}
						if json.Unmarshal([]byte(message.Content), &result) != nil || len(result.Documents) != 1 || result.Documents[0].Document.ID != write.Document.ID || result.Documents[0].Version.ID != write.Version.ID {
							t.Error("replayed directory result did not retain actual document and version")
						}
						found = true
					}
					if !found {
						t.Error("recovery omitted committed tool result or original call ID")
					}
					if payload.Stream {
						chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": answer}, "finish_reason": "stop"}}})
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": answer}, "finish_reason": "stop"}}})
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
				bridgePath, err := filepath.Abs(filepath.Join("..", "..", "bridges", "magent", "bridge.py"))
				if err != nil {
					t.Fatal(err)
				}
				config := agent.Config{Python: python, Script: bridgePath, HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "local-controlled-provider", Timeout: 20 * time.Second}
				var crashMarker string
				if crashPoint == "after_tool_round" {
					directory := t.TempDir()
					crashMarker = filepath.Join(directory, "crashed-once")
					shim := filepath.Join(directory, "interrupt_bridge.py")
					bridgeDirectoryJSON, _ := json.Marshal(filepath.Dir(bridgePath))
					markerJSON, _ := json.Marshal(crashMarker)
					code := fmt.Sprintf(`import sys, os
from pathlib import Path
sys.path.insert(0, %s)
import bridge
from magent_harness import AgentEngine
original_think = AgentEngine._think
marker = Path(%s)
async def interrupted_think(self, run):
    if run.round == 1 and not marker.exists():
        marker.write_text("committed tool round; interrupted before next model request")
        os._exit(73)
    return await original_think(self, run)
AgentEngine._think = interrupted_think
sys.exit(bridge.main())
`, bridgeDirectoryJSON, markerJSON)
					if err := os.WriteFile(shim, []byte(code), 0600); err != nil {
						t.Fatal(err)
					}
					config.Script = shim
				}
				newClient := func() *agent.ProcessClient {
					t.Helper()
					client, err := agent.NewProcessClient(config)
					if err != nil {
						t.Fatal(err)
					}
					return client
				}
				var firstClient agent.Client = newClient()
				if crashPoint == "after_completed_commit" {
					firstClient = &lostHarnessResult{client: firstClient.(*agent.ProcessClient)}
				}
				x.server.Close()
				x.server = httptest.NewServer(httpapi.New(x.store, firstClient, x.config))
				failed := x.call(http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "读取目录并继续原任务", "document_ids": []string{write.Document.ID}})
				failure := checkIntegrationResponse(t, failed, http.StatusBadGateway)
				if failure.Error.Code != "agent_failed" {
					t.Fatalf("unexpected initial failure: %s", failed.body)
				}
				assertJournalResponsePrivate(t, failed.body)
				messages := integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
				if len(messages) != 1 || messages[0].Role != "user" {
					t.Fatalf("interrupted run wrote a duplicate or assistant turn: %+v", messages)
				}
				runID := messages[0].RunID
				initialCalls := int32(1)
				if crashPoint == "after_completed_commit" {
					initialCalls = 2
				}
				if calls.Load() != initialCalls {
					t.Fatalf("crash missed intended boundary: calls=%d", calls.Load())
				}
				if crashMarker != "" {
					if _, err := os.Stat(crashMarker); err != nil {
						t.Fatal("real child did not reach the crash marker")
					}
				}
				assertHarnessJournalFacts(t, x, runID, crashPoint == "after_completed_commit")
				recoveryRestart(t, x)
				x.server.Close()
				x.server = httptest.NewServer(httpapi.New(x.store, newClient(), x.config))
				var completed recoveryHTTPResult
				if streaming {
					response, _, cancel := sseTestRequest(t, x.integrationFixture, recoveryPath(session, runID), owner.Token, map[string]any{}, "")
					raw, readErr := io.ReadAll(response.Body)
					cancel()
					_ = response.Body.Close()
					if readErr != nil || response.StatusCode != http.StatusOK || response.Header.Get("X-Run-ID") != runID {
						t.Fatalf("recovery SSE status=%d error=%v body=%s", response.StatusCode, readErr, raw)
					}
					assertJournalResponsePrivate(t, raw)
					terminals := 0
					for _, block := range strings.Split(string(raw), "\n\n") {
						if strings.HasPrefix(block, "event: run.failed\n") {
							t.Fatalf("recovery failed: %s", raw)
						}
						if strings.HasPrefix(block, "event: run.completed\n") {
							terminals++
							if err := json.Unmarshal([]byte(strings.TrimPrefix(block, "event: run.completed\ndata: ")), &completed); err != nil {
								t.Fatal(err)
							}
						}
					}
					if terminals != 1 {
						t.Fatalf("expected one committed terminal: %s", raw)
					}
					if crashPoint == "after_completed_commit" && strings.Contains(string(raw), "event: message.start\n") {
						t.Fatal("cached terminal replay started a new model response")
					}
				} else {
					response := x.call(http.MethodPost, recoveryPath(session, runID), owner.Token, map[string]any{})
					assertJournalResponsePrivate(t, response.body)
					completed = integrationData[recoveryHTTPResult](t, checkIntegrationResponse(t, response, http.StatusCreated))
				}
				if completed.Run.ID != runID || completed.Run.Status != "completed" || completed.Answer != answer || completed.Run.FinishedAt == nil || calls.Load() != 2 {
					t.Fatalf("recovery changed run or regenerated model work: %+v calls=%d", completed, calls.Load())
				}
				messages = integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
				if len(messages) != 2 || messages[0].Role != "user" || messages[0].RunID != runID || messages[1].Role != "assistant" || messages[1].RunID != runID || messages[1].Content != answer || messages[1].Status != "completed" {
					t.Fatalf("recovery did not commit exactly one original conversation turn: %+v", messages)
				}
				assertHarnessJournalFacts(t, x, runID, true)
				x.request(t, http.MethodPost, recoveryPath(session, runID), owner.Token, map[string]any{}, http.StatusConflict)
				if calls.Load() != 2 {
					t.Fatal("duplicate recovery reached the provider")
				}
			})
		}
	}
}

func assertJournalResponsePrivate(t *testing.T, raw []byte) {
	t.Helper()
	promptRequirePrivate(t, raw)
	for _, field := range []string{`"journal"`, `"digest"`, `"prev_hash"`, `"execution_generation"`, "journal-list-documents", "source_boundary", "/journal"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("public response leaked private execution field %q", field)
		}
	}
}

func assertHarnessJournalFacts(t *testing.T, x *indexFixture, runID string, completed bool) {
	t.Helper()
	toolResults, rounds, terminals := 0, 0, 0
	for _, raw := range storedJournalFacts(t, x, runID) {
		var record struct {
			Kind string `json:"kind"`
			Data struct {
				Record struct {
					Name   string `json:"name"`
					Result struct {
						Success bool `json:"success"`
					} `json:"result"`
				} `json:"record"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		if record.Kind == "tool_finished" && record.Data.Record.Name == "list_documents" && record.Data.Record.Result.Success {
			toolResults++
		}
		if record.Kind == "round_committed" {
			rounds++
		}
		if record.Kind == "completed" {
			terminals++
		}
	}
	wantTerminals := 0
	if completed {
		wantTerminals = 1
	}
	if toolResults != 1 || rounds != 1 || terminals != wantTerminals {
		t.Fatalf("real remote journal has wrong replay boundary: tools=%d rounds=%d terminals=%d want=%d", toolResults, rounds, terminals, wantTerminals)
	}
}
