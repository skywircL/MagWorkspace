package httpapi_test

import (
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

// Interrupt after the real Harness commits a fitted context, before its first
// answer request. Recovery must use that same private checkpoint exactly once.
func TestHTTPContextRealHarnessCommittedCompactionSurvivesRestart(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "contextrecovery")
			session := x.session(t, owner, owner.Workspaces[0].ID, "压缩后恢复")
			seedContextTurns(t, x.integrationFixture, owner, session, 24, strings.Repeat("界", 500))
			const query = "CURRENT_REQUEST_KEEP_EXACT"
			const checkpoint = "PRIVATE_CONTEXT_RESTART_CHECKPOINT"
			const answer = "恢复后继续当前任务。"
			var summaries, answers atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Stream    bool            `json:"stream"`
					Messages  []agent.Message `json:"messages"`
					MaxTokens int             `json:"max_tokens"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				if len(payload.Messages) > 0 && strings.Contains(payload.Messages[0].Content, "structured checkpoint") {
					summaries.Add(1)
					if payload.Stream || payload.MaxTokens <= 0 || payload.MaxTokens > 256 {
						t.Error("private checkpoint ignored deployment summary budget")
					}
					fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}]}`, checkpoint)
					return
				}
				answers.Add(1)
				encoded, _ := json.Marshal(payload.Messages)
				if !strings.Contains(string(encoded), checkpoint) || payload.Messages[len(payload.Messages)-1].Content != query || payload.MaxTokens != 512 || payload.Stream != streaming {
					t.Error("recovery lost its committed checkpoint, exact current request, budget or mode")
				}
				if payload.Stream {
					w.Header().Set("Content-Type", "text/event-stream")
					chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": answer}, "finish_reason": "stop"}}})
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
				} else {
					fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}]}`, answer)
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
			config := agent.Config{Python: python, Script: bridgePath, HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "local-model", Timeout: 20 * time.Second, MaxContextTokens: 8192, ReserveTokens: 1024, KeepRecentTokens: 512, MaxSummaryTokens: 256, MaxCompletionTokens: 512, SummaryTimeout: time.Second}
			directory := t.TempDir()
			marker := filepath.Join(directory, "compaction-committed")
			shim := filepath.Join(directory, "interrupt_after_context.py")
			bridgeJSON, _ := json.Marshal(filepath.Dir(bridgePath))
			markerJSON, _ := json.Marshal(marker)
			code := fmt.Sprintf(`import sys, os
from pathlib import Path
sys.path.insert(0,%s)
import bridge
from magent_harness import AgentEngine
original=AgentEngine._manage_context
marker=Path(%s)
async def interrupted(self,run,*args,**kwargs):
    messages=await original(self,run,*args,**kwargs)
    if any(m.kind=="compaction_summary" for m in messages) and not marker.exists():
        marker.write_text("committed private context; interrupted before provider answer")
        os._exit(73)
    return messages
AgentEngine._manage_context=interrupted
sys.exit(bridge.main())
`, bridgeJSON, markerJSON)
			if err := os.WriteFile(shim, []byte(code), 0600); err != nil {
				t.Fatal(err)
			}
			newClient := func(script string) *agent.ProcessClient {
				t.Helper()
				candidate := config
				candidate.Script = script
				client, err := agent.NewProcessClient(candidate)
				if err != nil {
					t.Fatal(err)
				}
				return client
			}
			x.server.Close()
			x.server = httptest.NewServer(httpapi.New(x.store, newClient(shim), x.config))
			failed := x.call(http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": query})
			checkIntegrationResponse(t, failed, 502)
			if _, err := os.Stat(marker); err != nil || summaries.Load() != 1 || answers.Load() != 0 {
				t.Fatalf("did not interrupt after committed compaction: summaries=%d answers=%d marker=%v", summaries.Load(), answers.Load(), err)
			}
			messages := integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 49 || messages[48].Role != "user" || messages[48].Content != query {
				t.Fatal("context interruption altered or duplicated ordinary turns")
			}
			runID := messages[48].RunID
			committed := false
			for _, raw := range storedJournalFacts(t, x, runID) {
				if strings.Contains(string(raw), `"kind":"context_committed"`) && strings.Contains(string(raw), checkpoint) {
					committed = true
				}
			}
			if !committed {
				t.Fatal("the private fitted checkpoint was not persisted before the crash")
			}
			recoveryRestart(t, x)
			x.server.Close()
			x.server = httptest.NewServer(httpapi.New(x.store, newClient(bridgePath), x.config))
			if streaming {
				response, _, cancel := sseTestRequest(t, x.integrationFixture, recoveryPath(session, runID), owner.Token, map[string]any{}, "")
				raw, err := io.ReadAll(response.Body)
				cancel()
				response.Body.Close()
				if err != nil || response.StatusCode != 200 || !strings.Contains(string(raw), "event: run.completed\n") || strings.Contains(string(raw), "event: run.failed\n") || strings.Contains(string(raw), checkpoint) {
					t.Fatalf("context recovery SSE failed or leaked checkpoint: %v %s", err, raw)
				}
			} else {
				response := x.call(http.MethodPost, recoveryPath(session, runID), owner.Token, map[string]any{})
				completed := integrationData[recoveryHTTPResult](t, checkIntegrationResponse(t, response, 201))
				if completed.Run.ID != runID || completed.Answer != answer || strings.Contains(string(response.body), checkpoint) {
					t.Fatal("JSON recovery changed its run or leaked checkpoint")
				}
			}
			if summaries.Load() != 1 || answers.Load() != 1 {
				t.Fatalf("recovery repeated private compaction or answer: summaries=%d answers=%d", summaries.Load(), answers.Load())
			}
			messages = integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 50 || messages[48].RunID != runID || messages[49].RunID != runID || messages[49].Content != answer {
				t.Fatal("recovery changed durable history or duplicated the current turn")
			}
		})
	}
}
