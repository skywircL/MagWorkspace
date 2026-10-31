package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Query plans belong to the durable transcript even when their original search
// capability is no longer available. Resume must still project that private
// message out of both ordinary provider input and derived context summaries.
func TestHarnessQueryPlanRecoveryAfterSearchRevocation(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/compact=%t", streaming, compact), func(t *testing.T) {
				request := retrievalRequest()
				request.Query = "先询问我部署范围，收到答复后继续"
				if compact {
					for range 12 {
						request.History = append(request.History,
							Message{Role: "user", Content: "讨论部署需求：" + strings.Repeat("界", 300)},
							Message{Role: "assistant", Content: "保留部署约束：" + strings.Repeat("界", 300)})
					}
				} else {
					request.History = []Message{{Role: "user", Content: "讨论本地部署"}, {Role: "assistant", Content: "可以先确认部署范围。"}}
				}
				journal := &memoryJournal{}
				request.JournalHandler = journal.handle
				var discovery, understanding, models, summaries, unexpected atomic.Int32
				request.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
					switch name {
					case "tool_capabilities":
						if discovery.Add(1) == 1 {
							return map[string]any{"tools": []string{"ask_user", "search_knowledge"}}, nil
						}
						return map[string]any{"tools": []string{"ask_user"}}, nil
					case "restore_evidence":
						return map[string]any{"evidence": []any{}}, nil
					default:
						unexpected.Add(1)
						return nil, errors.New("revoked or unrelated tool executed")
					}
				}
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					payload := knowledgeModelPayload(t, r)
					if len(payload.Messages) > 0 && strings.Contains(payload.Messages[0].Content, "QUERY_UNDERSTANDING_V1") {
						understanding.Add(1)
						fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"{\"rewrite_query\":\"询问用户部署范围\",\"intent\":\"other\"}"},"finish_reason":"stop"}]}`)
						return
					}
					for _, message := range payload.Messages {
						if strings.Contains(message.Content, "original_query_sha256") {
							t.Error("private persisted query plan entered provider or summary input after search revocation")
						}
					}
					if len(payload.Messages) > 0 && strings.Contains(payload.Messages[0].Content, "structured checkpoint") {
						if discovery.Load() != 2 || !compact {
							t.Error("fixture compacted before the capability change")
						}
						summaries.Add(1)
						fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"PRIVATE_QUERY_RECOVERY_CHECKPOINT"},"finish_reason":"stop"}]}`)
						return
					}
					if models.Add(1) == 1 {
						knowledgeReply(w, streaming, []any{knowledgeCall("query-plan-question", "ask_user", `{"question":"部署到哪里？"}`)})
						return
					}
					if len(payload.Tools) != 1 || payload.Tools[0].Function.Name != "ask_user" {
						t.Error("restored routing plan re-enabled the revoked search tool")
					}
					foundAnswer := false
					for _, message := range payload.Messages {
						foundAnswer = foundAnswer || (message.Role == "tool" && message.ID == "query-plan-question" && strings.Contains(message.Content, "本地"))
					}
					if !foundAnswer {
						t.Error("resume lost the authenticated clarification answer")
					}
					knowledgeReply(w, streaming, nil)
				}))
				defer provider.Close()
				client := mustClient(t, liveStreamConfig(t, provider.URL))
				first, err := runKnowledgeHarness(t, client, request, streaming)
				if err != nil || first.Prompt == nil || first.Status != "suspended" {
					t.Fatalf("suspension failed: %v %+v", err, first)
				}
				journal.mu.Lock()
				persisted := len(journal.records) > 0 && strings.Contains(string(journal.records[0]), "query_understanding")
				journal.mu.Unlock()
				if !persisted || !strings.Contains(first.Prompt.Snapshot, "query_understanding") {
					t.Fatal("fixture did not persist the private plan before capability revocation")
				}
				request.Resume = answerPrompt(t, request, first.Prompt, "本地")
				if compact {
					// Initial execution retains the original context. A restarted
					// service's smaller budget puts the old private plan through the
					// real Harness summary envelope while preserving the current query.
					config := contextProviderConfig(t, provider.URL)
					config.KeepRecentTokens = 1
					client = mustClient(t, config)
				}
				result, err := runKnowledgeHarness(t, client, request, streaming)
				if err != nil || result.Status != "completed" {
					t.Fatalf("resume failed: %v %+v", err, result)
				}
				if discovery.Load() != 2 || understanding.Load() != 1 || models.Load() != 2 || unexpected.Load() != 0 {
					t.Fatalf("resume repeated planning or executed a revoked tool: discovery=%d understanding=%d models=%d unexpected=%d", discovery.Load(), understanding.Load(), models.Load(), unexpected.Load())
				}
				if (compact && summaries.Load() != 1) || (!compact && summaries.Load() != 0) {
					t.Fatalf("unexpected summary count: %d", summaries.Load())
				}
				encoded, _ := json.Marshal(result)
				if strings.Contains(string(encoded), "original_query_sha256") || strings.Contains(string(encoded), "PRIVATE_QUERY_RECOVERY_CHECKPOINT") {
					t.Fatal("private query routing or compaction state escaped into the public transcript")
				}
			})
		}
	}
}
