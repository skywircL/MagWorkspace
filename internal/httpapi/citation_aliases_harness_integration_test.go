package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type citationDeliveryClient struct {
	client *agent.ProcessClient
	deny   atomic.Bool
}

func (c *citationDeliveryClient) request(req agent.Request) agent.Request {
	original := req.ToolHandler
	req.ToolHandler = func(ctx context.Context, name string, args json.RawMessage) (any, error) {
		if name == "resolve_citation_aliases" && c.deny.Load() {
			return nil, agent.ErrFailed
		}
		return original(ctx, name, args)
	}
	return req
}
func (c *citationDeliveryClient) Run(ctx context.Context, req agent.Request) (agent.Result, error) {
	return c.client.Run(ctx, c.request(req))
}
func (c *citationDeliveryClient) Stream(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
	return c.client.Stream(ctx, c.request(req), emit)
}

func TestHTTPCitationAliasesRealHarnessSecondTurnAndCommittedRecovery(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, recovering := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/recovery=%v", streaming, recovering), func(t *testing.T) {
				x := newIndexFixture(t)
				owner := x.register(t, "citationhistory")
				x.upload(t, owner, "单虚拟机建议2核6G，三台虚拟机每台1核4G。")
				x.process(t)
				session := x.session(t, owner, owner.Workspaces[0].ID, "连续提问")
				var mu sync.Mutex
				calls := 0
				oldID, currentID := "", ""
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					var in struct {
						Stream   bool
						Messages []struct{ Role, Content string }
						Tools    []struct{ Function struct{ Name string } }
					}
					if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
						t.Error(err)
						return
					}
					for _, tool := range in.Tools {
						if tool.Function.Name == "resolve_citation_aliases" {
							t.Error("private alias operation exposed to model")
						}
					}
					calls++
					step := calls
					message := map[string]any{"role": "assistant", "content": ""}
					reason := "stop"
					if step == 1 || step == 3 {
						if step == 3 {
							found := false
							for _, m := range in.Messages {
								if strings.Contains(m.Content, oldID) {
									t.Error("historical evidence ID leaked into model context")
								}
								if m.Role == "assistant" && strings.Contains(m.Content, "配置建议 [historical source; retrieve again]") {
									found = true
								}
							}
							if !found {
								t.Error("second turn lost historical prose or its non-citable source marker")
							}
						}
						message["tool_calls"] = []any{map[string]any{"index": 0, "id": fmt.Sprint("search-", step), "type": "function", "function": map[string]string{"name": "search_knowledge", "arguments": `{"query":"虚拟机配置"}`}}}
						reason = "tool_calls"
					} else if step == 2 || step == 4 {
						found := ""
						for _, m := range in.Messages {
							if m.Role != "tool" {
								continue
							}
							var result struct {
								Evidence []struct {
									ID string `json:"evidence_id"`
								}
							}
							if json.Unmarshal([]byte(m.Content), &result) == nil && len(result.Evidence) > 0 {
								found = result.Evidence[0].ID
							}
						}
						if found == "" {
							t.Error("real tool produced no evidence")
							return
						}
						if step == 2 {
							oldID = found
							message["content"] = "配置建议 [E:" + oldID + "]"
						} else {
							currentID = found
							message["content"] = "两种配置 [E:" + oldID + "] 和 [E:" + currentID + "]"
						}
					} else {
						t.Errorf("model replayed committed work: %d", step)
						http.Error(w, "unexpected replay", 500)
						return
					}
					if in.Stream {
						w.Header().Set("Content-Type", "text/event-stream")
						raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": message, "finish_reason": reason}}})
						fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
					} else {
						json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": reason}}})
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
				cfg := agent.Config{Python: python, Script: filepath.Join("..", "..", "bridges", "magent", "bridge.py"), HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-key", Model: "controlled", Timeout: 20 * time.Second}
				process, err := agent.NewProcessClient(cfg)
				if err != nil {
					t.Fatal(err)
				}
				client := &citationDeliveryClient{client: process}
				x.server.Close()
				x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
				x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "第一问配置"}, 201)
				client.deny.Store(recovering)
				call := func(path string, body any, success bool) {
					t.Helper()
					if streaming {
						response, _, cancel := sseTestRequest(t, x.integrationFixture, path, owner.Token, body, "")
						raw, e := io.ReadAll(response.Body)
						cancel()
						response.Body.Close()
						if e != nil {
							t.Fatal(e)
						}
						event := "run.completed"
						if !success {
							event = "run.failed"
						}
						if !strings.Contains(string(raw), "event: "+event+"\n") {
							t.Fatalf("missing %s terminal: %s", event, raw)
						}
					} else {
						status := 201
						if !success {
							status = 502
						}
						x.request(t, "POST", path, owner.Token, body, status)
					}
				}
				call(sessionPath(session)+"/messages", map[string]string{"content": "第二问两种配置"}, !recovering)
				messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
				if recovering {
					if len(messages) != 3 {
						t.Fatalf("failed delivery persisted answer: %d", len(messages))
					}
					runID := messages[len(messages)-1].RunID
					// A new service/process replays the original completed Harness journal.
					recoveryRestart(t, x)
					process, err = agent.NewProcessClient(cfg)
					if err != nil {
						t.Fatal(err)
					}
					x.server.Close()
					x.server = httptest.NewServer(httpapi.New(x.store, process, x.config))
					call(recoveryPath(session, runID), map[string]any{}, true)
					messages = integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
				}
				mu.Lock()
				defer mu.Unlock()
				if calls != 4 || oldID == currentID || len(messages) != 4 {
					t.Fatalf("unexpected turn state: calls=%d messages=%d", calls, len(messages))
				}
				want := "两种配置 [E:" + currentID + "] 和 [E:" + currentID + "]"
				if messages[3].Content != want || !strings.Contains(messages[1].Content, oldID) {
					t.Fatal("canonical answer or immutable first turn changed")
				}
				evidence, err := x.store.EvidenceForRun(context.Background(), owner.User.ID, owner.Workspaces[0].ID, session.ID, messages[3].RunID, true)
				if err != nil || len(evidence) != 1 || evidence[0].ID != currentID {
					t.Fatalf("wrong persisted citations: %v", err)
				}
			})
		}
	}
}
