package httpapi_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

// This test keeps HTTP, MySQL, ProcessClient, the actual Harness and all Go tool
// handlers real. Only the model provider and external object/index services are
// controlled local fixtures. No remote model or paid API is contacted.
func TestHTTPWorkspaceRealHarnessToolsSuspendRestartResumeCitation(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "workspaceharness")
	write := x.upload(t, owner, "住宿标准：国内差旅每晚最高报销 500 元。\n海外差旅需要另外审批。")
	x.process(t)
	historySession := x.session(t, owner, owner.Workspaces[0].ID, "上次讨论的配置")
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "历史配置：PORT=8080，MYSQL_HOST=mysql。"}, nil
	})
	history := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(historySession)+"/messages", owner.Token, map[string]string{"content": "记录开发配置"}, 201))
	session := x.session(t, owner, owner.Workspaces[0].ID, "真实工具与恢复")
	names := []string{"list_documents", "search_knowledge", "read_document", "search_conversations", "ask_user"}
	var mu sync.Mutex
	modelCalls := 0
	evidenceID := ""
	seen := map[string]bool{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		fail := func(format string, args ...any) {
			t.Errorf(format, args...)
			http.Error(w, "controlled provider validation failed", http.StatusBadRequest)
		}
		if r.Method != "POST" || r.URL.Path != "/chat/completions" {
			fail("unexpected provider endpoint %s %s", r.Method, r.URL.Path)
			return
		}
		var payload struct {
			Stream   bool `json:"stream"`
			Messages []struct {
				Role, Content string
				ToolCallID    string `json:"tool_call_id"`
			} `json:"messages"`
			Tools []struct {
				Function struct{ Name string } `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			fail("decode real Harness provider request: %v", err)
			return
		}
		if len(payload.Messages) > 0 && strings.Contains(payload.Messages[0].Content, "MagWorkspace query understanding") {
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"not-json"},"finish_reason":"stop"}]}`)
			return
		}
		advertised := map[string]bool{}
		for _, tool := range payload.Tools {
			advertised[tool.Function.Name] = true
		}
		if len(advertised) != len(names)+3 || !advertised["list_knowledge_bases"] || !advertised["search_memory"] || advertised["create_document"] || !advertised["update_document"] {
			fail("model received unexpected tool set: %v", advertised)
			return
		}
		for _, name := range names {
			if !advertised[name] {
				fail("real Harness did not expose %s", name)
				return
			}
		}
		step := modelCalls
		modelCalls++
		results := map[string]string{}
		for _, message := range payload.Messages {
			if message.Role == "tool" {
				if strings.HasPrefix(message.Content, "Error:") {
					fail("real tool callback failed for %s: %s", message.ToolCallID, message.Content)
					return
				}
				results[message.ToolCallID] = message.Content
			}
		}
		// Previously executed results must survive every model round and the
		// persisted native snapshot across a fresh HTTP server/child process.
		for i := 0; i < min(step, len(names)); i++ {
			name := names[i]
			content, ok := results["workspace-"+name]
			if !ok {
				fail("step %d lost earlier %s result", step, name)
				return
			}
			if name == "ask_user" {
				if !strings.Contains(content, "国内") {
					fail("resume omitted human answer: %s", content)
					return
				}
				seen[name] = true
				continue
			}
			var result struct {
				Documents []struct {
					Document domain.Document        `json:"document"`
					Version  domain.DocumentVersion `json:"version"`
				} `json:"documents"`
				Document domain.Document `json:"document"`
				Evidence []struct {
					ID         string `json:"evidence_id"`
					DocumentID string `json:"document_id"`
					VersionID  string `json:"version_id"`
					Content    string `json:"content"`
				} `json:"evidence"`
				Items []domain.ConversationMatch `json:"items"`
			}
			if err := json.Unmarshal([]byte(content), &result); err != nil {
				fail("%s output is not successful structured tool data: %v", name, err)
				return
			}
			switch name {
			case "list_documents":
				if len(result.Documents) != 1 || result.Documents[0].Document.ID != write.Document.ID || result.Documents[0].Version.ID != write.Version.ID {
					fail("list did not return uploaded document: %s", content)
					return
				}
			case "search_knowledge", "read_document":
				if len(result.Evidence) == 0 || result.Evidence[0].DocumentID != write.Document.ID || result.Evidence[0].VersionID != write.Version.ID || !strings.Contains(result.Evidence[0].Content, "500 元") || len(result.Evidence[0].ID) != 32 {
					fail("%s did not return actual indexed bytes: %s", name, content)
					return
				}
				if name == "search_knowledge" {
					if evidenceID != "" && evidenceID != result.Evidence[0].ID {
						fail("snapshot changed acquired evidence ID")
						return
					}
					evidenceID = result.Evidence[0].ID
				} else if result.Document.ID != write.Document.ID || result.Evidence[0].ID != evidenceID {
					fail("read_document did not navigate the acquired passage")
					return
				}
			case "search_conversations":
				if len(result.Items) != 1 || result.Items[0].RunID != history.Run.ID || result.Items[0].SessionID != historySession.ID || !strings.Contains(result.Items[0].Answer, "MYSQL_HOST=mysql") {
					fail("conversation tool lost actual private completed history: %s", content)
					return
				}
			}
			seen[name] = true
		}
		w.Header().Set("Content-Type", "application/json")
		if step < len(names) {
			if payload.Stream {
				fail("initial JSON execution unexpectedly requested provider streaming")
				return
			}
			args := []string{`{}`, `{"query":"住宿标准","mode":"keyword"}`, `{"evidence_id":"` + evidenceID + `","context":1}`, `{"query":"配置"}`, `{"question":"你指的是国内还是海外差旅？","options":["国内","海外"]}`}
			call := map[string]any{"id": "workspace-" + names[step], "type": "function", "function": map[string]any{"name": names[step], "arguments": args[step]}}
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
			return
		}
		if step != len(names) || !payload.Stream || evidenceID == "" {
			fail("unexpected post-resume round=%d stream=%v evidence=%s", step, payload.Stream, evidenceID)
			return
		}
		answer := "已确认国内差旅，每晚最高报销500元。[E:" + evidenceID + "]"
		chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": answer}, "finish_reason": "stop"}}})
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
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
	config := agent.Config{Python: python, Script: filepath.Join("..", "..", "bridges", "magent", "bridge.py"), HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "local-controlled-provider", Timeout: 20 * time.Second}
	startHarnessServer := func() {
		t.Helper()
		client, err := agent.NewProcessClient(config)
		if err != nil {
			t.Fatal(err)
		}
		x.server.Close()
		x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	}
	startHarnessServer()
	response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "查住宿标准，看看之前配置并确认差旅范围", "document_ids": []string{write.Document.ID}})
	promptRequirePrivate(t, response.body)
	pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	if pending.Run.Status != "suspended" || pending.Prompt.Status != "pending" || pending.Prompt.RunID != pending.Run.ID {
		t.Fatalf("real Harness question not durably suspended: %+v", pending)
	}
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var snapshot []byte
	var snapshotSHA string
	var snapshotSize int
	readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
	err = db.QueryRowContext(readCtx, `SELECT snapshot,snapshot_sha256,snapshot_size FROM run_prompts WHERE id=? AND run_id=?`, pending.Prompt.ID, pending.Run.ID).Scan(&snapshot, &snapshotSHA, &snapshotSize)
	cancelRead()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(snapshot)
	if len(snapshot) != snapshotSize || hex.EncodeToString(digest[:]) != snapshotSHA {
		t.Fatal("actual Harness snapshot SQL bytes differ from persisted integrity metadata")
	}
	var native struct {
		RunID string `json:"run_id"`
		Steps []struct {
			Calls []struct {
				Name   string `json:"name"`
				Result struct {
					Success bool `json:"success"`
				} `json:"result"`
			} `json:"tool_calls"`
		} `json:"steps"`
	}
	if json.Unmarshal(snapshot, &native) != nil || native.RunID != pending.Run.ID {
		t.Fatal("actual Harness snapshot not saved in MySQL")
	}
	snapshotted := map[string]bool{}
	for _, step := range native.Steps {
		for _, call := range step.Calls {
			if !call.Result.Success {
				t.Fatalf("native snapshot preserved failed tool %s", call.Name)
			}
			snapshotted[call.Name] = true
		}
	}
	for _, name := range names[:4] {
		if !snapshotted[name] {
			t.Fatalf("native snapshot omitted successful %s", name)
		}
	}
	startHarnessServer()
	resumePath := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
	sse, _, cancel := sseTestRequest(t, x.integrationFixture, resumePath, owner.Token, map[string]string{"answer": "国内"}, "")
	raw, err := io.ReadAll(sse.Body)
	cancel()
	sse.Body.Close()
	if err != nil || sse.StatusCode != 200 || sse.Header.Get("X-Run-ID") != pending.Run.ID {
		t.Fatalf("real resume SSE status=%d err=%v body=%s", sse.StatusCode, err, raw)
	}
	promptRequirePrivate(t, raw)
	var completed citedAnswer
	completedCount := 0
	for _, block := range strings.Split(string(raw), "\n\n") {
		if strings.Contains(block, "event: run.failed") || strings.Contains(block, "event: run.suspended") {
			t.Fatalf("unexpected real resume terminal: %s", raw)
		}
		if _, data, ok := strings.Cut(block, "event: run.completed\ndata: "); ok {
			completedCount++
			if err := json.Unmarshal([]byte(data), &completed); err != nil {
				t.Fatalf("decode real resume terminal: %v; %s", err, block)
			}
		}
	}
	mu.Lock()
	finalEvidenceID, finalModelCalls := evidenceID, modelCalls
	allToolsSeen := len(seen) == len(names)
	mu.Unlock()
	if completedCount != 1 || completed.Run.ID != pending.Run.ID || completed.Run.Status != "completed" || len(completed.Citations) != 1 || completed.Citations[0].Evidence.ID != finalEvidenceID || !strings.Contains(completed.Answer, "[E:"+finalEvidenceID+"]") || finalModelCalls != 6 || !allToolsSeen {
		t.Fatalf("real tool/resume/citation loop incomplete: %+v model_calls=%d tools=%v", completed, finalModelCalls, allToolsSeen)
	}
	persisted, err := x.store.EvidenceForRun(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, pending.Run.ID, true)
	if err != nil || len(persisted) != 1 || persisted[0].ID != finalEvidenceID || persisted[0].VersionID != write.Version.ID || !persisted[0].Cited {
		t.Fatalf("resumed evidence not durably cited: %+v err=%v", persisted, err)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 2 || messages[1].RunID != pending.Run.ID || messages[1].Content != completed.Answer || messages[1].Status != "completed" {
		t.Fatalf("resumed assistant message not committed exactly once: %+v", messages)
	}
}
