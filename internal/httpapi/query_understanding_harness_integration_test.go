package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
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
)

func queryUnderstandingRequest(request harnessHTTPModelRequest) bool {
	return len(request.Messages) > 0 && request.Messages[0].Role == "system" && strings.Contains(strings.ToLower(request.Messages[0].Content), "query understanding")
}

func queryUnderstandingEvidence(request harnessHTTPModelRequest) string {
	for _, message := range request.Messages {
		if message.Role != "tool" {
			continue
		}
		var result struct {
			Evidence []struct {
				ID string `json:"evidence_id"`
			} `json:"evidence"`
		}
		if json.Unmarshal([]byte(message.Content), &result) == nil && len(result.Evidence) > 0 {
			return result.Evidence[0].ID
		}
	}
	return ""
}

func queryUnderstandingSearches(x *indexFixture) []string {
	_, queries, _ := x.search.snapshot()
	result := make([]string, len(queries))
	for i, query := range queries {
		result[i] = query.text
	}
	return result
}

func assertQueryUnderstandingPrivate(t *testing.T, raw []byte) {
	t.Helper()
	for _, marker := range []string{"query_understanding", "rewrite_query"} {
		if strings.Contains(string(raw), marker) {
			t.Fatalf("private query plan leaked into an ordinary response: %s", marker)
		}
	}
}

// These requests traverse Go HTTP, the authenticated FastAPI worker, the real
// Harness and its normal tool callback. Only external model/index boundaries
// are controlled; query assertions inspect actual index adapter calls.
func TestHTTPQueryUnderstandingRealHarnessMultiTurnJSONAndSSE(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "query-understanding")
			x.upload(t, owner, "RAG架构先检索资料再生成答案。第二点是根据资料提供可核验来源。")
			x.process(t)
			session := x.session(t, owner, owner.Workspaces[0].ID, "RAG多轮理解")
			turns := []struct{ query, rewrite, intent string }{
				{"介绍RAG架构的两个特点", "介绍RAG架构的两个特点", ""},
				{"上面第二点展开讲讲", "展开RAG架构中可核验来源的说明", "follow_up"},
				{"它和传统搜索有什么区别", "RAG架构和传统搜索有什么区别", "kb_search"},
				{"这个话题还有哪些相关资料", "RAG架构与传统搜索对比的相关资料", "kb_search"},
				{"RAG架构故障排查资料", "RAG架构故障排查资料", "malformed"},
			}
			var mu sync.Mutex
			active, understandingCalls := 0, 0
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				request := harnessHTTPModelInput(t, r)
				turn := turns[active]
				if queryUnderstandingRequest(request) {
					understandingCalls++
					if active == 0 || request.Stream {
						t.Error("first turn or public stream unexpectedly used private understanding")
					}
					encoded, _ := json.Marshal(request.Messages)
					if !strings.Contains(string(encoded), turn.query) || !strings.Contains(string(encoded), "RAG") {
						t.Error("understanding lost current query or authenticated prior topic")
					}
					answer := `{"rewrite_query":`
					if turn.intent != "malformed" {
						raw, _ := json.Marshal(map[string]string{"rewrite_query": turn.rewrite, "intent": turn.intent})
						answer = string(raw)
					}
					harnessHTTPModelReply(w, request, answer, "", "")
					return
				}
				if request.Stream != streaming {
					t.Error("answer changed the requested JSON/SSE provider mode")
				}
				foundQuery := false
				for _, message := range request.Messages {
					foundQuery = foundQuery || (message.Role == "user" && message.Content == turn.query)
				}
				if !foundQuery {
					t.Error("original current user query was replaced by its rewrite")
				}
				evidenceID := queryUnderstandingEvidence(request)
				if turn.intent == "follow_up" {
					if evidenceID != "" {
						t.Error("history-sufficient follow-up performed an automatic search")
					}
					harnessHTTPModelReply(w, request, "第二点是让读者核对资料与回答之间的对应关系。", "", "")
					return
				}
				if evidenceID == "" {
					// Discovery and scope selection remain model decisions; the bridge
					// only rewrites the first actual search's query.
					args, _ := json.Marshal(map[string]string{"query": turn.query})
					harnessHTTPModelReply(w, request, "", "search_knowledge", string(args))
					return
				}
				harnessHTTPModelReply(w, request, "RAG的特点：第一点检索资料，第二点提供可核验来源。[E:"+evidenceID+"]", "", "")
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			installHarnessHTTPClient(x, harnessHTTPClient(t, service.url, callback))
			for index, turn := range turns {
				mu.Lock()
				active = index
				mu.Unlock()
				before := len(queryUnderstandingSearches(x))
				var runID string
				input := map[string]any{"content": turn.query}
				if streaming {
					result := harnessHTTPCompletedSSE(t, x, sessionPath(session)+"/messages", owner.Token, input)
					runID = result.Run.ID
					assertQueryUnderstandingPrivate(t, []byte(result.Answer))
				} else {
					response := x.call(http.MethodPost, sessionPath(session)+"/messages", owner.Token, input)
					assertQueryUnderstandingPrivate(t, response.body)
					result := integrationData[citedAnswer](t, checkIntegrationResponse(t, response, 201))
					runID = result.Run.ID
				}
				queries := queryUnderstandingSearches(x)[before:]
				if turn.intent == "follow_up" {
					if len(queries) != 0 {
						t.Fatalf("follow-up issued fresh index queries: %v", queries)
					}
				} else {
					if len(queries) == 0 {
						t.Fatal("expected actual index searches")
					}
					for _, query := range queries {
						if query != turn.rewrite {
							t.Fatalf("actual index query %q, want %q", query, turn.rewrite)
						}
					}
				}
				if turn.intent == "kb_search" || turn.intent == "follow_up" {
					facts := storedJournalFacts(t, x, runID)
					if len(facts) == 0 || !strings.Contains(string(facts[0]), "query_understanding") || !strings.Contains(string(facts[0]), turn.rewrite) {
						t.Fatal("query understanding was not stored in the initial MySQL journal fact")
					}
				}
			}
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 2*len(turns) {
				t.Fatalf("unexpected durable message count: %d", len(messages))
			}
			for index, turn := range turns {
				if messages[index*2].Content != turn.query {
					t.Fatal("rewrite mutated the durable user message")
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if understandingCalls != len(turns)-1 {
				t.Fatalf("wrong understanding call count: %d", understandingCalls)
			}
		})
	}
}

func TestHTTPQueryUnderstandingFiltersUnauthorizedHistoryBeforeModel(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "query-history-scope")
	private := x.upload(t, owner, "PRIVATE_QUERY_TOPIC_FROM_DOCUMENT")
	x.process(t)
	allowed := x.upload(t, owner, "当前范围允许的资料")
	x.process(t)
	session := x.session(t, owner, owner.Workspaces[0].ID, "历史范围检查")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		_, err := acquireTestEvidence(ctx, request)
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "PRIVATE_QUERY_TOPIC_FROM_DOCUMENT"}, err
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "PRIVATE_QUERY_TOPIC_FROM_DOCUMENT", "document_ids": []string{private.Document.ID}}, 201)
	seedContextTurns(t, x.integrationFixture, owner, session, 1, "SAFE_INDEPENDENT_HISTORY")
	var mu sync.Mutex
	understandingCalls := 0
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		request := harnessHTTPModelInput(t, r)
		encoded, _ := json.Marshal(request.Messages)
		if strings.Contains(string(encoded), "PRIVATE_QUERY_TOPIC_FROM_DOCUMENT") {
			t.Error("out-of-scope or deleted history reached a model")
		}
		if queryUnderstandingRequest(request) {
			understandingCalls++
			if !strings.Contains(string(encoded), "SAFE_INDEPENDENT_HISTORY") {
				t.Error("source filtering also discarded unrelated authorized history")
			}
			harnessHTTPModelReply(w, request, `{"rewrite_query":"展开独立会话内容","intent":"follow_up"}`, "", "")
			return
		}
		harnessHTTPModelReply(w, request, "仅使用当前仍可访问的会话内容。", "", "")
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	installHarnessHTTPClient(x, harnessHTTPClient(t, service.url, callback))
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "展开上面的说明", "document_ids": []string{allowed.Document.ID}}, 201)
	x.request(t, "DELETE", documentPath(private.Document), owner.Token, nil, 204)
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "继续说明"}, 201)
	mu.Lock()
	defer mu.Unlock()
	if understandingCalls != 2 {
		t.Fatalf("expected both authorized follow-ups to be understood, got %d", understandingCalls)
	}
}

func TestHTTPQueryUnderstandingCommittedSearchSurvivesRestart(t *testing.T) {
	for _, crash := range []struct {
		name  string
		round int
	}{{"created_before_first_plan", 0}, {"search_round_committed", 1}} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", crash.name, streaming), func(t *testing.T) {
				x := newIndexFixture(t)
				owner := x.register(t, "query-recovery")
				x.upload(t, owner, "RAG架构与传统搜索的差别在生成与证据结合。")
				x.process(t)
				session := x.session(t, owner, owner.Workspaces[0].ID, "理解后恢复")
				seedContextTurns(t, x.integrationFixture, owner, session, 1, "RAG架构")
				const query = "它和传统搜索有什么区别"
				const rewrite = "RAG架构和传统搜索有什么区别"
				var mu sync.Mutex
				understandingCalls, answerCalls := 0, 0
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					request := harnessHTTPModelInput(t, r)
					if queryUnderstandingRequest(request) {
						understandingCalls++
						harnessHTTPModelReply(w, request, `{"rewrite_query":"`+rewrite+`","intent":"kb_search"}`, "", "")
						return
					}
					answerCalls++
					evidenceID := queryUnderstandingEvidence(request)
					if evidenceID == "" {
						args, _ := json.Marshal(map[string]string{"query": query})
						harnessHTTPModelReply(w, request, "", "search_knowledge", string(args))
						return
					}
					harnessHTTPModelReply(w, request, "已根据资料继续比较。[E:"+evidenceID+"]", "", "")
				}))
				defer provider.Close()
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
				configuration := agent.Config{Python: python, Script: bridgePath, HarnessPath: harness, Mode: "live", BaseURL: provider.URL, APIKey: "local-test-key", Model: "controlled", Timeout: 20 * time.Second}
				directory := t.TempDir()
				marker, shim := filepath.Join(directory, "search-committed"), filepath.Join(directory, "interrupt_query.py")
				bridgeJSON, _ := json.Marshal(filepath.Dir(bridgePath))
				markerJSON, _ := json.Marshal(marker)
				code := fmt.Sprintf(`import sys, os
from pathlib import Path
sys.path.insert(0, %s)
import bridge
from magent_harness import AgentEngine
original_think = AgentEngine._think
marker = Path(%s)
async def interrupted_think(self, run):
    if run.round == %d and not marker.exists():
        marker.write_text("query-understanding recovery boundary")
        os._exit(73)
    return await original_think(self, run)
AgentEngine._think = interrupted_think
sys.exit(bridge.main())
`, bridgeJSON, markerJSON, crash.round)
				if err = os.WriteFile(shim, []byte(code), 0600); err != nil {
					t.Fatal(err)
				}
				newClient := func(script string) *agent.ProcessClient {
					t.Helper()
					candidate := configuration
					candidate.Script = script
					client, err := agent.NewProcessClient(candidate)
					if err != nil {
						t.Fatal(err)
					}
					return client
				}
				installHarnessHTTPClient(x, newClient(shim))
				response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": query})
				checkIntegrationResponse(t, response, 502)
				assertQueryUnderstandingPrivate(t, response.body)
				if _, err = os.Stat(marker); err != nil {
					t.Fatal("real worker did not interrupt at the requested durable boundary")
				}
				messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
				if len(messages) != 3 || messages[2].Content != query {
					t.Fatal("interruption changed or duplicated the original user query")
				}
				runID := messages[2].RunID
				facts := storedJournalFacts(t, x, runID)
				if len(facts) == 0 || !strings.Contains(string(facts[0]), "query_understanding") || !strings.Contains(string(facts[0]), rewrite) {
					t.Fatal("restart has no durable initial query-understanding plan")
				}
				before := queryUnderstandingSearches(x)
				if crash.round == 0 {
					if len(before) != 0 || len(facts) != 1 {
						t.Fatalf("first-plan interruption already executed work: searches=%d facts=%d", len(before), len(facts))
					}
				} else if len(before) == 0 {
					t.Fatal("post-search interruption occurred before the rewritten search")
				}
				for _, actual := range before {
					if actual != rewrite {
						t.Fatalf("search did not use rewrite: %q", actual)
					}
				}
				mu.Lock()
				if understandingCalls != 1 || answerCalls != crash.round {
					t.Errorf("wrong interruption boundary: understand=%d answer=%d", understandingCalls, answerCalls)
				}
				mu.Unlock()
				recoveryRestart(t, x)
				installHarnessHTTPClient(x, newClient(bridgePath))
				if streaming {
					harnessHTTPCompletedSSE(t, x, recoveryPath(session, runID), owner.Token, map[string]any{})
				} else {
					response = x.call("POST", recoveryPath(session, runID), owner.Token, map[string]any{})
					checkIntegrationResponse(t, response, 201)
					assertQueryUnderstandingPrivate(t, response.body)
				}
				after := queryUnderstandingSearches(x)
				if crash.round == 0 {
					if len(after) == 0 {
						t.Fatal("created-only recovery failed to execute its saved query plan")
					}
					for _, actual := range after {
						if actual != rewrite {
							t.Fatalf("created-only recovery did not use its saved rewrite: %q", actual)
						}
					}
				} else if len(after) != len(before) {
					t.Fatal("recovery repeated an already committed search")
				}
				mu.Lock()
				defer mu.Unlock()
				if understandingCalls != 1 || answerCalls != 2 {
					t.Fatalf("recovery repeated understanding or answer: understand=%d answer=%d", understandingCalls, answerCalls)
				}
				messages = integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
				if len(messages) != 4 || messages[2].RunID != runID || messages[3].RunID != runID {
					t.Fatal("recovery changed the run or duplicated messages")
				}
			})
		}
	}
}

// A successful "other" plan keeps native actions available. Resuming an
// ask_user snapshot must restore that private plan without another classifier
// request, while the authenticated user's original query stays immutable.
func TestHTTPQueryUnderstandingOtherPlanResumesQuestionAfterServiceRestart(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "query-plan-resume")
			x.upload(t, owner, "国内和海外部署的可用资料。")
			x.process(t)
			session := x.session(t, owner, owner.Workspaces[0].ID, "理解后的澄清恢复")
			seedContextTurns(t, x.integrationFixture, owner, session, 1, "准备部署配置")
			const query = "先问我部署范围，收到答复后再继续"
			const rewrite = "询问用户部署配置的国内或海外范围并等待答复"
			const answer = "已确认国内部署范围，可以继续。"
			var mu sync.Mutex
			understandingCalls, modelCalls := 0, 0
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				request := harnessHTTPModelInput(t, r)
				if queryUnderstandingRequest(request) {
					understandingCalls++
					harnessHTTPModelReply(w, request, `{"rewrite_query":"`+rewrite+`","intent":"other"}`, "", "")
					return
				}
				modelCalls++
				foundQuery, foundPlan, foundAnswer := false, false, false
				for _, message := range request.Messages {
					foundQuery = foundQuery || (message.Role == "user" && message.Content == query)
					foundPlan = foundPlan || (message.Role == "system" && strings.Contains(message.Content, rewrite) && strings.Contains(message.Content, `"intent":"other"`))
					foundAnswer = foundAnswer || (message.Role == "tool" && message.ToolCallID == "http-ask_user" && strings.Contains(message.Content, "国内"))
				}
				if !foundQuery || !foundPlan {
					t.Error("native action lost the original query or restored private routing plan")
				}
				if queryUnderstandingEvidence(request) != "" {
					t.Error("other intent unexpectedly injected automatic knowledge retrieval")
				}
				switch modelCalls {
				case 1:
					harnessHTTPModelReply(w, request, "", "ask_user", `{"question":"选择部署范围","options":["国内","海外"]}`)
				case 2:
					if !foundAnswer || request.Stream != streaming {
						t.Error("resume lost the authenticated human answer or requested stream mode")
					}
					harnessHTTPModelReply(w, request, answer, "", "")
				default:
					t.Error("resuming a question repeated completed model work")
					http.Error(w, "unexpected model replay", http.StatusBadRequest)
				}
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			client := harnessHTTPClient(t, service.url, callback)
			installHarnessHTTPClient(x, client)
			response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query})
			pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
			assertQueryUnderstandingPrivate(t, response.body)
			assertJournalResponsePrivate(t, response.body)
			if pending.Run.Status != "suspended" || pending.Prompt.Kind != "ask_user" {
				t.Fatal("other routing plan hijacked the native clarification action")
			}
			facts := storedJournalFacts(t, x, pending.Run.ID)
			if len(facts) == 0 || !strings.Contains(string(facts[0]), "query_understanding") || !strings.Contains(string(facts[0]), rewrite) {
				t.Fatal("suspended run lost its initial persisted query plan")
			}
			service.stop()
			service.start()
			recoveryRestart(t, x)
			installHarnessHTTPClient(x, client)
			path := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
			var completed recoveryHTTPResult
			if streaming {
				completed = harnessHTTPCompletedSSE(t, x, path, owner.Token, map[string]string{"answer": "国内"})
			} else {
				response = x.call("POST", path, owner.Token, map[string]string{"answer": "国内"})
				assertQueryUnderstandingPrivate(t, response.body)
				completed = integrationData[recoveryHTTPResult](t, checkIntegrationResponse(t, response, 201))
			}
			if completed.Run.ID != pending.Run.ID || completed.Answer != answer || len(queryUnderstandingSearches(x)) != 0 {
				t.Fatal("resume changed the run, omitted its answer, or introduced an unrelated search")
			}
			x.request(t, "POST", path, owner.Token, map[string]string{"answer": "海外"}, 409)
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 4 || messages[2].Content != query || messages[2].RunID != pending.Run.ID || messages[3].RunID != pending.Run.ID || messages[3].Content != answer {
				t.Fatal("question resume rewrote or duplicated the persisted current turn")
			}
			mu.Lock()
			defer mu.Unlock()
			if understandingCalls != 1 || modelCalls != 2 {
				t.Fatalf("resume repeated understanding or actions: understand=%d model=%d", understandingCalls, modelCalls)
			}
		})
	}
}

// A successful rewrite must wait for library discovery and preserve the chosen
// filters after suspension, service restart and JSON/SSE resumption.
func TestHTTPQueryUnderstandingKnowledgeBaseSelectionSurvivesResume(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "query-library-selection")
			wid := owner.Workspaces[0].ID
			target := createHTTPKnowledgeBase(t, x, owner, "旅行知识库")
			other := createHTTPKnowledgeBase(t, x, owner, "其他资料")
			selected := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, target.ID, "旅行住宿限额：每晚 500 元。")
			x.process(t)
			excluded := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, other.ID, "旅行住宿限额 OTHER_LIBRARY_SECRET")
			x.process(t)
			writes, _, _ := x.search.snapshot()
			selectedGeneration := ""
			for _, write := range writes {
				if len(write.chunks) > 0 && write.chunks[0].DocumentID == selected.Document.ID {
					selectedGeneration = write.generation
				}
			}
			if selectedGeneration == "" {
				t.Fatal("selected library fixture is not indexed")
			}
			session := x.session(t, owner, wid, "追问选择知识库")
			seedContextTurns(t, x.integrationFixture, owner, session, 1, "正在规划旅行住宿")
			const query = "继续，只在旅行知识库查它的限额，检索前先和我确认"
			const rewrite = "只在旅行知识库检索旅行住宿限额"
			var mu sync.Mutex
			understandingCalls, modelCalls := 0, 0
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				request := harnessHTTPModelInput(t, r)
				for _, message := range request.Messages {
					if strings.Contains(message.Content, "OTHER_LIBRARY_SECRET") || strings.Contains(message.Content, excluded.Document.ID) {
						t.Error("query understanding widened the requested library before model selection")
					}
				}
				if queryUnderstandingRequest(request) {
					understandingCalls++
					harnessHTTPModelReply(w, request, `{"rewrite_query":"`+rewrite+`","intent":"kb_search"}`, "", "")
					return
				}
				modelCalls++
				switch modelCalls {
				case 1:
					if len(queryUnderstandingSearches(x)) != 0 {
						t.Error("rewrite searched before the model discovered knowledge bases")
					}
					harnessHTTPModelReply(w, request, "", "list_knowledge_bases", `{"limit":20}`)
				case 2:
					found := false
					for _, message := range request.Messages {
						if message.Role == "tool" && message.ToolCallID == "http-list_knowledge_bases" {
							var page struct {
								KnowledgeBases []httpKnowledgeBase `json:"knowledge_bases"`
							}
							if json.Unmarshal([]byte(message.Content), &page) == nil {
								for _, kb := range page.KnowledgeBases {
									found = found || (kb.ID == target.ID && kb.Name == target.Name)
								}
							}
						}
					}
					if !found {
						t.Error("model did not discover the selected knowledge base")
					}
					harnessHTTPModelReply(w, request, "", "ask_user", `{"question":"仅查询旅行知识库，对吗？","options":["确认","取消"]}`)
				case 3:
					confirmed := false
					for _, message := range request.Messages {
						confirmed = confirmed || (message.Role == "tool" && message.ToolCallID == "http-ask_user" && strings.Contains(message.Content, "确认"))
					}
					if !confirmed || request.Stream != streaming {
						t.Error("resume did not preserve the human answer or requested stream mode")
					}
					args, _ := json.Marshal(map[string]any{"query": "它的限额", "knowledge_base_ids": []string{target.ID}, "mode": "keyword", "limit": 1})
					harnessHTTPModelReply(w, request, "", "search_knowledge", string(args))
				case 4:
					evidenceID := queryUnderstandingEvidence(request)
					if evidenceID == "" {
						t.Error("selected-library search returned no evidence")
					}
					for _, message := range request.Messages {
						if message.Role == "tool" && message.ToolCallID == "http-search_knowledge" {
							var result struct {
								Evidence []struct {
									DocumentID string `json:"document_id"`
								} `json:"evidence"`
							}
							if json.Unmarshal([]byte(message.Content), &result) != nil || len(result.Evidence) != 1 || result.Evidence[0].DocumentID != selected.Document.ID {
								t.Error("rewritten search lost its selected library or limit")
							}
						}
					}
					harnessHTTPModelReply(w, request, "旅行知识库的住宿限额为每晚500元。[E:"+evidenceID+"]", "", "")
				default:
					t.Error("query routing repeated discovery or committed model work")
					http.Error(w, "unexpected replay", http.StatusBadRequest)
				}
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			client := harnessHTTPClient(t, service.url, callback)
			installHarnessHTTPClient(x, client)
			response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query})
			pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
			if pending.Prompt.Kind != "ask_user" || len(queryUnderstandingSearches(x)) != 0 {
				t.Fatal("library confirmation was bypassed by an automatic workspace search")
			}
			facts := storedJournalFacts(t, x, pending.Run.ID)
			if len(facts) == 0 || !strings.Contains(string(facts[0]), rewrite) {
				t.Fatal("suspension lost the original query-understanding plan")
			}
			service.stop()
			service.start()
			recoveryRestart(t, x)
			installHarnessHTTPClient(x, client)
			path := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
			var completed recoveryHTTPResult
			if streaming {
				completed = harnessHTTPCompletedSSE(t, x, path, owner.Token, map[string]string{"answer": "确认"})
			} else {
				completed = integrationData[recoveryHTTPResult](t, x.request(t, "POST", path, owner.Token, map[string]string{"answer": "确认"}, 201))
			}
			if completed.Run.ID != pending.Run.ID || !strings.Contains(completed.Answer, "500") {
				t.Fatal("selected-library resume lost its run or answer")
			}
			_, queries, _ := x.search.snapshot()
			if len(queries) != 1 {
				t.Fatalf("expected one selected-library query, got %d", len(queries))
			}
			for _, actual := range queries {
				if actual.text != rewrite || actual.mode != "lexical" || len(actual.generations) != 1 || actual.generations[0] != selectedGeneration {
					t.Fatalf("rewrite changed search filters: query=%+v", actual)
				}
			}
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 4 || messages[2].Content != query || messages[2].RunID != pending.Run.ID || messages[3].RunID != pending.Run.ID {
				t.Fatal("resume modified or duplicated the user question")
			}
			mu.Lock()
			defer mu.Unlock()
			if understandingCalls != 1 || modelCalls != 4 {
				t.Fatalf("resume repeated understanding or work: understand=%d model=%d", understandingCalls, modelCalls)
			}
		})
	}
}
