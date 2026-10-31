package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/store"
)

func storedResourceSelectionScope(t *testing.T, x *indexFixture, runID string) domain.ResourceScope {
	t.Helper()
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT scope FROM run_search_targets WHERE run_id=?`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var scope domain.ResourceScope
	if err := json.Unmarshal(raw, &scope); err != nil || scope.Validate() != nil {
		t.Fatalf("invalid stored resource selection: %s %v", raw, err)
	}
	return scope
}

func resourceSelectionSuspendedSSE(t *testing.T, x *indexFixture, path, token string, input any) promptHTTPResult {
	t.Helper()
	response, _, cancel := sseTestRequest(t, x.integrationFixture, path, token, input, "")
	raw, err := io.ReadAll(response.Body)
	cancel()
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("resource selection SSE status=%d error=%v body=%s", response.StatusCode, err, raw)
	}
	assertJournalResponsePrivate(t, raw)
	var pending promptHTTPResult
	terminals := 0
	for _, block := range strings.Split(string(raw), "\n\n") {
		if strings.Contains(block, "event: run.failed") || strings.Contains(block, "event: run.completed") {
			t.Fatalf("unexpected terminal before question: %s", raw)
		}
		if !strings.Contains(block, "event: run.suspended\n") {
			continue
		}
		terminals++
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "data: ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &pending); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if terminals != 1 || pending.Run.Status != "suspended" || pending.Prompt.ID == "" || pending.Run.ID != response.Header.Get("X-Run-ID") {
		t.Fatalf("suspension was not durably emitted exactly once: %s", raw)
	}
	return pending
}

func TestHTTPResourceSelectionHarnessUnionRestartJSONAndSSE(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "resource-selection-harness")
			wid := owner.Workspaces[0].ID
			a, b := createHTTPKnowledgeBase(t, x, owner, "完整政策库"), createHTTPKnowledgeBase(t, x, owner, "只选单文档库")
			var documents []domain.DocumentWrite
			for i, item := range []struct{ kb, text string }{{a.ID, "政策甲：每日500元。"}, {a.ID, "政策乙：每月2000元。"}, {b.ID, "单选政策：提前3天申请。"}, {b.ID, "UNSELECTED_B_SIBLING_SECRET"}} {
				write := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, knowledgeBasePath(wid, item.kb)+"/documents", owner.Token, 201,
					documentFile(fmt.Sprintf("policy-%d.txt", i), []byte(item.text)), documentField("title", fmt.Sprintf("政策条目%d", i))))
				documents = append(documents, write)
				x.process(t)
			}
			allowedIDs := []string{documents[0].Document.ID, documents[1].Document.ID, documents[2].Document.ID}
			allowedGenerations := []string{}
			for _, write := range documents[:3] {
				allowedGenerations = append(allowedGenerations, x.job(t, owner, write).Generation)
			}
			selectedGeneration := allowedGenerations[2]
			session := x.session(t, owner, wid, "混合资源范围")
			var modelCalls, objectReads atomic.Int32
			x.objects.readHook(func(context.Context, string) error { objectReads.Add(1); return nil })
			assertDocuments := func(request harnessHTTPModelRequest, want []string) {
				var result store.ToolDocumentPage
				if err := json.Unmarshal([]byte(latestRunScopeTool(t, request, "list_documents")), &result); err != nil || len(result.Documents) != len(want) {
					t.Errorf("document selection mismatch: %+v %v", result, err)
					return
				}
				for _, item := range result.Documents {
					if !slices.Contains(want, item.Document.ID) {
						t.Errorf("document directory escaped selection: %s", item.Document.ID)
					}
				}
			}
			assertEvidence := func(request harnessHTTPModelRequest, want []string) string {
				var result struct {
					Evidence []domain.Evidence `json:"evidence"`
				}
				if err := json.Unmarshal([]byte(latestRunScopeTool(t, request, "search_knowledge")), &result); err != nil || len(result.Evidence) != len(want) {
					t.Errorf("evidence selection mismatch: %+v %v", result, err)
					return ""
				}
				seen := map[string]bool{}
				for _, item := range result.Evidence {
					seen[item.DocumentID] = true
					if !slices.Contains(want, item.DocumentID) {
						t.Errorf("retrieval escaped selection: %s", item.DocumentID)
					}
				}
				if len(seen) != len(want) {
					t.Error("retrieval omitted a selected document or full-library sibling")
				}
				return result.Evidence[0].ID
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := harnessHTTPModelInput(t, r)
				if queryUnderstandingRequest(request) {
					t.Error("initial request or resume unexpectedly invoked query rewriting")
					harnessHTTPModelReply(w, request, "not-json", "", "")
					return
				}
				if request.Stream != streaming {
					t.Error("Harness changed JSON/SSE transport")
				}
				for _, message := range request.Messages {
					if strings.Contains(message.Content, "UNSELECTED_B_SIBLING_SECRET") {
						t.Error("unselected sibling source reached the model")
					}
				}
				switch step := modelCalls.Add(1); step {
				case 1:
					harnessHTTPModelReply(w, request, "", "list_documents", `{"limit":20}`)
				case 2:
					assertDocuments(request, allowedIDs)
					harnessHTTPModelReply(w, request, "", "search_knowledge", `{"query":"政策规则","mode":"keyword","limit":10}`)
				case 3:
					assertEvidence(request, allowedIDs)
					args, _ := json.Marshal(map[string]any{"query": "单选政策", "mode": "keyword", "knowledge_base_ids": []string{b.ID}})
					harnessHTTPModelReply(w, request, "", "search_knowledge", string(args))
				case 4:
					assertEvidence(request, []string{documents[2].Document.ID})
					args, _ := json.Marshal(map[string]any{"document_id": documents[3].Document.ID, "source": true})
					harnessHTTPModelReply(w, request, "", "read_document", string(args))
				case 5:
					result := latestRunScopeTool(t, request, "read_document")
					if result != "Error: tool_unavailable" || objectReads.Load() != 0 {
						t.Errorf("unselected sibling read was not blocked before storage: %q reads=%d", result, objectReads.Load())
					}
					harnessHTTPModelReply(w, request, "", "ask_user", `{"question":"继续使用当前资源？","options":["继续","取消"]}`)
				case 6:
					if !strings.Contains(latestRunScopeTool(t, request, "ask_user"), "继续") {
						t.Error("resume lost the user answer")
					}
					args, _ := json.Marshal(map[string]any{"knowledge_base_ids": []string{b.ID}, "limit": 20})
					harnessHTTPModelReply(w, request, "", "list_documents", string(args))
				case 7:
					assertDocuments(request, []string{documents[2].Document.ID})
					harnessHTTPModelReply(w, request, "", "search_knowledge", `{"query":"恢复后政策规则","mode":"keyword","limit":10}`)
				case 8:
					id := assertEvidence(request, allowedIDs)
					harnessHTTPModelReply(w, request, "已核验整库和单文档的联合范围。[E:"+id+"]", "", "")
				default:
					t.Errorf("unexpected model replay: %d", step)
					http.Error(w, "unexpected model replay", http.StatusBadRequest)
				}
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			client := harnessHTTPClient(t, service.url, callback)
			installHarnessHTTPClient(x, client)
			input := map[string]any{"content": "核验完整政策库以及选中的单份政策，继续前询问我", "knowledge_base_ids": []string{a.ID}, "document_ids": []string{documents[2].Document.ID}}
			var pending promptHTTPResult
			if streaming {
				pending = resourceSelectionSuspendedSSE(t, x, sessionPath(session)+"/messages", owner.Token, input)
			} else {
				pending = integrationData[promptHTTPResult](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, http.StatusAccepted))
			}
			before := storedResourceSelectionScope(t, x, pending.Run.ID)
			if before.All || len(before.Targets) != 2 || !before.AllowsCreate(a.ID) || before.AllowsCreate(b.ID) || !before.AllowsDocument(a.ID, documents[1].Document.ID) || !before.AllowsDocument(b.ID, documents[2].Document.ID) || before.AllowsDocument(b.ID, documents[3].Document.ID) {
				t.Fatalf("mixed selection was not fixed with the run: %+v", before)
			}
			service.stop()
			service.start()
			recoveryRestart(t, x)
			installHarnessHTTPClient(x, client)
			path := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
			x.request(t, "POST", path, owner.Token, map[string]any{"answer": "继续", "document_ids": []string{documents[3].Document.ID}}, http.StatusBadRequest)
			if modelCalls.Load() != 5 {
				t.Fatal("injected replacement scope reached the model or consumed the prompt")
			}
			var completed recoveryHTTPResult
			if streaming {
				completed = harnessHTTPCompletedSSE(t, x, path, owner.Token, map[string]string{"answer": "继续"})
			} else {
				completed = integrationData[recoveryHTTPResult](t, x.request(t, "POST", path, owner.Token, map[string]string{"answer": "继续"}, http.StatusCreated))
			}
			if completed.Run.ID != pending.Run.ID || completed.Run.Status != "completed" || !strings.Contains(completed.Answer, "联合范围") || modelCalls.Load() != 8 || !reflect.DeepEqual(before, storedResourceSelectionScope(t, x, pending.Run.ID)) {
				t.Fatalf("resume changed scope or replayed execution: %+v modelCalls=%d", completed, modelCalls.Load())
			}
			_, queries, _ := x.search.snapshot()
			if len(queries) != 3 {
				t.Fatalf("expected three successful search calls, got %d", len(queries))
			}
			for i, query := range queries {
				want := allowedGenerations
				if i == 1 {
					want = []string{selectedGeneration}
				}
				if query.workspace != wid || query.mode != "lexical" || len(query.generations) != len(want) {
					t.Fatalf("external scope mismatch: %+v", query)
				}
				for _, generation := range want {
					if !slices.Contains(query.generations, generation) {
						t.Fatalf("selected generation omitted: %+v", query)
					}
				}
			}
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
			if len(messages) != 2 || messages[0].Content != input["content"] {
				t.Fatal("resume duplicated messages or changed the question")
			}
		})
	}
}

func TestHTTPResourceSelectionDocumentSearchPaginationAndPermissions(t *testing.T) {
	x := newIndexFixture(t)
	owner, outsider := x.register(t, "mention-document-owner"), x.register(t, "mention-document-outsider")
	wid := owner.Workspaces[0].ID
	a, b := createHTTPKnowledgeBase(t, x, owner, "检索库甲"), createHTTPKnowledgeBase(t, x, owner, "检索库乙")
	selected := map[string]bool{}
	for _, item := range []struct{ kb, title string }{{a.ID, "needle document A"}, {b.ID, "needle document B"}, {a.ID, "unmatched"}, {b.ID, "100% literal"}} {
		write := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, knowledgeBasePath(wid, item.kb)+"/documents", owner.Token, 201,
			documentFile("source.txt", []byte("source body not returned by picker")), documentField("title", item.title)))
		if strings.Contains(item.title, "needle") {
			selected[write.Document.ID] = true
		}
	}
	seen := map[string]bool{}
	for _, page := range []int{1, 2, 3} {
		response := x.request(t, "GET", documentsPath(wid)+fmt.Sprintf("?keyword=needle&page=%d&page_size=1", page), owner.Token, nil, http.StatusOK)
		items := integrationData[[]domain.Document](t, response)
		want := 1
		if page == 3 {
			want = 0
		}
		if response.Total != 2 || len(items) != want {
			t.Fatalf("keyword pagination mismatch: %+v total=%d", items, response.Total)
		}
		for _, item := range items {
			if !selected[item.ID] || seen[item.ID] || (item.KnowledgeBaseID != a.ID && item.KnowledgeBaseID != b.ID) {
				t.Fatalf("picker page leaked, repeated, or omitted ownership: %+v", item)
			}
			seen[item.ID] = true
		}
	}
	if len(seen) != 2 {
		t.Fatal("picker could not reach matching documents across knowledge bases")
	}
	literal := x.request(t, "GET", documentsPath(wid)+"?keyword="+url.QueryEscape("%"), owner.Token, nil, http.StatusOK)
	items := integrationData[[]domain.Document](t, literal)
	if literal.Total != 1 || len(items) != 1 || items[0].Title != "100% literal" {
		t.Fatal("keyword interpreted as a wildcard instead of literal text")
	}
	x.request(t, "GET", documentsPath(wid)+"?keyword=needle", outsider.Token, nil, http.StatusNotFound)
	x.request(t, "GET", documentsPath(wid)+"?keyword="+strings.Repeat("x", 257), owner.Token, nil, http.StatusBadRequest)
}

func TestHTTPResourceSelectionWholeLibraryCoversOwnDocument(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "resource-whole-library")
	wid := owner.Workspaces[0].ID
	library := createHTTPKnowledgeBase(t, x, owner, "Whole library")
	first := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, library.ID, "First explicitly mentioned document")
	x.process(t)
	second := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, library.ID, "Second document covered by whole library")
	x.process(t)
	for _, wholeLibrary := range []bool{false, true} {
		t.Run(fmt.Sprint("whole-library=", wholeLibrary), func(t *testing.T) {
			session := x.session(t, owner, wid, "Whole library dominates own document")
			want := []string{first.Document.ID}
			if wholeLibrary {
				want = append(want, second.Document.ID)
			}
			x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
				var page store.ToolDocumentPage
				if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{"knowledge_base_ids": []string{library.ID}}, &page); err != nil {
					return agent.Result{}, err
				}
				if len(page.Documents) != len(want) {
					return agent.Result{}, fmt.Errorf("whole-library precedence returned %d documents, want %d", len(page.Documents), len(want))
				}
				for _, item := range page.Documents {
					if !slices.Contains(want, item.Document.ID) {
						return agent.Result{}, fmt.Errorf("unexpected document %s", item.Document.ID)
					}
				}
				return agent.Result{RunID: req.RunID, Answer: "scope verified", Mode: "demo"}, nil
			})
			input := map[string]any{"content": "Compare explicit document and whole library", "document_ids": []string{first.Document.ID}}
			if wholeLibrary {
				input["knowledge_base_ids"] = []string{library.ID}
			}
			completed := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, http.StatusCreated))
			scope := storedResourceSelectionScope(t, x, completed.Run.ID)
			if len(scope.Targets) != 1 || scope.All || (scope.Targets[0].DocumentIDs == nil) != wholeLibrary || scope.AllowsDocument(library.ID, second.Document.ID) != wholeLibrary {
				t.Fatalf("same-library document/full-library normalization failed: %+v", scope)
			}
		})
	}
}
