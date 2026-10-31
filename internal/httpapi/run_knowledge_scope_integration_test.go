package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/store"
)

func storedRunKnowledgeScope(t *testing.T, x *indexFixture, runID string) []string {
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
		t.Fatalf("invalid persisted resource scope: %s, error=%v", raw, err)
	}
	if scope.All {
		return nil
	}
	ids := []string{}
	for _, target := range scope.Targets {
		if target.DocumentIDs == nil {
			ids = append(ids, target.KnowledgeBaseID)
		}
	}
	return ids
}

func latestRunScopeTool(t *testing.T, request harnessHTTPModelRequest, name string) string {
	t.Helper()
	for i := len(request.Messages) - 1; i >= 0; i-- {
		message := request.Messages[i]
		if message.Role == "tool" && message.ToolCallID == "http-"+name {
			return message.Content
		}
	}
	t.Errorf("provider did not receive %s tool result", name)
	return ""
}

// The caller chooses A once. All later model requests, including omitted
// filters and guessed B identifiers, traverse the real authenticated callbacks.
func TestHTTPRunKnowledgeScopeHarnessRestartJSONAndSSE(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "run-kb-harness")
			wid := owner.Workspaces[0].ID
			selectedKB := createHTTPKnowledgeBase(t, x, owner, "允许的旅行库")
			otherKB := createHTTPKnowledgeBase(t, x, owner, "OUTSIDE_LIBRARY_NAME")
			selected := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, selectedKB.ID, "旅行规则：每晚 500 元。")
			x.process(t)
			other := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, otherKB.ID, "OUTSIDE_LIBRARY_SOURCE_SECRET")
			x.process(t)
			selectedGeneration := x.job(t, owner, selected).Generation
			session := x.session(t, owner, wid, "后端整轮选库")
			var modelCalls, sourceReads atomic.Int32
			x.objects.readHook(func(context.Context, string) error { sourceReads.Add(1); return nil })
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := harnessHTTPModelInput(t, r)
				for _, message := range request.Messages {
					if strings.Contains(message.Content, "OUTSIDE_LIBRARY_SOURCE_SECRET") || strings.Contains(message.Content, otherKB.Name) {
						t.Error("out-of-scope source or library metadata reached the model")
					}
				}
				if queryUnderstandingRequest(request) {
					t.Error("first turn or resume unexpectedly reinterpreted the request")
					harnessHTTPModelReply(w, request, "not-json", "", "")
					return
				}
				switch step := modelCalls.Add(1); step {
				case 1:
					harnessHTTPModelReply(w, request, "", "list_knowledge_bases", `{"limit":20}`)
				case 2:
					var result struct {
						KnowledgeBases []httpKnowledgeBase `json:"knowledge_bases"`
					}
					if json.Unmarshal([]byte(latestRunScopeTool(t, request, "list_knowledge_bases")), &result) != nil || len(result.KnowledgeBases) != 1 || result.KnowledgeBases[0].ID != selectedKB.ID {
						t.Error("omitted model library filter broadened discovery beyond caller scope")
					}
					harnessHTTPModelReply(w, request, "", "list_documents", `{"limit":20}`)
				case 3:
					var result store.ToolDocumentPage
					if json.Unmarshal([]byte(latestRunScopeTool(t, request, "list_documents")), &result) != nil || len(result.Documents) != 1 || result.Documents[0].Document.ID != selected.Document.ID {
						t.Error("omitted model library filter broadened document directory")
					}
					harnessHTTPModelReply(w, request, "", "search_knowledge", `{"query":"旅行规则","mode":"keyword"}`)
				case 4:
					var result struct {
						Evidence []domain.Evidence `json:"evidence"`
					}
					if json.Unmarshal([]byte(latestRunScopeTool(t, request, "search_knowledge")), &result) != nil || len(result.Evidence) != 1 || result.Evidence[0].DocumentID != selected.Document.ID {
						t.Error("omitted model library filter broadened search")
					}
					args, _ := json.Marshal(map[string]any{"query": "猜测其他库", "knowledge_base_ids": []string{selectedKB.ID, otherKB.ID}})
					harnessHTTPModelReply(w, request, "", "search_knowledge", string(args))
				case 5:
					// Ordinary domain errors use the callback's fixed redacted code;
					// the Store tests separately assert ErrForbidden itself.
					result := latestRunScopeTool(t, request, "search_knowledge")
					if result != "Error: tool_unavailable" || len(queryUnderstandingSearches(x)) != 1 {
						t.Errorf("mixed selection was not wholly denied before external recall: result=%q calls=%d", result, len(queryUnderstandingSearches(x)))
					}
					args, _ := json.Marshal(map[string]any{"document_id": other.Document.ID, "source": true})
					harnessHTTPModelReply(w, request, "", "read_document", string(args))
				case 6:
					result := latestRunScopeTool(t, request, "read_document")
					if result != "Error: tool_unavailable" || sourceReads.Load() != 0 {
						t.Errorf("guessed out-of-scope source was not denied before object storage: result=%q reads=%d", result, sourceReads.Load())
					}
					harnessHTTPModelReply(w, request, "", "ask_user", `{"question":"继续按当前范围核验？","options":["继续","取消"]}`)
				case 7:
					if request.Stream != streaming || !strings.Contains(latestRunScopeTool(t, request, "ask_user"), "继续") {
						t.Error("resume lost the user's answer or JSON/SSE mode")
					}
					harnessHTTPModelReply(w, request, "", "search_knowledge", `{"query":"恢复后核验","mode":"keyword"}`)
				case 8:
					var result struct {
						Evidence []domain.Evidence `json:"evidence"`
					}
					if json.Unmarshal([]byte(latestRunScopeTool(t, request, "search_knowledge")), &result) != nil || len(result.Evidence) != 1 || result.Evidence[0].DocumentID != selected.Document.ID {
						t.Error("restart or omitted filter broadened the persisted run scope")
					}
					evidenceID := queryUnderstandingEvidence(request)
					harnessHTTPModelReply(w, request, "已核验当前库，限额500元。[E:"+evidenceID+"]", "", "")
				default:
					t.Errorf("unexpected model replay: %d", step)
					http.Error(w, "unexpected replay", http.StatusBadRequest)
				}
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			client := harnessHTTPClient(t, service.url, callback)
			installHarnessHTTPClient(x, client)
			const query = "核验当前知识库，继续前询问我"
			pending := integrationData[promptHTTPResult](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token,
				map[string]any{"content": query, "knowledge_base_ids": []string{selectedKB.ID}}, http.StatusAccepted))
			if pending.Prompt.Kind != "ask_user" || pending.Run.Status != "suspended" || !reflect.DeepEqual(storedRunKnowledgeScope(t, x, pending.Run.ID), []string{selectedKB.ID}) {
				t.Fatal("suspension did not persist the caller's selected knowledge base")
			}
			service.stop()
			service.start()
			recoveryRestart(t, x)
			installHarnessHTTPClient(x, client)
			path := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
			x.request(t, "POST", path, owner.Token, map[string]any{"answer": "继续", "knowledge_base_ids": []string{otherKB.ID}}, http.StatusBadRequest)
			if modelCalls.Load() != 6 {
				t.Fatal("tampered answer reached the model or consumed the pending prompt")
			}
			var completed recoveryHTTPResult
			if streaming {
				completed = harnessHTTPCompletedSSE(t, x, path, owner.Token, map[string]string{"answer": "继续"})
			} else {
				completed = integrationData[recoveryHTTPResult](t, x.request(t, "POST", path, owner.Token, map[string]string{"answer": "继续"}, http.StatusCreated))
			}
			if completed.Run.ID != pending.Run.ID || completed.Run.Status != "completed" || !strings.Contains(completed.Answer, "500") || modelCalls.Load() != 8 {
				t.Fatalf("resume changed execution identity or replayed work: %+v, calls=%d", completed, modelCalls.Load())
			}
			_, queries, _ := x.search.snapshot()
			if len(queries) != 2 {
				t.Fatalf("expected exactly two authorized searches, got %d", len(queries))
			}
			for _, actual := range queries {
				if actual.workspace != wid || actual.mode != "lexical" || !reflect.DeepEqual(actual.generations, []string{selectedGeneration}) {
					t.Fatalf("external recall escaped authoritative run scope: %+v", actual)
				}
			}
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
			if len(messages) != 2 || messages[0].Content != query || !reflect.DeepEqual(storedRunKnowledgeScope(t, x, pending.Run.ID), []string{selectedKB.ID}) {
				t.Fatal("resume replaced the original question or persisted scope")
			}
		})
	}
}

func TestHTTPRunKnowledgeScopeRequestValidationAndEmptySelection(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "run-kb-validation")
	foreign := x.register(t, "run-kb-foreign")
	wid := owner.Workspaces[0].ID
	a, b := createHTTPKnowledgeBase(t, x, owner, "A"), createHTTPKnowledgeBase(t, x, owner, "B")
	outside := createHTTPKnowledgeBase(t, x, foreign, "Foreign")
	docA := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, a.ID, "allowed")
	x.process(t)
	docB := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, b.ID, "excluded")
	x.process(t)
	tooMany := make([]string, 21)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("%032x", i+1)
	}
	for _, test := range []struct {
		name string
		kb   any
		docs []string
		code int
	}{
		{"malformed", []string{"bad"}, nil, 400},
		{"duplicate", []string{a.ID, a.ID}, nil, 400},
		{"too_many", tooMany, nil, 400},
		{"non_array", a.ID, nil, 400},
		{"missing", []string{strings.Repeat("f", 32)}, nil, 404},
		{"foreign", []string{a.ID, outside.ID}, nil, 404},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := x.session(t, owner, wid, test.name)
			input := map[string]any{"content": "must reject before run creation", "knowledge_base_ids": test.kb}
			if test.docs != nil {
				input["document_ids"] = test.docs
			}
			x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, test.code)
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 0 || len(x.agent.calls()) != 0 {
				t.Fatal("invalid request created a message or reached the agent")
			}
			db, err := sql.Open("mysql", x.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var runs int
			if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE session_id=?`, session.ID).Scan(&runs); err != nil || runs != 0 {
				t.Fatalf("invalid selection left an orphan run: count=%d error=%v", runs, err)
			}
		})
	}
	for _, test := range []struct {
		name    string
		include bool
		ids     []string
		docs    []string
		count   int
	}{
		{"omitted", false, nil, nil, 3},
		{"null", true, nil, nil, 3},
		{"none", true, []string{}, nil, 0},
		{"selected", true, []string{a.ID}, nil, 1},
		{"whole_library_covers_document", true, []string{a.ID}, []string{docA.Document.ID}, 1},
		{"mixed_union", true, []string{a.ID}, []string{docB.Document.ID}, 2},
		{"document_only", true, []string{}, []string{docA.Document.ID}, 1},
		{"empty_document_selection_keeps_explicit_library", true, []string{a.ID}, []string{}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := x.session(t, owner, wid, test.name)
			x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
				var page store.ToolKnowledgeBasePage
				if err := invokeKnowledgeTool(ctx, req, "list_knowledge_bases", map[string]any{"limit": 20}, &page); err != nil {
					return agent.Result{}, err
				}
				if len(page.KnowledgeBases) != test.count {
					return agent.Result{}, fmt.Errorf("knowledge scope changed: count=%d want=%d", len(page.KnowledgeBases), test.count)
				}
				var evidence struct {
					Evidence []domain.Evidence `json:"evidence"`
				}
				if err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]string{"query": "scope", "mode": "keyword"}, &evidence); err != nil {
					return agent.Result{}, err
				}
				wantEvidence := test.count
				if wantEvidence == 3 {
					wantEvidence = 2 // The default library has no documents.
				}
				if len(evidence.Evidence) != wantEvidence {
					return agent.Result{}, fmt.Errorf("request scope returned %d evidence items, want %d", len(evidence.Evidence), wantEvidence)
				}
				if test.name == "document_only" && len(evidence.Evidence) > 0 && evidence.Evidence[0].DocumentID != docA.Document.ID {
					return agent.Result{}, errors.New("knowledge scope widened the explicit document target")
				}
				return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "scope verified"}, nil
			})
			input := map[string]any{"content": "inspect scope"}
			if test.include {
				input["knowledge_base_ids"] = test.ids
			}
			if test.docs != nil {
				input["document_ids"] = test.docs
			}
			before := len(queryUnderstandingSearches(x))
			answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, 201))
			if !reflect.DeepEqual(storedRunKnowledgeScope(t, x, answer.Run.ID), test.ids) {
				t.Fatal("SQL lost omitted/null/empty/selected scope semantics")
			}
			if test.count == 0 && len(queryUnderstandingSearches(x)) != before {
				t.Fatal("explicit empty knowledge scope reached external recall")
			}
		})
	}
}

func TestHTTPRunKnowledgeScopeFiltersHistoryBeforeHarness(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "run-kb-history")
	wid := owner.Workspaces[0].ID
	a := createHTTPKnowledgeBase(t, x, owner, "Current library")
	b := createHTTPKnowledgeBase(t, x, owner, "PRIVATE_OUTSIDE_LIBRARY_METADATA")
	doc := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, b.ID, "PRIVATE_OUTSIDE_LIBRARY_HISTORY")
	x.process(t)
	session := x.session(t, owner, wid, "历史库范围")
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		var read knowledgeReadResponse
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]string{"document_id": doc.Document.ID}, &read); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "PRIVATE_OUTSIDE_LIBRARY_HISTORY"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "PRIVATE_OUTSIDE_LIBRARY_HISTORY", "knowledge_base_ids": []string{b.ID}}, 201)
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		var page store.ToolKnowledgeBasePage
		if err := invokeKnowledgeTool(ctx, req, "list_knowledge_bases", map[string]string{"keyword": b.Name}, &page); err != nil {
			return agent.Result{}, err
		}
		if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != b.ID {
			return agent.Result{}, errors.New("metadata-only history fixture did not read the selected library")
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: b.Name}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "PRIVATE_OUTSIDE_LIBRARY_METADATA", "knowledge_base_ids": []string{b.ID}}, 201)
	seedContextTurns(t, x.integrationFixture, owner, session, 1, "SAFE_INDEPENDENT_HISTORY")
	var understanding, answers atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := harnessHTTPModelInput(t, r)
		raw, _ := json.Marshal(request.Messages)
		if strings.Contains(string(raw), "PRIVATE_OUTSIDE_LIBRARY") || strings.Contains(string(raw), doc.Document.ID) {
			t.Error("out-of-scope history reached the understanding or answer model")
		}
		if !strings.Contains(string(raw), "SAFE_INDEPENDENT_HISTORY") {
			t.Error("knowledge scope also removed unrelated authorized conversation history")
		}
		if queryUnderstandingRequest(request) {
			understanding.Add(1)
			harnessHTTPModelReply(w, request, `{"rewrite_query":"展开独立会话的说明","intent":"follow_up"}`, "", "")
			return
		}
		answers.Add(1)
		harnessHTTPModelReply(w, request, "只依据当前仍可访问的会话继续说明。", "", "")
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	installHarnessHTTPClient(x, harnessHTTPClient(t, service.url, callback))
	for _, streaming := range []bool{false, true} {
		input := map[string]any{"content": "继续展开说明", "knowledge_base_ids": []string{a.ID}}
		if streaming {
			harnessHTTPCompletedSSE(t, x, sessionPath(session)+"/messages", owner.Token, input)
		} else {
			x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, 201)
		}
	}
	if understanding.Load() != 2 || answers.Load() != 2 {
		t.Fatalf("expected actual understanding and answer calls in both modes: understanding=%d answers=%d", understanding.Load(), answers.Load())
	}
}

func TestHTTPRunKnowledgeScopeDeletedEmptyLibraryRejectsResume(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "run-kb-empty-delete")
	wid := owner.Workspaces[0].ID
	kb := createHTTPKnowledgeBase(t, x, owner, "Selected empty library")
	session := x.session(t, owner, wid, "尚未读取已选库")
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := harnessHTTPModelInput(t, r)
		if calls.Add(1) != 1 {
			t.Error("deleted explicit selection reached the model during resume")
		}
		harnessHTTPModelReply(w, request, "", "ask_user", `{"question":"开始读取吗？","options":["开始","取消"]}`)
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	pending := integrationData[promptHTTPResult](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token,
		map[string]any{"content": "读取之前请先确认", "knowledge_base_ids": []string{kb.ID}}, 202))
	if !reflect.DeepEqual(storedRunKnowledgeScope(t, x, pending.Run.ID), []string{kb.ID}) {
		t.Fatal("unused empty selection was not durably stored")
	}
	x.request(t, "DELETE", knowledgeBasePath(wid, kb.ID), owner.Token, nil, 204)
	service.stop()
	service.start()
	recoveryRestart(t, x)
	installHarnessHTTPClient(x, client)
	path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
	x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "开始"}, 409)
	if calls.Load() != 1 {
		t.Fatal("deleted selection was validated only after the model started")
	}
	if !reflect.DeepEqual(storedRunKnowledgeScope(t, x, pending.Run.ID), []string{kb.ID}) {
		t.Fatal("knowledge base deletion erased the execution's narrowing constraint")
	}
}

func TestHTTPRunKnowledgeScopeDeletedUnusedLibraryRejectsRecovery(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "run-kb-recover-delete")
	wid := owner.Workspaces[0].ID
	kb := createHTTPKnowledgeBase(t, x, owner, "Unused selected library")
	session := x.session(t, owner, wid, "未读取知识库即中断")
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if err := recoveryAppendFact(ctx, req, 1); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{}, agent.ErrFailed
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token,
		map[string]any{"content": "开始前失败", "knowledge_base_ids": []string{kb.ID}}, 502)
	requests := x.agent.calls()
	if len(requests) != 1 || requests[0].Generation == 0 || len(storedJournalFacts(t, x, requests[0].RunID)) != 1 {
		t.Fatal("failed run did not retain a real durable recovery fact")
	}
	runID := requests[0].RunID
	x.request(t, "DELETE", knowledgeBasePath(wid, kb.ID), owner.Token, nil, 204)
	recoveryRestart(t, x)
	x.request(t, "POST", recoveryPath(session, runID), owner.Token, map[string]any{"knowledge_base_ids": []string{}}, 400)
	x.request(t, "POST", recoveryPath(session, runID), owner.Token, map[string]any{}, 409)
	if len(x.agent.calls()) != 1 || !reflect.DeepEqual(storedRunKnowledgeScope(t, x, runID), []string{kb.ID}) {
		t.Fatal("recovery reached the agent or broadened the deleted persisted selection")
	}
}
