package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
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

type evidenceResponse struct {
	Evidence   domain.Evidence `json:"evidence"`
	Citation   string          `json:"citation"`
	SourceURL  string          `json:"source_url"`
	PassageURL string          `json:"passage_url"`
	Locator    struct {
		Kind      string `json:"kind"`
		StartByte int    `json:"start_byte"`
		EndByte   int    `json:"end_byte"`
	} `json:"locator"`
}

type citedAnswer struct {
	Run       domain.Run         `json:"run"`
	Answer    string             `json:"answer"`
	Citations []evidenceResponse `json:"citations"`
}

func acquireTestEvidence(ctx context.Context, request agent.Request) ([]domain.Evidence, error) {
	if request.ToolHandler == nil {
		return nil, errors.New("retrieval tools not enabled")
	}
	value, err := request.ToolHandler(ctx, "search_documents", json.RawMessage(`{"query":"住宿标准","mode":"lexical","limit":10}`))
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var response struct {
		Evidence []domain.Evidence `json:"evidence"`
	}
	err = json.Unmarshal(raw, &response)
	return response.Evidence, err
}

func answerFromEvidence(ctx context.Context, request agent.Request) (agent.Result, error) {
	evidence, err := acquireTestEvidence(ctx, request)
	if err != nil {
		return agent.Result{}, err
	}
	answer := "没有可读取的证据。"
	if len(evidence) > 0 {
		answer = "每晚 500 元。[E:" + evidence[0].ID + "]"
	}
	return agent.Result{RunID: request.RunID, Answer: answer, Mode: "demo"}, nil
}

func TestHTTPRetrievalCitationsPinVersionAndEnforcePrivateAccess(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "evidenceowner")
	other := x.register(t, "evidenceother")
	write := x.upload(t, owner, "住宿标准：每晚 500 元。")
	x.process(t)
	session := x.session(t, owner, write.Document.WorkspaceID, "引用")
	x.agent.set(answerFromEvidence)
	answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准"}, 201))
	if len(answer.Citations) != 1 || answer.Run.Status != "completed" {
		t.Fatalf("missing committed citations: %+v", answer)
	}
	citation := answer.Citations[0]
	if citation.Evidence.VersionID != write.Version.ID || !citation.Evidence.Cited || citation.Evidence.Content != "住宿标准：每晚 500 元。" || citation.Locator.Kind != "extracted_text_utf8_bytes" || citation.Locator.EndByte != len(citation.Evidence.Content) || !strings.Contains(citation.SourceURL, write.Version.ID) {
		t.Fatalf("citation is not bound to the original passage: %+v", citation)
	}
	loaded := integrationData[evidenceResponse](t, x.request(t, "GET", citation.PassageURL, owner.Token, nil, 200))
	if loaded.Evidence.ID != citation.Evidence.ID || loaded.Evidence.Content != citation.Evidence.Content {
		t.Fatal("citation did not resolve its original content")
	}
	x.request(t, "GET", citation.PassageURL, other.Token, nil, 404)
	// Being a Workspace member does not grant another user's private run.
	x.request(t, "POST", workspacePath(write.Document.WorkspaceID)+"/members", owner.Token, map[string]string{"email": other.User.Email, "role": "viewer"}, 201)
	x.request(t, "GET", citation.PassageURL, other.Token, nil, 404)
	updated := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentPath(write.Document)+"/versions", owner.Token, 201,
		documentField("base_version_id", write.Version.ID), documentFile("新版.txt", []byte("住宿标准：每晚 600 元。"))))
	x.process(t)
	if updated.Version.ID == write.Version.ID {
		t.Fatal("new version did not advance")
	}
	loaded = integrationData[evidenceResponse](t, x.request(t, "GET", citation.PassageURL, owner.Token, nil, 200))
	if loaded.Evidence.VersionID != write.Version.ID || loaded.Evidence.Content != citation.Evidence.Content {
		t.Fatal("old citation followed the current document head")
	}
	list := integrationData[struct {
		Citations []evidenceResponse `json:"citations"`
	}](t, x.request(t, "GET", sessionPath(session)+"/runs/"+answer.Run.ID+"/citations", owner.Token, nil, 200))
	if len(list.Citations) != 1 || list.Citations[0].Evidence.ID != citation.Evidence.ID {
		t.Fatal("citations were not persisted with the answer")
	}
	x.request(t, "DELETE", documentPath(write.Document), owner.Token, nil, 204)
	x.request(t, "GET", citation.PassageURL, owner.Token, nil, 404)
}

func TestHTTPRetrievalSelectedDocumentsAndEmptyScope(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "scopedowner")
	a := x.upload(t, owner, "甲住宿标准，只有甲的内容。")
	x.process(t)
	b := x.upload(t, owner, "乙住宿标准，只有乙的内容。")
	x.process(t)
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil {
			return agent.Result{}, err
		}
		if request.DocumentIDs == nil {
			return agent.Result{}, errors.New("scope was not passed to Magent")
		}
		if len(*request.DocumentIDs) == 0 {
			if len(evidence) != 0 {
				return agent.Result{}, errors.New("empty scope widened to Workspace")
			}
		} else if len(evidence) != 1 || evidence[0].DocumentID != b.Document.ID {
			return agent.Result{}, errors.New("retrieval escaped the selected document")
		}
		// Model-supplied identity is rejected by the tool admission boundary.
		if _, err := request.ToolHandler(ctx, "search_documents", json.RawMessage(`{"query":"住宿标准","workspace_id":"forged"}`)); !errors.Is(err, domain.ErrInvalid) {
			return agent.Result{}, errors.New("tool accepted a model-supplied workspace")
		}
		return agent.Result{RunID: request.RunID, Answer: "检查完成。", Mode: "demo"}, nil
	})
	// A malicious search provider can inject an otherwise valid, unselected ID.
	x.search.setSearch(func(ctx context.Context, q indexTestQuery, hits []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		writes, _, _ := x.search.snapshot()
		for _, write := range writes {
			for _, chunk := range write.chunks {
				if chunk.DocumentID == a.Document.ID {
					hits = append(hits, domain.IndexCandidate{ChunkID: chunk.ID, IndexID: chunk.IndexID, Generation: write.generation, Score: 100})
				}
			}
		}
		return hits, ctx.Err()
	})
	session := x.session(t, owner, a.Document.WorkspaceID, "范围")
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准", "document_ids": []string{b.Document.ID}}, 201)
	_, before, _ := x.search.snapshot()
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准", "document_ids": []string{}}, 201)
	_, after, _ := x.search.snapshot()
	if len(after) != len(before) {
		t.Fatal("explicit empty scope reached the search provider")
	}
	foreign := x.register(t, "scopedforeign")
	foreignDoc := x.upload(t, foreign, "外部内容。")
	count := len(x.agent.calls())
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准", "document_ids": []string{foreignDoc.Document.ID}}, 404)
	if len(x.agent.calls()) != count {
		t.Fatal("invalid document scope reached Magent")
	}
}

func TestHTTPRetrievalRejectsUnknownMalformedAndCrossRunCitations(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "invalidcitation")
	write := x.upload(t, owner, "住宿标准：每晚 500 元。")
	x.process(t)
	session := x.session(t, owner, write.Document.WorkspaceID, "拒绝伪造")
	x.agent.set(answerFromEvidence)
	first := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "住宿标准"}, 201))
	for _, marker := range []string{"[E:" + strings.Repeat("f", 32) + "]", "[E:broken]", first.Citations[0].Citation} {
		x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
			return agent.Result{RunID: request.RunID, Answer: "伪造答案 " + marker, Mode: "demo"}, nil
		})
		response := x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "住宿标准"}, 502)
		if response.Error.Code != "invalid_citation" {
			t.Fatalf("unexpected invalid citation error: %+v", response.Error)
		}
	}
	messages, _, err := x.store.Messages(context.Background(), owner.User.ID, write.Document.WorkspaceID, session.ID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	assistants := 0
	for _, message := range messages {
		if message.Role == "assistant" {
			assistants++
		}
	}
	if assistants != 1 {
		t.Fatal("unverified answer was persisted as a completed assistant message")
	}
}

func TestHTTPRetrievalCorruptedSQLChunkPreventsAnswerPublication(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "corruptcitation")
	write := x.upload(t, owner, "住宿标准：每晚 500 元。")
	x.process(t)
	session := x.session(t, owner, write.Document.WorkspaceID, "损坏")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		result, err := answerFromEvidence(ctx, request)
		if err != nil {
			return result, err
		}
		outboxSQL(t, x, `UPDATE chunks SET content=REPEAT('x',OCTET_LENGTH(content)) WHERE index_id IN (SELECT id FROM document_indexes WHERE document_id=?) AND kind='parent'`, write.Document.ID)
		return result, nil
	})
	response := x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "住宿标准"}, 502)
	// SQL integrity is checked while validating the citation, before the
	// content hydration that previously reported an object-store failure.
	if response.Error.Code != "invalid_citation" {
		t.Fatalf("corrupt evidence was not rejected: %+v", response.Error)
	}
	messages, _, err := x.store.Messages(context.Background(), owner.User.ID, write.Document.WorkspaceID, session.ID, 50, 0)
	if err != nil || len(messages) != 1 || messages[0].Role != "user" {
		t.Fatal("corrupted SQL evidence produced a persisted assistant answer")
	}
}

func TestHTTPRetrievalHistoryFiltersUncitedSourcesAndDeletion(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "historyprovenance")
	a := x.upload(t, owner, "甲住宿标准 PRIVATE_A。")
	x.process(t)
	b := x.upload(t, owner, "乙住宿标准 PRIVATE_B。")
	x.process(t)
	session := x.session(t, owner, a.Document.WorkspaceID, "历史来源")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil || len(evidence) != 1 || evidence[0].DocumentID != a.Document.ID {
			return agent.Result{}, errors.New("first turn did not acquire A")
		}
		return agent.Result{RunID: request.RunID, Answer: "PRIVATE_A was read without a citation.", Mode: "demo"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准", "document_ids": []string{a.Document.ID}}, 201)
	// A source can propagate through a turn which calls no tools and emits no
	// markers. Its descendants must inherit the earlier provenance nonetheless.
	x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		found := false
		for _, item := range request.History {
			found = found || strings.Contains(item.Content, "PRIVATE_A")
		}
		if !found {
			return agent.Result{}, errors.New("compatible history was unexpectedly removed")
		}
		return agent.Result{RunID: request.RunID, Answer: "PRIVATE_A paraphrased with no tools or markers.", Mode: "demo"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "复述刚才的信息", "document_ids": []string{a.Document.ID}}, 201)
	x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		for _, item := range request.History {
			if strings.Contains(item.Content, "PRIVATE_A") {
				return agent.Result{}, errors.New("uncited source escaped the new scope")
			}
		}
		return agent.Result{RunID: request.RunID, Answer: "B-only turn.", Mode: "demo"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "后续问题", "document_ids": []string{b.Document.ID}}, 201)
	x.request(t, "DELETE", documentPath(a.Document), owner.Token, nil, 204)
	// The next unrestricted turn must still drop the deleted A-derived history.
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "再问一次"}, 201)
	stored, _, err := x.store.Messages(context.Background(), owner.User.ID, a.Document.WorkspaceID, session.ID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range stored {
		found = found || strings.Contains(item.Content, "PRIVATE_A")
	}
	if !found {
		t.Fatal("model hydration filtering changed durable conversation history")
	}
}

func TestHTTPRetrievalUncitedDeletedSourceCannotCommit(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[streaming], func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "unciteddelete")
			write := x.upload(t, owner, "住宿标准 PRIVATE_SOURCE。")
			x.process(t)
			session := x.session(t, owner, write.Document.WorkspaceID, "删除后提交")
			finish := func(ctx context.Context, request agent.Request) (agent.Result, error) {
				if _, err := acquireTestEvidence(ctx, request); err != nil {
					return agent.Result{}, err
				}
				if err := x.store.DeleteDocument(ctx, owner.User.ID, write.Document.WorkspaceID, write.Document.ID); err != nil {
					return agent.Result{}, err
				}
				return agent.Result{RunID: request.RunID, Answer: "PRIVATE_SOURCE paraphrase without a marker.", Mode: "demo"}, nil
			}
			if !streaming {
				x.agent.set(finish)
				response := x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "住宿标准"}, 502)
				if response.Error.Code != "invalid_citation" {
					t.Fatalf("unavailable uncited source committed: %+v", response.Error)
				}
			} else {
				continueAgent := make(chan struct{})
				var release sync.Once
				releaseAgent := func() { release.Do(func() { close(continueAgent) }) }
				defer releaseAgent()
				client := &sseIntegrationAgent{stream: func(ctx context.Context, request agent.Request, emit func(agent.Event) error) (agent.Result, error) {
					select {
					case <-continueAgent:
					case <-ctx.Done():
						return agent.Result{}, ctx.Err()
					}
					result, err := finish(ctx, request)
					if err != nil {
						return result, err
					}
					if err := emitSSETestMessage(request, emit, 1, result.Answer); err != nil {
						return agent.Result{}, err
					}
					return result, nil
				}}
				x.server.Close()
				x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
				connection := openSSETestConnection(t, x.integrationFixture, sessionPath(session)+"/messages", owner.Token, "住宿标准")
				defer connection.close()
				started := connection.next(t, "run.started")
				releaseAgent()
				// Cache readers reauthorize before exposing any newly produced text.
				unavailable := connection.next(t, "stream.unavailable")
				if unavailable.Code != "access_or_execution_changed" {
					t.Fatal(unavailable)
				}
				connection.ended(t)
				run := waitStreamRun(t, x.integrationFixture, owner.Token, sessionPath(session)+"/runs/"+started.RunID, "failed")
				if run.ErrorCode != "invalid_citation" {
					t.Fatalf("uncited source was not rejected durably: %+v", run)
				}
			}
			stored, _, err := x.store.Messages(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, 50, 0)
			if err != nil || len(stored) != 1 || stored[0].Role != "user" {
				t.Fatalf("unverified paraphrase saved as completed: %+v err=%v", stored, err)
			}
		})
	}
}

func TestHTTPRetrievalCitationBatchRechecksSQLSourcesBeforeReturning(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "batchcitation")
	a := x.upload(t, owner, "甲住宿标准，A secret。")
	x.process(t)
	_ = x.upload(t, owner, "乙住宿标准，B secret。")
	x.process(t)
	session := x.session(t, owner, a.Document.WorkspaceID, "批量引用")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil || len(evidence) != 2 {
			return agent.Result{}, errors.New("expected two passages")
		}
		return agent.Result{RunID: request.RunID, Answer: "来源 [E:" + evidence[0].ID + "] [E:" + evidence[1].ID + "]", Mode: "demo"}, nil
	})
	answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "住宿标准"}, 201))
	before := integrationData[struct {
		Citations []evidenceResponse `json:"citations"`
	}](t, x.request(t, "GET", sessionPath(session)+"/runs/"+answer.Run.ID+"/citations", owner.Token, nil, 200))
	first := before.Citations[0].Evidence
	if err := x.store.DeleteDocument(context.Background(), owner.User.ID, a.Document.WorkspaceID, first.DocumentID); err != nil {
		t.Fatal(err)
	}
	after := integrationData[struct {
		Citations []evidenceResponse `json:"citations"`
	}](t, x.request(t, "GET", sessionPath(session)+"/runs/"+answer.Run.ID+"/citations", owner.Token, nil, 200))
	if len(after.Citations) != 1 || after.Citations[0].Evidence.DocumentID == first.DocumentID {
		t.Fatal("citation batch returned a deleted SQL source from an earlier read")
	}
}

func TestHTTPRetrievalSSERejectsInvalidCitationBeforeCompletion(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "sseinvalidcitation")
	session := x.session(t, owner, owner.Workspaces[0].ID, "流引用")
	client := &sseIntegrationAgent{stream: func(ctx context.Context, request agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		answer := "无法验证 [E:" + strings.Repeat("f", 32) + "]"
		if err := emitSSETestMessage(request, emit, 1, answer); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: request.RunID, Answer: answer, Mode: "demo"}, ctx.Err()
	}}
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	connection := openSSETestConnection(t, x.integrationFixture, sessionPath(session)+"/messages", owner.Token, "住宿标准")
	defer connection.close()
	connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	failed := connection.next(t, "run.failed")
	if failed.Error.Code != "invalid_citation" {
		t.Fatalf("SSE invalid citation produced wrong terminal: %+v", failed)
	}
	stored, _, err := x.store.Messages(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, 50, 0)
	if err != nil || len(stored) != 1 || stored[0].Role != "user" {
		t.Fatalf("SSE persisted unverified assistant: %+v err=%v", stored, err)
	}
}

func TestHTTPRetrievalRealHarnessJSONAndSSE(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "realharnesscitation")
	write := x.upload(t, owner, "住宿标准：每晚 500 元。")
	x.process(t)
	harness := os.Getenv("MAGENT_TEST_HARNESS_PATH")
	if harness == "" {
		harness = filepath.Join("..", "..", "..", "Magent Harness")
	}
	python := os.Getenv("MAGENT_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	client, err := agent.NewProcessClient(agent.Config{Python: python, Script: filepath.Join("..", "..", "bridges", "magent", "bridge.py"), HarnessPath: harness, Mode: "demo", Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	session := x.session(t, owner, write.Document.WorkspaceID, "真实 Harness")
	response := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "住宿标准", "document_ids": []string{write.Document.ID}}, 201))
	if len(response.Citations) != 1 || !strings.Contains(response.Answer, response.Citations[0].Citation) {
		t.Fatalf("real Harness did not use backend-issued evidence: %+v", response)
	}
	connection := openSSETestConnection(t, x.integrationFixture, sessionPath(session)+"/messages", owner.Token, "住宿标准")
	defer connection.close()
	for {
		var event sseTestEvent
		select {
		case received, ok := <-connection.events:
			if !ok || received.err != nil {
				t.Fatalf("SSE ended before verified completion: %v", received.err)
			}
			event = received
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for retrieval SSE")
		}
		if event.typeName == "run.failed" {
			t.Fatalf("Harness retrieval SSE failed: %s", event.data)
		}
		if event.typeName == "run.completed" {
			var completed citedAnswer
			if err := json.Unmarshal(event.data, &completed); err != nil || len(completed.Citations) != 1 || !strings.Contains(completed.Answer, completed.Citations[0].Citation) {
				t.Fatalf("SSE completion lacked verified citations: %s err=%v", event.data, err)
			}
			break
		}
	}
}
