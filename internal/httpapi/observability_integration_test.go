package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/observability"
	"magworkspace/internal/reranking"
	"magworkspace/internal/store"
)

type observabilityTestLog struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *observabilityTestLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}

func (b *observabilityTestLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func newHTTPTestTelemetry(t *testing.T) (*observability.Telemetry, *observabilityTestLog) {
	t.Helper()
	logs := &observabilityTestLog{}
	telemetry, err := observability.Init(context.Background(), observability.Config{Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := telemetry.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return telemetry, logs
}

func httpTestMetrics(t *testing.T, telemetry *observability.Telemetry) string {
	t.Helper()
	response := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", response.Code)
	}
	return response.Body.String()
}

func requireHTTPTestMetric(t *testing.T, telemetry *observability.Telemetry, name, operation, outcome string, count int) {
	t.Helper()
	want := fmt.Sprintf("%s{operation=%q,outcome=%q} %d\n", name, operation, outcome, count)
	if metrics := httpTestMetrics(t, telemetry); !strings.Contains(metrics, want) {
		t.Fatalf("missing application metric %q", want)
	}
}

func assertHTTPPrivateTelemetry(t *testing.T, telemetry *observability.Telemetry, logs *observabilityTestLog, forbidden ...string) {
	t.Helper()
	metrics, recorded := httpTestMetrics(t, telemetry), logs.String()
	for _, value := range forbidden {
		if value != "" && (strings.Contains(metrics, value) || strings.Contains(recorded, value)) {
			t.Fatalf("telemetry leaked private value %q", value)
		}
	}
	for _, label := range []string{"user_id=", "workspace_id=", "session_id=", "run_id=", "document_id=", "query="} {
		if strings.Contains(metrics, label) {
			t.Fatalf("resource-dependent metric label %q", label)
		}
	}
	if !strings.Contains(recorded, `"request_id":`) {
		t.Fatal("request logs lack correlation IDs")
	}
}

func configureHTTPTestIndexing(t *testing.T, x *indexFixture, execution string, telemetry *observability.Telemetry) {
	t.Helper()
	x.server.Close()
	x.service = indexing.NewService(x.store, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{
		JobTimeout: 10 * time.Second, Execution: execution, Telemetry: telemetry,
	})
	x.config.Indexing = x.service
	x.config.Telemetry = telemetry
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
}

func TestHTTPObservabilitySSEFlushesBeforeCompletionAndRecordsMetrics(t *testing.T) {
	telemetry, logs := newHTTPTestTelemetry(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "private-otel-delta"); err != nil {
			return agent.Result{}, err
		}
		select {
		case <-release:
			return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "private-otel-final-answer"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true, Telemetry: telemetry}, client)
	t.Cleanup(unblock)
	owner := f.register(t, "observabilitysse")
	session := f.session(t, owner, owner.Workspaces[0].ID, "private-otel-session-title")
	connection := openSSETestConnection(t, f, sessionPath(session)+"/messages", owner.Token, "private-otel-user-query")
	defer connection.close()
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	delta := connection.next(t, "message.delta")
	if delta.Delta != "private-otel-delta" || connection.response.Header.Get("X-Request-ID") == "" {
		t.Fatalf("middleware lost streaming text or request correlation: %+v", delta)
	}
	// The model cannot finish until release closes. Receiving its delta and
	// observing a running row proves telemetry preserves actual HTTP flushes.
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, sessionPath(session)+"/runs/"+started.RunID, owner.Token, nil, 200))
	if run.Status != "running" {
		t.Fatalf("run completed before release: %+v", run)
	}
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 1 || messages[0].Role != "user" {
		t.Fatalf("provisional answer was persisted: %+v", messages)
	}
	if strings.Contains(httpTestMetrics(t, telemetry), `magworkspace_operations_total{operation="agent.sse",outcome="success"}`) {
		t.Fatal("stream operation completed while the model was blocked")
	}
	unblock()
	completed := connection.next(t, "run.completed")
	connection.ended(t)
	if completed.Run.Status != "completed" || completed.Answer != "private-otel-final-answer" {
		t.Fatalf("invalid terminal response: %+v", completed)
	}
	messages = integrationData[[]domain.Message](t, f.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 2 || messages[1].Content != completed.Answer || messages[1].Status != "completed" {
		t.Fatalf("terminal answer was not durable: %+v", messages)
	}
	requireHTTPTestMetric(t, telemetry, "magworkspace_operations_total", "agent.sse", "success", 1)
	requireHTTPTestMetric(t, telemetry, "magworkspace_observed_items_total", "run.finish", "success", 1)
	assertHTTPPrivateTelemetry(t, telemetry, logs, owner.Token, integrationPassword, owner.User.ID, session.WorkspaceID, session.ID, started.RunID,
		"private-otel-session-title", "private-otel-user-query", "private-otel-delta", completed.Answer)
}

func TestHTTPObservabilityToolAndPromptLifecycleMetrics(t *testing.T) {
	x := newIndexFixture(t)
	telemetry, logs := newHTTPTestTelemetry(t)
	configureHTTPTestIndexing(t, x, "optimized", telemetry)
	owner := x.register(t, "observabilityprompt")
	write := x.upload(t, owner, "private-otel-document-body")
	x.process(t)
	session := x.session(t, owner, owner.Workspaces[0].ID, "private-otel-prompt-session")
	const promptID = "91919191919191919191919191919191"
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if req.Resume != nil {
			return agent.Result{}, errors.New("unexpected initial resume")
		}
		var page store.ToolDocumentPage
		if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{"limit": 5}, &page); err != nil {
			return agent.Result{}, err
		}
		if len(page.Documents) != 1 || page.Documents[0].Document.ID != write.Document.ID {
			return agent.Result{}, errors.New("tool did not read the authenticated workspace")
		}
		result := promptTestResult(req, promptID)
		result.Prompt.Question = "private-otel-clarification-question"
		result.Prompt.Snapshot = promptTestSnapshot(req, result.Prompt)
		return result, nil
	})
	response := x.call(http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "private-otel-prompt-query"})
	pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	promptRequirePrivate(t, response.body)
	if pending.Run.Status != "suspended" {
		t.Fatalf("question was not durably suspended: %+v", pending)
	}
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		if req.Resume == nil || req.RunID != pending.Run.ID {
			return agent.Result{}, errors.New("resume did not retain the run")
		}
		var native struct {
			Response struct{ Answer string }
		}
		if json.Unmarshal(req.Resume.Prompt, &native) != nil || native.Response.Answer != "private-otel-user-clarification" {
			return agent.Result{}, errors.New("resume did not receive the user's answer")
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "private-otel-resumed-answer"}, nil
	})
	path := promptPath(session, pending.Run.ID, promptID)
	response = x.call(http.MethodPost, path+"/answer", owner.Token, map[string]string{"answer": "private-otel-user-clarification"})
	completed := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 201))
	if completed.Run.Status != "completed" || completed.Run.ID != pending.Run.ID {
		t.Fatalf("answer did not complete the suspended run: %+v", completed)
	}
	prompt := integrationData[domain.RunPrompt](t, x.request(t, http.MethodGet, path, owner.Token, nil, 200))
	if prompt.Status != "done" || prompt.Answer != "private-otel-user-clarification" {
		t.Fatalf("clarification was not durable: %+v", prompt)
	}
	for operation, count := range map[string]int{"agent.json": 2, "tool.list_documents": 1, "prompt.suspend": 1, "prompt.resume": 1, "index.process": 1, "index.publish": 1} {
		requireHTTPTestMetric(t, telemetry, "magworkspace_operations_total", operation, "success", count)
	}
	requireHTTPTestMetric(t, telemetry, "magworkspace_observed_items_total", "run.finish", "suspended", 1)
	requireHTTPTestMetric(t, telemetry, "magworkspace_observed_items_total", "run.finish", "success", 1)
	assertHTTPPrivateTelemetry(t, telemetry, logs, owner.Token, integrationPassword, owner.User.ID, session.WorkspaceID, session.ID,
		write.Document.ID, write.Version.ID, pending.Run.ID, promptID, promptSnapshotTestSecret,
		"private-otel-document-body", "private-otel-prompt-session", "private-otel-prompt-query", "private-otel-clarification-question", prompt.Answer, completed.Answer)
}

func TestHTTPOptimizedRetrievalMatchesBaselineAndScope(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "optimizedparity")
	outsider := x.register(t, "optimizedforeign")
	write := x.upload(t, owner, strings.Repeat("知识库可信内容及排序。", 500))
	x.process(t)
	x.upload(t, outsider, "foreign-optimized-secret")
	x.process(t)
	// Return every workspace's hints, deliberately ignoring the provider-side
	// filter. Both strategies must use the same authorized SQL chunk
	// chain and suppress the foreign document before producing results.
	writes, _, _ := x.search.snapshot()
	var candidates []domain.IndexCandidate
	for _, item := range writes {
		for _, chunk := range item.chunks {
			candidates = append(candidates, domain.IndexCandidate{ChunkID: chunk.ID, IndexID: chunk.IndexID, Generation: item.generation, Score: 1})
		}
	}
	x.search.setSearch(func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return candidates, nil
	})
	baseline := x.find(t, owner.Token, owner.Workspaces[0].ID, "hybrid").Hits
	configureHTTPTestIndexing(t, x, "optimized", nil)
	optimized := x.find(t, owner.Token, owner.Workspaces[0].ID, "hybrid").Hits
	if len(optimized) < 2 || !reflect.DeepEqual(optimized, baseline) {
		t.Fatalf("strategy changed trusted results or ranking: baseline=%+v optimized=%+v", baseline, optimized)
	}
	for _, hit := range optimized {
		if hit.Chunk.DocumentID != write.Document.ID || strings.Contains(hit.Chunk.Content, "foreign-optimized-secret") {
			t.Fatalf("untrusted foreign candidate escaped scope: %+v", hit)
		}
	}
}

func TestHTTPOptimizedRetrievalRevalidatesAfterExternalIO(t *testing.T) {
	for _, strategy := range []string{"baseline", "optimized"} {
		for _, boundary := range []string{"search", "rerank"} {
			for _, change := range []string{"revoke", "new_head", "delete"} {
				t.Run(strategy+"/"+boundary+"/"+change, func(t *testing.T) {
					x := newIndexFixture(t)
					configureHTTPTestIndexing(t, x, strategy, nil)
					owner := x.register(t, "optimizedraceowner")
					viewer := x.register(t, "optimizedraceviewer")
					wid := owner.Workspaces[0].ID
					x.request(t, http.MethodPost, workspacePath(wid)+"/members", owner.Token, map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
					write := x.upload(t, owner, "知识库并发变更前的可信原文")
					x.process(t)
					entered, release := make(chan struct{}, 2), make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					t.Cleanup(unblock)
					if boundary == "search" {
						x.search.setSearch(func(ctx context.Context, _ indexTestQuery, candidates []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
							if err := indexGate(ctx, entered, release); err != nil {
								return nil, err
							}
							return candidates, nil
						})
					} else {
						attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, passages []string) ([]reranking.Score, error) {
							if err := indexGate(ctx, entered, release); err != nil {
								return nil, err
							}
							return rerankScores(passages, .9), nil
						}))
					}
					result := make(chan integrationResponse, 1)
					go func() {
						result <- x.call(http.MethodPost, workspacePath(wid)+"/search", viewer.Token, map[string]string{"query": "知识库", "mode": "hybrid"})
					}()
					indexAwait(t, entered)
					switch change {
					case "revoke":
						x.request(t, http.MethodDelete, workspacePath(wid)+"/members/"+viewer.User.ID, owner.Token, nil, 204)
					case "new_head":
						documentUpload(t, x.integrationFixture, documentPath(write.Document)+"/versions", owner.Token, 201,
							documentFile("new.txt", []byte("当前版本已经改变")), documentField("base_version_id", write.Version.ID))
					case "delete":
						x.request(t, http.MethodDelete, documentPath(write.Document), owner.Token, nil, 204)
					}
					unblock()
					select {
					case response := <-result:
						if change == "revoke" {
							checkIntegrationResponse(t, response, 404)
						} else if hits := integrationData[indexSearchResponse](t, checkIntegrationResponse(t, response, 200)).Hits; len(hits) != 0 {
							t.Fatalf("%s returned stale content after %s: %+v", strategy, change, hits)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("retrieval failed to stop after external I/O")
					}
				})
			}
		}
	}
}
