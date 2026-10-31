package httpapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/observability"
	"magworkspace/internal/reranking"
)

type integrationReranker func(context.Context, string, []string) ([]reranking.Score, error)

func (f integrationReranker) Score(ctx context.Context, query string, passages []string) ([]reranking.Score, error) {
	return f(ctx, query, passages)
}

func attachIntegrationReranker(t *testing.T, x *indexFixture, model reranking.Model) {
	t.Helper()
	x.server.Close()
	options := reranking.DefaultOptions()
	options.MaxCandidates = 50
	x.service = indexing.NewService(x.store, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{
		JobTimeout: 10 * time.Second, Reranker: model, RerankOptions: options, RerankModelID: "controlled-rerank-model",
	})
	x.config.Indexing = x.service
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
}

type rerankSearchResponse struct {
	ProfileID         string                `json:"profile_id"`
	RetrievalPolicyID string                `json:"retrieval_policy_id"`
	Hits              []domain.SearchHit    `json:"hits"`
	Rerank            reranking.Diagnostics `json:"rerank"`
}

func rerankScores(passages []string, score float64) []reranking.Score {
	scores := make([]reranking.Score, len(passages))
	for i := range passages {
		scores[i] = reranking.Score{Index: i, RelevanceScore: score}
	}
	return scores
}

func TestHTTPRerankingCandidateWindowCanonicalEvidenceAndToolShared(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "rerank-shared")
	wid := owner.Workspaces[0].ID
	var calls atomic.Int32
	attachIntegrationReranker(t, x, integrationReranker(func(_ context.Context, query string, passages []string) ([]reranking.Score, error) {
		calls.Add(1)
		if query != "退款期限" || len(passages) != 3 {
			return nil, fmt.Errorf("expected full candidate window, got %d", len(passages))
		}
		scores := rerankScores(passages, 0.01)
		for i, passage := range passages {
			if strings.Contains(passage, "7 天内可申请退款") {
				if !strings.Contains(passage, "售后服务") || strings.Contains(passage, "**") {
					return nil, errors.New("missing title or formatted passage was not cleaned")
				}
				scores[i].RelevanceScore = 0.95
			}
		}
		return scores, nil
	}))
	first := x.upload(t, owner, "配送预计 3 天，退款流程请另查。")
	x.process(t)
	second := x.upload(t, owner, "退款申请需要订单号，不涉及期限。")
	x.process(t)
	source := "# 退款政策\n**7 天内可申请退款**，请保留交易记录。\n"
	correct := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(wid), owner.Token, 201,
		documentFile("refund.md", []byte(source)), documentField("title", "售后服务")))
	x.process(t)
	// The answer is deliberately last in retrieval order. TopK=1 must only
	// truncate after reranking, and the fake search boundary preserves ranking.
	x.search.setSearch(func(_ context.Context, _ indexTestQuery, candidates []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		for i := range candidates {
			candidates[i].Score = 3 - float64(i)
		}
		return candidates, nil
	})
	path := workspacePath(wid) + "/search"
	baseline := integrationData[rerankSearchResponse](t, x.request(t, "POST", path, owner.Token,
		map[string]any{"query": "退款期限", "mode": "lexical", "limit": 1, "rerank": false}, 200))
	if calls.Load() != 0 || baseline.Rerank.Outcome != "disabled" || len(baseline.Hits) != 1 ||
		baseline.Hits[0].Chunk.DocumentID != first.Document.ID {
		t.Fatalf("baseline did not bypass rerank: %+v calls=%d", baseline, calls.Load())
	}
	got := integrationData[rerankSearchResponse](t, x.request(t, "POST", path, owner.Token,
		map[string]any{"query": "退款期限", "mode": "lexical", "limit": 1}, 200))
	if len(got.Hits) != 1 || got.Hits[0].Chunk.DocumentID != correct.Document.ID ||
		got.Rerank.Outcome != "ok" || !got.Rerank.Applied || got.Rerank.CandidateCount != 3 ||
		got.RetrievalPolicyID == "" || got.RetrievalPolicyID == baseline.RetrievalPolicyID ||
		got.ProfileID != baseline.ProfileID || got.ProfileID != x.profile.ID {
		t.Fatalf("shared rerank or independent policy identity missing: %+v", got)
	}
	hit := got.Hits[0]
	if hit.RetrievalScore == nil || hit.RerankScore == nil || hit.NormalizedRetrievalScore == nil ||
		*hit.RerankScore != 0.95 || *hit.NormalizedRetrievalScore <= 0 {
		t.Fatalf("scores not separated: %+v", hit)
	}
	job := x.job(t, owner, correct)
	chunk := integrationData[domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(correct, job)+"/"+hit.Chunk.ID, owner.Token, nil, 200))
	sum := sha256.Sum256([]byte(chunk.Content))
	if !reflect.DeepEqual(chunk, hit.Chunk) || chunk.Content != source ||
		chunk.SHA256 != hex.EncodeToString(sum[:]) || chunk.SourceSHA256 != correct.Version.SHA256 {
		t.Fatal("rerank changed canonical content, byte location or hashes")
	}
	session := x.session(t, owner, wid, "shared-rerank")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		var result struct {
			Evidence []domain.Evidence `json:"evidence"`
		}
		if err := invokeKnowledgeTool(ctx, request, "search_knowledge", map[string]any{
			"query": "退款期限", "mode": "keyword", "limit": 1,
		}, &result); err != nil {
			return agent.Result{}, err
		}
		if len(result.Evidence) != 1 || result.Evidence[0].DocumentID != correct.Document.ID ||
			result.Evidence[0].Content != source {
			return agent.Result{}, errors.New("tool bypassed rerank or canonical evidence")
		}
		return agent.Result{RunID: request.RunID, Answer: "7 天内可申请退款。[E:" + result.Evidence[0].ID + "]", Mode: "demo"}, nil
	})
	answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token,
		map[string]any{"content": "退款期限", "document_ids": []string{first.Document.ID, second.Document.ID, correct.Document.ID}}, 201))
	if len(answer.Citations) != 1 || answer.Citations[0].Evidence.ChunkID != chunk.ID ||
		answer.Run.Status != "completed" || calls.Load() != 2 {
		t.Fatalf("tool result not committed as immutable citation: %+v calls=%d", answer, calls.Load())
	}
}

func TestHTTPRerankingUnavailableDiffersFromSuccessfulLowScores(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		score   float64
		outcome string
		hits    int
		applied bool
	}{
		{"model error", errors.New("private provider text and secret"), 0, "model_error", 1, false},
		{"malformed provider", reranking.ErrInvalidResponse, 0, "model_error", 1, false},
		{"low confidence", nil, 0.01, "all_below_threshold", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "rerank-outcome")
			attachIntegrationReranker(t, x, integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
				return rerankScores(passages, test.score), test.err
			}))
			write := x.upload(t, owner, "退款需要有效订单。")
			x.process(t)
			result := x.request(t, "POST", workspacePath(write.Document.WorkspaceID)+"/search", owner.Token,
				map[string]any{"query": "退款期限", "mode": "lexical", "limit": 1}, 200)
			got := integrationData[rerankSearchResponse](t, result)
			if len(got.Hits) != test.hits || got.Rerank.Outcome != test.outcome || got.Rerank.Applied != test.applied {
				t.Fatalf("outage confused with successful rejection: %+v", got)
			}
			raw, _ := json.Marshal(got)
			if bytes.Contains(raw, []byte("private provider")) || bytes.Contains(raw, []byte("secret")) {
				t.Fatal("provider text exposed through diagnostics")
			}
			if test.err != nil && (got.Hits[0].RerankScore != nil || !got.Rerank.FallbackUsed) {
				t.Fatal("degraded retrieval mislabeled as successful model scoring")
			}
		})
	}
}

func TestHTTPRerankingRevalidatesAfterExternalModel(t *testing.T) {
	for _, change := range []string{"revoke", "new head", "delete"} {
		for _, modelOutcome := range []string{"success", "empty", "error"} {
			t.Run(change+"/"+modelOutcome, func(t *testing.T) {
				x := newIndexFixture(t)
				owner := x.register(t, "rerank-change-owner")
				viewer := x.register(t, "rerank-change-viewer")
				wid := owner.Workspaces[0].ID
				x.request(t, "POST", workspacePath(wid)+"/members", owner.Token,
					map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
				entered, release := make(chan struct{}, 1), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, passages []string) ([]reranking.Score, error) {
					if err := indexGate(ctx, entered, release); err != nil {
						return nil, err
					}
					if modelOutcome == "error" {
						return nil, reranking.ErrModelUnavailable
					}
					score := 0.9
					if modelOutcome == "empty" {
						score = 0.01
					}
					return rerankScores(passages, score), nil
				}))
				write := x.upload(t, owner, "退款期限七天，旧版本仅用于并发测试。")
				x.process(t)
				done := make(chan integrationResponse, 1)
				go func() {
					done <- x.call("POST", workspacePath(wid)+"/search", viewer.Token,
						map[string]any{"query": "退款期限", "mode": "lexical", "limit": 1})
				}()
				indexAwait(t, entered)
				switch change {
				case "revoke":
					x.request(t, "DELETE", workspacePath(wid)+"/members/"+viewer.User.ID, owner.Token, nil, 204)
				case "new head":
					x.request(t, "PUT", documentPath(write.Document)+"/content", owner.Token,
						map[string]string{"base_version_id": write.Version.ID, "content": "退款期限三天，这是当前版本。"}, 200)
				case "delete":
					x.request(t, "DELETE", documentPath(write.Document), owner.Token, nil, 204)
				}
				once.Do(func() { close(release) })
				select {
				case response := <-done:
					if change == "revoke" {
						checkIntegrationResponse(t, response, 404)
					} else {
						got := integrationData[rerankSearchResponse](t, checkIntegrationResponse(t, response, 200))
						if len(got.Hits) != 0 || got.Rerank.ResultCount != 0 {
							t.Fatalf("stale evidence escaped model boundary: %+v", got)
						}
					}
				case <-time.After(10 * time.Second):
					t.Fatal("search did not finish after releasing reranker")
				}
			})
		}
	}
}

func TestHTTPRerankingAuthorizationBeforeModelAndScopedCandidates(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "rerank-scope-owner")
	outsider := x.register(t, "rerank-scope-other")
	var calls atomic.Int32
	attachIntegrationReranker(t, x, integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
		calls.Add(1)
		if len(passages) != 1 || !strings.Contains(passages[0], "允许阅读") || strings.Contains(passages[0], "外部秘密") {
			return nil, errors.New("untrusted candidate reached reranker")
		}
		return rerankScores(passages, 0.9), nil
	}))
	local := x.upload(t, owner, "允许阅读的退款说明。")
	x.process(t)
	foreign := x.upload(t, outsider, "外部秘密不得送入重排。")
	x.process(t)
	writes, _, _ := x.search.snapshot()
	var candidates []domain.IndexCandidate
	for _, write := range writes {
		chunk := write.chunks[0]
		candidates = append(candidates, domain.IndexCandidate{
			ChunkID: chunk.ID, IndexID: chunk.IndexID, Generation: write.generation, Score: 1,
		})
	}
	x.search.setSearch(func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return candidates, nil
	})
	path := workspacePath(local.Document.WorkspaceID) + "/search"
	x.request(t, "POST", path, outsider.Token, map[string]any{"query": "退款期限", "mode": "lexical"}, 404)
	if calls.Load() != 0 {
		t.Fatal("unauthorized caller reached reranker")
	}
	result := integrationData[rerankSearchResponse](t, x.request(t, "POST", path, owner.Token,
		map[string]any{"query": "退款期限", "mode": "lexical"}, 200))
	if calls.Load() != 1 || len(result.Hits) != 1 || result.Hits[0].Chunk.DocumentID != local.Document.ID {
		t.Fatalf("foreign candidate was not filtered: %+v", result)
	}
	// An authenticated but explicitly empty scope cannot call the model.
	hits, err := x.service.SearchScoped(context.Background(), owner.User.ID, local.Document.WorkspaceID, "退款期限", "lexical", 1, []string{})
	if err != nil || len(hits) != 0 || calls.Load() != 1 {
		t.Fatalf("empty document scope reached model: %+v %v", hits, err)
	}
	_ = foreign
}

func TestHTTPRerankingCancellationReachesModel(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "rerank-cancel")
	entered, exited := make(chan struct{}, 1), make(chan struct{}, 1)
	attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, _ []string) ([]reranking.Score, error) {
		entered <- struct{}{}
		<-ctx.Done()
		exited <- struct{}{}
		return nil, ctx.Err()
	}))
	write := x.upload(t, owner, "退款期限需要检索。")
	x.process(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", x.server.URL+workspacePath(write.Document.WorkspaceID)+"/search",
		strings.NewReader(`{"query":"退款期限","mode":"lexical","limit":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+owner.Token)
	req.Header.Set("Content-Type", "application/json")
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(req)
		if response != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		done <- err
	}()
	indexAwait(t, entered)
	cancel()
	indexAwait(t, exited)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled request degraded to successful retrieval: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled request did not exit")
	}
}

// This logger hook synchronizes immediately after the dependency stage,
// including when its reserved budget prevented any model call. It permits a
// real database revocation at that boundary without adding production hooks.
type rerankBoundaryLog struct {
	inner slog.Handler
	after func()
}

func (h rerankBoundaryLog) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}
func (h rerankBoundaryLog) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "rerank finished" {
		h.after()
	}
	return h.inner.Handle(ctx, record)
}
func (h rerankBoundaryLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return rerankBoundaryLog{inner: h.inner.WithAttrs(attrs), after: h.after}
}
func (h rerankBoundaryLog) WithGroup(name string) slog.Handler {
	return rerankBoundaryLog{inner: h.inner.WithGroup(name), after: h.after}
}

func TestHTTPRerankingReservedBudgetSkipsModelAndRechecksAuthorization(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprintf("revoke=%v", revoke), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "rerank-budget-owner")
			viewer := x.register(t, "rerank-budget-viewer")
			wid := owner.Workspaces[0].ID
			x.request(t, "POST", workspacePath(wid)+"/members", owner.Token,
				map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
			write := x.upload(t, owner, "退款期限七天。")
			x.process(t)
			var calls atomic.Int32
			reached, mutationErr := false, error(nil)
			logger := slog.New(rerankBoundaryLog{inner: slog.NewTextHandler(io.Discard, nil), after: func() {
				reached = true
				if revoke {
					mutationErr = x.store.DeleteMember(context.Background(), owner.User.ID, wid, viewer.User.ID)
				}
			}})
			telemetry, err := observability.Init(context.Background(), observability.Config{Logger: logger})
			if err != nil {
				t.Fatal(err)
			}
			defer telemetry.Shutdown(context.Background())
			options := reranking.DefaultOptions()
			x.service = indexing.NewService(x.store, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{
				Telemetry: telemetry, RerankOptions: options, RerankModelID: "controlled-rerank-model",
				Reranker: integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
					calls.Add(1)
					return rerankScores(passages, 0.9), nil
				}),
			})
			ctx, cancel := context.WithTimeout(context.Background(), 1800*time.Millisecond)
			defer cancel()
			result, err := x.service.SearchScopedDetailed(ctx, viewer.User.ID, wid, "退款期限", "lexical", 1, nil, nil)
			if calls.Load() != 0 || !reached || mutationErr != nil || ctx.Err() != nil {
				t.Fatalf("budget did not reserve final checks: calls=%d reached=%v mutation=%v parent=%v", calls.Load(), reached, mutationErr, ctx.Err())
			}
			if revoke {
				if !errors.Is(err, domain.ErrNotFound) || len(result.Hits) != 0 {
					t.Fatalf("budget fallback skipped fresh authorization: %+v %v", result, err)
				}
			} else if err != nil || len(result.Hits) != 1 || result.Hits[0].Chunk.DocumentID != write.Document.ID ||
				result.Rerank.Outcome != "model_error" || result.Rerank.ErrorCode != "rerank_timeout" ||
				result.Rerank.Applied || !result.Rerank.FallbackUsed {
				t.Fatalf("budget exhausted before model did not safely fall back: %+v %v", result, err)
			}
		})
	}
}
