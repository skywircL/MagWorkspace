package httpapi_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"magworkspace/internal/domain"
	"magworkspace/internal/reranking"
)

func TestHTTPResourceScopeIndexUnifiedRecallAndRanking(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "resource-ranking")
	wid := owner.Workspaces[0].ID
	a, b := createHTTPKnowledgeBase(t, x, owner, "Whole A"), createHTTPKnowledgeBase(t, x, owner, "Partial B")
	first := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, a.ID, "Allowed A content")
	x.process(t)
	second := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, b.ID, "Allowed B content")
	x.process(t)
	blocked := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, b.ID, "BLOCKED B content")
	x.process(t)
	firstJob, secondJob, blockedJob := x.job(t, owner, first), x.job(t, owner, second), x.job(t, owner, blocked)
	blockedChunks := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(blocked, blockedJob)+"?kind=child", owner.Token, nil, 200))
	var modelCalls atomic.Int32
	attachIntegrationReranker(t, x, integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
		modelCalls.Add(1)
		if len(passages) != 2 {
			t.Errorf("model should see one merged resource pool, got %d passages", len(passages))
		}
		scores := rerankScores(passages, .6)
		for i, passage := range passages {
			if strings.Contains(passage, "BLOCKED") {
				t.Error("forged out-of-scope child reached reranking")
			}
			if strings.Contains(passage, "Allowed B") {
				scores[i].RelevanceScore = .95
			}
		}
		return scores, nil
	}))
	x.search.setSearch(func(_ context.Context, query indexTestQuery, candidates []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		if len(query.generations) != 2 || !slices.Contains(query.generations, firstJob.Generation) || !slices.Contains(query.generations, secondJob.Generation) || slices.Contains(query.generations, blockedJob.Generation) {
			t.Errorf("recall did not receive the exact union: %v", query.generations)
		}
		return append(candidates, domain.IndexCandidate{IndexID: blockedJob.ID, Generation: blockedJob.Generation, ChunkID: blockedChunks[0].ID, Score: 100}), nil
	})
	scope := domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: a.ID}, {KnowledgeBaseID: b.ID, DocumentIDs: []string{second.Document.ID}}}}
	hits, err := x.service.SearchResourcesScoped(context.Background(), owner.User.ID, wid, "Allowed content", "lexical", 1, scope)
	if err != nil || len(hits) != 1 || hits[0].Chunk.DocumentID != second.Document.ID || modelCalls.Load() != 1 {
		t.Fatalf("global ranking over union failed: %+v %v modelCalls=%d", hits, err, modelCalls.Load())
	}
	_, queries, _ := x.search.snapshot()
	if len(queries) != 1 {
		t.Fatalf("resource union split into %d separate search calls", len(queries))
	}
	x.search.setSearch(nil)
	foreign := x.register(t, "resource-ranking-foreign")
	foreignKB := createHTTPKnowledgeBase(t, x, foreign, "Foreign")
	for _, tc := range []struct {
		name  string
		scope domain.ResourceScope
		err   bool
	}{
		{"none", domain.ResourceScope{}, false},
		{"foreign", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: foreignKB.ID}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := x.service.SearchResourcesScoped(context.Background(), owner.User.ID, wid, "Allowed content", "lexical", 1, tc.scope)
			if (err != nil) != tc.err || len(got) != 0 {
				t.Fatalf("empty/foreign scope admitted results: %+v %v", got, err)
			}
			_, after, _ := x.search.snapshot()
			if len(after) != 1 || modelCalls.Load() != 1 {
				t.Fatal("rejected or empty resource scope called external dependencies")
			}
		})
	}
}

func TestHTTPResourceScopeIndexRevalidatesDeletedSelectionAfterExternalCalls(t *testing.T) {
	for _, stage := range []string{"search-empty", "rerank-results", "rerank-empty", "rerank-error"} {
		t.Run(stage, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "resource-delete")
			wid := owner.Workspaces[0].ID
			a, empty := createHTTPKnowledgeBase(t, x, owner, "Searchable"), createHTTPKnowledgeBase(t, x, owner, "Selected empty")
			uploadHTTPKnowledgeBase(t, x, owner.Token, wid, a.ID, "Selected resources cannot disappear during external work")
			x.process(t)
			remove := func(ctx context.Context) error { return x.store.DeleteKnowledgeBase(ctx, owner.User.ID, wid, empty.ID) }
			if stage == "search-empty" {
				x.search.setSearch(func(ctx context.Context, _ indexTestQuery, _ []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
					return nil, remove(ctx)
				})
			} else {
				attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, passages []string) ([]reranking.Score, error) {
					if err := remove(ctx); err != nil {
						t.Error(err)
						return nil, err
					}
					if stage == "rerank-error" {
						return nil, reranking.ErrModelUnavailable
					}
					score := .9
					if stage == "rerank-empty" {
						score = .01
					}
					return rerankScores(passages, score), nil
				}))
			}
			scope := domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: a.ID}, {KnowledgeBaseID: empty.ID}}}
			hits, err := x.service.SearchResourcesScoped(context.Background(), owner.User.ID, wid, "Selected resources", "lexical", 1, scope)
			if !errors.Is(err, domain.ErrNotFound) || len(hits) != 0 {
				t.Fatalf("deleted scope accepted after %s: %+v %v", stage, hits, err)
			}
		})
	}
}
