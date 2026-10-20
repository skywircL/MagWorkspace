package httpapi_test

import (
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"testing"

	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/reranking"
)

type nativeIndexTestSearch struct{ *indexTestSearch }

func (*nativeIndexTestSearch) NativeFusion() bool { return true }

func TestHTTPQdrantNativeFusionNormalizesScoresAndPreservesFinalAuthorization(t *testing.T) {
	x := newIndexFixture(t)
	x.server.Close()
	x.service = indexing.NewService(x.store, x.parser, x.embedder, &nativeIndexTestSearch{x.search}, x.objects, x.profile, indexing.Options{})
	x.config.Indexing = x.service
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
	owner := x.register(t, "native-fusion")
	write := x.upload(t, owner, "虚拟机内存不足导致进程终止，检查推荐配置。")
	x.process(t)
	x.search.setSearch(func(_ context.Context, q indexTestQuery, items []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		if q.mode != "hybrid" || len(q.vector) != 3 {
			t.Error("expected one native hybrid query")
		}
		for i := range items {
			items[i].Score = 0.032123
		}
		return items, nil
	})
	result := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid")
	if len(result.Hits) != 1 || math.Abs(result.Hits[0].Score-0.032123/(2.0/61)) > 1e-12 || x.service.Execution() != "qdrant_native" {
		t.Fatalf("native fusion reranked or not hydrated: %+v", result)
	}
	_, queries, _ := x.search.snapshot()
	if len(queries) != 1 {
		t.Fatalf("native fusion issued %d queries", len(queries))
	}
	// A document deleted after candidate retrieval cannot escape the final DB check.
	x.search.setSearch(func(ctx context.Context, q indexTestQuery, items []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		if err := x.store.DeleteDocument(ctx, owner.User.ID, write.Document.WorkspaceID, write.Document.ID); err != nil {
			t.Fatal(err)
		}
		return items, nil
	})
	if got := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid"); len(got.Hits) != 0 {
		t.Fatal("deleted source escaped authorization")
	}
}

func TestHTTPQdrantTheoreticalScoreFeedsRerankAndFallback(t *testing.T) {
	x := newIndexFixture(t)
	x.server.Close()
	var modelErr error
	x.service = indexing.NewService(x.store, x.parser, x.embedder, &nativeIndexTestSearch{x.search}, x.objects, x.profile, indexing.Options{
		Reranker: integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
			return rerankScores(passages, 0.8), modelErr
		}),
		RerankOptions: reranking.DefaultOptions(),
	})
	x.config.Indexing = x.service
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
	owner := x.register(t, "theoretical-fusion")
	write := x.upload(t, owner, "虚拟机内存不足导致进程终止。")
	x.process(t)
	raw := 0.9 * (2.0 / 61)
	x.search.setSearch(func(_ context.Context, _ indexTestQuery, items []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		for i := range items {
			items[i].Score = raw
		}
		return items, nil
	})
	find := func(enabled bool) rerankSearchResponse {
		return integrationData[rerankSearchResponse](t, x.request(t, "POST",
			workspacePath(write.Document.WorkspaceID)+"/search", owner.Token,
			map[string]any{"query": "内存不足", "mode": "hybrid", "limit": 1, "rerank": enabled}, 200))
	}
	baseline, ranked := find(false), find(true)
	if len(baseline.Hits) != 1 || len(ranked.Hits) != 1 {
		t.Fatal("missing normalized hit")
	}
	hit := ranked.Hits[0]
	if math.Abs(baseline.Hits[0].Score-0.9) > 1e-12 || math.Abs(hit.Score-0.85) > 1e-12 ||
		hit.RetrievalScore == nil || hit.NormalizedRetrievalScore == nil ||
		math.Abs(*hit.RetrievalScore-0.9) > 1e-12 || *hit.NormalizedRetrievalScore != *hit.RetrievalScore ||
		ranked.ProfileID != baseline.ProfileID || ranked.Rerank.Outcome != "ok" {
		t.Fatalf("fusion score was rescaled by reranker: %+v", ranked)
	}
	// A single-branch first-place hit stays 0.5 even when it is the only hit.
	raw = 1.0 / 61
	single := find(true)
	if len(single.Hits) != 1 || math.Abs(single.Hits[0].Score-0.73) > 1e-12 {
		t.Fatalf("one-branch hit inflated: %+v", single)
	}
	modelErr = errors.New("unavailable")
	fallback := find(true)
	if len(fallback.Hits) != 1 || fallback.Hits[0].Score != 0.5 ||
		fallback.Rerank.Outcome != "model_error" || !fallback.Rerank.FallbackUsed {
		t.Fatalf("fallback changed fusion score: %+v", fallback)
	}
}
