package httpapi_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
)

type nativeIndexTestSearch struct{ *indexTestSearch }

func (*nativeIndexTestSearch) NativeFusion() bool { return true }

func TestHTTPQdrantNativeFusionPreservesScoresAndFinalAuthorization(t *testing.T) {
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
	if len(result.Hits) != 1 || result.Hits[0].Score != 0.032123 || x.service.Execution() != "qdrant_native" {
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
