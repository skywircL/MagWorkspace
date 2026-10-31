package indexing

import (
	"reflect"
	"testing"

	"magworkspace/internal/domain"
)

func TestParentExpansionPreservesExactIdentityAndDistinctTopK(t *testing.T) {
	p1 := domain.IndexChunk{ID: "parent1", Kind: "parent", Content: "父段：命中在尾部。", StartByte: 12, EndByte: 39, SHA256: "parent-digest"}
	p2 := domain.IndexChunk{ID: "parent2", Kind: "parent", Content: "另一个父段"}
	hits := []domain.SearchHit{
		{Chunk: domain.IndexChunk{ID: "child1"}, Parent: &p1, Score: .9},
		{Chunk: domain.IndexChunk{ID: "child2"}, Parent: &p1, Score: .8},
		{Chunk: domain.IndexChunk{ID: "child3"}, Parent: &p2, Score: .7},
	}
	got := expandParentHits(hits, 2)
	if len(got) != 2 || !reflect.DeepEqual(got[0].Chunk, p1) || !reflect.DeepEqual(got[1].Chunk, p2) || got[0].Score != .9 || got[0].Parent != nil || len(got[0].MatchedChildIDs) != 2 {
		t.Fatalf("incorrect distinct parent expansion: %+v", got)
	}
	if hits[0].Chunk.ID != "child1" || p1.ID != "parent1" {
		t.Fatal("expansion mutated source evidence")
	}
}
