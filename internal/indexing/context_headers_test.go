package indexing

import (
	"reflect"
	"testing"

	"magworkspace/internal/domain"
)

func TestContextHeadersReadPersistedFieldsForArbitrarySubset(t *testing.T) {
	chunks := []domain.IndexChunk{
		{ID: "later-child", Kind: "child", StartByte: 900, EndByte: 950, Content: "partial source", ContextHeader: "产品 → 配额"},
		{ID: "parent", Kind: "parent", StartByte: 0, EndByte: 1600, Content: "context only", ContextHeader: "产品"},
		{ID: "earlier-child", Kind: "child", StartByte: 50, EndByte: 80, Content: "other partial source", ContextHeader: "产品 → 部署"},
	}
	before := append([]domain.IndexChunk(nil), chunks...)
	got := contextHeaders(chunks)
	want := map[string]string{"later-child": "产品 → 配额", "parent": "产品", "earlier-child": "产品 → 部署"}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(chunks, before) {
		t.Fatal("context lookup reconstructed partial text or mutated evidence")
	}
}
