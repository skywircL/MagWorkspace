package store

import (
	"context"
	"slices"
	"strings"
	"testing"

	"magworkspace/internal/domain"
)

func TestMySQLResourceScopeIndexUnionAndUntrustedHints(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	profile := testIndexProfile()
	extra, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.b.ID, "Unselected B", "extra.md", []byte("extra B"))
	must(t, err)
	writes := []domain.DocumentWrite{f.docA, f.docB, extra}
	jobs := make([]domain.IndexJob, len(writes))
	ids := []string{}
	hints := []domain.IndexCandidate{}
	for i, write := range writes {
		jobs[i] = publishTestIndex(t, f.s, f.u.ID, f.w.ID, write, profile)
		chunks, err := f.s.ReadIndexChunks(ctx, f.u.ID, f.w.ID, jobs[i], nil)
		must(t, err)
		ids = append(ids, jobs[i].ID)
		hints = append(hints, domain.IndexCandidate{IndexID: jobs[i].ID, Generation: jobs[i].Generation, ChunkID: chunks[1].ID, Score: float64(i + 1)})
	}
	union := domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: f.a.ID}, {KnowledgeBaseID: f.b.ID, DocumentIDs: []string{f.docB.Document.ID}}}}
	narrow, err := union.NarrowKnowledgeBases([]string{f.b.ID})
	must(t, err)
	for _, tc := range []struct {
		name  string
		scope domain.ResourceScope
		want  []int
	}{
		{"full A plus single B", union, []int{0, 1}},
		{"selecting B stays single document", narrow, []int{1}},
		{"workspace", domain.ResourceScope{All: true}, []int{0, 1, 2}},
		{"empty", domain.ResourceScope{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generations, err := f.s.SearchIndexScopeForResources(ctx, f.u.ID, f.w.ID, profile.ID, tc.scope)
			must(t, err)
			refs, err := f.s.ResolveIndexesForResources(ctx, f.u.ID, f.w.ID, profile.ID, append(ids, "untrusted", ids[0]), tc.scope)
			must(t, err)
			badGeneration := hints[0]
			badGeneration.Generation = strings.Repeat("f", 32)
			hits, err := f.s.ResolveIndexChunksForResources(ctx, f.u.ID, f.w.ID, profile.ID, append(append([]domain.IndexCandidate{}, hints...), hints[0], badGeneration), tc.scope)
			must(t, err)
			if len(generations) != len(tc.want) || len(refs) != len(tc.want) || len(hits) != len(tc.want) {
				t.Fatalf("scope count generations=%d indexes=%d chunks=%d want=%d", len(generations), len(refs), len(hits), len(tc.want))
			}
			for i, index := range tc.want {
				if !slices.Contains(generations, jobs[index].Generation) || refs[i].Job.ID != jobs[index].ID || hits[i].Chunk.DocumentID != writes[index].Document.ID || hits[i].Score != hints[index].Score || hits[i].Parent == nil {
					t.Fatalf("scope or trusted parent hydration changed: indexes=%+v hits=%+v", refs, hits)
				}
			}
		})
	}
	// A replacement head retires its old index even though its library remains
	// selected. The other library's selected document remains searchable.
	_, err = f.s.AppendDocumentVersion(ctx, f.u.ID, f.w.ID, f.docA.Document.ID, f.docA.Version.ID, "new.md", []byte("new A head"))
	must(t, err)
	hits, err := f.s.ResolveIndexChunksForResources(ctx, f.u.ID, f.w.ID, profile.ID, hints, union)
	must(t, err)
	if len(hits) != 1 || hits[0].Chunk.DocumentID != f.docB.Document.ID {
		t.Fatal("retired generation escaped resource scope hydration")
	}
	// Existing APIs retain their original intersecting filters.
	hits, err = f.s.ResolveIndexChunksInKnowledgeBases(ctx, f.u.ID, f.w.ID, profile.ID, []string{f.a.ID}, hints, []string{f.docB.Document.ID})
	must(t, err)
	if len(hits) != 0 {
		t.Fatal("legacy API semantics changed to union")
	}
}

func TestMySQLResourceScopeIndexRejectsInvalidResourcesWithEmptyHints(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	foreign, other := testUser(t, f.s, "resource-index-foreign")
	foreignWrite, err := f.s.CreateDocument(ctx, foreign.ID, other.ID, "Foreign", "foreign.md", []byte("private"))
	must(t, err)
	deleted, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Deleted empty", "")
	must(t, err)
	must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, deleted.ID))
	deletedDoc, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.b.ID, "Deleted doc", "deleted.md", []byte("deleted"))
	must(t, err)
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, deletedDoc.Document.ID))
	for _, tc := range []struct {
		name  string
		scope domain.ResourceScope
	}{
		{"foreign library", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: foreignWrite.Document.KnowledgeBaseID}}}},
		{"foreign document", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: f.a.ID, DocumentIDs: []string{foreignWrite.Document.ID}}}}},
		{"wrong owning library", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: f.a.ID, DocumentIDs: []string{f.docB.Document.ID}}}}},
		{"deleted library", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: deleted.ID}}}},
		{"deleted document", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: f.b.ID, DocumentIDs: []string{deletedDoc.Document.ID}}}}},
		{"malformed", domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: "invalid"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := f.s.SearchIndexScopeForResources(ctx, f.u.ID, f.w.ID, testIndexProfile().ID, tc.scope); err == nil || len(got) != 0 {
				t.Fatalf("invalid scope admitted generations: %+v %v", got, err)
			}
			if got, err := f.s.ResolveIndexesForResources(ctx, f.u.ID, f.w.ID, testIndexProfile().ID, nil, tc.scope); err == nil || len(got) != 0 {
				t.Fatalf("empty index hints bypassed scope: %+v %v", got, err)
			}
			if got, err := f.s.ResolveIndexChunksForResources(ctx, f.u.ID, f.w.ID, testIndexProfile().ID, nil, tc.scope); err == nil || len(got) != 0 {
				t.Fatalf("empty chunk hints bypassed scope: %+v %v", got, err)
			}
		})
	}
	for _, call := range []func() error{
		func() error {
			_, err := f.s.SearchIndexScopeForResources(ctx, foreign.ID, f.w.ID, testIndexProfile().ID, domain.ResourceScope{All: true})
			return err
		},
		func() error {
			_, err := f.s.ResolveIndexesForResources(ctx, foreign.ID, f.w.ID, testIndexProfile().ID, nil, domain.ResourceScope{})
			return err
		},
		func() error {
			_, err := f.s.ResolveIndexChunksForResources(ctx, foreign.ID, f.w.ID, testIndexProfile().ID, nil, domain.ResourceScope{})
			return err
		},
	} {
		wantError(t, call(), domain.ErrNotFound)
	}
}
