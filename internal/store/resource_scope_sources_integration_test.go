package store

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLResourceScopeMixedHistoryAndConversation(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	sibling, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.b.ID, "B sibling", "sibling.md", []byte("excluded B sibling"))
	must(t, err)
	empty, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Empty selected library", "observable description")
	must(t, err)
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "RESOURCE_HISTORY")
	must(t, err)
	sources := []domain.Run{}
	for _, doc := range []domain.DocumentWrite{f.docA, f.docB, sibling} {
		run, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "RESOURCE_HISTORY", time.Minute, domain.ResourceSelection{})
		must(t, err)
		_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, doc.Document.ID, "", "", "")
		must(t, err)
		f.finish(t, ss, run, "RESOURCE_HISTORY derived from "+doc.Document.Title)
		sources = append(sources, run)
	}
	selection := domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID, empty.ID}, DocumentIDs: []string{f.docB.Document.ID}}
	target, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "mixed continuation", time.Minute, selection)
	must(t, err)
	for index, source := range sources {
		allowed, err := f.s.CanHydrateRunHistoryForRun(ctx, f.u.ID, f.w.ID, ss.ID, source.ID, target.ID)
		must(t, err)
		if allowed != (index < 2) {
			t.Fatalf("mixed history source %d allowed=%v", index, allowed)
		}
	}
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, target.ID, []string{sources[0].ID, sources[2].ID}), domain.ErrInvalid)
	var bound int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_history_sources WHERE run_id=?`, target.ID).Scan(&bound))
	if bound != 0 {
		t.Fatal("failed mixed history binding left partial provenance")
	}
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, target.ID, []string{sources[0].ID, sources[1].ID}))
	libraries, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, target.ID, "Empty selected", 0, 20)
	must(t, err)
	if len(libraries.KnowledgeBases) != 1 || libraries.KnowledgeBases[0].ID != empty.ID {
		t.Fatal("empty full-library selection disappeared from metadata")
	}
	f.finish(t, ss, target, "RESOURCE_DERIVED across whole A and one B document")
	narrow, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "cannot inherit full provenance", time.Minute, domain.ResourceSelection{DocumentIDs: []string{f.docB.Document.ID}})
	must(t, err)
	allowed, err := f.s.CanHydrateRunHistoryForRun(ctx, f.u.ID, f.w.ID, ss.ID, target.ID, narrow.ID)
	must(t, err)
	if allowed {
		t.Fatal("derived history erased full-library or metadata provenance")
	}
	_, searchSession, searchRun := beginResourceToolRun(t, f, selection)
	recalled, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, searchSession.ID, searchRun.ID, "RESOURCE_HISTORY", 8, 0)
	must(t, err)
	ids := []string{}
	for _, item := range recalled.Items {
		ids = append(ids, item.RunID)
	}
	if !slices.Contains(ids, sources[0].ID) || !slices.Contains(ids, sources[1].ID) || slices.Contains(ids, sources[2].ID) {
		t.Fatalf("conversation recall failed mixed boundary: %v", ids)
	}
}

func TestMySQLResourceScopeEvidenceRejectsSiblingAndStaleSource(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	other, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Whole selected library", "")
	must(t, err)
	sibling, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.write.Document.KnowledgeBaseID, "Excluded sibling", "sibling.md", []byte("sibling body"))
	must(t, err)
	siblingIndex := publishTestIndex(t, f.s, f.u.ID, f.w.ID, sibling, testIndexProfile())
	siblingHit := domain.SearchHit{Chunk: testIndexChunks(siblingIndex, "sibling body")[0]}
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "mixed evidence")
	must(t, err)
	run, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "mixed evidence", time.Minute, domain.ResourceSelection{KnowledgeBaseIDs: []string{other.ID}, DocumentIDs: []string{f.write.Document.ID}})
	must(t, err)
	// A valid document and its unselected sibling in one response must roll back
	// together even if the selected evidence was processed first.
	_, err = f.s.RecordRunKnowledgeEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, []domain.SearchHit{f.hit, siblingHit}, nil)
	wantError(t, err, domain.ErrForbidden)
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, run.ID).Scan(&count))
	if count != 0 {
		t.Fatal("mixed rejected evidence batch partially committed")
	}
	evidence, err := f.s.RecordRunKnowledgeEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, []domain.SearchHit{f.hit}, []string{f.write.Document.KnowledgeBaseID})
	must(t, err)
	if len(evidence) != 1 {
		t.Fatal("model selection of document target's KB lost selected evidence")
	}
	// Simulate an acquired source outside a corrupted/narrowed durable scope.
	// Read and finalization independently enforce current backend authority.
	raw, err := json.Marshal(domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: other.ID}}})
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `UPDATE run_search_targets SET scope=? WHERE run_id=?`, raw, run.ID)
	must(t, err)
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, evidence[0].ID)
	wantError(t, err, domain.ErrForbidden)
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "uncited source-derived content", "test"), domain.ErrInvalid)
}
