package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func beginKnowledgeSourceRun(t *testing.T, s *Store, uid, wid, sid, query string, scope domain.RetrievalScope) domain.Run {
	t.Helper()
	run, _, _, err := s.BeginRunWithScope(context.Background(), uid, wid, sid, query, time.Minute, scope)
	must(t, err)
	return run
}

func TestMySQLRunKnowledgeScopeEvidenceAcquisition(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	other, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Outside the selected library", "")
	must(t, err)
	inside := f.write.Document.KnowledgeBaseID
	for _, scenario := range []struct {
		name      string
		scope     domain.RetrievalScope
		selection []string
		hits      []domain.SearchHit
		wantErr   error
	}{
		{"inherit run", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}}, nil, []domain.SearchHit{f.hit}, nil},
		{"explicit inside", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}}, []string{inside}, []domain.SearchHit{f.hit}, nil},
		{"outside hit", domain.RetrievalScope{KnowledgeBaseIDs: []string{other.ID}}, nil, []domain.SearchHit{f.hit}, domain.ErrForbidden},
		{"empty run", domain.RetrievalScope{KnowledgeBaseIDs: []string{}}, nil, []domain.SearchHit{f.hit}, domain.ErrForbidden},
		{"empty tool filter", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}}, []string{}, []domain.SearchHit{f.hit}, domain.ErrForbidden},
		{"mixed selection before empty result", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}}, []string{inside, other.ID}, nil, domain.ErrForbidden},
		{"document intersection", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}, DocumentIDs: []string{}}, nil, []domain.SearchHit{f.hit}, domain.ErrForbidden},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, scenario.name)
			must(t, err)
			run := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "scope evidence", scenario.scope)
			got, err := f.s.RecordRunKnowledgeEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, scenario.hits, scenario.selection)
			if scenario.wantErr == nil {
				must(t, err)
				if len(got) != 1 || got[0].DocumentID != f.write.Document.ID {
					t.Fatalf("permitted evidence was not acquired: %+v", got)
				}
				return
			}
			wantError(t, err, scenario.wantErr)
			if len(got) != 0 {
				t.Fatal("rejected acquisition returned partial evidence")
			}
			var count int
			must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, run.ID).Scan(&count))
			if count != 0 {
				t.Fatal("rejected acquisition persisted evidence")
			}
		})
	}
	// The older search and neighboring-passage entry points must inherit the
	// same durable boundary even though they accept no model KB parameter.
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "legacy and adjacent evidence")
	must(t, err)
	run := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "scope evidence", domain.RetrievalScope{KnowledgeBaseIDs: []string{other.ID}})
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrForbidden)
	_, err = f.s.RecordRunDocumentEvidence(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrForbidden)
}

func TestMySQLRunKnowledgeScopeHistoryAndConversationClosure(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "KB scope source history")
	must(t, err)
	scopeA := domain.RetrievalScope{KnowledgeBaseIDs: []string{f.a.ID}}
	source := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "KB_HISTORY_SOURCE", scopeA)
	_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, source.ID, f.docA.Document.ID, "", "", "")
	must(t, err)
	f.finish(t, ss, source, "KB_HISTORY_SOURCE derived from A")
	derived := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "paraphrase", scopeA)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, derived.ID, []string{source.ID}))
	f.finish(t, ss, derived, "KB_HISTORY_DERIVED without reading any new document")
	for _, run := range []domain.Run{source, derived} {
		for _, selection := range []struct {
			name string
			ids  []string
			want bool
		}{{"all", nil, true}, {"same", []string{f.a.ID}, true}, {"other", []string{f.b.ID}, false}, {"none", []string{}, false}} {
			t.Run(run.ID+"/"+selection.name, func(t *testing.T) {
				got, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil, selection.ids)
				must(t, err)
				if got != selection.want {
					t.Fatalf("history allowed=%v want=%v", got, selection.want)
				}
			})
		}
	}
	blocked := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "narrowed to B", domain.RetrievalScope{KnowledgeBaseIDs: []string{f.b.ID}})
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, blocked.ID, []string{derived.ID}), domain.ErrInvalid)
	var sources int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_history_sources WHERE run_id=?`, blocked.ID).Scan(&sources))
	if sources != 0 {
		t.Fatal("rejected history binding left source rows")
	}
	for _, selection := range []struct {
		name string
		ids  []string
		want int
	}{{"same", []string{f.a.ID}, 1}, {"other", []string{f.b.ID}, 0}, {"none", []string{}, 0}} {
		searchSession, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "recall "+selection.name)
		must(t, err)
		searchRun := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, searchSession.ID, "recall", domain.RetrievalScope{KnowledgeBaseIDs: selection.ids})
		result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, searchSession.ID, searchRun.ID, "KB_HISTORY_DERIVED", 1, 0)
		must(t, err)
		if len(result.Items) != selection.want || result.HasMore || result.NextOffset != nil {
			t.Fatalf("conversation source escaped %s: %+v", selection.name, result)
		}
		if selection.want == 0 {
			continue
		}
		f.finish(t, searchSession, searchRun, "KB_HISTORY_RECALLED in a different session")
		next := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, searchSession.ID, "recall paraphrase", scopeA)
		must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, searchSession.ID, next.ID, []string{searchRun.ID}))
		f.finish(t, searchSession, next, "KB_HISTORY_CHAINED without new tools")
		allowed, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, searchSession.ID, next.ID, nil, []string{f.b.ID})
		must(t, err)
		if allowed {
			t.Fatal("cross-session paraphrases lost their original KB boundary")
		}
	}
}

func TestMySQLRunKnowledgeScopeEmptyLibraryMetadata(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	empty, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Empty but private library", "metadata remains a source")
	must(t, err)
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "empty KB metadata")
	must(t, err)
	run := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "inspect empty KB", domain.RetrievalScope{KnowledgeBaseIDs: []string{empty.ID}})
	page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", 0, 1)
	must(t, err)
	if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != empty.ID || page.HasMore {
		t.Fatalf("scoped empty KB not independently discoverable: %+v", page)
	}
	f.finish(t, ss, run, "EMPTY_LIBRARY_SCOPE description")
	for _, scenario := range []struct {
		docs []string
		kbs  []string
		want bool
	}{{nil, nil, true}, {nil, []string{empty.ID}, true}, {nil, []string{f.a.ID}, false}, {nil, []string{}, false}, {[]string{}, []string{empty.ID}, false}} {
		allowed, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, scenario.docs, scenario.kbs)
		must(t, err)
		if allowed != scenario.want {
			t.Fatalf("empty KB history allowed=%v want=%v", allowed, scenario.want)
		}
	}
	selected := beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, ss.ID, "selected but not yet observed", domain.RetrievalScope{KnowledgeBaseIDs: []string{empty.ID}})
	must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, empty.ID))
	if err = f.s.FinishRun(ctx, f.u.ID, f.w.ID, ss.ID, selected.ID, "answer after selected KB deletion", "test"); err == nil {
		t.Fatal("deleted hard selection allowed finalization without acquired sources")
	}
}

func TestMySQLRunKnowledgeScopeEvidenceReadsAliasesAndFinalization(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	old := f.record(t, f.hit)[0]
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "old [E:"+old.ID+"]", "test"))
	prior := f.run.ID
	inside := f.write.Document.KnowledgeBaseID
	f.run = beginKnowledgeSourceRun(t, f.s, f.u.ID, f.w.ID, f.session.ID, "continue", domain.RetrievalScope{KnowledgeBaseIDs: []string{inside}})
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []string{prior}))
	current := f.record(t, f.hit)[0]
	aliases, err := f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []string{old.ID})
	must(t, err)
	if aliases[old.ID] != current.ID {
		t.Fatal("valid same-library alias missing")
	}
	other, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "newly narrowed destination", "")
	must(t, err)
	// Inject stale persisted observations against a narrower durable boundary.
	// Reads, restoration, aliases and final commit must independently reject
	// them even when their chunk digests and ownership remain correct.
	raw, err := json.Marshal([]string{other.ID})
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `UPDATE run_knowledge_base_scopes SET knowledge_base_ids=? WHERE run_id=?`, raw, f.run.ID)
	must(t, err)
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, current.ID)
	wantError(t, err, domain.ErrForbidden)
	restored, err := f.s.EvidenceForRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, false)
	must(t, err)
	if len(restored) != 0 {
		t.Fatal("out-of-scope evidence restored into the current registry")
	}
	_, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []string{old.ID})
	wantError(t, err, domain.ErrForbidden)
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "uncited paraphrase", "test"), domain.ErrInvalid)
	state, err := f.s.Run(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID)
	must(t, err)
	if state.Status != "running" {
		t.Fatal("rejected finalization changed the active run")
	}
}
