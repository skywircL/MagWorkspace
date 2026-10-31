package store

import (
	"context"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

type knowledgeSourceFixture struct {
	s          *Store
	u          domain.User
	w          domain.Workspace
	a, b       domain.KnowledgeBase
	docA, docB domain.DocumentWrite
}

func newKnowledgeSourceFixture(t *testing.T) knowledgeSourceFixture {
	t.Helper()
	s, _ := testStore(t)
	u, w := testUser(t, s, "kb-source-owner")
	f := knowledgeSourceFixture{s: s, u: u, w: w}
	var err error
	f.a, err = s.CreateKnowledgeBase(context.Background(), u.ID, w.ID, "Scoped A", "private library description")
	must(t, err)
	f.b, err = s.CreateKnowledgeBase(context.Background(), u.ID, w.ID, "Scoped B", "second library description")
	must(t, err)
	f.docA, err = s.CreateKnowledgeBaseDocument(context.Background(), u.ID, w.ID, f.a.ID, "A", "a.md", []byte("A content"))
	must(t, err)
	f.docB, err = s.CreateKnowledgeBaseDocument(context.Background(), u.ID, w.ID, f.b.ID, "B", "b.md", []byte("B content"))
	must(t, err)
	return f
}

func (f knowledgeSourceFixture) begin(t *testing.T, title string, scope []string) (domain.Session, domain.Run) {
	t.Helper()
	ss, err := f.s.CreateSession(context.Background(), f.u.ID, f.w.ID, title)
	must(t, err)
	run, _, _, err := f.s.BeginRun(context.Background(), f.u.ID, f.w.ID, ss.ID, "inspect library metadata", time.Minute, scope)
	must(t, err)
	return ss, run
}

func (f knowledgeSourceFixture) hydrate(t *testing.T, ss domain.Session, run domain.Run, scope []string, want bool) {
	t.Helper()
	got, err := f.s.CanHydrateRunHistory(context.Background(), f.u.ID, f.w.ID, ss.ID, run.ID, scope)
	must(t, err)
	if got != want {
		t.Errorf("metadata history hydration=%v want=%v for scope=%v", got, want, scope)
	}
}

func (f knowledgeSourceFixture) finish(t *testing.T, ss domain.Session, run domain.Run, answer string) {
	t.Helper()
	must(t, f.s.FinishRun(context.Background(), f.u.ID, f.w.ID, ss.ID, run.ID, answer, "test"))
}

func TestMySQLKnowledgeBaseSourcesHydrationHistoryAndConversation(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	scopeA := []string{f.docA.Document.ID}
	scopeB := []string{f.docB.Document.ID}
	ss, source := f.begin(t, "original library source", scopeA)
	page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, source.ID, "Scoped", 0, 20)
	must(t, err)
	if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != f.a.ID {
		t.Fatal("scope fixture did not expose only library A")
	}
	f.finish(t, ss, source, "LIBRARY_SOURCE_ALPHA: private library description")
	f.hydrate(t, ss, source, nil, true)
	f.hydrate(t, ss, source, scopeA, true)
	f.hydrate(t, ss, source, []string{}, false)
	f.hydrate(t, ss, source, scopeB, false)

	derived, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, ss.ID, "paraphrase library", time.Minute, scopeA)
	must(t, err)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, derived.ID, []string{source.ID}))
	f.finish(t, ss, derived, "DERIVED_LIBRARY_BETA: a metadata-only paraphrase")
	f.hydrate(t, ss, derived, scopeA, true)
	f.hydrate(t, ss, derived, []string{}, false)
	f.hydrate(t, ss, derived, scopeB, false)
	narrow, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, ss.ID, "narrowed continuation", time.Minute, []string{})
	must(t, err)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, ss.ID, narrow.ID, []string{derived.ID}), domain.ErrInvalid)

	for _, scope := range []struct {
		name string
		ids  []string
	}{{"empty", []string{}}, {"other library", scopeB}} {
		searchSession, searchRun := f.begin(t, scope.name, scope.ids)
		result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, searchSession.ID, searchRun.ID, "LIBRARY_SOURCE_ALPHA", 5, 0)
		must(t, err)
		if len(result.Items) != 0 || result.HasMore {
			t.Errorf("metadata-only history escaped %s scope through conversation search", scope.name)
		}
	}
	recalledSession, recalled := f.begin(t, "permitted recall", scopeA)
	result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, recalledSession.ID, recalled.ID, "LIBRARY_SOURCE_ALPHA", 5, 0)
	must(t, err)
	if len(result.Items) != 1 || result.Items[0].RunID != source.ID {
		t.Fatal("permitted metadata-only conversation could not be recalled")
	}
	f.finish(t, recalledSession, recalled, "RECALLED_LIBRARY_GAMMA: a second-session paraphrase")
	f.hydrate(t, recalledSession, recalled, scopeA, true)
	f.hydrate(t, recalledSession, recalled, []string{}, false)
	f.hydrate(t, recalledSession, recalled, scopeB, false)
	third, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, recalledSession.ID, "continue recalled history", time.Minute, scopeA)
	must(t, err)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, recalledSession.ID, third.ID, []string{recalled.ID}))
	f.finish(t, recalledSession, third, "THIRD_LIBRARY_DELTA: another tool-free paraphrase")
	f.hydrate(t, recalledSession, third, scopeA, true)
	f.hydrate(t, recalledSession, third, []string{}, false)

	_, err = f.s.UpdateKnowledgeBase(ctx, f.u.ID, f.w.ID, f.a.ID, f.a.Name, "changed authoritative description")
	must(t, err)
	for _, item := range []struct {
		session domain.Session
		run     domain.Run
	}{{ss, source}, {ss, derived}, {recalledSession, recalled}, {recalledSession, third}} {
		f.hydrate(t, item.session, item.run, nil, false)
	}
}

func TestMySQLKnowledgeBaseSourcesDeletionRejectsFinalization(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "populated"
		if empty {
			name = "empty library"
		}
		t.Run(name, func(t *testing.T) {
			f := newKnowledgeSourceFixture(t)
			ctx := context.Background()
			kb := f.a
			if empty {
				var err error
				kb, err = f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Empty Source", "sensitive empty library metadata")
				must(t, err)
			}
			// Unrestricted runs have no selected document whose deletion could
			// accidentally make this pass without a dedicated library source.
			ss, run := f.begin(t, "metadata then deletion", nil)
			page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, kb.Name, 0, 20)
			must(t, err)
			if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != kb.ID {
				t.Fatal("library source fixture was not acquired")
			}
			must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, kb.ID))
			wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "uncited metadata paraphrase", "test"), domain.ErrInvalid)
			state, err := f.s.Run(ctx, f.u.ID, f.w.ID, ss.ID, run.ID)
			must(t, err)
			messages, total, err := f.s.Messages(ctx, f.u.ID, f.w.ID, ss.ID, 20, 0)
			must(t, err)
			if state.Status != "running" || total != 1 || len(messages) != 1 || messages[0].Role != "user" {
				t.Fatal("invalid metadata source partially committed an answer")
			}
		})
	}
}

func TestMySQLKnowledgeBaseSourcesPaginationLookahead(t *testing.T) {
	for _, scenario := range []string{"history scope", "deleted lookahead"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKnowledgeSourceFixture(t)
			ctx := context.Background()
			scope := []string{f.docA.Document.ID, f.docB.Document.ID}
			ss, run := f.begin(t, "one visible row and an existence bit", scope)
			page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "Scoped", 0, 1)
			must(t, err)
			if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != f.a.ID || !page.HasMore || page.NextOffset == nil || *page.NextOffset != 1 {
				t.Fatalf("invalid lookahead fixture: %+v", page)
			}
			if scenario == "history scope" {
				f.finish(t, ss, run, "At least two matching libraries exist.")
				f.hydrate(t, ss, run, scope, true)
				f.hydrate(t, ss, run, []string{f.docA.Document.ID}, false)
				f.hydrate(t, ss, run, []string{}, false)
				return
			}
			must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, f.b.ID))
			wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "There is another matching library.", "test"), domain.ErrInvalid)
		})
	}
}

func TestMySQLKnowledgeBaseSourcesDescriptionChangeAndPromptResume(t *testing.T) {
	for _, scenario := range []string{"changed description", "deleted before resume"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKnowledgeSourceFixture(t)
			ctx := context.Background()
			ss, run := f.begin(t, "library source lifecycle", nil)
			page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.a.Name, 0, 20)
			must(t, err)
			if len(page.KnowledgeBases) != 1 {
				t.Fatal("metadata not acquired")
			}
			if scenario == "changed description" {
				_, err = f.s.UpdateKnowledgeBase(ctx, f.u.ID, f.w.ID, f.a.ID, f.a.Name, "modified before final answer")
				must(t, err)
				repeated, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.a.Name, 0, 20)
				wantError(t, err, domain.ErrConflict)
				if len(repeated.KnowledgeBases) != 0 || repeated.HasMore || repeated.NextOffset != nil {
					t.Fatal("conflicting reread exposed a partial updated directory")
				}
				wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "outdated metadata text", "test"), domain.ErrInvalid)
				return
			}
			prompt := promptInputForRun(t, f.s, f.u.ID, f.w.ID, ss.ID, run.ID)
			_, err = f.s.SuspendRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "demo", prompt)
			must(t, err)
			must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, f.a.ID))
			_, err = f.s.ResumePrompt(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, prompt.ID, "国内", time.Minute)
			wantError(t, err, domain.ErrConflict)
			state, err := f.s.Run(ctx, f.u.ID, f.w.ID, ss.ID, run.ID)
			must(t, err)
			saved, err := f.s.RunPrompt(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, prompt.ID)
			must(t, err)
			if state.Status != "suspended" || saved.Status != "pending" {
				t.Fatal("rejected metadata resume consumed the pending answer")
			}
		})
	}
}
