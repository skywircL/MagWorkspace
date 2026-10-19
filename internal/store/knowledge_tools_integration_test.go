package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLKnowledgeMetadataScopeHistoryAndDeletion(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t)
	u, w := testUser(t, s, "toolmeta")
	a, err := s.CreateDocument(ctx, u.ID, w.ID, "住宿标准", "policy.txt", []byte("甲"))
	must(t, err)
	b, err := s.CreateDocument(ctx, u.ID, w.ID, "内部标题", "private.txt", []byte("乙"))
	must(t, err)
	session, err := s.CreateSession(ctx, u.ID, w.ID, "metadata")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "列出文档", time.Minute, []string{a.Document.ID})
	must(t, err)
	page, err := s.RunToolDocuments(ctx, u.ID, w.ID, session.ID, run.ID, "", "", 0, 20)
	must(t, err)
	if len(page.Documents) != 1 || page.Documents[0].Document.ID != a.Document.ID || page.HasMore || page.Documents[0].Index != nil {
		t.Fatalf("unindexed metadata escaped scope: %+v", page)
	}
	_, err = s.RunToolDocument(ctx, u.ID, w.ID, session.ID, run.ID, b.Document.ID, "", "", "")
	wantError(t, err, domain.ErrForbidden)
	page, err = s.RunToolDocuments(ctx, u.ID, w.ID, session.ID, run.ID, "private", "", 0, 10)
	must(t, err)
	if len(page.Documents) != 0 || page.HasMore {
		t.Fatal("filename filter disclosed unselected document")
	}
	must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, "住宿标准", "test"))
	allowed, err := s.CanHydrateRunHistory(ctx, u.ID, w.ID, session.ID, run.ID, []string{b.Document.ID})
	must(t, err)
	if allowed {
		t.Fatal("metadata-only source escaped narrowed history scope")
	}
	next, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "复述", time.Minute)
	must(t, err)
	must(t, s.BindRunHistory(ctx, u.ID, w.ID, session.ID, next.ID, []string{run.ID}))
	must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, next.ID, "仍是住宿标准", "test"))
	allowed, err = s.CanHydrateRunHistory(ctx, u.ID, w.ID, session.ID, next.ID, []string{})
	must(t, err)
	if allowed {
		t.Fatal("paraphrase dropped metadata provenance")
	}
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, a.Document.ID))
	allowed, err = s.CanHydrateRunHistory(ctx, u.ID, w.ID, session.ID, next.ID, nil)
	must(t, err)
	if allowed {
		t.Fatal("deleted metadata survived through inherited history")
	}
}

func TestMySQLKnowledgePinnedVersionAndEvidenceCompatibility(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	meta, err := f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, f.write.Document.ID, "", "", f.job.ProfileID)
	must(t, err)
	if meta.Index == nil || meta.Index.ID != f.job.ID {
		t.Fatal("ready metadata did not pin index")
	}
	_, err = f.s.AppendDocumentVersion(ctx, f.u.ID, f.w.ID, f.write.Document.ID, f.write.Version.ID, "next.txt", []byte("新内容"))
	must(t, err)
	fixed, err := f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, f.write.Document.ID, f.write.Version.ID, f.job.ID, f.job.ProfileID)
	must(t, err)
	if fixed.Version.ID != f.write.Version.ID {
		t.Fatal("fixed read switched head")
	}
	// Existing retrieval acquisition remains current-only.
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrNotFound)
	e, err := f.s.RecordRunDocumentEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	must(t, err)
	if len(e) != 1 || e[0].VersionID != f.write.Version.ID {
		t.Fatal("pinned neighboring evidence unavailable")
	}
	other, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "other")
	must(t, err)
	run, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, other.ID, "read", time.Minute)
	must(t, err)
	_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, other.ID, run.ID, f.write.Document.ID, f.write.Version.ID, f.job.ID, f.job.ProfileID)
	wantError(t, err, domain.ErrConflict)
	_, err = f.s.RecordRunDocumentEvidence(ctx, f.u.ID, f.w.ID, other.ID, run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrNotFound)
}

func TestMySQLKnowledgePaginationBoundsAndPrivateRun(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	_, err := f.s.CreateDocument(ctx, f.u.ID, f.w.ID, "第二个", "two.txt", []byte("二"))
	must(t, err)
	page, err := f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "", "", 0, 1)
	must(t, err)
	if len(page.Documents) != 1 || !page.HasMore || page.NextOffset == nil || *page.NextOffset != 1 {
		t.Fatalf("bad pagination: %+v", page)
	}
	next, err := f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "", "", 1, 1)
	must(t, err)
	if len(next.Documents) != 1 || next.HasMore || next.Documents[0].Document.ID == page.Documents[0].Document.ID {
		t.Fatal("pagination duplicated or missed a row")
	}
	literal, err := f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "%", "", 0, 10)
	must(t, err)
	if len(literal.Documents) != 0 {
		t.Fatal("wildcard treated as literal SQL pattern")
	}
	for _, offset := range []int{-1, 10001} {
		_, err = f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "", "", offset, 1)
		wantError(t, err, domain.ErrInvalid)
	}
	_, err = f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, strings.Repeat("字", 100), "", 0, 1)
	wantError(t, err, domain.ErrInvalid)
	other, _ := testUser(t, f.s, "othermeta")
	_, err = f.s.RunToolDocuments(ctx, other.ID, f.w.ID, f.session.ID, f.run.ID, "", "", 0, 1)
	wantError(t, err, domain.ErrNotFound)
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "完成", "test"))
	_, err = f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "", "", 0, 1)
	wantError(t, err, domain.ErrConflict)
}
