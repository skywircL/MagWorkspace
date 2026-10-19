package store

import (
	"context"
	"magworkspace/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestMySQLCitationAliasesRequireBoundHistoryAndExactAcquiredPassage(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	old := f.record(t, f.hit)[0]
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "first [E:"+old.ID+"]", "test"))
	firstRun := f.run.ID
	next, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "second", time.Minute)
	must(t, err)
	f.run = next
	resolve := func(ids ...string) (map[string]string, error) {
		return f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, ids)
	}
	_, err = resolve(old.ID)
	wantError(t, err, domain.ErrInvalid)
	current := f.record(t, f.hit)[0]
	_, err = resolve(old.ID)
	wantError(t, err, domain.ErrInvalid) // Acquisition alone is insufficient.
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{firstRun}))
	aliases, err := resolve(old.ID)
	must(t, err)
	if len(aliases) != 1 || aliases[old.ID] != current.ID || old.ID == current.ID {
		t.Fatal("not a current-run alias")
	}
	_, err = resolve(old.ID, strings.Repeat("f", 32))
	wantError(t, err, domain.ErrInvalid)
	_, err = resolve(old.ID, old.ID)
	wantError(t, err, domain.ErrInvalid)
	// The old marker remains invalid at the final commit boundary.
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, "old [E:"+old.ID+"]", "test"), domain.ErrInvalid)
	for _, field := range []string{"sha256", "source_sha256", "generation", "chunk_id", "start_byte", "end_byte"} {
		t.Run(field, func(t *testing.T) {
			var original any
			if field == "start_byte" || field == "end_byte" {
				var v int
				must(t, f.s.pool.QueryRowContext(ctx, "SELECT "+field+" FROM run_evidence WHERE id=?", current.ID).Scan(&v))
				original = v
				_, err = f.s.pool.ExecContext(ctx, "UPDATE run_evidence SET "+field+"=? WHERE id=?", v+1, current.ID)
			} else {
				var v string
				must(t, f.s.pool.QueryRowContext(ctx, "SELECT "+field+" FROM run_evidence WHERE id=?", current.ID).Scan(&v))
				original = v
				_, err = f.s.pool.ExecContext(ctx, "UPDATE run_evidence SET "+field+"=? WHERE id=?", strings.Repeat("a", len(v)), current.ID)
			}
			must(t, err)
			if _, err = resolve(old.ID); err == nil {
				t.Fatal("changed passage accepted")
			}
			_, err = f.s.pool.ExecContext(ctx, "UPDATE run_evidence SET "+field+"=? WHERE id=?", original, current.ID)
			must(t, err)
		})
	}
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
	if _, err = resolve(old.ID); err == nil {
		t.Fatal("deleted source accepted")
	}
}

func TestMySQLCitationAliasesRejectOtherSessionUserAndNarrowedScope(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	old := f.record(t, f.hit)[0]
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "first [E:"+old.ID+"]", "test"))
	other, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "other")
	must(t, err)
	run, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, other.ID, "other", time.Minute)
	must(t, err)
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, other.ID, run.ID, []domain.SearchHit{f.hit})
	must(t, err)
	_, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, other.ID, run.ID, []string{old.ID})
	wantError(t, err, domain.ErrInvalid)
	outsider, _ := testUser(t, f.s, "citation-outsider")
	if _, err = f.s.ResolveRunCitationAliases(ctx, outsider.ID, f.w.ID, other.ID, run.ID, []string{old.ID}); err == nil {
		t.Fatal("foreign user accepted")
	}
	narrow, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "narrow", time.Minute, []string{})
	must(t, err)
	if _, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, narrow.ID, []string{old.ID}); err == nil {
		t.Fatal("out-of-scope history accepted")
	}
}

func TestMySQLCitationAliasesDoNotAcquireHistoryOrAcceptStaleExecution(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	old := f.record(t, f.hit)[0]
	first := f.run.ID
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, first, "first [E:"+old.ID+"]", "test"))
	next, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "next", time.Minute)
	must(t, err)
	f.run = next
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{first}))
	_, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{old.ID})
	wantError(t, err, domain.ErrInvalid)
	current := f.record(t, f.hit)[0]
	// Simulate an old worker from another execution. No mapping may be returned.
	stale := WithRunExecution(ctx, next.ID, 999)
	if aliases, e := f.s.ResolveRunCitationAliases(stale, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{old.ID}); e == nil || aliases != nil {
		t.Fatal("stale execution accepted")
	}
	// A historical marker that was never cited in the completed answer is not an alias.
	_, err = f.s.pool.ExecContext(ctx, `UPDATE run_evidence SET cited=FALSE WHERE id=?`, old.ID)
	must(t, err)
	_, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{old.ID})
	wantError(t, err, domain.ErrInvalid)
	_, err = f.s.pool.ExecContext(ctx, `UPDATE run_evidence SET cited=TRUE WHERE id=?`, old.ID)
	must(t, err)
	aliases, err := f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{old.ID})
	must(t, err)
	if aliases[old.ID] != current.ID {
		t.Fatal("valid alias was lost")
	}
	_, err = f.s.pool.ExecContext(ctx, `UPDATE runs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, next.ID)
	must(t, err)
	_, err = f.s.ResolveRunCitationAliases(ctx, f.u.ID, f.w.ID, f.session.ID, next.ID, []string{old.ID})
	wantError(t, err, domain.ErrConflict)
}
