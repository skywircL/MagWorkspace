package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func promptInput(t *testing.T) domain.SuspendedPrompt {
	t.Helper()
	id, err := newID()
	must(t, err)
	return domain.SuspendedPrompt{ID: id, ToolCallID: "question-call", Question: "国内还是海外差旅？", Options: []string{"国内", "海外"}, Snapshot: `{"private":"snapshot bytes"}`, ExpiresAt: time.Now().Add(time.Hour)}
}

func TestMySQLPromptsPersistResumeOnceAndHistory(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "promptpersist")
	session, err := s.CreateSession(ctx, u.ID, w.ID, "澄清")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "住宿标准", time.Minute, []string{})
	must(t, err)
	in := promptInput(t)
	p, err := s.SuspendRun(ctx, u.ID, w.ID, session.ID, run.ID, "demo", in)
	must(t, err)
	if p.Status != "pending" || p.Question != in.Question {
		t.Fatalf("bad prompt: %+v", p)
	}
	public, _ := json.Marshal(p)
	if strings.Contains(string(public), "snapshot") {
		t.Fatal("snapshot leaked")
	}
	if _, _, _, err = s.BeginRun(ctx, u.ID, w.ID, session.ID, "another", time.Minute); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("suspended session admitted another run: %v", err)
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	p, err = reopened.RunPrompt(ctx, u.ID, w.ID, session.ID, run.ID, in.ID)
	must(t, err)
	if p.Status != "pending" {
		t.Fatal("restart lost pending question")
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := reopened.ResumePrompt(ctx, u.ID, w.ID, session.ID, run.ID, in.ID, "国内", time.Minute)
			if err == nil {
				successes.Add(1)
				if out.DocumentIDs == nil || len(out.DocumentIDs) != 0 {
					t.Error("empty scope widened on resume")
				}
				var native map[string]any
				if json.Unmarshal(out.Prompt, &native) != nil || native["snapshot"] != in.Snapshot || native["user_id"] != u.ID {
					t.Error("bad private restore envelope")
				}
			} else if !errors.Is(err, domain.ErrConflict) {
				t.Errorf("unexpected claim error: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("claims: %d", successes.Load())
	}
	must(t, reopened.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, "国内标准为准。", "demo"))
	query, err := reopened.ClarifiedQuery(ctx, u.ID, w.ID, session.ID, run.ID, "住宿标准")
	must(t, err)
	if !strings.Contains(query, "用户答复：国内") {
		t.Fatal("clarification omitted from history")
	}
	if _, err = reopened.ResumePrompt(ctx, u.ID, w.ID, session.ID, run.ID, in.ID, "海外", time.Minute); !errors.Is(err, domain.ErrConflict) {
		t.Fatal("replayed answer")
	}
}

func TestMySQLPromptsCancelExpireRevokeAndIntegrity(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "promptowner")
	other, _ := testUser(t, s, "promptother")
	_, err := s.AddMember(ctx, owner.ID, w.ID, other.Email, "viewer")
	must(t, err)
	create := func(uid string) (domain.Session, domain.Run, domain.SuspendedPrompt) {
		ss, e := s.CreateSession(ctx, uid, w.ID, "Q")
		must(t, e)
		r, _, _, e := s.BeginRun(ctx, uid, w.ID, ss.ID, "question", time.Minute)
		must(t, e)
		in := promptInput(t)
		_, e = s.SuspendRun(ctx, uid, w.ID, ss.ID, r.ID, "demo", in)
		must(t, e)
		return ss, r, in
	}
	ss, r, in := create(owner.ID)
	if _, err = s.RunPrompt(ctx, other.ID, w.ID, ss.ID, r.ID, in.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("foreign prompt visible")
	}
	must(t, s.CancelPrompt(ctx, owner.ID, w.ID, ss.ID, r.ID, in.ID))
	if _, err = s.ResumePrompt(ctx, owner.ID, w.ID, ss.ID, r.ID, in.ID, "x", time.Minute); !errors.Is(err, domain.ErrConflict) {
		t.Fatal("canceled prompt resumed")
	}
	ss, r, in = create(owner.ID)
	_, err = s.pool.ExecContext(ctx, `UPDATE runs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, r.ID)
	must(t, err)
	p, err := s.RunPrompt(ctx, owner.ID, w.ID, ss.ID, r.ID, in.ID)
	must(t, err)
	if p.Status != "expired" {
		t.Fatalf("not expired: %+v", p)
	}
	if _, _, _, err = s.BeginRun(ctx, owner.ID, w.ID, ss.ID, "next", time.Minute); err != nil {
		t.Fatalf("expired run held session: %v", err)
	}
	ss, r, in = create(other.ID)
	must(t, s.DeleteMember(ctx, owner.ID, w.ID, other.ID))
	if _, err = s.ResumePrompt(ctx, other.ID, w.ID, ss.ID, r.ID, in.ID, "x", time.Minute); err == nil {
		t.Fatal("revoked user resumed")
	}
	ss, r, in = create(owner.ID)
	_, err = s.pool.ExecContext(ctx, `UPDATE run_prompts SET snapshot_sha256=REPEAT('0',64) WHERE id=?`, in.ID)
	must(t, err)
	if _, err = s.ResumePrompt(ctx, owner.ID, w.ID, ss.ID, r.ID, in.ID, "x", time.Minute); !errors.Is(err, blob.ErrIntegrity) {
		t.Fatalf("corrupt snapshot accepted: %v", err)
	}
	got, err := s.Run(ctx, owner.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	if got.Status != "suspended" {
		t.Fatal("failed snapshot read consumed prompt")
	}
}

func TestMySQLPromptsAtomicDatabaseStorageAndCascade(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "promptatomic")
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "Q")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Q", time.Minute)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER reject_suspend BEFORE UPDATE ON runs FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced failure'`)
	must(t, err)
	in := promptInput(t)
	if _, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r.ID, "demo", in); err == nil {
		t.Fatal("database failure ignored")
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_prompts WHERE run_id=?`, r.ID).Scan(&count))
	if count != 0 {
		t.Fatal("snapshot committed without suspended run")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER reject_suspend`)
	must(t, err)
	_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r.ID, "demo", in)
	must(t, err)
	var raw string
	must(t, s.pool.QueryRowContext(ctx, `SELECT snapshot FROM run_prompts WHERE id=?`, in.ID).Scan(&raw))
	if raw != in.Snapshot {
		t.Fatal("database did not preserve snapshot bytes")
	}
	assertObjectQueueCount(t, s, 0)
	must(t, s.DeleteSession(ctx, u.ID, w.ID, ss.ID))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_prompts WHERE id=?`, in.ID).Scan(&count))
	if count != 0 {
		t.Fatal("deleted session snapshot leaked")
	}
}

func TestMySQLPromptsMigrationReentry(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	// The first ALTER is independently committed before a simulated restart.
	_, err := s.pool.ExecContext(ctx, `UPDATE schema_version SET version=6 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	var v int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&v))
	if v != len(migrationFiles) {
		t.Fatal("migration did not advance")
	}
}
