package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

func cloneIntegrityPrompt(t *testing.T, s *Store, source, id, status string) error {
	t.Helper()
	_, err := s.pool.Exec(`INSERT INTO run_prompts(id,run_id,tool_call_id,question,options,status,answer,snapshot,snapshot_sha256,snapshot_size,expires_at) SELECT ?,run_id,CONCAT(tool_call_id,'-history'),question,options,?,answer,snapshot,snapshot_sha256,snapshot_size,expires_at FROM run_prompts WHERE id=?`, id, status, source)
	return err
}

func requireActivePromptConflict(t *testing.T, err error) {
	t.Helper()
	var dbErr *mysql.MySQLError
	if !errors.As(err, &dbErr) || dbErr.Number != 1062 || !strings.Contains(dbErr.Message, "run_prompts_one_active_run") {
		t.Fatalf("expected active prompt uniqueness rejection: %v", err)
	}
}

func retirePromptIntegrity(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE run_document_approvals DROP FOREIGN KEY document_approval_prompt_run_call_fk`,
		`ALTER TABLE run_prompts DROP INDEX run_prompts_id_run_call`,
		`ALTER TABLE run_prompts DROP INDEX run_prompts_one_active_run`,
		`ALTER TABLE run_prompts DROP COLUMN active_marker`,
		`ALTER TABLE run_prompts MODIFY COLUMN tool_call_id varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL`,
		`ALTER TABLE run_document_approvals MODIFY COLUMN tool_call_id varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL`,
		`UPDATE schema_version SET version=25 WHERE singleton=1`,
	} {
		_, err := s.pool.Exec(statement)
		must(t, err)
	}
}

func TestMySQLPromptIntegritySingleActiveAndApprovalBinding(t *testing.T) {
	s, _ := testStore(t)
	f := newRunRelationshipFixture(t, s, "prompt-integrity", true)
	other := newRunRelationshipFixture(t, s, "prompt-integrity-other", false)
	id, err := newID()
	must(t, err)
	for _, status := range []string{"pending", "resuming"} {
		requireActivePromptConflict(t, cloneIntegrityPrompt(t, s, f.prompt.ID, id, status))
	}
	must(t, cloneIntegrityPrompt(t, s, f.prompt.ID, id, "done"))
	for _, status := range []string{"pending", "resuming"} {
		_, err = s.pool.Exec(`UPDATE run_prompts SET status=? WHERE id=?`, status, id)
		requireActivePromptConflict(t, err)
	}
	_, err = s.pool.Exec(`INSERT INTO run_document_approvals(prompt_id,run_id,tool_call_id,metadata) VALUES(?,?,?,JSON_OBJECT())`, other.prompt.ID, other.run.ID, "wrong-call")
	requireRunRelationshipFKError(t, err, "document_approval_prompt_run_call_fk")
	for _, call := range []string{"wrong-call", strings.ToUpper(f.prompt.ToolCallID), f.prompt.ToolCallID + " "} {
		_, err = s.pool.Exec(`UPDATE run_document_approvals SET tool_call_id=? WHERE prompt_id=?`, call, f.prompt.ID)
		requireRunRelationshipFKError(t, err, "document_approval_prompt_run_call_fk")
	}
	_, err = s.pool.Exec(`UPDATE run_prompts SET tool_call_id='changed' WHERE id=?`, f.prompt.ID)
	requireRunRelationshipFKError(t, err, "document_approval_prompt_run_call_fk")
	for _, table := range []string{"run_prompts", "run_document_approvals"} {
		var collation string
		must(t, s.pool.QueryRow(`SELECT collation_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name='tool_call_id'`, table).Scan(&collation))
		if collation != "utf8mb4_0900_bin" {
			t.Fatalf("%s tool identifiers are not binary: %s", table, collation)
		}
	}
	assertRunRelationshipFixture(t, s, f, true)
	must(t, s.DeleteSession(context.Background(), f.user.ID, f.workspace.ID, f.session.ID))
	for _, table := range []string{"run_prompts", "run_document_approvals"} {
		var count int
		must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE run_id=?`, f.run.ID).Scan(&count))
		if count != 0 {
			t.Fatalf("cascade retained %s", table)
		}
	}
	assertRunRelationshipFixture(t, s, other, false)
}

func TestMySQLPromptIntegrityMigrationRejectsHistoryAndReenters(t *testing.T) {
	for _, phase := range []string{"duplicate-active", "mismatched-call", "padded-call", "after-ddl"} {
		t.Run(phase, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			retirePromptIntegrity(t, s)
			f := newRunRelationshipFixture(t, s, "prompt-integrity-migration", true)
			historyID, err := newID()
			must(t, err)
			must(t, cloneIntegrityPrompt(t, s, f.prompt.ID, historyID, "done"))
			badID, err := newID()
			must(t, err)
			mismatchedCall := strings.ToUpper(f.prompt.ToolCallID)
			if phase == "padded-call" {
				mismatchedCall = f.prompt.ToolCallID + " "
			}
			switch phase {
			case "duplicate-active":
				must(t, cloneIntegrityPrompt(t, s, f.prompt.ID, badID, "pending"))
			case "mismatched-call", "padded-call":
				_, err = s.pool.Exec(`UPDATE run_document_approvals SET tool_call_id=? WHERE prompt_id=?`, mismatchedCall, f.prompt.ID)
				must(t, err)
			case "after-ddl":
				_, err = s.pool.Exec(`CREATE TRIGGER prompt_integrity_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced marker failure'`)
				must(t, err)
			}
			for range 2 {
				if err = s.Migrate(ctx); err == nil {
					t.Fatal("incomplete migration succeeded")
				}
				var version int
				must(t, s.pool.QueryRow(`SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
				if version != 25 {
					t.Fatalf("failed migration advanced marker: %d", version)
				}
			}
			// Repair only this deliberately corrupt test fixture. Migration itself
			// must never choose an answer or rewrite an approval's tool identity.
			switch phase {
			case "duplicate-active":
				var status string
				must(t, s.pool.QueryRow(`SELECT status FROM run_prompts WHERE id=?`, badID).Scan(&status))
				if status != "pending" {
					t.Fatal("migration silently retired conflicting prompt")
				}
				_, err = s.pool.Exec(`DELETE FROM run_prompts WHERE id=?`, badID)
			case "mismatched-call", "padded-call":
				var call string
				must(t, s.pool.QueryRow(`SELECT tool_call_id FROM run_document_approvals WHERE prompt_id=?`, f.prompt.ID).Scan(&call))
				if call != mismatchedCall {
					t.Fatal("migration silently rewrote approval")
				}
				_, err = s.pool.Exec(`UPDATE run_document_approvals SET tool_call_id=? WHERE prompt_id=?`, f.prompt.ToolCallID, f.prompt.ID)
			case "after-ddl":
				_, err = s.pool.Exec(`DROP TRIGGER prompt_integrity_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			_, err = s.pool.Exec(`UPDATE schema_version SET version=25 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertRunRelationshipFixture(t, s, f, true)
			var status, snapshot string
			must(t, s.pool.QueryRow(`SELECT status,snapshot FROM run_prompts WHERE id=?`, historyID).Scan(&status, &snapshot))
			if status != "done" || snapshot != f.prompt.Snapshot {
				t.Fatal("migration changed historical prompt")
			}
		})
	}
}

func replaceIntegritySnapshot(t *testing.T, s *Store, pid string, raw []byte) {
	t.Helper()
	digest := sha256.Sum256(raw)
	_, err := s.pool.Exec(`UPDATE run_prompts SET snapshot=?,snapshot_size=?,snapshot_sha256=? WHERE id=?`, raw, len(raw), hex.EncodeToString(digest[:]), pid)
	must(t, err)
}

func assertPromptClaimUnchanged(t *testing.T, s *Store, rid, pid, runState, promptState, answer string, generation uint64) {
	t.Helper()
	var gotRun, gotPrompt, gotAnswer string
	var gotGeneration uint64
	must(t, s.pool.QueryRow(`SELECT r.status,p.status,p.answer,h.execution_generation FROM runs r JOIN run_prompts p ON p.run_id=r.id JOIN run_journal_heads h ON h.run_id=r.id WHERE r.id=? AND p.id=?`, rid, pid).Scan(&gotRun, &gotPrompt, &gotAnswer, &gotGeneration))
	if gotRun != runState || gotPrompt != promptState || gotAnswer != answer || gotGeneration != generation {
		t.Fatalf("invalid snapshot consumed claim: run=%s prompt=%s answer=%q generation=%d", gotRun, gotPrompt, gotAnswer, gotGeneration)
	}
}

func TestMySQLPromptSnapshotValidatesBeforeSuspendResumeAndRecover(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "prompt-snapshot")
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "snapshot")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "original query", time.Minute, []string{})
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", strings.Repeat("a", 64)))
	in := promptInput(t)
	snapshot := nativePromptSnapshot(t, u.ID, w.ID, ss.ID, r.ID, "original query", []string{}, in)
	snapshot["version"] = 3
	in.Snapshot = string(encodePromptSnapshot(t, snapshot))
	_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r.ID, "live", in)
	wantError(t, err, domain.ErrInvalid)
	var state string
	var count int
	must(t, s.pool.QueryRow(`SELECT status FROM runs WHERE id=?`, r.ID).Scan(&state))
	must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM run_prompts WHERE run_id=?`, r.ID).Scan(&count))
	if state != "running" || count != 0 {
		t.Fatal("invalid snapshot partially suspended run")
	}
	snapshot["version"] = 2
	validRaw := encodePromptSnapshot(t, snapshot)
	in.Snapshot = string(validRaw)
	_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r.ID, "live", in)
	must(t, err)
	for _, bad := range []string{
		strings.Replace(in.Snapshot, `"version":2`, `"version":3`, 1),
		strings.Replace(in.Snapshot, `"version":2`, `"version":1`, 1),
		strings.Replace(in.Snapshot, `"version":2`, `"version":2,"version":2`, 1),
		strings.Replace(in.Snapshot, `,"version":2`, ``, 1),
		strings.Replace(in.Snapshot, `"query":"original query"`, `"query":"changed"`, 1),
	} {
		replaceIntegritySnapshot(t, s, in.ID, []byte(bad))
		_, err = s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "国内", time.Minute)
		wantError(t, err, domain.ErrInvalid)
		assertPromptClaimUnchanged(t, s, r.ID, in.ID, "suspended", "pending", "", gen)
	}
	replaceIntegritySnapshot(t, s, in.ID, validRaw)
	resumed, err := s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "国内", time.Minute)
	must(t, err)
	if resumed.Generation != gen+1 {
		t.Fatal("current snapshot did not claim exactly one generation")
	}
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, resumed.Generation), u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	snapshot["version"] = 3
	replaceIntegritySnapshot(t, s, in.ID, encodePromptSnapshot(t, snapshot))
	_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrInvalid)
	assertPromptClaimUnchanged(t, s, r.ID, in.ID, "failed", "failed", "国内", resumed.Generation)
	snapshot["version"] = 2
	replaceIntegritySnapshot(t, s, in.ID, encodePromptSnapshot(t, snapshot))
	recovered, err := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	must(t, err)
	if recovered.Generation != resumed.Generation+1 || recovered.Query != "original query" || len(recovered.Prompt) == 0 {
		t.Fatal("valid version 2 recovery lost original prompt")
	}
}
