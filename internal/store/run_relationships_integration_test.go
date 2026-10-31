package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

type runRelationshipFixture struct {
	user      domain.User
	workspace domain.Workspace
	session   domain.Session
	run       domain.Run
	prompt    domain.SuspendedPrompt
}

func newRunRelationshipFixture(t *testing.T, s *Store, name string, approval bool) runRelationshipFixture {
	t.Helper()
	ctx := context.Background()
	u, w := testUser(t, s, name)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, name)
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute)
	must(t, err)
	prompt := promptInputForRun(t, s, u.ID, w.ID, ss.ID, run.ID)
	if approval {
		prompt = writeProposal(t, u.ID, w.ID, ss.ID, run.ID, "create_document", domain.DocumentMutation{Title: "reviewed", Filename: "reviewed.md", Content: "unchanged proposal"}, nil)
	}
	_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, run.ID, "demo", prompt)
	must(t, err)
	return runRelationshipFixture{u, w, ss, run, prompt}
}

func retireRunRelationshipConstraints(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE memory_capture_jobs DROP FOREIGN KEY capture_run_lineage_fk`,
		`ALTER TABLE memory_capture_jobs DROP FOREIGN KEY capture_source_lineage_fk`,
		`ALTER TABLE run_document_approvals DROP FOREIGN KEY document_approval_prompt_run_call_fk`,
		`ALTER TABLE messages DROP FOREIGN KEY messages_run_session_fk`,
		`ALTER TABLE runs DROP INDEX runs_id_session`,
		`ALTER TABLE run_document_approvals DROP FOREIGN KEY document_approval_prompt_run_fk`,
		`ALTER TABLE run_prompts DROP INDEX run_prompts_id_run`,
		`UPDATE schema_version SET version=23 WHERE singleton=1`,
	} {
		_, err := s.pool.ExecContext(context.Background(), statement)
		must(t, err)
	}
}

func requireRunRelationshipFKError(t *testing.T, err error, constraints ...string) {
	t.Helper()
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1451 || mysqlErr.Number == 1452) {
		for _, constraint := range constraints {
			if strings.Contains(mysqlErr.Message, constraint) {
				return
			}
		}
	}
	t.Fatalf("expected foreign-key rejection by one of %v, got %v", constraints, err)
}

func assertRunRelationshipFixture(t *testing.T, s *Store, f runRelationshipFixture, approval bool) {
	t.Helper()
	ctx := context.Background()
	messages, total, err := s.Messages(ctx, f.user.ID, f.workspace.ID, f.session.ID, 20, 0)
	must(t, err)
	if total != 1 || len(messages) != 1 || messages[0].RunID != f.run.ID || messages[0].SessionID != f.session.ID || messages[0].Content != "write" {
		t.Fatalf("message relationship or content changed: %+v", messages)
	}
	prompt, err := s.RunPrompt(ctx, f.user.ID, f.workspace.ID, f.session.ID, f.run.ID, f.prompt.ID)
	must(t, err)
	if prompt.RunID != f.run.ID || prompt.Status != "pending" || (prompt.Approval != nil) != approval {
		t.Fatalf("prompt relationship or state changed: %+v", prompt)
	}
	if approval && string(prompt.Approval.Arguments) != string(f.prompt.Approval.Arguments) {
		t.Fatal("approval arguments changed")
	}
	var snapshot string
	must(t, s.pool.QueryRowContext(ctx, `SELECT snapshot FROM run_prompts WHERE id=?`, f.prompt.ID).Scan(&snapshot))
	if snapshot != f.prompt.Snapshot {
		t.Fatal("private snapshot changed")
	}
}

func TestMySQLRunRelationshipsRejectMismatchesAndCascade(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	a := newRunRelationshipFixture(t, s, "relationships-a", true)
	b := newRunRelationshipFixture(t, s, "relationships-b", false)
	id, err := newID()
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `INSERT INTO messages(id,session_id,run_id,role,content) VALUES(?,?,?,'assistant','foreign')`, id, b.session.ID, a.run.ID)
	requireRunRelationshipFKError(t, err, "messages_run_session_fk")
	_, err = s.pool.ExecContext(ctx, `UPDATE messages SET session_id=? WHERE run_id=?`, b.session.ID, a.run.ID)
	requireRunRelationshipFKError(t, err, "messages_run_session_fk")
	empty, err := s.CreateSession(ctx, a.user.ID, a.workspace.ID, "empty destination")
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE runs SET session_id=? WHERE id=?`, empty.ID, a.run.ID)
	requireRunRelationshipFKError(t, err, "messages_run_session_fk", "capture_run_lineage_fk")
	_, err = s.pool.ExecContext(ctx, `INSERT INTO run_document_approvals(prompt_id,run_id,tool_call_id,metadata) VALUES(?,?,?,JSON_OBJECT())`, b.prompt.ID, a.run.ID, "foreign-approval")
	requireRunRelationshipFKError(t, err, "document_approval_prompt_run_fk", "document_approval_prompt_run_call_fk")
	_, err = s.pool.ExecContext(ctx, `UPDATE run_document_approvals SET run_id=? WHERE prompt_id=?`, b.run.ID, a.prompt.ID)
	requireRunRelationshipFKError(t, err, "document_approval_prompt_run_fk", "document_approval_prompt_run_call_fk")
	// Use a run without an active prompt so the parent relationship check is
	// independent of the newer one-active-prompt constraint.
	otherRun, _, _, err := s.BeginRun(ctx, a.user.ID, a.workspace.ID, empty.ID, "destination", time.Minute)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE run_prompts SET run_id=? WHERE id=?`, otherRun.ID, a.prompt.ID)
	requireRunRelationshipFKError(t, err, "document_approval_prompt_run_fk", "document_approval_prompt_run_call_fk")
	assertRunRelationshipFixture(t, s, a, true)
	assertRunRelationshipFixture(t, s, b, false)
	must(t, s.DeleteSession(ctx, a.user.ID, a.workspace.ID, a.session.ID))
	for _, query := range []string{
		`SELECT COUNT(*) FROM runs WHERE id=?`,
		`SELECT COUNT(*) FROM messages WHERE run_id=?`,
		`SELECT COUNT(*) FROM run_prompts WHERE run_id=?`,
		`SELECT COUNT(*) FROM run_document_approvals WHERE run_id=?`,
	} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, query, a.run.ID).Scan(&count))
		if count != 0 {
			t.Fatalf("deleted run left dependent rows: %s", query)
		}
	}
	assertRunRelationshipFixture(t, s, b, false)
}

func TestMySQLRunRelationshipsMigrationPreservesHistoryAndReenters(t *testing.T) {
	for _, phase := range []string{"before-message-fk", "before-approval-fk", "after-ddl"} {
		t.Run(phase, func(t *testing.T) {
			s, dsn := testStore(t)
			ctx := context.Background()
			retireRunRelationshipConstraints(t, s)
			f := newRunRelationshipFixture(t, s, "relationships-migration", true)
			historySession, err := s.CreateSession(ctx, f.user.ID, f.workspace.ID, "completed history")
			must(t, err)
			historyRun, _, _, err := s.BeginRun(ctx, f.user.ID, f.workspace.ID, historySession.ID, "original query", time.Minute)
			must(t, err)
			must(t, s.FinishRun(ctx, f.user.ID, f.workspace.ID, historySession.ID, historyRun.ID, "original answer", "demo"))
			history, total, err := s.Messages(ctx, f.user.ID, f.workspace.ID, historySession.ID, 20, 0)
			must(t, err)
			if total != 2 {
				t.Fatal("missing completed historical pair")
			}
			constraint := "messages_run_session_fk"
			if phase == "before-approval-fk" {
				constraint = "document_approval_prompt_run_fk"
			}
			if phase == "after-ddl" {
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER relationship_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced marker failure'`)
			} else {
				_, err = s.pool.ExecContext(ctx, `CREATE TABLE relationship_migration_blocker (run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT `+constraint+` FOREIGN KEY(run_id) REFERENCES runs(id)) ENGINE=InnoDB`)
			}
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("interrupted relationship migration succeeded")
			}
			var version int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if version != 23 {
				t.Fatalf("failed migration advanced schema marker: %d", version)
			}
			assertRunRelationshipFixture(t, s, f, true)
			if phase == "after-ddl" {
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER relationship_marker_failure`)
			} else {
				_, err = s.pool.ExecContext(ctx, `DROP TABLE relationship_migration_blocker`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=23 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			reopened, err := Open(ctx, dsn)
			must(t, err)
			defer reopened.Close()
			assertRunRelationshipFixture(t, reopened, f, true)
			after, afterTotal, err := reopened.Messages(ctx, f.user.ID, f.workspace.ID, historySession.ID, 20, 0)
			must(t, err)
			if afterTotal != total || !reflect.DeepEqual(after, history) {
				t.Fatal("migration changed completed message history")
			}
			must(t, reopened.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if version != len(migrationFiles) {
				t.Fatal("resumed migration did not advance schema marker")
			}
		})
	}
}

func TestMySQLRunRelationshipsMigrationRejectsConflictingHistory(t *testing.T) {
	for _, relationship := range []string{"message", "approval"} {
		t.Run(relationship, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			retireRunRelationshipConstraints(t, s)
			a := newRunRelationshipFixture(t, s, "relationships-legacy-a", true)
			b := newRunRelationshipFixture(t, s, "relationships-legacy-b", false)
			query := `UPDATE messages SET session_id=? WHERE run_id=?`
			foreign, owner, target := b.session.ID, a.session.ID, a.run.ID
			check := `SELECT session_id FROM messages WHERE run_id=?`
			constraint := "messages_run_session_fk"
			if relationship == "approval" {
				query = `UPDATE run_document_approvals SET run_id=? WHERE prompt_id=?`
				foreign, owner, target = b.run.ID, a.run.ID, a.prompt.ID
				check = `SELECT run_id FROM run_document_approvals WHERE prompt_id=?`
				constraint = "document_approval_prompt_run_fk"
			}
			_, err := s.pool.ExecContext(ctx, query, foreign, target)
			must(t, err)
			for range 2 {
				err = s.Migrate(ctx)
				requireRunRelationshipFKError(t, err, constraint)
				var version int
				must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
				var retained string
				must(t, s.pool.QueryRowContext(ctx, check, target).Scan(&retained))
				if version != 23 || retained != foreign {
					t.Fatal("rejected migration advanced or rewrote conflicting history")
				}
			}
			// The test owns the original relationship, so it can repair its corrupt
			// fixture explicitly. The migration itself never guesses an owner.
			_, err = s.pool.ExecContext(ctx, query, owner, target)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertRunRelationshipFixture(t, s, a, true)
			assertRunRelationshipFixture(t, s, b, false)
		})
	}
}
