package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

type memoryCaptureLineageFixture struct {
	user      domain.User
	workspace domain.Workspace
	session   domain.Session
	run       domain.Run
	messages  []domain.Message
	sourceID  string
	answerID  string
}

func newMemoryCaptureLineageFixture(t *testing.T, s *Store, name string) memoryCaptureLineageFixture {
	t.Helper()
	ctx := context.Background()
	u, w := testUser(t, s, name)
	ss, run := captureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "unchanged answer", "test"))
	messages, total, err := s.Messages(ctx, u.ID, w.ID, ss.ID, 20, 0)
	must(t, err)
	if total != 2 {
		t.Fatal("missing completed source pair")
	}
	f := memoryCaptureLineageFixture{user: u, workspace: w, session: ss, run: run, messages: messages}
	for _, m := range messages {
		if m.Role == "user" {
			f.sourceID = m.ID
		} else if m.Role == "assistant" {
			f.answerID = m.ID
		}
	}
	return f
}

func requireMemoryCaptureLineageMySQLError(t *testing.T, err error, number uint16) {
	t.Helper()
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != number {
		t.Fatalf("expected MySQL error %d, got %v", number, err)
	}
}

func assertMemoryCaptureLineageFixture(t *testing.T, s *Store, f memoryCaptureLineageFixture) {
	t.Helper()
	ctx := context.Background()
	var wid, uid, sid, source, role, status string
	must(t, s.pool.QueryRowContext(ctx, `SELECT workspace_id,user_id,session_id,source_message_id,source_role,status FROM memory_capture_jobs WHERE run_id=?`, f.run.ID).Scan(&wid, &uid, &sid, &source, &role, &status))
	if wid != f.workspace.ID || uid != f.user.ID || sid != f.session.ID || source != f.sourceID || role != "user" || status != "queued" {
		t.Fatalf("capture source or state changed: workspace=%s user=%s session=%s source=%s role=%s status=%s", wid, uid, sid, source, role, status)
	}
	messages, total, err := s.Messages(ctx, f.user.ID, f.workspace.ID, f.session.ID, 20, 0)
	must(t, err)
	if total != len(f.messages) || !reflect.DeepEqual(messages, f.messages) {
		t.Fatal("capture lineage migration changed source conversation")
	}
	assertCaptureOutboxCount(t, s, f.run.ID, 1)
}

func TestMySQLMemoryCaptureLineageRejectsMismatchedSources(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	a := newMemoryCaptureLineageFixture(t, s, "capture-lineage-a")
	b := newMemoryCaptureLineageFixture(t, s, "capture-lineage-b")
	_, err := s.AddMember(ctx, a.user.ID, a.workspace.ID, b.user.Email, "viewer")
	must(t, err)
	otherSession, err := s.CreateSession(ctx, a.user.ID, a.workspace.ID, "different private session")
	must(t, err)
	// A message from a different run is invalid even inside the same private
	// session, where all Workspace/User checks would otherwise pass.
	otherRun, _, _, err := s.BeginRun(ctx, a.user.ID, a.workspace.ID, a.session.ID, "second user source", time.Minute)
	must(t, err)
	must(t, s.FinishRun(ctx, a.user.ID, a.workspace.ID, a.session.ID, otherRun.ID, "second answer", "test"))
	var otherSource string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id FROM messages WHERE run_id=? AND role='user'`, otherRun.ID).Scan(&otherSource))
	for _, mismatch := range []struct{ name, workspace, user, session, source string }{
		{"workspace", b.workspace.ID, a.user.ID, a.session.ID, a.sourceID},
		{"user", a.workspace.ID, b.user.ID, a.session.ID, a.sourceID},
		{"session", a.workspace.ID, a.user.ID, b.session.ID, a.sourceID},
		{"same-owner-session", a.workspace.ID, a.user.ID, otherSession.ID, a.sourceID},
		{"foreign-message", a.workspace.ID, a.user.ID, a.session.ID, b.sourceID},
		{"same-session-other-run", a.workspace.ID, a.user.ID, a.session.ID, otherSource},
		{"assistant-message", a.workspace.ID, a.user.ID, a.session.ID, a.answerID},
	} {
		t.Run(mismatch.name, func(t *testing.T) {
			_, err := s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET workspace_id=?,user_id=?,session_id=?,source_message_id=? WHERE run_id=?`, mismatch.workspace, mismatch.user, mismatch.session, mismatch.source, a.run.ID)
			requireMemoryCaptureLineageMySQLError(t, err, 1452)
			tx, err := s.pool.BeginTx(ctx, nil)
			must(t, err)
			defer tx.Rollback()
			_, err = tx.ExecContext(ctx, `DELETE FROM memory_capture_jobs WHERE run_id=?`, a.run.ID)
			must(t, err)
			_, err = tx.ExecContext(ctx, `INSERT INTO memory_capture_jobs(run_id,workspace_id,user_id,session_id,source_message_id,epoch) VALUES(?,?,?,?,?,0)`, a.run.ID, mismatch.workspace, mismatch.user, mismatch.session, mismatch.source)
			requireMemoryCaptureLineageMySQLError(t, err, 1452)
			must(t, tx.Rollback())
		})
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET source_role='assistant' WHERE run_id=?`, a.run.ID)
	requireMemoryCaptureLineageMySQLError(t, err, 3105)
	job, claimed, err := s.ClaimMemoryCapture(ctx, a.run.ID, time.Minute)
	must(t, err)
	if !claimed || job.Query != "I prefer concise answers" {
		t.Fatal("failed forged writes changed the authoritative user source")
	}
	must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate(job.Query, "answer style")))
	items, err := s.Memories(ctx, a.user.ID, a.workspace.ID, "active")
	must(t, err)
	if len(items) != 1 || items[0].Content != job.Query || items[0].SourceRunID != a.run.ID || items[0].SourceSessionID != a.session.ID {
		t.Fatal("valid capture lost its source or content")
	}
	// Deleting the source session removes all its pending/done jobs and outbox
	// deliveries. Personal memory and its original audit IDs remain independent.
	must(t, s.DeleteSession(ctx, a.user.ID, a.workspace.ID, a.session.ID))
	for _, rid := range []string{a.run.ID, otherRun.ID} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM memory_capture_jobs WHERE run_id=?`, rid).Scan(&count))
		if count != 0 {
			t.Fatal("source deletion retained a capture job")
		}
		assertCaptureOutboxCount(t, s, rid, 0)
	}
	after, err := s.Memories(ctx, a.user.ID, a.workspace.ID, "active")
	must(t, err)
	if !reflect.DeepEqual(after, items) {
		t.Fatal("source session deletion changed independent personal memory")
	}
	assertMemoryCaptureLineageFixture(t, s, b)
}

func retireMemoryCaptureLineage(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE memory_capture_jobs DROP FOREIGN KEY capture_source_lineage_fk`,
		`ALTER TABLE memory_capture_jobs DROP FOREIGN KEY capture_run_lineage_fk`,
		`ALTER TABLE memory_capture_jobs DROP FOREIGN KEY capture_session_lineage_fk`,
		`ALTER TABLE memory_capture_jobs ADD KEY capture_legacy_source_message(source_message_id)`,
		`ALTER TABLE memory_capture_jobs DROP INDEX capture_source_lineage_fk`,
		`ALTER TABLE memory_capture_jobs DROP COLUMN source_role`,
		`ALTER TABLE sessions DROP INDEX sessions_capture_identity`,
		`ALTER TABLE messages DROP INDEX messages_capture_source`,
		`UPDATE schema_version SET version=26 WHERE singleton=1`,
	} {
		_, err := s.pool.ExecContext(context.Background(), statement)
		must(t, err)
	}
}

func TestMySQLMemoryCaptureLineageMigrationPreservesSourcesAndResumes(t *testing.T) {
	for _, phase := range []string{"before-session-fk", "before-source-fk", "after-ddl"} {
		t.Run(phase, func(t *testing.T) {
			s, dsn := testStore(t)
			ctx := context.Background()
			f := newMemoryCaptureLineageFixture(t, s, "capture-lineage-history")
			retireMemoryCaptureLineage(t, s)
			constraint := "capture_session_lineage_fk"
			if phase == "before-source-fk" {
				constraint = "capture_source_lineage_fk"
			}
			var err error
			if phase == "after-ddl" {
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER capture_lineage_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced capture marker failure'`)
			} else {
				_, err = s.pool.ExecContext(ctx, `CREATE TABLE capture_lineage_migration_blocker (run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT `+constraint+` FOREIGN KEY(run_id) REFERENCES runs(id)) ENGINE=InnoDB`)
			}
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("forced capture migration interruption did not fail")
			}
			var version, columns int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='memory_capture_jobs' AND column_name='source_role' AND extra='STORED GENERATED'`).Scan(&columns))
			if version != 26 || columns != 1 {
				t.Fatalf("wrong partial migration state: version=%d generated columns=%d", version, columns)
			}
			assertMemoryCaptureLineageFixture(t, s, f)
			if phase == "after-ddl" {
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_lineage_marker_failure`)
			} else {
				_, err = s.pool.ExecContext(ctx, `DROP TABLE capture_lineage_migration_blocker`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=26 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			fresh, err := Open(ctx, dsn)
			must(t, err)
			defer fresh.Close()
			must(t, fresh.Migrate(ctx))
			assertMemoryCaptureLineageFixture(t, fresh, f)
			must(t, fresh.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if version != len(migrationFiles) {
				t.Fatal("resumed capture migration did not advance its marker")
			}
			job, claimed, err := fresh.ClaimMemoryCapture(ctx, f.run.ID, time.Minute)
			must(t, err)
			if !claimed || job.Query != "I prefer concise answers" {
				t.Fatal("migrated capture could not claim its original source")
			}
			must(t, fresh.CompleteMemoryCapture(ctx, job, captureCandidate(job.Query, "answer style")))
			captureStatus(t, fresh, job, "done", 1)
		})
	}
}

func TestMySQLMemoryCaptureLineageMigrationRejectsConflictingHistory(t *testing.T) {
	for _, relationship := range []string{"workspace", "user", "session", "same-owner-session", "source-run", "assistant-source"} {
		t.Run(relationship, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			a := newMemoryCaptureLineageFixture(t, s, "capture-lineage-old-a")
			b := newMemoryCaptureLineageFixture(t, s, "capture-lineage-old-b")
			otherSession, err := s.CreateSession(ctx, a.user.ID, a.workspace.ID, "different old private session")
			must(t, err)
			retireMemoryCaptureLineage(t, s)
			var column, bad, original string
			switch relationship {
			case "workspace":
				column, bad, original = "workspace_id", b.workspace.ID, a.workspace.ID
			case "user":
				column, bad, original = "user_id", b.user.ID, a.user.ID
			case "session":
				column, bad, original = "session_id", b.session.ID, a.session.ID
			case "same-owner-session":
				column, bad, original = "session_id", otherSession.ID, a.session.ID
			case "source-run":
				column, bad, original = "source_message_id", b.sourceID, a.sourceID
			case "assistant-source":
				column, bad, original = "source_message_id", a.answerID, a.sourceID
			}
			query := `UPDATE memory_capture_jobs SET ` + column + `=? WHERE run_id=?`
			_, err = s.pool.ExecContext(ctx, query, bad, a.run.ID)
			must(t, err)
			for range 2 {
				err = s.Migrate(ctx)
				requireMemoryCaptureLineageMySQLError(t, err, 1452)
				var version int
				var retained string
				must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
				must(t, s.pool.QueryRowContext(ctx, `SELECT `+column+` FROM memory_capture_jobs WHERE run_id=?`, a.run.ID).Scan(&retained))
				if version != 26 || retained != bad {
					t.Fatal("refused migration advanced or guessed a replacement source")
				}
				messages, total, err := s.Messages(ctx, a.user.ID, a.workspace.ID, a.session.ID, 20, 0)
				must(t, err)
				if total != 2 || !reflect.DeepEqual(messages, a.messages) {
					t.Fatal("refused migration changed source messages")
				}
			}
			_, err = s.pool.ExecContext(ctx, query, original, a.run.ID)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertMemoryCaptureLineageFixture(t, s, a)
			assertMemoryCaptureLineageFixture(t, s, b)
		})
	}
}
