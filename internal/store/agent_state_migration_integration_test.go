package store

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

// Only this test's fresh database is rewound. Keeping the old column names and
// indexes exercises the same retirement path as an actual v15 installation.
func agentStateLegacySchema(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE run_prompts DROP CHECK run_prompts_snapshot_content`,
		`ALTER TABLE run_prompts DROP COLUMN snapshot, ADD COLUMN snapshot_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, ADD KEY run_prompts_object(snapshot_key)`,
		`ALTER TABLE run_journal_records DROP CHECK run_journal_records_content`,
		`ALTER TABLE run_journal_records DROP COLUMN content, ADD COLUMN object_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, ADD UNIQUE KEY run_journal_records_object(object_key), RENAME COLUMN content_sha256 TO object_sha256`,
		`UPDATE schema_version SET version=15 WHERE singleton=1`,
	} {
		_, err := s.pool.ExecContext(context.Background(), statement)
		must(t, err)
	}
}

func agentStateLegacyPrompt(t *testing.T, s *Store, rid, key string) string {
	t.Helper()
	id, err := newID()
	must(t, err)
	_, err = s.pool.ExecContext(context.Background(), `INSERT INTO run_prompts(id,run_id,tool_call_id,question,options,answer,snapshot_key,snapshot_sha256,snapshot_size,expires_at) VALUES(?,?,'old-call','old question',JSON_ARRAY(),'legacy answer',?,?,2,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 HOUR))`, id, rid, key, strings.Repeat("a", 64))
	must(t, err)
	return id
}

func agentStateLegacyJournal(t *testing.T, s *Store, rid, key string) {
	t.Helper()
	_, err := s.pool.ExecContext(context.Background(), `INSERT INTO run_journal_heads(run_id,seq,digest,total_bytes) VALUES(?,1,?,3)`, rid, strings.Repeat("b", 64))
	must(t, err)
	_, err = s.pool.ExecContext(context.Background(), `INSERT INTO run_journal_records(run_id,seq,digest,object_key,object_sha256,size_bytes) VALUES(?,1,?,?,?,3)`, rid, strings.Repeat("b", 64), key, strings.Repeat("c", 64))
	must(t, err)
}

func agentStateAssertCleanup(t *testing.T, s *Store, keys ...string) {
	t.Helper()
	rows, err := s.pool.QueryContext(context.Background(), `SELECT object_key FROM object_cleanup ORDER BY object_key`)
	must(t, err)
	defer rows.Close()
	got := make(map[string]int)
	for rows.Next() {
		var key string
		must(t, rows.Scan(&key))
		got[key]++
	}
	must(t, rows.Err())
	if len(got) != len(keys) {
		t.Fatalf("retired object cleanup count=%d, want %d", len(got), len(keys))
	}
	for _, key := range keys {
		if got[key] != 1 {
			t.Fatalf("cleanup registrations for %q=%d, want one", key, got[key])
		}
	}
}

func TestMySQLAgentStateMigrationRetiresLegacyAndPreservesNewSQLOnReplay(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	agentStateLegacySchema(t, s)
	u, w := testUser(t, s, "agentmigration")
	newRun := func(title string) (domain.Session, domain.Run) {
		ss, err := s.CreateSession(ctx, u.ID, w.ID, title)
		must(t, err)
		r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, title, time.Hour)
		must(t, err)
		return ss, r
	}
	completeSession, complete := newRun("preserved message")
	must(t, s.FinishRun(ctx, u.ID, w.ID, completeSession.ID, complete.ID, "preserved answer", "demo"))
	beforeMessages, beforeTotal, err := s.Messages(ctx, u.ID, w.ID, completeSession.ID, 20, 0)
	must(t, err)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "preserved document", "source.txt", []byte("文件原文保持不变"))
	must(t, err)
	_, suspended := newRun("old suspended")
	_, running := newRun("old running")
	_, emptyJournal := newRun("empty journal")
	_, unaffected := newRun("no old persistence")
	prompt := agentStateLegacyPrompt(t, s, suspended.ID, "retired/prompt")
	_, err = s.pool.ExecContext(ctx, `INSERT INTO run_document_approvals(prompt_id,run_id,tool_call_id,metadata) VALUES(?,?,'old-call',JSON_OBJECT())`, prompt, suspended.ID)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE runs SET status='suspended' WHERE id=?`, suspended.ID)
	must(t, err)
	agentStateLegacyJournal(t, s, running.ID, "retired/running-record")
	agentStateLegacyPrompt(t, s, complete.ID, "retired/completed-prompt")
	agentStateLegacyJournal(t, s, complete.ID, "retired/completed-record")
	_, err = s.pool.ExecContext(ctx, `INSERT INTO run_journal_heads(run_id) VALUES(?)`, emptyJournal.ID)
	must(t, err)
	var messageCount int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&messageCount))
	must(t, s.Migrate(ctx))
	for _, r := range []domain.Run{suspended, running, emptyJournal} {
		var status, code string
		var finished bool
		must(t, s.pool.QueryRowContext(ctx, `SELECT status,error_code,finished_at IS NOT NULL FROM runs WHERE id=?`, r.ID).Scan(&status, &code, &finished))
		if status != "failed" || code != "persistence_reset" || !finished {
			t.Fatalf("retired active run not fenced: status=%s code=%s finished=%t", status, code, finished)
		}
	}
	for _, want := range []struct{ id, status string }{{complete.ID, "completed"}, {unaffected.ID, "running"}} {
		var status, code string
		must(t, s.pool.QueryRowContext(ctx, `SELECT status,error_code FROM runs WHERE id=?`, want.id).Scan(&status, &code))
		if status != want.status || code != "" {
			t.Fatalf("unaffected run changed: status=%s code=%s", status, code)
		}
	}
	for _, table := range []string{"run_prompts", "run_document_approvals", "run_journal_records", "run_journal_heads"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 0 {
			t.Fatalf("retired %s rows remain: %d", table, count)
		}
	}
	agentStateAssertCleanup(t, s, "retired/prompt", "retired/running-record", "retired/completed-prompt", "retired/completed-record")
	var afterCount, oldColumns, newColumns int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&afterCount))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND ((table_name='run_prompts' AND column_name='snapshot_key') OR (table_name='run_journal_records' AND column_name IN ('object_key','object_sha256')))`).Scan(&oldColumns))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND ((table_name='run_prompts' AND column_name='snapshot') OR (table_name='run_journal_records' AND column_name='content')) AND data_type='mediumblob' AND is_nullable='NO'`).Scan(&newColumns))
	if afterCount != messageCount || oldColumns != 0 || newColumns != 2 {
		t.Fatalf("migration integrity: messages=%d/%d old columns=%d new columns=%d", afterCount, messageCount, oldColumns, newColumns)
	}
	afterMessages, afterTotal, err := s.Messages(ctx, u.ID, w.ID, completeSession.ID, 20, 0)
	must(t, err)
	if afterTotal != beforeTotal || !reflect.DeepEqual(afterMessages, beforeMessages) {
		t.Fatal("migration changed persisted conversation")
	}
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("文件原文保持不变"))

	// New-format rows must survive both a lost v16 marker and a completely
	// missing marker, which replays every historical migration at startup.
	ss, r := newRun("new SQL content")
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	record := journalRecord(1, "", strings.Repeat("d", 64))
	appendJournal(t, s, ctx, u, w, ss, r, gen, record)
	in := promptInputForRun(t, s, u.ID, w.ID, ss.ID, r.ID)
	_, err = s.SuspendRun(WithRunExecution(ctx, r.ID, gen), u.ID, w.ID, ss.ID, r.ID, "demo", in)
	must(t, err)
	for _, reset := range []string{`UPDATE schema_version SET version=15 WHERE singleton=1`, `DELETE FROM schema_version`} {
		_, err = s.pool.ExecContext(ctx, reset)
		must(t, err)
		must(t, s.Migrate(ctx))
		var snapshot, content []byte
		var version int
		var status string
		must(t, s.pool.QueryRowContext(ctx, `SELECT snapshot FROM run_prompts WHERE id=?`, in.ID).Scan(&snapshot))
		must(t, s.pool.QueryRowContext(ctx, `SELECT content FROM run_journal_records WHERE run_id=? AND seq=1`, r.ID).Scan(&content))
		must(t, s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, r.ID).Scan(&status))
		must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
		if !bytes.Equal(snapshot, []byte(in.Snapshot)) || !bytes.Equal(content, append(append([]byte{}, record...), '\n')) || status != "suspended" || version != len(migrationFiles) {
			t.Fatal("replayed migration changed new SQL state")
		}
	}
	resumed, err := s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "国内", time.Hour)
	must(t, err)
	loaded, err := s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, resumed.Generation, "load", nil)
	must(t, err)
	if records := loaded.(map[string]any)["records"].([]json.RawMessage); len(records) != 1 || !bytes.Equal(records[0], record) {
		t.Fatal("replayed schema cannot read new journal")
	}
	if _, err = s.pool.ExecContext(ctx, `UPDATE run_prompts SET snapshot=CONCAT(snapshot,'x') WHERE id=?`, in.ID); err == nil {
		t.Fatal("new snapshot schema accepts inconsistent byte length")
	}
	if _, err = s.pool.ExecContext(ctx, `UPDATE run_journal_records SET content=CONCAT(content,'x') WHERE run_id=?`, r.ID); err == nil {
		t.Fatal("new journal schema accepts inconsistent byte length")
	}
	agentStateAssertCleanup(t, s, "retired/prompt", "retired/running-record", "retired/completed-prompt", "retired/completed-record")
}

func TestMySQLAgentStateMigrationResumesInterruptedDDL(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	agentStateLegacySchema(t, s)
	u, w := testUser(t, s, "agentddlresume")
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "old agent state")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "old query", time.Hour)
	must(t, err)
	agentStateLegacyPrompt(t, s, r.ID, "retired/interrupted-prompt")
	agentStateLegacyJournal(t, s, r.ID, "retired/interrupted-record")
	// The multi-column check prevents MySQL from silently removing the check
	// with snapshot_key, forcing failure after retirement and ADD snapshot.
	_, err = s.pool.ExecContext(ctx, `ALTER TABLE run_prompts ADD CONSTRAINT agent_state_migration_blocker CHECK(OCTET_LENGTH(snapshot_key)>0 AND snapshot_size>0)`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "migration 016") {
		t.Fatalf("expected interrupted v16 DDL, got %v", err)
	}
	var version, snapshotColumns, promptRows, recordRows int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='run_prompts' AND column_name IN ('snapshot','snapshot_key')`).Scan(&snapshotColumns))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_prompts`).Scan(&promptRows))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_journal_records`).Scan(&recordRows))
	if version != 15 || snapshotColumns != 2 || promptRows != 0 || recordRows != 1 {
		t.Fatalf("unexpected partial migration state: version=%d columns=%d prompts=%d records=%d", version, snapshotColumns, promptRows, recordRows)
	}
	agentStateAssertCleanup(t, s, "retired/interrupted-prompt")
	_, err = s.pool.ExecContext(ctx, `ALTER TABLE run_prompts DROP CHECK agent_state_migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_journal_records`).Scan(&recordRows))
	if version != len(migrationFiles) || recordRows != 0 {
		t.Fatal("interrupted migration did not finish")
	}
	agentStateAssertCleanup(t, s, "retired/interrupted-prompt", "retired/interrupted-record")
}
