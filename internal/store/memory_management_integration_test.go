package store

import (
	"context"
	"errors"
	"magworkspace/internal/domain"
	"testing"
	"time"
)

func TestMySQLMemorySettingsUpdateCASClearAndBatchAuthority(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "manage-memory")
	source, err := s.CreateSession(ctx, u.ID, w.ID, "source")
	must(t, err)
	input := memoryInput("数据库使用 MySQL", "")
	input.SourceSessionID = source.ID
	old, err := s.CreateMemory(ctx, u.ID, w.ID, input)
	must(t, err)
	must(t, s.DeleteSession(ctx, u.ID, w.ID, source.ID))
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, text := range []string{"数据库使用 MariaDB", "数据库使用 PostgreSQL"} {
		go func(text string) {
			<-start
			_, err := s.UpdateMemory(ctx, u.ID, w.ID, old.ID, memoryInput(text, ""))
			results <- err
		}(text)
	}
	close(start)
	one, two := <-results, <-results
	if !((one == nil && errors.Is(two, domain.ErrConflict)) || (two == nil && errors.Is(one, domain.ErrConflict))) {
		t.Fatalf("CAS: %v %v", one, two)
	}
	active, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(active) != 1 || active[0].SourceSessionID != source.ID {
		t.Fatal("replacement lost provenance or published twice")
	}
	newer := active[0]

	past := float64(time.Now().Unix()) + 100
	expiredInput := memoryInput("expired independent note", "")
	expiredInput.ExpiresAt = &past
	expired, err := s.CreateMemory(ctx, u.ID, w.ID, expiredInput)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE user_memories SET expires_at=1 WHERE id=?`, expired.ID)
	must(t, err)
	all, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(all) != 3 {
		t.Fatal("management omitted expired history")
	}
	sess, err := s.CreateSession(ctx, u.ID, w.ID, "reader")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, sess.ID, "query", time.Minute)
	must(t, err)
	ids := []string{old.ID, newer.ID, newer.ID}
	got, err := memoryCall(t, s, u.ID, w.ID, sess.ID, run.ID, 0, "get_items", map[string]any{"item_ids": ids})
	must(t, err)
	if len(got.([]domain.Memory)) != 1 {
		t.Fatal("stale or duplicate ID returned")
	}
	_, err = memoryCall(t, s, u.ID, w.ID, sess.ID, run.ID, 0, "touch", map[string]any{"item_ids": ids, "now": 1})
	must(t, err)
	disabled := false
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &disabled)
	must(t, err)
	for _, op := range []string{"list", "get_items", "touch"} {
		got, err = memoryCall(t, s, u.ID, w.ID, sess.ID, run.ID, 0, op, map[string]any{"item_ids": ids, "now": 1})
		must(t, err)
		if op != "touch" && len(got.([]domain.Memory)) != 0 {
			t.Fatal("disabled memory leaked")
		}
	}
	exported, err := s.ExportMemories(ctx, u.ID, w.ID)
	must(t, err)
	if len(exported) != 3 {
		t.Fatal("export lost history")
	}
	for _, v := range exported {
		if v.ID == newer.ID && v.UseCount != 1 {
			t.Fatal("duplicate IDs or disabled touch changed counters")
		}
	}
	fresh, err := Open(ctx, dsn)
	must(t, err)
	defer fresh.Close()
	setting, err := fresh.MemorySettings(ctx, u.ID, w.ID, nil)
	must(t, err)
	if setting.Enabled {
		t.Fatal("setting did not persist")
	}
	must(t, s.ClearMemories(ctx, u.ID, w.ID))
	must(t, s.ClearMemories(ctx, u.ID, w.ID))
	enabled := true
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &enabled)
	must(t, err)
	got, err = memoryCall(t, s, u.ID, w.ID, sess.ID, run.ID, 0, "get_items", map[string]any{"item_ids": ids})
	must(t, err)
	if len(got.([]domain.Memory)) != 0 {
		t.Fatal("clear revived cached candidate")
	}
	for _, text := range []string{old.Content, newer.Content} {
		_, err = s.CreateMemory(ctx, u.ID, w.ID, memoryInput(text, "new topic"))
		if !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("clear tombstone bypass: %v", err)
		}
	}
}

func TestMySQLMemorySettingsMigrationTwelveResumes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memory-setting-migration")
	item, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("keep legacy memory", ""))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE user_memory_settings`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=11`)
	must(t, err)
	must(t, s.Migrate(ctx))
	no := false
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &no)
	must(t, err)
	// DDL succeeded but schema marker did not: retry must preserve the saved switch.
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=11`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	setting, err := s.MemorySettings(ctx, u.ID, w.ID, nil)
	must(t, err)
	if setting.Enabled {
		t.Fatal("migration reset existing preference")
	}
	all, err := s.ExportMemories(ctx, u.ID, w.ID)
	must(t, err)
	if len(all) != 1 || all[0].ID != item.ID {
		t.Fatal("migration changed memory")
	}
}

func TestMySQLMemoryReplacementFailurePreservesCurrent(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memory-atomic-edit")
	old, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("old content", "topic"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER fail_memory_insert BEFORE INSERT ON user_memories FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='controlled insert failure'`)
	must(t, err)
	_, err = s.UpdateMemory(ctx, u.ID, w.ID, old.ID, memoryInput("new content", "topic"))
	if err == nil {
		t.Fatal("expected injected failure")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER fail_memory_insert`)
	must(t, err)
	active, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(active) != 1 || active[0].ID != old.ID || active[0].SupersededBy != "" {
		t.Fatal("failed edit changed current memory")
	}
}
