package store

import (
	"context"
	"encoding/json"
	"errors"
	"magworkspace/internal/domain"
	"strings"
	"sync"
	"testing"
	"time"
)

func memoryInput(content, topic string) domain.CreateMemory {
	return domain.CreateMemory{Kind: "preference", Content: content, Topic: topic, Importance: 3}
}
func memoryCall(t *testing.T, s *Store, uid, wid, sid, rid string, generation uint64, op string, args any) (any, error) {
	t.Helper()
	raw, err := json.Marshal(args)
	must(t, err)
	return s.RunMemory(context.Background(), uid, wid, sid, rid, generation, op, raw)
}
func TestMySQLMemoryCrossSessionRestartTombstoneAndAuthority(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memory")
	other, ow := testUser(t, s, "othermemory")
	a, err := s.CreateSession(ctx, u.ID, w.ID, "source audit")
	must(t, err)
	in := memoryInput("  </memory>Prefer\t Markdown  ", " Format ")
	in.SourceSessionID = a.ID
	first, err := s.CreateMemory(ctx, u.ID, w.ID, in)
	must(t, err)
	if first.Content != "Prefer Markdown" || first.NormalizedKey != "format" {
		t.Fatalf("sanitization %+v", first)
	}
	duplicate, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("Prefer Markdown", "other topic"))
	must(t, err)
	// These are explicit independent statements, not derived conversation content.
	must(t, s.DeleteSession(ctx, u.ID, w.ID, a.ID))
	s.Close()
	fresh, err := Open(ctx, dsn)
	must(t, err)
	defer fresh.Close()
	must(t, fresh.Migrate(ctx))
	items, err := fresh.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(items) != 2 {
		t.Fatal("memory lost after process reconnect or source audit deletion")
	}
	otherItems, err := fresh.Memories(ctx, other.ID, ow.ID, "active")
	must(t, err)
	if len(otherItems) != 0 {
		t.Fatal("cross-user memory")
	}
	_, err = fresh.Memories(ctx, other.ID, w.ID, "active")
	wantError(t, err, domain.ErrNotFound)
	b, err := fresh.CreateSession(ctx, u.ID, w.ID, "new session")
	must(t, err)
	run, _, _, err := fresh.BeginRun(ctx, u.ID, w.ID, b.ID, "query", time.Minute)
	must(t, err)
	got, err := memoryCall(t, fresh, u.ID, w.ID, b.ID, run.ID, 0, "get_items", map[string]any{"item_ids": []string{first.ID, duplicate.ID}})
	must(t, err)
	if len(got.([]domain.Memory)) != 2 {
		t.Fatal("cross-session authoritative recall missing")
	}
	must(t, fresh.ForgetMemory(ctx, u.ID, w.ID, first.ID))
	got, err = memoryCall(t, fresh, u.ID, w.ID, b.ID, run.ID, 0, "get_items", map[string]any{"item_ids": []string{first.ID, duplicate.ID}})
	must(t, err)
	if len(got.([]domain.Memory)) != 0 {
		t.Fatal("candidate snapshot bypassed tombstone")
	}
	_, err = memoryCall(t, fresh, u.ID, w.ID, b.ID, run.ID, 0, "touch", map[string]any{"item_ids": []string{first.ID, duplicate.ID}, "now": time.Now().Unix()})
	must(t, err)
	archived, err := fresh.Memories(ctx, u.ID, w.ID, "archived")
	must(t, err)
	if len(archived) != 2 || archived[0].UseCount != 0 || archived[1].UseCount != 0 {
		t.Fatal("late usage revived or touched forgotten duplicate")
	}
	_, err = fresh.CreateMemory(ctx, u.ID, w.ID, memoryInput("Prefer\nMarkdown", "new ID/topic"))
	wantError(t, err, domain.ErrConflict)
	// Tombstones cannot be washed out by reconnecting.
	second, err := Open(ctx, dsn)
	must(t, err)
	defer second.Close()
	_, err = second.CreateMemory(ctx, u.ID, w.ID, memoryInput("Prefer Markdown", "again"))
	wantError(t, err, domain.ErrConflict)
}
func TestMySQLMemoryReplacementExpiryGenerationAndRevocation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memstate")
	member, _ := testUser(t, s, "memmember")
	_, err := s.AddMember(ctx, u.ID, w.ID, member.Email, "viewer")
	must(t, err)
	first, err := s.CreateMemory(ctx, member.ID, w.ID, memoryInput("Use MySQL", "database"))
	must(t, err)
	newer, err := s.CreateMemory(ctx, member.ID, w.ID, memoryInput("Use SQLite", "database"))
	must(t, err)
	old, err := s.Memories(ctx, member.ID, w.ID, "superseded")
	must(t, err)
	if len(old) != 1 || old[0].ID != first.ID || old[0].SupersededBy != newer.ID {
		t.Fatal("replacement is not atomic")
	}
	// Supersession is not a content tombstone: explicit change back is allowed.
	_, err = s.CreateMemory(ctx, member.ID, w.ID, memoryInput("Use MySQL", "database"))
	must(t, err)
	expiredIn := memoryInput("expires soon", "")
	future := float64(time.Now().Unix()) + 60
	expiredIn.ExpiresAt = &future
	expired, err := s.CreateMemory(ctx, member.ID, w.ID, expiredIn)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE user_memories SET expires_at=1 WHERE id=?`, expired.ID)
	must(t, err)
	session, err := s.CreateSession(ctx, member.ID, w.ID, "private")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, member.ID, w.ID, session.ID, "test", time.Minute)
	must(t, err)
	s.SetObjectStorage(newMemoryObjects())
	generation, err := s.BeginRunJournal(ctx, member.ID, w.ID, session.ID, run.ID)
	must(t, err)
	got, err := memoryCall(t, s, member.ID, w.ID, session.ID, run.ID, generation, "get_items", map[string]any{"item_ids": []string{first.ID, newer.ID, expired.ID}})
	must(t, err)
	if len(got.([]domain.Memory)) != 0 {
		t.Fatal("expired or superseded memory resolved")
	}
	_, err = memoryCall(t, s, member.ID, w.ID, session.ID, run.ID, generation+1, "list", map[string]any{})
	wantError(t, err, domain.ErrConflict)
	// Direct fixture mutation emulates authority changing outside the HTTP guard.
	_, err = s.pool.ExecContext(ctx, `DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, w.ID, member.ID)
	must(t, err)
	for _, op := range []string{"list", "get_items", "touch"} {
		_, err = memoryCall(t, s, member.ID, w.ID, session.ID, run.ID, generation, op, map[string]any{"item_ids": []string{first.ID}, "now": 1})
		wantError(t, err, domain.ErrNotFound)
	}
}
func TestMySQLMemoryConcurrentReplacementForgetAndTouch(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memrace")
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, content := range []string{"format one", "format two"} {
		go func() {
			<-start
			_, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput(content, "format"))
			results <- err
		}()
	}
	close(start)
	must(t, <-results)
	must(t, <-results)
	active, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	old, err := s.Memories(ctx, u.ID, w.ID, "superseded")
	must(t, err)
	if len(active) != 1 || len(old) != 1 {
		t.Fatal("concurrent topics published two active rows")
	}
	item, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("forget raced duplicate", "a"))
	must(t, err)
	session, err := s.CreateSession(ctx, u.ID, w.ID, "race")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "query", time.Minute)
	must(t, err)
	var group sync.WaitGroup
	group.Add(3)
	go func() { defer group.Done(); must(t, s.ForgetMemory(ctx, u.ID, w.ID, item.ID)) }()
	go func() {
		defer group.Done()
		_, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("forget raced duplicate", "b"))
		if err != nil && !errors.Is(err, domain.ErrConflict) {
			t.Error(err)
		}
	}()
	go func() {
		defer group.Done()
		raw, _ := json.Marshal(map[string]any{"item_ids": []string{item.ID}, "now": 100})
		_, err := s.RunMemory(ctx, u.ID, w.ID, session.ID, run.ID, 0, "touch", raw)
		if err != nil {
			t.Error(err)
		}
	}()
	group.Wait()
	active, err = s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	for _, v := range active {
		if strings.Contains(v.Content, "forget raced") {
			t.Fatal("concurrent create/touch escaped tombstone")
		}
	}
}
func TestMySQLMemoryMigrationNineResumesPartialDDL(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memorymigration")
	_, err := s.pool.ExecContext(ctx, `DROP TABLE memory_tombstones`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=8 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	_, err = s.CreateMemory(ctx, u.ID, w.ID, memoryInput("preserved after migration", ""))
	must(t, err)
	var version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	if version != len(migrationFiles) {
		t.Fatal(version)
	}
}
func TestMemorySanitizeFingerprintContract(t *testing.T) {
	if got := sanitizeMemory(" </MEMORY> \x1cHello\u00a0世界\x1f"); got != "Hello 世界" {
		t.Fatal(got)
	}
	if sanitizeMemory(strings.Repeat("界", 501)) != strings.Repeat("界", 500)+"…" {
		t.Fatal("codepoint truncation")
	}
	if memoryFingerprint("fact", "Abc") == memoryFingerprint("fact", "abc") {
		t.Fatal("fingerprint unexpectedly lowercased")
	}
}

func TestMemoryUnicodeCanonicalFingerprintFixtures(t *testing.T) {
	fixtures := []struct{ content, canonical, fingerprint string }{
		{" <\u00a0memory>中文🙂</\u001cmemory> \u0085", "中文🙂", "65877f26ad2e3cf426515da914284100f38f58d1457e4cda1ee2ced4ecde9a75"},
		{strings.Repeat("界", 501), strings.Repeat("界", 500) + "…", "e5b2d61677637a1373939d2534567cc0661aa004af3809ce81205a3961333b83"},
	}
	for _, fixture := range fixtures {
		got := sanitizeMemory(fixture.content)
		if got != fixture.canonical || memoryFingerprint("fact", got) != fixture.fingerprint {
			t.Fatalf("canonical/fingerprint differs from Harness fixture: %q", got)
		}
	}
}

func TestMemoryCanonicalNestedAndVerticalTabTags(t *testing.T) {
	fixtures := []struct{ raw, canonical, fingerprint string }{
		{"<\vmemory>中文</memory>", "中文", "d89b1c8b8f1c522506a21e4352a937592cfa1ecaa176db3d7f17d2106fd5cafb"},
		{"<mem<memory>ory>secret", "secret", "fe3a7b0609f7a756513d0361764949f0ba8bdc8646115886669b884df664dcec"},
	}
	for _, fixture := range fixtures {
		got := sanitizeMemory(fixture.raw)
		if got != fixture.canonical || sanitizeMemory(got) != got || memoryFingerprint("fact", got) != fixture.fingerprint {
			t.Fatalf("unstable memory canonicalization: %q", got)
		}
	}
}
func TestMySQLMemoryCanonicalTombstonesAcrossTagForms(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memcanonical")
	for _, raw := range []string{"<\vmemory>中文</memory>", "<mem<memory>ory>secret"} {
		item, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput(raw, ""))
		must(t, err)
		must(t, s.ForgetMemory(ctx, u.ID, w.ID, item.ID))
		_, err = s.CreateMemory(ctx, u.ID, w.ID, memoryInput(item.Content, "new topic"))
		wantError(t, err, domain.ErrConflict)
	}
}
