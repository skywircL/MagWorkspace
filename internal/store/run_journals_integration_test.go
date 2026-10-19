package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func journalRecord(seq uint64, previous, digest string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"version":1,"seq":%d,"kind":"created","data":{"value":1.0,"text":"中文"},"prev_hash":%q,"digest":%q}`, seq, previous, digest))
}
func appendJournal(t *testing.T, s *Store, ctx context.Context, u domain.User, w domain.Workspace, ss domain.Session, r domain.Run, gen uint64, record json.RawMessage) {
	t.Helper()
	args := append(append([]byte(`{"record":`), record...), '}')
	got, err := s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, gen, "append", args)
	must(t, err)
	if !bytes.Equal(got.(json.RawMessage), record) {
		t.Fatal("append changed canonical record bytes")
	}
}

func TestRunJournalEnvelopeBoundary(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := journalRecord(1, "", digest)
	if _, err := decodeJournalEnvelope(valid); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"version":1,"version":1,"seq":1,"kind":"x","data":{},"prev_hash":"","digest":"` + digest + `"}`,
		`{"version":1,"seq":1,"kind":"x","data":{"x":1,"x":2},"prev_hash":"","digest":"` + digest + `"}`,
		`{"version":1,"seq":1,"kind":"x","data":null,"prev_hash":"","digest":"` + digest + `"}`,
		`{"version":1,"seq":1,"kind":"x","data":{},"prev_hash":"","digest":"not-a-digest"}`,
	} {
		if _, err := decodeJournalEnvelope([]byte(raw)); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("accepted invalid envelope: %v", err)
		}
	}
}

func TestMySQLRunJournalPersistsExactBytesAndFencesRecovery(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "journalpersist")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "Journal")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Original query", time.Minute, []string{})
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	record := journalRecord(1, "", strings.Repeat("a", 64))
	appendJournal(t, s, ctx, u, w, ss, r, gen, record)
	oldctx := WithRunExecution(ctx, r.ID, gen)
	must(t, s.FailRun(oldctx, u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(objects)
	var winners atomic.Int32
	var recovered domain.RecoveredRun
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := reopened.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
			if e == nil {
				winners.Add(1)
				mu.Lock()
				recovered = out
				mu.Unlock()
			} else if !errors.Is(e, domain.ErrConflict) {
				t.Errorf("unexpected recovery error: %v", e)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 || recovered.Generation != gen+1 || recovered.Query != "Original query" || recovered.DocumentIDs == nil || len(recovered.DocumentIDs) != 0 {
		t.Fatalf("bad recovery claim: winners=%d out=%+v", winners.Load(), recovered)
	}
	if _, err = reopened.RunDocumentScope(oldctx, u.ID, w.ID, ss.ID, r.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("old worker tool read allowed: %v", err)
	}
	if err = reopened.FinishRun(oldctx, u.ID, w.ID, ss.ID, r.ID, "late answer", "demo"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("old worker completed recovery: %v", err)
	}
	if err = reopened.FailRun(oldctx, u.ID, w.ID, ss.ID, r.ID, "agent_failed"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("old worker failed recovery: %v", err)
	}
	if _, err = reopened.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, gen, "load", nil); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale journal capability accepted: %v", err)
	}
	loaded, err := reopened.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, recovered.Generation, "load", nil)
	must(t, err)
	records := loaded.(map[string]any)["records"].([]json.RawMessage)
	if len(records) != 1 || !bytes.Equal(records[0], record) {
		t.Fatalf("canonical record changed: %s", records)
	}
	// A broken object is rejected independently of Python's hash-chain replay.
	objects.mu.Lock()
	for key := range objects.data {
		if strings.Contains(key, "/journal/") {
			objects.data[key] = []byte("corrupt\n")
		}
	}
	objects.mu.Unlock()
	if _, err = reopened.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, recovered.Generation, "load", nil); !errors.Is(err, blob.ErrIntegrity) {
		t.Fatalf("corrupt record accepted: %v", err)
	}
}

func TestMySQLRunJournalCASUploadFailureAndCleanup(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "journalcleanup")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "J")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Q", time.Minute)
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	record := journalRecord(1, "", strings.Repeat("b", 64))
	args := append(append([]byte(`{"record":`), record...), '}')
	objects.putHook = func(context.Context, string) error { return blob.ErrUnavailable }
	if _, err = s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, gen, "append", args); !errors.Is(err, blob.ErrUnavailable) {
		t.Fatalf("upload failure ignored: %v", err)
	}
	objects.putHook = nil
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, gen, "append", args)
			if e == nil {
				successes.Add(1)
			} else if !errors.Is(e, domain.ErrConflict) {
				t.Errorf("unexpected append error: %v", e)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("append CAS winners=%d", successes.Load())
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatalf("live journal removed or orphan retained: %d", objects.count())
	}
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, gen), u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	must(t, s.DeleteSession(ctx, u.ID, w.ID, ss.ID))
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("journal object leaked after session deletion")
	}
}

func TestMySQLRunJournalRejectsRecoveryBoundaries(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "journaldeny")
	s.SetObjectStorage(newMemoryObjects())
	create := func(journal bool) (domain.Session, domain.Run, uint64) {
		ss, e := s.CreateSession(ctx, u.ID, w.ID, "J")
		must(t, e)
		r, _, _, e := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Q", time.Minute)
		must(t, e)
		gen := uint64(0)
		if journal {
			gen, e = s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
			must(t, e)
		}
		return ss, r, gen
	}
	ss, r, _ := create(false)
	must(t, s.FailRun(ctx, u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	_, err := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrConflict)
	ss, r, _ = create(true)
	must(t, s.FailRun(ctx, u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrConflict)
	for _, code := range []string{"agent_canceled", "agent_timeout", "question_canceled", "prompt_expired"} {
		ss, r, gen := create(true)
		appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", strings.Repeat("c", 64)))
		must(t, s.FailRun(ctx, u.ID, w.ID, ss.ID, r.ID, code))
		_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
		wantError(t, err, domain.ErrConflict)
	}
	ss, r, gen := create(true)
	appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", strings.Repeat("d", 64)))
	_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrConflict)
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, r.ID, "done", "demo"))
	_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrConflict)
	ss, r, gen = create(true)
	appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", strings.Repeat("e", 64)))
	must(t, s.FailRun(ctx, u.ID, w.ID, ss.ID, r.ID, "interrupted"))
	_, _, _, err = s.BeginRun(ctx, u.ID, w.ID, ss.ID, "new round", time.Minute)
	must(t, err)
	_, err = s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLRunJournalAnsweredPromptRecoveryPreservesAnswer(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "journalprompt")
	s.SetObjectStorage(newMemoryObjects())
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "J")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Q", time.Minute)
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", strings.Repeat("f", 64)))
	in := promptInput(t)
	_, err = s.SuspendRun(WithRunExecution(ctx, r.ID, gen), u.ID, w.ID, ss.ID, r.ID, "demo", in)
	must(t, err)
	resumed, err := s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "国内", time.Minute)
	must(t, err)
	if resumed.Generation != gen+1 {
		t.Fatalf("prompt did not fence previous worker: %+v", resumed)
	}
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, resumed.Generation), u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	out, err := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
	must(t, err)
	if out.Generation != resumed.Generation+1 {
		t.Fatal("recovery did not advance generation")
	}
	var prompt map[string]any
	must(t, json.Unmarshal(out.Prompt, &prompt))
	if prompt["status"] != "answered" || prompt["resolved_by"] != u.ID || prompt["snapshot"] != in.Snapshot || prompt["response"].(map[string]any)["answer"] != "国内" {
		t.Fatalf("answer changed on recovery: %+v", prompt)
	}
	p, err := s.RunPrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID)
	must(t, err)
	if p.Status != "resuming" || p.Answer != "国内" {
		t.Fatalf("bad durable prompt: %+v", p)
	}
	if _, err = s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "海外", time.Minute); !errors.Is(err, domain.ErrConflict) {
		t.Fatal("original answer replayed")
	}
}

func TestMySQLRunJournalRechecksGenerationAfterObjectIO(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "journalio")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "J")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "Q", time.Minute)
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	first := strings.Repeat("1", 64)
	appendJournal(t, s, ctx, u, w, ss, r, gen, journalRecord(1, "", first))
	var next uint64
	objects.putHook = func(context.Context, string) error {
		must(t, s.FailRun(WithRunExecution(ctx, r.ID, gen), u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
		recovered, e := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
		must(t, e)
		next = recovered.Generation
		return nil
	}
	record := journalRecord(2, first, strings.Repeat("2", 64))
	args := append(append([]byte(`{"record":`), record...), '}')
	_, err = s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, gen, "append", args)
	wantError(t, err, domain.ErrConflict)
	objects.putHook = nil
	loaded, err := s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, next, "load", nil)
	must(t, err)
	if len(loaded.(map[string]any)["records"].([]json.RawMessage)) != 1 {
		t.Fatal("stale Put published after recovery claim")
	}
	var once atomic.Bool
	objects.readHook = func(context.Context, string) error {
		if once.CompareAndSwap(false, true) {
			must(t, s.FailRun(WithRunExecution(ctx, r.ID, next), u.ID, w.ID, ss.ID, r.ID, "agent_failed"))
			_, e := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, r.ID, time.Minute)
			must(t, e)
		}
		return nil
	}
	_, err = s.RunJournal(ctx, u.ID, w.ID, ss.ID, r.ID, next, "load", nil)
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLRunJournalRecoveryReauthorizesAndRejectsDeletedSources(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "journalauthorizer")
	member, _ := testUser(t, s, "journalmember")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	create := func() (domain.Session, domain.Run, uint64) {
		ss, e := s.CreateSession(ctx, member.ID, w.ID, "J")
		must(t, e)
		r, _, _, e := s.BeginRun(ctx, member.ID, w.ID, ss.ID, "Q", time.Minute)
		must(t, e)
		gen, e := s.BeginRunJournal(ctx, member.ID, w.ID, ss.ID, r.ID)
		must(t, e)
		appendJournal(t, s, ctx, member, w, ss, r, gen, journalRecord(1, "", strings.Repeat("3", 64)))
		return ss, r, gen
	}
	ss, r, gen := create()
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, gen), member.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	must(t, s.UpdateMember(ctx, owner.ID, w.ID, member.ID, "viewer"))
	out, err := s.RecoverRun(ctx, member.ID, w.ID, ss.ID, r.ID, time.Minute)
	must(t, err)
	if out.Role != "viewer" {
		t.Fatal("old role survived recovery")
	}
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, out.Generation), member.ID, w.ID, ss.ID, r.ID, "agent_failed"))
	must(t, s.DeleteMember(ctx, owner.ID, w.ID, member.ID))
	if _, err = s.RecoverRun(ctx, member.ID, w.ID, ss.ID, r.ID, time.Minute); err == nil {
		t.Fatal("revoked user recovered run")
	}
	_, err = s.AddMember(ctx, owner.ID, w.ID, member.Email, "viewer")
	must(t, err)
	ss, r, gen = create()
	doc, err := s.CreateDocument(ctx, owner.ID, w.ID, "Source", "source.txt", []byte("fixed original"))
	must(t, err)
	_, err = s.RunToolDocument(WithRunExecution(ctx, r.ID, gen), member.ID, w.ID, ss.ID, r.ID, doc.Document.ID, "", "", "")
	must(t, err)
	must(t, s.FailRun(WithRunExecution(ctx, r.ID, gen), member.ID, w.ID, ss.ID, r.ID, "interrupted"))
	must(t, s.DeleteDocument(ctx, owner.ID, w.ID, doc.Document.ID))
	if _, err = s.RecoverRun(ctx, member.ID, w.ID, ss.ID, r.ID, time.Minute); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("deleted source recovered: %v", err)
	}
}

func TestMySQLRunJournalMigrationReentry(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	// Simulate interruption after either idempotent CREATE TABLE has committed.
	_, err := s.pool.ExecContext(ctx, `UPDATE schema_version SET version=7 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	var version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	if version != len(migrationFiles) {
		t.Fatal("journal migration did not finish")
	}
}
