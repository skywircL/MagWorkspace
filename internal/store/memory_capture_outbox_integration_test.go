package store

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func captureDelivery(t *testing.T, s *Store, runID string) MemoryCaptureDelivery {
	t.Helper()
	var delivery MemoryCaptureDelivery
	delivery.JobID = runID
	must(t, s.pool.QueryRowContext(context.Background(), `SELECT id FROM memory_capture_outbox WHERE run_id=?`, runID).Scan(&delivery.ID))
	return delivery
}

func assertCaptureOutboxCount(t *testing.T, s *Store, runID string, want int) {
	t.Helper()
	var count int
	must(t, s.pool.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?`, runID).Scan(&count))
	if count != want {
		t.Fatalf("memory capture outbox rows=%d, want=%d", count, want)
	}
}

func captureDeliveryDue(t *testing.T, s *Store, runID string) MemoryCaptureDelivery {
	t.Helper()
	_, err := s.pool.ExecContext(context.Background(), `UPDATE memory_capture_jobs SET next_attempt_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`, runID)
	must(t, err)
	_, err = s.pool.ExecContext(context.Background(), `UPDATE memory_capture_outbox SET next_dispatch_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`, runID)
	must(t, err)
	return captureDelivery(t, s, runID)
}

func claimCaptureDelivery(t *testing.T, s *Store, delivery MemoryCaptureDelivery) domain.MemoryCaptureJob {
	t.Helper()
	job, claimed, err := s.ClaimMemoryCaptureDelivery(context.Background(), delivery.JobID, delivery.ID, time.Minute)
	must(t, err)
	if !claimed {
		t.Fatal("expected durable delivery to be claimed")
	}
	return job
}

func TestMySQLMemoryCaptureOutboxAnswerPublicationIsAtomic(t *testing.T) {
	for _, failure := range []string{"outbox_insert", "after_outbox"} {
		t.Run(failure, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "capture-outbox-atomic")
			ss, run := captureRun(t, s, u.ID, w.ID, "I prefer concise answers")
			assertCaptureOutboxCount(t, s, run.ID, 0)
			trigger := `CREATE TRIGGER capture_publication_failure BEFORE INSERT ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced outbox failure'`
			if failure == "after_outbox" {
				trigger = `CREATE TRIGGER capture_publication_failure BEFORE UPDATE ON sessions FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced answer failure'`
			}
			_, err := s.pool.ExecContext(ctx, trigger)
			must(t, err)
			if err = s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"); err == nil {
				t.Fatal("publication failure was ignored")
			}
			var runStatus, jobStatus string
			var assistants int
			must(t, s.pool.QueryRowContext(ctx, `SELECT r.status,j.status FROM runs r JOIN memory_capture_jobs j ON j.run_id=r.id WHERE r.id=?`, run.ID).Scan(&runStatus, &jobStatus))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE run_id=? AND role='assistant'`, run.ID).Scan(&assistants))
			if runStatus != "running" || jobStatus != "waiting" || assistants != 0 {
				t.Fatal("failed publication split answer, run and memory eligibility")
			}
			assertCaptureOutboxCount(t, s, run.ID, 0)
			_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_publication_failure`)
			must(t, err)
			must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
			delivery := captureDelivery(t, s, run.ID)
			if !validHexID(delivery.ID) {
				t.Fatal("missing dispatch identity after successful answer")
			}
			wantError(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "duplicate", "test"), domain.ErrConflict)
			if captureDelivery(t, s, run.ID) != delivery {
				t.Fatal("repeated answer publication rotated the existing token")
			}
		})
	}
}

func TestMySQLMemoryCaptureOutboxDispatchRetryAndStaleTokens(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-outbox-retry")
	ss, run := captureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
	first := captureDelivery(t, s, run.ID)
	must(t, s.MarkMemoryCaptureDeliverySent(ctx, first))
	pending, err := s.PendingMemoryCaptureDeliveries(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("successful dispatch ignored replenishment cooldown")
	}
	assertCaptureOutboxCount(t, s, run.ID, 1)
	captureDeliveryDue(t, s, run.ID)
	pending, err = s.PendingMemoryCaptureDeliveries(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0] != first {
		t.Fatal("lost Redis delivery did not remain reconstructible")
	}
	var wg sync.WaitGroup
	jobs, errs := make(chan domain.MemoryCaptureJob, 2), make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			job, ok, err := s.ClaimMemoryCaptureDelivery(ctx, first.JobID, first.ID, time.Minute)
			if ok {
				jobs <- job
			}
			errs <- err
		})
	}
	wg.Wait()
	close(jobs)
	for range 2 {
		must(t, <-errs)
	}
	if len(jobs) != 1 {
		t.Fatal("duplicate deliveries acquired several live executions")
	}
	job := <-jobs
	// An outbox write error must roll the retry decision back to running.
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER capture_retry_failure BEFORE UPDATE ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced retry failure'`)
	must(t, err)
	if err = s.FailMemoryCapture(ctx, job, "provider_unavailable"); err == nil {
		t.Fatal("retry outbox failure was ignored")
	}
	captureStatus(t, s, job, "running", 0)
	if captureDelivery(t, s, run.ID) != first {
		t.Fatal("failed retry changed the dispatch token")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_retry_failure`)
	must(t, err)
	must(t, s.FailMemoryCapture(ctx, job, "provider_unavailable"))
	second := captureDelivery(t, s, run.ID)
	if second.ID == first.ID {
		t.Fatal("retry reused its previous dispatch token")
	}
	var before, after time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT next_dispatch_at FROM memory_capture_outbox WHERE run_id=?`, run.ID).Scan(&before))
	must(t, s.MarkMemoryCaptureDeliverySent(ctx, first))
	must(t, s.pool.QueryRowContext(ctx, `SELECT next_dispatch_at FROM memory_capture_outbox WHERE run_id=?`, run.ID).Scan(&after))
	if !before.Equal(after) {
		t.Fatal("old acknowledgement postponed a new retry")
	}
	_, ok, err := s.ClaimMemoryCaptureDelivery(ctx, second.JobID, second.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("new token bypassed the job retry deadline")
	}
	captureDeliveryDue(t, s, run.ID)
	_, ok, err = s.ClaimMemoryCaptureDelivery(ctx, first.JobID, first.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("old token claimed a due newer retry")
	}
	secondJob := claimCaptureDelivery(t, s, second)
	if secondJob.Attempt != 2 || secondJob.Generation != job.Generation+1 {
		t.Fatal("retry did not retain the attempt/generation budget")
	}
	must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate(job.Query, "")))
	if captureDelivery(t, s, run.ID) != second {
		t.Fatal("late worker consumed the newer delivery")
	}
	must(t, s.FailMemoryCapture(ctx, secondJob, "provider_unavailable"))
	third := captureDeliveryDue(t, s, run.ID)
	thirdJob := claimCaptureDelivery(t, s, third)
	must(t, s.FailMemoryCapture(ctx, thirdJob, "provider_unavailable"))
	captureStatus(t, s, thirdJob, "failed", 0)
	assertCaptureOutboxCount(t, s, run.ID, 0)
	must(t, s.MarkMemoryCaptureDeliverySent(ctx, third))
	_, ok, err = s.ClaimMemoryCaptureDelivery(ctx, third.JobID, third.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("terminal delivery was revived")
	}
}

func TestMySQLMemoryCaptureOutboxLeaseRecoveryAndCompletionAtomicity(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-outbox-recovery")
	ss, run := captureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
	delivery := captureDelivery(t, s, run.ID)
	old := claimCaptureDelivery(t, s, delivery)
	_, err := s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`, run.ID)
	must(t, err)
	s.Close()
	fresh, err := Open(ctx, dsn)
	must(t, err)
	defer fresh.Close()
	pending, err := fresh.PendingMemoryCaptureDeliveries(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0] != delivery {
		t.Fatal("lost worker lease did not redispatch the persistent token")
	}
	current := claimCaptureDelivery(t, fresh, delivery)
	if current.Generation != old.Generation+1 || current.Attempt != 2 {
		t.Fatal("lease recovery failed to isolate execution generations")
	}
	must(t, fresh.FailMemoryCapture(ctx, old, "provider_unavailable"))
	captureStatus(t, fresh, current, "running", 0)
	_, err = fresh.pool.ExecContext(ctx, `CREATE TRIGGER capture_completion_failure BEFORE DELETE ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced completion failure'`)
	must(t, err)
	if err = fresh.CompleteMemoryCapture(ctx, current, captureCandidate(current.Query, "")); err == nil {
		t.Fatal("completion outbox failure was ignored")
	}
	captureStatus(t, fresh, current, "running", 0)
	items, err := fresh.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(items) != 0 {
		t.Fatal("failed completion committed partial memory content")
	}
	assertCaptureOutboxCount(t, fresh, run.ID, 1)
	_, err = fresh.pool.ExecContext(ctx, `DROP TRIGGER capture_completion_failure`)
	must(t, err)
	must(t, fresh.CompleteMemoryCapture(ctx, current, captureCandidate(current.Query, "")))
	captureStatus(t, fresh, current, "done", 1)
	assertCaptureOutboxCount(t, fresh, run.ID, 0)
	must(t, fresh.CompleteMemoryCapture(ctx, current, captureCandidate(current.Query, "")))
}

func TestMySQLMemoryCaptureOutboxInvalidationAndSourceCascade(t *testing.T) {
	for _, action := range []string{"disable", "forget", "clear", "revoke", "session", "workspace", "opt_out"} {
		t.Run(action, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "capture-outbox-cancel")
			member, _ := testUser(t, s, "capture-outbox-member")
			_, err := s.AddMember(ctx, u.ID, w.ID, member.Email, "viewer")
			must(t, err)
			memory, err := s.CreateMemory(ctx, member.ID, w.ID, memoryInput("Existing preference", ""))
			must(t, err)
			query := "I prefer concise answers"
			if action == "opt_out" {
				query = "don't remember I prefer tea"
			}
			ss, run := captureRun(t, s, member.ID, w.ID, query)
			must(t, s.FinishRun(ctx, member.ID, w.ID, ss.ID, run.ID, "answer", "test"))
			delivery := captureDelivery(t, s, run.ID)
			switch action {
			case "disable":
				no := false
				_, err = s.MemorySettings(ctx, member.ID, w.ID, &no)
				must(t, err)
			case "forget":
				must(t, s.ForgetMemory(ctx, member.ID, w.ID, memory.ID))
			case "clear":
				must(t, s.ClearMemories(ctx, member.ID, w.ID))
			case "revoke":
				must(t, s.DeleteMember(ctx, u.ID, w.ID, member.ID))
			case "session":
				must(t, s.DeleteSession(ctx, member.ID, w.ID, ss.ID))
			case "workspace":
				must(t, s.DeleteWorkspace(ctx, u.ID, w.ID))
			}
			_, claimed, err := s.ClaimMemoryCaptureDelivery(ctx, delivery.JobID, delivery.ID, time.Minute)
			must(t, err)
			if claimed {
				t.Fatal("invalidated source was claimed")
			}
			assertCaptureOutboxCount(t, s, run.ID, 0)
			must(t, s.MarkMemoryCaptureDeliverySent(ctx, delivery))
			assertCaptureOutboxCount(t, s, run.ID, 0)
		})
	}
}

func TestMySQLMemoryCaptureOutboxInvalidationFailureRollsBack(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-outbox-invalidation")
	_, _, job := completeCaptureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	delivery := captureDelivery(t, s, job.ID)
	_, err := s.pool.ExecContext(ctx, `CREATE TRIGGER capture_invalidation_failure BEFORE DELETE ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced invalidation failure'`)
	must(t, err)
	no := false
	if _, err = s.MemorySettings(ctx, u.ID, w.ID, &no); err == nil {
		t.Fatal("invalidation failure was ignored")
	}
	valid, err := s.ValidateMemoryCapture(ctx, job)
	must(t, err)
	if !valid || captureDelivery(t, s, job.ID) != delivery {
		t.Fatal("failed invalidation partially changed settings, epoch or delivery")
	}
	captureStatus(t, s, job, "running", 0)
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_invalidation_failure`)
	must(t, err)
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &no)
	must(t, err)
	captureStatus(t, s, job, "cancelled", 0)
	assertCaptureOutboxCount(t, s, job.ID, 0)
}

func TestMySQLMemoryCaptureOutboxMigrationResumesWithoutReset(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-outbox-migration")
	queuedSession, queued := captureRun(t, s, u.ID, w.ID, "queued preference")
	must(t, s.FinishRun(ctx, u.ID, w.ID, queuedSession.ID, queued.ID, "answer", "test"))
	_, _, running := completeCaptureRun(t, s, u.ID, w.ID, "running preference")
	_, _, done := completeCaptureRun(t, s, u.ID, w.ID, "done preference")
	must(t, s.CompleteMemoryCapture(ctx, done, nil))
	_, waiting := captureRun(t, s, u.ID, w.ID, "waiting preference")
	_, err := s.pool.ExecContext(ctx, `DROP TABLE memory_capture_outbox`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=18 WHERE singleton=1`)
	must(t, err)
	// Persist only CREATE, then fail backfill, matching a partially applied DDL.
	content, err := migrations.ReadFile("migrations/019_memory_capture_outbox.sql")
	must(t, err)
	_, err = s.pool.ExecContext(ctx, strings.Split(string(content), ";")[0])
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER capture_migration_failure BEFORE INSERT ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced backfill failure'`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted outbox backfill")
	}
	var version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	if version != 18 {
		t.Fatal("interrupted backfill advanced the schema version")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_migration_failure`)
	must(t, err)
	must(t, s.Migrate(ctx))
	first := captureDelivery(t, s, queued.ID)
	assertCaptureOutboxCount(t, s, running.ID, 1)
	assertCaptureOutboxCount(t, s, done.ID, 0)
	assertCaptureOutboxCount(t, s, waiting.ID, 0)
	must(t, s.MarkMemoryCaptureDeliverySent(ctx, first))
	var before, after time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT next_dispatch_at FROM memory_capture_outbox WHERE run_id=?`, first.JobID).Scan(&before))
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=18 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	must(t, s.pool.QueryRowContext(ctx, `SELECT next_dispatch_at FROM memory_capture_outbox WHERE run_id=?`, first.JobID).Scan(&after))
	if captureDelivery(t, s, queued.ID) != first || !before.Equal(after) {
		t.Fatal("migration replay reset token or dispatch cooldown")
	}
}
