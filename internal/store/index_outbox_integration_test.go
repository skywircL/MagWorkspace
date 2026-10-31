package store

import (
	"context"
	"reflect"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func outboxDocument(t *testing.T, s *Store, name string) (domain.User, domain.Workspace, domain.DocumentWrite, IndexDelivery) {
	t.Helper()
	ctx := context.Background()
	u, w := testUser(t, s, name)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, name, "source.txt", []byte("immutable source"))
	must(t, err)
	job, queued, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
	must(t, err)
	if !queued {
		t.Fatal("new index was not admitted")
	}
	var delivery IndexDelivery
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,job_id,? FROM index_outbox WHERE job_id=?`, job.ProfileID, job.ID).Scan(&delivery.ID, &delivery.JobID, &delivery.ProfileID))
	return u, w, write, delivery
}

func claimDelivery(t *testing.T, s *Store, delivery IndexDelivery) *domain.IndexInput {
	t.Helper()
	input, err := s.ClaimIndexDelivery(context.Background(), delivery.ProfileID, delivery.JobID, delivery.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("due delivery was not claimed")
	}
	return input
}

func makeDeliveryDue(t *testing.T, s *Store, jobID string) IndexDelivery {
	t.Helper()
	ctx := context.Background()
	_, err := s.pool.ExecContext(ctx, `UPDATE index_outbox SET next_attempt_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND),next_dispatch_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE job_id=?`, jobID)
	must(t, err)
	var delivery IndexDelivery
	must(t, s.pool.QueryRowContext(ctx, `SELECT o.id,o.job_id,i.profile_id FROM index_outbox o JOIN document_indexes i ON i.id=o.job_id WHERE o.job_id=?`, jobID).Scan(&delivery.ID, &delivery.JobID, &delivery.ProfileID))
	return delivery
}

func TestMySQLIndexOutboxAdmissionRollback(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	auto, err := Open(ctx, dsn, testIndexProfile())
	must(t, err)
	defer auto.Close()
	u, w := testUser(t, s, "outboxrollback")
	first, err := auto.CreateDocument(ctx, u.ID, w.ID, "Initial", "initial.txt", []byte("initial"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_outbox_failure BEFORE INSERT ON index_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced outbox failure'`)
	must(t, err)
	if _, err = auto.CreateDocument(ctx, u.ID, w.ID, "Rejected", "new.txt", []byte("new")); err == nil {
		t.Fatal("outbox failure did not roll back new document")
	}
	if _, err = auto.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "updated.txt", []byte("updated")); err == nil {
		t.Fatal("outbox failure did not roll back version append")
	}
	for _, table := range []string{"documents", "document_versions", "document_indexes", "index_outbox"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 1 {
			t.Fatalf("partial %s records after rollback: %d", table, count)
		}
	}
	current, err := s.Document(ctx, u.ID, w.ID, first.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, first.Document) {
		t.Fatal("outbox failure changed document head")
	}
}

func TestMySQLIndexOutboxReplayAndReconnect(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	_, _, _, delivery := outboxDocument(t, s, "outboxreplay")
	for range 2 {
		pending, err := s.PendingIndexDeliveries(ctx, delivery.ProfileID, 20)
		must(t, err)
		if !reflect.DeepEqual(pending, []IndexDelivery{delivery}) {
			t.Fatalf("unacknowledged dispatch was lost: %+v", pending)
		}
	}
	must(t, s.MarkIndexDeliverySent(ctx, delivery))
	pending, err := s.PendingIndexDeliveries(ctx, delivery.ProfileID, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("successful dispatch was not throttled")
	}
	// No Redis state is needed to recover a sent-but-lost delivery.
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	makeDeliveryDue(t, reopened, delivery.JobID)
	pending, err = reopened.PendingIndexDeliveries(ctx, delivery.ProfileID, 20)
	must(t, err)
	if !reflect.DeepEqual(pending, []IndexDelivery{delivery}) {
		t.Fatal("durable intent did not survive dispatcher restart")
	}
	type claim struct {
		input *domain.IndexInput
		err   error
	}
	results := make(chan claim, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			input, err := reopened.ClaimIndexDelivery(ctx, delivery.ProfileID, delivery.JobID, delivery.ID, time.Minute)
			results <- claim{input, err}
		}()
	}
	close(start)
	var claimed *domain.IndexInput
	for range 2 {
		result := <-results
		must(t, result.err)
		if result.input != nil {
			if claimed != nil {
				t.Fatal("duplicate delivery executed concurrently")
			}
			claimed = result.input
		}
	}
	if claimed == nil || claimed.Job.Attempts != 1 {
		t.Fatal("delivery was not claimed exactly once under its live lease")
	}
	must(t, finishTestIndex(reopened, claimed.Job))
	again, err := reopened.ClaimIndexDelivery(ctx, delivery.ProfileID, delivery.JobID, delivery.ID, time.Minute)
	must(t, err)
	if again != nil {
		t.Fatal("ready index was executed on replay")
	}
	pending, err = reopened.PendingIndexDeliveries(ctx, delivery.ProfileID, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("completed intent remained dispatchable")
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_outbox`).Scan(&count))
	if count != 0 {
		t.Fatal("ready publication did not retire intent atomically")
	}
}

func TestMySQLIndexOutboxRetryBackoffFencingAndExhaustion(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w, write, firstDelivery := outboxDocument(t, s, "outboxretry")
	first := claimDelivery(t, s, firstDelivery)
	_, err := s.pool.ExecContext(ctx, `CREATE TRIGGER test_outbox_retry_failure BEFORE UPDATE ON index_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced retry failure'`)
	must(t, err)
	retried, err := s.RetryDocumentIndex(ctx, first.Job, "embedding_unavailable", 2, time.Hour)
	if err == nil || retried {
		t.Fatal("failed persistence was acknowledged as a scheduled retry")
	}
	current, err := s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, firstDelivery.ProfileID)
	must(t, err)
	if current.Status != "running" || current.Generation != first.Job.Generation {
		t.Fatal("failed retry transaction changed job")
	}
	var cleanups int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_cleanup`).Scan(&cleanups))
	if cleanups != 0 {
		t.Fatal("failed retry transaction left partial cleanup")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_outbox_retry_failure`)
	must(t, err)
	retried, err = s.RetryDocumentIndex(ctx, first.Job, "embedding_unavailable", 2, time.Hour)
	must(t, err)
	if !retried {
		t.Fatal("retry budget was not used")
	}
	var nextID string
	var beforeDispatch time.Time
	var failures int
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,failures,next_dispatch_at FROM index_outbox WHERE job_id=?`, firstDelivery.JobID).Scan(&nextID, &failures, &beforeDispatch))
	if nextID == firstDelivery.ID || failures != 1 {
		t.Fatal("retry did not rotate its token and persist its budget")
	}
	_, queued, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
	must(t, err)
	if queued {
		t.Fatal("duplicate admission reset active retry")
	}
	must(t, s.MarkIndexDeliverySent(ctx, firstDelivery))
	var afterID string
	var afterDispatch time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,failures,next_dispatch_at FROM index_outbox WHERE job_id=?`, firstDelivery.JobID).Scan(&afterID, &failures, &afterDispatch))
	if afterID != nextID || failures != 1 || !afterDispatch.Equal(beforeDispatch) {
		t.Fatal("duplicate admission or late acknowledgement changed the new retry")
	}
	pending, err := s.PendingIndexDeliveries(ctx, firstDelivery.ProfileID, 20)
	must(t, err)
	jobs, err := s.PendingIndexJobs(ctx, firstDelivery.ProfileID)
	must(t, err)
	if len(pending) != 0 || len(jobs) != 0 {
		t.Fatal("pending scan ignored durable backoff")
	}
	for _, method := range []func() (*domain.IndexInput, error){
		func() (*domain.IndexInput, error) {
			return s.ClaimIndexJob(ctx, firstDelivery.ProfileID, time.Minute)
		},
		func() (*domain.IndexInput, error) {
			return s.ClaimIndexJobByID(ctx, firstDelivery.ProfileID, firstDelivery.JobID, time.Minute)
		},
		func() (*domain.IndexInput, error) {
			return s.ClaimIndexDelivery(ctx, firstDelivery.ProfileID, firstDelivery.JobID, nextID, time.Minute)
		},
	} {
		input, err := method()
		must(t, err)
		if input != nil {
			t.Fatal("claim bypassed durable backoff")
		}
	}
	secondDelivery := makeDeliveryDue(t, s, firstDelivery.JobID)
	stale, err := s.ClaimIndexDelivery(ctx, firstDelivery.ProfileID, firstDelivery.JobID, firstDelivery.ID, time.Minute)
	must(t, err)
	if stale != nil {
		t.Fatal("obsolete token consumed a new retry")
	}
	second := claimDelivery(t, s, secondDelivery)
	if second.Job.Generation == first.Job.Generation || second.Job.Attempts != 2 {
		t.Fatal("retry did not fence the old generation")
	}
	wantError(t, finishTestIndex(s, first.Job), domain.ErrConflict)
	_, err = s.RetryDocumentIndex(ctx, first.Job, "embedding_unavailable", 2, 0)
	wantError(t, err, domain.ErrConflict)
	retried, err = s.RetryDocumentIndex(ctx, second.Job, "embedding_unavailable", 2, 0)
	must(t, err)
	if !retried {
		t.Fatal("second retry was lost")
	}
	thirdDelivery := makeDeliveryDue(t, s, firstDelivery.JobID)
	third := claimDelivery(t, s, thirdDelivery)
	retried, err = s.RetryDocumentIndex(ctx, third.Job, "embedding_unavailable", 2, 0)
	must(t, err)
	if retried {
		t.Fatal("exhausted retry budget was ignored")
	}
	current, err = s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, firstDelivery.ProfileID)
	must(t, err)
	if current.Status != "failed" || current.ErrorCode != "embedding_unavailable" || current.Attempts != 3 {
		t.Fatalf("exhaustion was not durable: %+v", current)
	}
	pending, err = s.PendingIndexDeliveries(ctx, firstDelivery.ProfileID, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("exhausted job is still dispatchable")
	}
	_, queued, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
	must(t, err)
	if !queued {
		t.Fatal("manual recovery was not admitted")
	}
	manual := makeDeliveryDue(t, s, firstDelivery.JobID)
	must(t, s.pool.QueryRowContext(ctx, `SELECT failures FROM index_outbox WHERE job_id=?`, manual.JobID).Scan(&failures))
	if manual.ID == thirdDelivery.ID || failures != 0 {
		t.Fatal("manual recovery retained an old token or retry budget")
	}
	must(t, finishTestIndex(s, claimDelivery(t, s, manual).Job))
}

func TestMySQLIndexOutboxLeaseRecoveryBound(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w, write, delivery := outboxDocument(t, s, "outboxcrash")
	first := claimDelivery(t, s, delivery)
	previous := first
	for range maxIndexLeaseRecoveries {
		expireTestIndex(t, s, delivery.JobID)
		recovered := claimDelivery(t, s, delivery)
		if recovered.Job.Generation == previous.Job.Generation {
			t.Fatal("lease recovery reused a generation")
		}
		previous = recovered
	}
	expireTestIndex(t, s, delivery.JobID)
	stopped, err := s.ClaimIndexDelivery(ctx, delivery.ProfileID, delivery.JobID, delivery.ID, time.Minute)
	must(t, err)
	if stopped != nil {
		t.Fatal("unbounded crashed-worker recovery")
	}
	current, err := s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, delivery.ProfileID)
	must(t, err)
	if current.Status != "failed" || current.ErrorCode != "lease_recovery_exhausted" || current.Attempts != 1+maxIndexLeaseRecoveries {
		t.Fatalf("crash exhaustion was not persisted: %+v", current)
	}
	wantError(t, finishTestIndex(s, previous.Job), domain.ErrConflict)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
	must(t, err)
	manual := makeDeliveryDue(t, s, delivery.JobID)
	var recoveries int
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_recoveries FROM index_outbox WHERE job_id=?`, delivery.JobID).Scan(&recoveries))
	if recoveries != 0 || manual.ID == delivery.ID {
		t.Fatal("manual retry did not reset crash recovery budget")
	}
	must(t, finishTestIndex(s, claimDelivery(t, s, manual).Job))
}

func TestMySQLIndexOutboxDeletedAndRevokedDelivery(t *testing.T) {
	for _, action := range []string{"document", "workspace", "permission"} {
		t.Run(action, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w, write, delivery := outboxDocument(t, s, "outboxremoved")
			switch action {
			case "document":
				must(t, s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
			case "workspace":
				must(t, s.DeleteWorkspace(ctx, u.ID, w.ID))
			case "permission":
				_, err := s.pool.ExecContext(ctx, `DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, w.ID, u.ID)
				must(t, err)
			}
			input, err := s.ClaimIndexDelivery(ctx, delivery.ProfileID, delivery.JobID, delivery.ID, time.Minute)
			must(t, err)
			if input != nil {
				t.Fatal("removed or revoked document was indexed")
			}
			must(t, s.MarkIndexDeliverySent(ctx, delivery))
			var count int
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_outbox`).Scan(&count))
			if count != 0 {
				t.Fatal("invalid document retained dispatch intent")
			}
		})
	}
}

func TestMySQLIndexOutboxMigrationBackfillAndResume(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	_, _, _, queued := outboxDocument(t, s, "outboxoldqueued")
	_, _, _, running := outboxDocument(t, s, "outboxoldrunning")
	claimDelivery(t, s, running)
	_, _, _, failed := outboxDocument(t, s, "outboxoldfailed")
	must(t, s.FailDocumentIndex(ctx, claimDelivery(t, s, failed).Job, "parse_failed"))
	_, err := s.pool.ExecContext(ctx, `DROP TABLE index_outbox`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=13`)
	must(t, err)
	must(t, s.Migrate(ctx))
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_outbox`).Scan(&count))
	if count != 2 {
		t.Fatal("migration did not backfill only unfinished historical jobs")
	}
	var id string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id FROM index_outbox WHERE job_id=?`, queued.JobID).Scan(&id))
	_, err = s.pool.ExecContext(ctx, `UPDATE index_outbox SET failures=2,next_attempt_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 HOUR) WHERE job_id=?`, queued.JobID)
	must(t, err)
	// Simulate interruption after only one row of the backfill was persisted.
	_, err = s.pool.ExecContext(ctx, `DELETE FROM index_outbox WHERE job_id=?`, running.JobID)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=13`)
	must(t, err)
	must(t, s.Migrate(ctx))
	var resumedID string
	var failures int
	var deferred bool
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,failures,next_attempt_at>UTC_TIMESTAMP(6) FROM index_outbox WHERE job_id=?`, queued.JobID).Scan(&resumedID, &failures, &deferred))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_outbox`).Scan(&count))
	if resumedID != id || failures != 2 || !deferred || count != 2 {
		t.Fatal("resumed migration reset an existing delivery or missed a job")
	}
}
