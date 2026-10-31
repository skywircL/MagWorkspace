package store

import (
	"context"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

// The database and production Store methods are real. Only external object
// storage is replaced so unavailable deletes and in-flight uploads are bounded.
func TestMySQLDocumentDeletionCascadesVersionsJobsAndRetriesObjects(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "deleteallversions")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	profile := testIndexProfile()
	s.indexProfile = &profile
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete all versions", "first.txt", []byte("first source"))
	must(t, err)
	ready := publishTestIndex(t, s, u.ID, w.ID, first, profile)
	second, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "second.txt", []byte("second source"))
	must(t, err)
	running, err := s.ClaimIndexJob(ctx, profile.ID, 10*time.Minute)
	must(t, err)
	if running == nil || running.Job.VersionID != second.Version.ID {
		t.Fatal("expected second version to hold an indexing lease")
	}
	var lease time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_expires_at FROM document_indexes WHERE id=?`, running.Job.ID).Scan(&lease))
	third, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, second.Version.ID, "third.txt", []byte("third source"))
	must(t, err)
	deliveries, err := s.PendingIndexDeliveries(ctx, profile.ID, 20)
	must(t, err)
	if len(deliveries) != 1 {
		t.Fatalf("expected one queued delivery: %+v", deliveries)
	}
	staleDelivery := deliveries[0]
	// A separate live document proves cleanup is scoped to the deleted source.
	s.indexProfile = nil
	live, err := s.CreateDocument(ctx, u.ID, w.ID, "Keep live", "keep.txt", []byte("live source"))
	must(t, err)
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, first.Document.ID))
	for _, version := range []domain.DocumentVersion{first.Version, second.Version, third.Version} {
		checkDocumentUnavailable(t, s, u.ID, w.ID, first.Document.ID, version.ID)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM documents WHERE id=?`,
		`SELECT COUNT(*) FROM document_versions WHERE document_id=?`,
		`SELECT COUNT(*) FROM document_indexes WHERE document_id=?`,
	} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, query, first.Document.ID).Scan(&count))
		if count != 0 {
			t.Fatalf("document deletion retained owned rows for %s", query)
		}
	}
	for _, table := range []string{"chunks", "index_outbox"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 0 {
			t.Fatalf("document deletion retained %s", table)
		}
	}
	assertObjectQueueCount(t, s, 3)
	var protectedUntil time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE generation=?`, running.Job.Generation).Scan(&protectedUntil))
	if protectedUntil.Before(lease.Add(30 * time.Second)) {
		t.Fatal("deletion discarded the in-flight worker cleanup protection")
	}
	var cleanupCount int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_cleanup WHERE generation IN (?,?)`, ready.Generation, running.Job.Generation).Scan(&cleanupCount))
	if cleanupCount != 2 {
		t.Fatal("deletion did not retain every external index generation")
	}
	_, err = s.LoadIndexSource(ctx, *running)
	wantError(t, err, domain.ErrNotFound)
	wantError(t, s.FinishDocumentIndex(ctx, running.Job, testIndexChunks(running.Job, "second source")), domain.ErrNotFound)
	retried, err := s.RetryDocumentIndex(ctx, running.Job, "qdrant_unavailable", 3, time.Second)
	wantError(t, err, domain.ErrNotFound)
	if retried {
		t.Fatal("late worker revived a deleted document")
	}
	claimed, err := s.ClaimIndexDelivery(ctx, profile.ID, staleDelivery.JobID, staleDelivery.ID, time.Minute)
	must(t, err)
	if claimed != nil {
		t.Fatal("queued delivery revived a deleted document")
	}
	must(t, s.MarkIndexDeliverySent(ctx, staleDelivery))
	objects.deleteHook = func(context.Context, string) error { return blob.ErrUnavailable }
	wantError(t, s.CleanupObjects(ctx), blob.ErrUnavailable)
	assertObjectQueueCount(t, s, 3)
	if objects.count() != 4 {
		t.Fatal("unavailable deletion removed source bytes")
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(objects)
	objects.deleteHook = nil
	_, err = reopened.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, reopened.CleanupObjects(ctx))
	must(t, reopened.CleanupObjects(ctx))
	assertObjectQueueCount(t, reopened, 0)
	if objects.count() != 1 {
		t.Fatal("restart did not reclaim all deleted versions independently of live sources")
	}
	checkDocumentContent(t, reopened, u.ID, w.ID, live.Version, []byte("live source"))
}

func TestMySQLDocumentDeletionRollsBackAllCleanupIntents(t *testing.T) {
	for _, stage := range []string{"index_cleanup", "object_cleanup", "documents"} {
		t.Run(stage, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "deleteatomic")
			objects := newMemoryObjects()
			s.SetObjectStorage(objects)
			write, err := s.CreateDocument(ctx, u.ID, w.ID, "Retain on failure", "source.txt", []byte("protected source"))
			must(t, err)
			job := publishTestIndex(t, s, u.ID, w.ID, write, testIndexProfile())
			operation := "INSERT"
			if stage == "documents" {
				operation = "DELETE"
			}
			_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_delete_failure BEFORE `+operation+` ON `+stage+` FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced deletion failure'`)
			must(t, err)
			if err = s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID); err == nil {
				t.Fatal("injected delete failure was ignored")
			}
			checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("protected source"))
			chunks, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
			must(t, err)
			if len(chunks) != 2 {
				t.Fatal("failed deletion removed published chunks")
			}
			for _, table := range []string{"object_cleanup", "index_cleanup"} {
				var count int
				must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
				if count != 0 {
					t.Fatalf("failed deletion committed partial %s", table)
				}
			}
			_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_delete_failure`)
			must(t, err)
			must(t, s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
			must(t, s.CleanupObjects(ctx))
			if objects.count() != 0 {
				t.Fatal("retry after transaction rollback did not reclaim the source")
			}
		})
	}
}

func TestMySQLDocumentDeletionFencesLateUpload(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "deletelateupload")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete during upload", "source.txt", []byte("old source"))
	must(t, err)
	objects.putHook = func(ctx context.Context, _ string) error {
		return s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID)
	}
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, "new.txt", []byte("late source"))
	wantError(t, err, domain.ErrNotFound)
	checkDocumentUnavailable(t, s, u.ID, w.ID, write.Document.ID, write.Version.ID)
	assertObjectQueueCount(t, s, 2)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("deletion lost the staged object or retained the deleted source")
	}
	assertObjectQueueCount(t, s, 0)
}

func TestMySQLDocumentDeletionFencesSourceReadInFlight(t *testing.T) {
	for _, reader := range []string{"download", "worker"} {
		t.Run(reader, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "deleteread")
			objects := newMemoryObjects()
			s.SetObjectStorage(objects)
			write, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete during read", "source.txt", []byte("private source"))
			must(t, err)
			var input *domain.IndexInput
			if reader == "worker" {
				_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
				must(t, err)
				input, err = s.ClaimIndexJob(ctx, testIndexProfile().ID, time.Minute)
				must(t, err)
				if input == nil {
					t.Fatal("missing worker lease")
				}
			}
			objects.readHook = func(ctx context.Context, _ string) error {
				return s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID)
			}
			var content []byte
			if reader == "worker" {
				content, err = s.LoadIndexSource(ctx, *input)
			} else {
				_, content, err = s.DocumentContent(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID)
			}
			wantError(t, err, domain.ErrNotFound)
			if content != nil {
				t.Fatal("deleted source bytes escaped an in-flight read")
			}
		})
	}
}
