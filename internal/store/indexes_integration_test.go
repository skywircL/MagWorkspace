package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func testIndexProfile() domain.IndexProfile {
	return domain.IndexProfile{ID: strings.Repeat("a", 64), Model: "test-embedding", Dimensions: 3}
}

func testIndexChunks(job domain.IndexJob, content string) []domain.IndexChunk {
	if content == "" {
		content = "source"
	}
	digest := sha256.Sum256([]byte(content))
	parent := domain.IndexChunk{IndexID: job.ID, Generation: job.Generation, DocumentID: job.DocumentID, VersionID: job.VersionID, SourceSHA256: job.SourceSHA256, Kind: "parent", ParentOrdinal: -1, StartByte: 0, EndByte: len(content), Content: content, SHA256: hex.EncodeToString(digest[:])}
	identity := sha256.Sum256([]byte(job.ID + job.Generation + "parent" + content))
	parent.ID = hex.EncodeToString(identity[:16])
	child := parent
	child.Kind = "child"
	child.ParentID = parent.ID
	child.ParentOrdinal = 0
	identity = sha256.Sum256([]byte(job.ID + job.Generation + "child" + content))
	child.ID = hex.EncodeToString(identity[:16])
	return []domain.IndexChunk{parent, child}
}

func finishTestIndex(s *Store, job domain.IndexJob) error {
	var content []byte
	if err := s.pool.QueryRowContext(context.Background(), `SELECT COALESCE(content,'') FROM document_versions WHERE id=?`, job.VersionID).Scan(&content); err != nil {
		return dbError(err)
	}
	return s.FinishDocumentIndex(context.Background(), job, testIndexChunks(job, string(content)))
}

func publishTestIndex(t *testing.T, s *Store, userID, workspaceID string, write domain.DocumentWrite, profile domain.IndexProfile) domain.IndexJob {
	t.Helper()
	ctx := context.Background()
	job, _, err := s.EnqueueDocumentIndex(ctx, userID, workspaceID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil || input.Job.ID != job.ID {
		t.Fatalf("wrong claimed job: %+v", input)
	}
	must(t, finishTestIndex(s, input.Job))
	ready, err := s.DocumentIndex(ctx, userID, workspaceID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	return ready
}

func expireTestIndex(t *testing.T, s *Store, id string) {
	t.Helper()
	_, err := s.pool.ExecContext(context.Background(), `UPDATE document_indexes SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, id)
	must(t, err)
}

func expireTestIndexCleanup(t *testing.T, s *Store, generation string) {
	t.Helper()
	_, err := s.pool.ExecContext(context.Background(), `UPDATE index_cleanup SET not_before=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE generation=?`, generation)
	must(t, err)
}

func TestMySQLIndexAutomaticAdmissionAndUploadRollback(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "autoadmission")
	profile := testIndexProfile()
	auto, err := Open(ctx, dsn, profile)
	must(t, err)
	defer auto.Close()
	write, err := auto.CreateDocument(ctx, u.ID, w.ID, "Auto indexed", "notes.txt", []byte("原文保持不变"))
	must(t, err)
	job, err := auto.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	if job.Status != "queued" || job.SourceSHA256 != write.Version.SHA256 || job.CreatedBy != u.ID || job.Attempts != 0 || job.Generation != "" {
		t.Fatalf("upload did not admit exact source version: %+v", job)
	}
	duplicate, queued, err := auto.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	if queued || duplicate.ID != job.ID {
		t.Fatalf("duplicate admission was not idempotent: %+v", duplicate)
	}
	wrongProfile := profile
	wrongProfile.Dimensions++
	_, _, err = auto.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, wrongProfile)
	wantError(t, err, domain.ErrConflict)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_enqueue_failure BEFORE INSERT ON document_indexes FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced admission failure'`)
	must(t, err)
	_, err = auto.CreateDocument(ctx, u.ID, w.ID, "Must roll back", "rollback.txt", []byte("new document"))
	if err == nil {
		t.Fatal("failed admission did not fail document creation")
	}
	_, err = auto.AppendDocumentVersion(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, "new.txt", []byte("new version"))
	if err == nil {
		t.Fatal("failed admission did not fail version append")
	}
	current, err := s.Document(ctx, u.ID, w.ID, write.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, write.Document) {
		t.Fatalf("admission failure changed head or timestamp: %+v", current)
	}
	for _, table := range []string{"documents", "document_versions", "document_indexes"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 1 {
			t.Fatalf("admission failure left partial %s rows: %d", table, count)
		}
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_enqueue_failure`)
	must(t, err)
	appended, err := auto.AppendDocumentVersion(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, "second.txt", nil)
	must(t, err)
	_, err = s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, appended.Version.ID, profile.ID)
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("原文保持不变"))
	_, err = Open(ctx, dsn, profile, profile)
	wantError(t, err, domain.ErrInvalid)
}

func TestMySQLIndexLeaseFencingAndProtectedCleanup(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "leasefence")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Lease", "lease.txt", []byte("source"))
	must(t, err)
	job, _, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	type claim struct {
		input *domain.IndexInput
		err   error
	}
	start := make(chan struct{})
	claims := make(chan claim, 2)
	for range 2 {
		go func() {
			<-start
			input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
			claims <- claim{input, err}
		}()
	}
	close(start)
	var first *domain.IndexInput
	for range 2 {
		result := <-claims
		must(t, result.err)
		if result.input != nil {
			if first != nil {
				t.Fatal("two workers claimed the same live lease")
			}
			first = result.input
		}
	}
	if first == nil || first.Job.ID != job.ID || first.Job.Attempts != 1 || !reflect.DeepEqual(first.Version, write.Version) {
		t.Fatalf("invalid first claim: %+v", first)
	}
	loaded, err := s.LoadIndexSource(ctx, *first)
	must(t, err)
	if string(loaded) != "source" {
		t.Fatal("claimed legacy source differs from its immutable version")
	}
	must(t, s.QueueIndexCleanup(ctx, profile.ID, first.Job.Generation))
	expireTestIndexCleanup(t, s, first.Job.Generation)
	pending, err := s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("live running generation became cleanup eligible")
	}
	expireTestIndex(t, s, job.ID)
	wantError(t, finishTestIndex(s, first.Job), domain.ErrConflict)
	second, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if second == nil || second.Job.ID != job.ID || second.Job.Generation == first.Job.Generation || second.Job.Attempts != 2 {
		t.Fatalf("lease was not fenced on reclaim: %+v", second)
	}
	wantError(t, finishTestIndex(s, first.Job), domain.ErrConflict)
	wantError(t, s.FailDocumentIndex(ctx, first.Job, "late_worker"), domain.ErrConflict)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("reclaimed generation bypassed cleanup grace period")
	}
	expireTestIndexCleanup(t, s, first.Job.Generation)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != first.Job.Generation {
		t.Fatalf("reclaim did not retain old generation cleanup: %+v", pending)
	}
	oldCleanupID := pending[0].ID
	// A late worker may write vectors after the first delete has started.
	must(t, s.QueueIndexCleanup(ctx, profile.ID, first.Job.Generation))
	must(t, s.CompleteIndexCleanup(ctx, oldCleanupID))
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("late-worker requeue did not restore cleanup grace")
	}
	expireTestIndexCleanup(t, s, first.Job.Generation)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].ID == oldCleanupID {
		t.Fatal("old cleanup acknowledgement removed a later cleanup request")
	}
	must(t, finishTestIndex(s, second.Job))
	must(t, s.QueueIndexCleanup(ctx, profile.ID, second.Job.Generation))
	expireTestIndexCleanup(t, s, second.Job.Generation)
	wantError(t, s.FailDocumentIndex(ctx, second.Job, "lost_commit_ack"), domain.ErrConflict)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != first.Job.Generation {
		t.Fatalf("committed ready generation became cleanup eligible: %+v", pending)
	}
	must(t, s.CompleteIndexCleanup(ctx, pending[0].ID))
	must(t, s.CompleteIndexCleanup(ctx, pending[0].ID))
	ready, queued, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	if queued || ready.Status != "ready" || ready.Generation != second.Job.Generation {
		t.Fatal("ready index was unnecessarily queued again")
	}
}

func TestMySQLIndexPermissionRevocationAndRetry(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "indexowner")
	member, other := testUser(t, s, "indexmember")
	profile := testIndexProfile()
	_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	write, err := s.CreateDocument(ctx, owner.ID, w.ID, "Permissions", "source.txt", []byte("source"))
	must(t, err)
	job, _, err := s.EnqueueDocumentIndex(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	must(t, s.DeleteMember(ctx, owner.ID, w.ID, member.ID))
	input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input != nil {
		t.Fatal("revoked creator's queued job was executed")
	}
	failed, err := s.DocumentIndex(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	if failed.Status != "failed" || failed.ErrorCode != "permission_revoked" || failed.Attempts != 0 {
		t.Fatalf("revoked job did not fail safely: %+v", failed)
	}
	_, err = s.DocumentIndex(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	wantError(t, err, domain.ErrNotFound)
	_, _, err = s.EnqueueDocumentIndex(ctx, member.ID, other.ID, write.Document.ID, write.Version.ID, profile)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	_, queued, err := s.EnqueueDocumentIndex(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	if !queued {
		t.Fatal("failed job was not retryable")
	}
	input, err = s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("retry was not claimed")
	}
	must(t, s.UpdateMember(ctx, owner.ID, w.ID, member.ID, "viewer"))
	wantError(t, finishTestIndex(s, input.Job), domain.ErrForbidden)
	_, _, err = s.EnqueueDocumentIndex(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	wantError(t, err, domain.ErrForbidden)
	must(t, s.FailDocumentIndex(ctx, input.Job, "permission_revoked"))
	retry, queued, err := s.EnqueueDocumentIndex(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	if !queued || retry.ID != job.ID || retry.CreatedBy != owner.ID || retry.Generation != "" {
		t.Fatalf("new authorized actor could not retry: %+v", retry)
	}
	input, err = s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil || input.Job.CreatedBy != owner.ID {
		t.Fatal("retry retained revoked creator")
	}
	must(t, finishTestIndex(s, input.Job))
}

func TestMySQLIndexScopeResolvesOnlyCurrentAuthorizedManifests(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "trustedsearch")
	viewer, _ := testUser(t, s, "searchviewer")
	outsider, other := testUser(t, s, "searchoutsider")
	_, err := s.AddMember(ctx, u.ID, w.ID, viewer.Email, "viewer")
	must(t, err)
	profile := testIndexProfile()
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Trusted title", "first.txt", []byte("first"))
	must(t, err)
	firstJob := publishTestIndex(t, s, u.ID, w.ID, first, profile)
	scope, err := s.SearchIndexScope(ctx, viewer.ID, w.ID, profile.ID)
	must(t, err)
	if len(scope) != 1 || scope[0] != firstJob.Generation {
		t.Fatalf("ready current index missing from scope: %+v", scope)
	}
	second, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "second.txt", []byte("second"))
	must(t, err)
	scope, err = s.SearchIndexScope(ctx, u.ID, w.ID, profile.ID)
	must(t, err)
	if len(scope) != 0 {
		t.Fatalf("historical generation remained in search: %+v", scope)
	}
	// A fixed historical index remains locatable, independently of search scope.
	old, err := s.DocumentIndexByID(ctx, viewer.ID, w.ID, first.Document.ID, first.Version.ID, firstJob.ID)
	must(t, err)
	if !reflect.DeepEqual(old, firstJob) {
		t.Fatal("fixed historical manifest changed")
	}
	secondJob := publishTestIndex(t, s, u.ID, w.ID, second, profile)
	foreign, err := s.CreateDocument(ctx, outsider.ID, other.ID, "Foreign", "foreign.txt", nil)
	must(t, err)
	foreignJob := publishTestIndex(t, s, outsider.ID, other.ID, foreign, profile)
	otherProfile := profile
	otherProfile.ID = strings.Repeat("b", 64)
	// Historical versions may finish processing, but never enter current scope.
	oldOther := publishTestIndex(t, s, u.ID, w.ID, first, otherProfile)
	scope, err = s.SearchIndexScope(ctx, viewer.ID, w.ID, otherProfile.ID)
	must(t, err)
	if len(scope) != 0 || oldOther.Status != "ready" {
		t.Fatal("historical publication leaked into current search")
	}
	otherJob := publishTestIndex(t, s, u.ID, w.ID, second, otherProfile)
	scope, err = s.SearchIndexScope(ctx, viewer.ID, w.ID, profile.ID)
	must(t, err)
	if len(scope) != 1 || scope[0] != secondJob.Generation || scope[0] == firstJob.Generation {
		t.Fatalf("scope did not isolate current profile: %+v", scope)
	}
	hits, err := s.ResolveIndexes(ctx, viewer.ID, w.ID, profile.ID, []string{firstJob.ID, foreignJob.ID, secondJob.ID, secondJob.ID, "forged", otherJob.ID})
	must(t, err)
	if len(hits) != 1 || !reflect.DeepEqual(hits[0].Job, secondJob) || hits[0].Title != "Trusted title" {
		t.Fatalf("untrusted index IDs changed authorized manifest: %+v", hits)
	}
	_, err = s.DocumentIndexByID(ctx, u.ID, w.ID, first.Document.ID, second.Version.ID, firstJob.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.DocumentIndexByID(ctx, outsider.ID, other.ID, first.Document.ID, first.Version.ID, firstJob.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.SearchIndexScope(ctx, outsider.ID, w.ID, profile.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.ResolveIndexes(ctx, outsider.ID, w.ID, profile.ID, nil)
	wantError(t, err, domain.ErrNotFound)
	must(t, s.DeleteMember(ctx, u.ID, w.ID, viewer.ID))
	_, err = s.ResolveIndexes(ctx, viewer.ID, w.ID, profile.ID, []string{secondJob.ID})
	wantError(t, err, domain.ErrNotFound)
	_, err = s.ResolveIndexes(ctx, u.ID, w.ID, profile.ID, make([]string, 201))
	wantError(t, err, domain.ErrInvalid)
}

func TestMySQLIndexChunkValidationAndPublicationRollback(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "finishvalidation")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Validate chunks", "source.txt", []byte("source"))
	must(t, err)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing job")
	}
	for _, change := range []func([]domain.IndexChunk){
		func(c []domain.IndexChunk) { c[0].SHA256 = "invalid" },
		func(c []domain.IndexChunk) { c[1].ParentID = strings.Repeat("f", 32) },
		func(c []domain.IndexChunk) { c[1].Generation = strings.Repeat("f", 32) },
		func(c []domain.IndexChunk) { c[1].Content = "forged" },
		func(c []domain.IndexChunk) { c[1].Ordinal = 1 },
		func(c []domain.IndexChunk) { c[0].Kind = "unknown" },
	} {
		chunks := testIndexChunks(input.Job, "source")
		change(chunks)
		wantError(t, s.FinishDocumentIndex(ctx, input.Job, chunks), domain.ErrInvalid)
	}
	wantError(t, s.FinishDocumentIndex(ctx, input.Job, nil), domain.ErrInvalid)
	forged := input.Job
	forged.SourceSHA256 = strings.Repeat("f", 64)
	wantError(t, finishTestIndex(s, forged), domain.ErrConflict)
	forged = input.Job
	forged.Generation = strings.Repeat("0", 32)
	wantError(t, finishTestIndex(s, forged), domain.ErrConflict)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_publication_failure BEFORE UPDATE ON document_indexes FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced publication failure'`)
	must(t, err)
	if err = finishTestIndex(s, input.Job); err == nil {
		t.Fatal("expected publication failure")
	}
	unchanged, err := s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	if !reflect.DeepEqual(unchanged, input.Job) || unchanged.ChunkCount != 0 || unchanged.ParentCount != 0 {
		t.Fatalf("failed publication changed task state: %+v", unchanged)
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks`).Scan(&count))
	if count != 0 {
		t.Fatal("failed publication left partial chunks")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_publication_failure`)
	must(t, err)
	must(t, finishTestIndex(s, input.Job))
	ready, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, input.Job.ID)
	must(t, err)
	if ready.Status != "ready" || ready.ChunkCount != 1 || ready.ParentCount != 1 {
		t.Fatalf("chunk counts were not published together: %+v", ready)
	}
	stored, err := s.ReadIndexChunks(ctx, u.ID, w.ID, ready, nil)
	must(t, err)
	if !reflect.DeepEqual(stored, testIndexChunks(ready, "source")) {
		t.Fatalf("chunk bodies did not publish: %+v", stored)
	}
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("source"))
}

func TestMySQLIndexDeletionQueuesCleanupWithoutLeakingSearch(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "indexdelete")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete ready", "source.txt", nil)
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
	_, err = s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	wantError(t, err, domain.ErrNotFound)
	hits, err := s.ResolveIndexes(ctx, u.ID, w.ID, profile.ID, []string{job.ID})
	must(t, err)
	if len(hits) != 0 {
		t.Fatal("deleted document leaked through candidate resolution")
	}
	pending, err := s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("deleted ready generation bypassed cleanup grace")
	}
	expireTestIndexCleanup(t, s, job.Generation)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != job.Generation {
		t.Fatalf("document deletion did not queue index cleanup: %+v", pending)
	}
	runningDoc, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete running", "running.txt", nil)
	must(t, err)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, runningDoc.Document.ID, runningDoc.Version.ID, profile)
	must(t, err)
	running, err := s.ClaimIndexJob(ctx, profile.ID, 10*time.Minute)
	must(t, err)
	if running == nil {
		t.Fatal("missing running job")
	}
	var lease, protectedUntil time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_expires_at FROM document_indexes WHERE id=?`, running.Job.ID).Scan(&lease))
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, runningDoc.Document.ID))
	wantError(t, finishTestIndex(s, running.Job), domain.ErrNotFound)
	must(t, s.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE generation=?`, running.Job.Generation).Scan(&protectedUntil))
	if protectedUntil.Before(lease.Add(30 * time.Second)) {
		t.Fatal("document deletion did not copy the running lease into durable cleanup protection")
	}
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != job.Generation {
		t.Fatal("deleted running generation was cleaned before its lease protection expired")
	}
	must(t, s.DeleteWorkspace(ctx, u.ID, w.ID))
	for _, table := range []string{"document_indexes"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 0 {
			t.Fatalf("workspace cascade retained %s", table)
		}
	}
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != job.Generation {
		t.Fatal("workspace cascade changed an earlier cleanup or lost live lease protection")
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	var persistedProtection time.Time
	must(t, reopened.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE generation=?`, running.Job.Generation).Scan(&persistedProtection))
	if persistedProtection.Before(protectedUntil) {
		t.Fatal("workspace cascade or restart shortened the copied lease")
	}
	pending, err = reopened.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != job.Generation {
		t.Fatal("restart forgot the durable cleanup wait")
	}
	expireTestIndexCleanup(t, reopened, job.Generation)
	expireTestIndexCleanup(t, reopened, running.Job.Generation)
	pending, err = reopened.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 2 {
		t.Fatal("workspace cascade removed eventual cleanup records")
	}
}

func TestMySQLIndexFailureRetainsCleanupLeaseAndGrace(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "indexfailuregrace")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Long processing", "source.txt", []byte("source"))
	must(t, err)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, profile.ID, 10*time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing claimed job")
	}
	var lease, firstProtection time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_expires_at FROM document_indexes WHERE id=?`, input.Job.ID).Scan(&lease))
	must(t, s.FailDocumentIndex(ctx, input.Job, "processing_timeout"))
	var leaseCleared bool
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_expires_at IS NULL FROM document_indexes WHERE id=?`, input.Job.ID).Scan(&leaseCleared))
	must(t, s.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE generation=?`, input.Job.Generation).Scan(&firstProtection))
	if !leaseCleared || firstProtection.Before(lease.Add(30*time.Second)) {
		t.Fatal("failure cleared its lease without copying cleanup protection")
	}
	// Requeue sees a failed job with no lease, but must preserve the longer
	// deadline copied by Fail and rotate its acknowledgement token.
	var firstID string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id FROM index_cleanup WHERE generation=?`, input.Job.Generation).Scan(&firstID))
	must(t, s.QueueIndexCleanup(ctx, profile.ID, input.Job.Generation))
	var secondID string
	var secondProtection time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,not_before FROM index_cleanup WHERE generation=?`, input.Job.Generation).Scan(&secondID, &secondProtection))
	if firstID == secondID || secondProtection.Before(firstProtection) {
		t.Fatal("late-worker queue shortened lease protection or reused its acknowledgement")
	}
	must(t, s.CompleteIndexCleanup(ctx, firstID))
	pending, err := s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("failed generation was deleted before remote processing protection elapsed")
	}
	expireTestIndexCleanup(t, s, input.Job.Generation)
	pending, err = s.PendingIndexCleanup(ctx, 20)
	must(t, err)
	if len(pending) != 1 || pending[0].ID != secondID {
		t.Fatal("protected failure did not eventually become cleanup eligible")
	}
}

func TestMySQLIndexMigrationPreservesV2AndRetriesPartialDDL(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	for _, table := range []string{"chunks", "index_outbox", "document_indexes", "index_cleanup"} {
		_, err := s.pool.ExecContext(ctx, `DROP TABLE `+table)
		must(t, err)
	}
	_, err := s.pool.ExecContext(ctx, `UPDATE schema_version SET version=2 WHERE singleton=1`)
	must(t, err)
	u, w := testUser(t, s, "indexupgrade")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Existing v2 document", "old.txt", []byte("existing bytes"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TABLE migration_blocker (workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT document_indexes_document_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id)) ENGINE=InnoDB`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted v3 migration")
	}
	var version, count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != 2 {
		t.Fatal("partial v3 DDL advanced the schema marker")
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_cleanup`).Scan(&count))
	_, err = s.pool.ExecContext(ctx, `DROP TABLE migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("existing bytes"))
	profile := testIndexProfile()
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	s.Close()
	reopened, err := Open(ctx, dsn, profile)
	must(t, err)
	defer reopened.Close()
	must(t, reopened.Migrate(ctx))
	loaded, err := reopened.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	if loaded.Status != "ready" || loaded.Generation != job.Generation || loaded.SourceSHA256 != write.Version.SHA256 {
		t.Fatalf("published index lost after reconnect: %+v", loaded)
	}
	loadedManifest, err := reopened.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	if !reflect.DeepEqual(loadedManifest, job) {
		t.Fatal("fixed manifest changed after reconnect")
	}
}
