package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func documentUpdateStore(t *testing.T) (*Store, *memoryObjects) {
	t.Helper()
	_, dsn := testStore(t)
	s, err := Open(context.Background(), dsn, testIndexProfile())
	must(t, err)
	t.Cleanup(s.Close)
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	return s, objects
}

func TestMySQLDocumentUpdateTitlePreservesSourceAndIndex(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "titleupdate")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Original title", "notes.md", []byte("original source"))
	must(t, err)
	profile := testIndexProfile()
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	updated, err := s.UpdateDocument(ctx, u.ID, w.ID, write.Document.ID, "  新标题  ")
	must(t, err)
	expected := write.Document
	expected.Title = "新标题"
	expected.UpdatedAt = updated.UpdatedAt
	if !reflect.DeepEqual(updated, expected) || updated.UpdatedAt.Before(write.Document.UpdatedAt) {
		t.Fatalf("title update changed document identity or head: %+v", updated)
	}
	versions, total, err := s.DocumentVersions(ctx, u.ID, w.ID, write.Document.ID, 20, 0)
	must(t, err)
	if total != 1 || len(versions) != 1 || !reflect.DeepEqual(versions[0], write.Version) || objects.count() != 1 {
		t.Fatal("renaming created or altered source versions or objects")
	}
	loaded, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	if !reflect.DeepEqual(loaded, job) {
		t.Fatal("renaming modified immutable index publication metadata")
	}
	refs, err := s.ResolveIndexes(ctx, u.ID, w.ID, profile.ID, []string{job.ID})
	must(t, err)
	if len(refs) != 1 || refs[0].Title != "新标题" || !reflect.DeepEqual(refs[0].Job, job) {
		t.Fatal("search metadata did not receive renamed title without reindexing")
	}
	var indexCount int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_indexes`).Scan(&indexCount))
	if indexCount != 1 {
		t.Fatal("renaming queued a new index")
	}
	assertObjectQueueCount(t, s, 0)
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("original source"))
}

func TestMySQLDocumentUpdateTextAppendsAndKeepsHistoricalIndex(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "textupdate")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Editable", "Notes.MARKDOWN", []byte("first source"))
	must(t, err)
	profile := testIndexProfile()
	oldJob := publishTestIndex(t, s, u.ID, w.ID, first, profile)
	text := "前言\r\n\t正文\f结尾\n"
	second, err := s.UpdateDocumentText(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, text)
	must(t, err)
	if second.Document.ID != first.Document.ID || second.Document.Title != first.Document.Title || second.Document.CurrentVersionID != second.Version.ID || second.Version.ID == first.Version.ID || second.Version.Number != 2 || second.Version.Filename != first.Version.Filename || second.Version.ObjectKey == "" || second.Version.ObjectKey == first.Version.ObjectKey {
		t.Fatalf("text update did not append a distinct immutable version: %+v", second)
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, []byte("first source"))
	checkDocumentContent(t, s, u.ID, w.ID, second.Version, []byte(text))
	job, err := s.DocumentIndex(ctx, u.ID, w.ID, first.Document.ID, second.Version.ID, profile.ID)
	must(t, err)
	if job.Status != "queued" || job.SourceSHA256 != second.Version.SHA256 || job.CreatedBy != u.ID {
		t.Fatal("text version and exact-source index admission were not committed together")
	}
	old, err := s.DocumentIndexByID(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, oldJob.ID)
	must(t, err)
	if !reflect.DeepEqual(old, oldJob) {
		t.Fatal("text update altered the historical manifest reference")
	}
	scope, err := s.SearchIndexScope(ctx, u.ID, w.ID, profile.ID)
	must(t, err)
	if len(scope) != 0 {
		t.Fatal("old ready index remained in search while new head was queued")
	}
	newJob := publishTestIndex(t, s, u.ID, w.ID, second, profile)
	scope, err = s.SearchIndexScope(ctx, u.ID, w.ID, profile.ID)
	must(t, err)
	if len(scope) != 1 || scope[0] != newJob.Generation {
		t.Fatal("newly published current version did not become searchable")
	}
	third, err := s.UpdateDocumentText(ctx, u.ID, w.ID, first.Document.ID, second.Version.ID, "")
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, third.Version, nil)
	if third.Version.Number != 3 || objects.count() != 3 {
		t.Fatal("empty edit was not retained as a distinct source version")
	}
}

func TestMySQLDocumentUpdateAuthorizationAndReferenceScope(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "updateowner")
	admin, _ := testUser(t, s, "updateadmin")
	contributor, _ := testUser(t, s, "updatecontributor")
	viewer, _ := testUser(t, s, "updateviewer")
	outsider, other := testUser(t, s, "updateoutsider")
	for _, member := range []struct{ email, role string }{{admin.Email, "admin"}, {contributor.Email, "contributor"}, {viewer.Email, "viewer"}} {
		_, err := s.AddMember(ctx, owner.ID, w.ID, member.email, member.role)
		must(t, err)
	}
	write, err := s.CreateDocument(ctx, owner.ID, w.ID, "Shared", "notes.txt", []byte("first"))
	must(t, err)
	for _, writer := range []domain.User{admin, contributor} {
		_, err = s.UpdateDocument(ctx, writer.ID, w.ID, write.Document.ID, writer.Name)
		must(t, err)
		write, err = s.UpdateDocumentText(ctx, writer.ID, w.ID, write.Document.ID, write.Version.ID, writer.Name)
		must(t, err)
		if write.Version.CreatedBy != writer.ID {
			t.Fatal("text edit did not retain its authenticated creator")
		}
	}
	otherDoc, err := s.CreateDocument(ctx, owner.ID, w.ID, "Different document", "other.md", nil)
	must(t, err)
	before := objects.count()
	for _, access := range []struct {
		user, workspace string
		want            error
	}{
		{viewer.ID, w.ID, domain.ErrForbidden},
		{outsider.ID, w.ID, domain.ErrNotFound},
		{owner.ID, other.ID, domain.ErrNotFound},
		{outsider.ID, other.ID, domain.ErrNotFound},
	} {
		_, err = s.UpdateDocument(ctx, access.user, access.workspace, write.Document.ID, "Forbidden title")
		wantError(t, err, access.want)
		_, err = s.UpdateDocumentText(ctx, access.user, access.workspace, write.Document.ID, write.Version.ID, "Forbidden bytes")
		wantError(t, err, access.want)
	}
	_, err = s.UpdateDocumentText(ctx, owner.ID, w.ID, write.Document.ID, otherDoc.Version.ID, "Foreign base")
	wantError(t, err, domain.ErrNotFound)
	must(t, s.DeleteMember(ctx, owner.ID, w.ID, contributor.ID))
	_, err = s.UpdateDocument(ctx, contributor.ID, w.ID, write.Document.ID, "Revoked")
	wantError(t, err, domain.ErrNotFound)
	_, err = s.UpdateDocumentText(ctx, contributor.ID, w.ID, write.Document.ID, write.Version.ID, "Revoked")
	wantError(t, err, domain.ErrNotFound)
	if objects.count() != before {
		t.Fatal("denied updates reached object storage")
	}
	current, err := s.Document(ctx, owner.ID, w.ID, write.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, write.Document) {
		t.Fatal("denied updates changed the document")
	}
	must(t, s.DeleteDocument(ctx, owner.ID, w.ID, write.Document.ID))
	_, err = s.UpdateDocument(ctx, owner.ID, w.ID, write.Document.ID, "Deleted")
	wantError(t, err, domain.ErrNotFound)
	_, err = s.UpdateDocumentText(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, "Deleted")
	wantError(t, err, domain.ErrNotFound)
}

func TestMySQLDocumentUpdateFailureAndConcurrentRevocation(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "updatefailureowner")
	member, _ := testUser(t, s, "updatefailuremember")
	_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	write, err := s.CreateDocument(ctx, owner.ID, w.ID, "Stable", "notes.md", []byte("stable"))
	must(t, err)
	objects.putHook = func(ctx context.Context, _ string) error {
		return s.DeleteMember(ctx, owner.ID, w.ID, member.ID)
	}
	_, err = s.UpdateDocumentText(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, "Revoked while uploading")
	wantError(t, err, domain.ErrNotFound)
	objects.putHook = nil
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	objects.putHook = func(context.Context, string) error { return blob.ErrUnavailable }
	_, err = s.UpdateDocumentText(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, "Upload acknowledgement lost")
	wantError(t, err, blob.ErrUnavailable)
	objects.putHook = nil
	// Only this failed upload remains queued, with its uncertain-write grace.
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_text_admission_failure BEFORE INSERT ON document_indexes FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced text admission failure'`)
	must(t, err)
	_, err = s.UpdateDocumentText(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, "Index admission must roll back")
	if err == nil {
		t.Fatal("text update ignored index admission failure")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_text_admission_failure`)
	must(t, err)
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	current, err := s.Document(ctx, owner.ID, w.ID, write.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, write.Document) || objects.count() != 1 {
		t.Fatal("failed edit changed the head, timestamp or source objects")
	}
	for _, table := range []string{"document_versions", "document_indexes"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 1 {
			t.Fatalf("failed edit retained %s rows", table)
		}
	}
	checkDocumentContent(t, s, owner.ID, w.ID, write.Version, []byte("stable"))
}

func TestMySQLDocumentUpdateConcurrentCASPreservesRenamedTitle(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	u, w := testUser(t, s, "updatetextcas")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Original title", "notes.md", []byte("first"))
	must(t, err)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	objects.putHook = func(ctx context.Context, _ string) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	type result struct {
		write domain.DocumentWrite
		err   error
	}
	results := make(chan result, 2)
	for _, text := range []string{"Left edit", "Right edit"} {
		go func(content string) {
			write, err := s.UpdateDocumentText(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, content)
			results <- result{write, err}
		}(text)
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("concurrent edit preflight held a database lock during object upload")
		}
	}
	_, err = s.UpdateDocument(ctx, u.ID, w.ID, first.Document.ID, "Renamed during edits")
	must(t, err)
	close(release)
	var winner domain.DocumentWrite
	var successes, conflicts int
	for range 2 {
		outcome := <-results
		if outcome.err == nil {
			successes++
			winner = outcome.write
		} else if errors.Is(outcome.err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(outcome.err)
		}
	}
	objects.putHook = nil
	if successes != 1 || conflicts != 1 || winner.Version.Number != 2 || winner.Document.Title != "Renamed during edits" {
		t.Fatalf("text CAS lost serialization or renamed title: success=%d conflicts=%d winner=%+v", successes, conflicts, winner)
	}
	_, err = s.UpdateDocumentText(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "Stale edit")
	wantError(t, err, domain.ErrConflict)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 2 {
		t.Fatal("CAS loser left an object or removed a committed version")
	}
	current, err := s.Document(ctx, u.ID, w.ID, first.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, winner.Document) {
		t.Fatal("stale edit changed the document")
	}
	for _, table := range []string{"document_versions", "document_indexes"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 2 {
			t.Fatalf("CAS loser retained %s metadata", table)
		}
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, []byte("first"))
}

func TestMySQLDocumentUpdateValidationAndTextLimits(t *testing.T) {
	s, objects := documentUpdateStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "updatevalidation")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Original", "notes.TxT", []byte("original"))
	must(t, err)
	for _, title := range []string{"", " \t\n ", "con\ntrol", strings.Repeat("x", 256), string([]byte{0xff})} {
		_, err = s.UpdateDocument(ctx, u.ID, w.ID, write.Document.ID, title)
		wantError(t, err, domain.ErrInvalid)
	}
	for _, content := range []string{strings.Repeat("x", domain.MaxEditableDocumentBytes+1), string([]byte{0xff}), "nul\x00", "vertical\vtab", "delete\x7f", "next\u0085line"} {
		_, err = s.UpdateDocumentText(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, content)
		wantError(t, err, domain.ErrInvalid)
	}
	_, err = s.UpdateDocumentText(ctx, u.ID, w.ID, write.Document.ID, "", "missing base")
	wantError(t, err, domain.ErrInvalid)
	if objects.count() != 1 {
		t.Fatal("invalid input reached object storage")
	}
	current, err := s.Document(ctx, u.ID, w.ID, write.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, write.Document) {
		t.Fatal("invalid input changed document state")
	}
	maxTitle := strings.Repeat("文", 85)
	_, err = s.UpdateDocument(ctx, u.ID, w.ID, write.Document.ID, "  "+maxTitle+"  ")
	must(t, err)
	maxText := strings.Repeat("x", domain.MaxEditableDocumentBytes-3) + "文"
	maxWrite, err := s.UpdateDocumentText(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, maxText)
	must(t, err)
	if maxWrite.Version.SizeBytes != domain.MaxEditableDocumentBytes || maxWrite.Document.Title != maxTitle || maxWrite.Version.Filename != "notes.TxT" {
		t.Fatal("exact byte limits or case-insensitive extension handling failed")
	}
	checkDocumentContent(t, s, u.ID, w.ID, maxWrite.Version, []byte(maxText))
	for _, filename := range []string{"file.pdf", "file.docx", "file.json", "noextension"} {
		binary, err := s.CreateDocument(ctx, u.ID, w.ID, "Not editable", filename, []byte("source bytes"))
		must(t, err)
		before := objects.count()
		_, err = s.UpdateDocumentText(ctx, u.ID, w.ID, binary.Document.ID, binary.Version.ID, "text")
		wantError(t, err, domain.ErrInvalid)
		if objects.count() != before {
			t.Fatal("non-text edit created an object")
		}
		checkDocumentContent(t, s, u.ID, w.ID, binary.Version, []byte("source bytes"))
	}
}
