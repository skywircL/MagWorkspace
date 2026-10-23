package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func checkDocumentContent(t *testing.T, s *Store, userID, workspaceID string, expected domain.DocumentVersion, content []byte) {
	t.Helper()
	version, loaded, err := s.DocumentContent(context.Background(), userID, workspaceID, expected.DocumentID, expected.ID)
	must(t, err)
	if !reflect.DeepEqual(version, expected) || !bytes.Equal(loaded, content) {
		t.Fatalf("immutable version changed: expected=%+v loaded=%+v content length=%d", expected, version, len(loaded))
	}
	digest := sha256.Sum256(content)
	if version.SizeBytes != len(content) || version.SHA256 != hex.EncodeToString(digest[:]) || version.MediaType != http.DetectContentType(content) {
		t.Fatalf("server-derived metadata disagrees with bytes: %+v", version)
	}
}

func checkDocumentUnavailable(t *testing.T, s *Store, userID, workspaceID, documentID, versionID string) {
	t.Helper()
	ctx := context.Background()
	_, err := s.Document(ctx, userID, workspaceID, documentID)
	wantError(t, err, domain.ErrNotFound)
	_, _, err = s.DocumentVersions(ctx, userID, workspaceID, documentID, 20, 0)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.DocumentVersion(ctx, userID, workspaceID, documentID, versionID)
	wantError(t, err, domain.ErrNotFound)
	_, _, err = s.DocumentContent(ctx, userID, workspaceID, documentID, versionID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.AppendDocumentVersion(ctx, userID, workspaceID, documentID, versionID, "new.txt", []byte("new"))
	wantError(t, err, domain.ErrNotFound)
	wantError(t, s.DeleteDocument(ctx, userID, workspaceID, documentID), domain.ErrNotFound)
}

func TestMySQLDocumentVersionsPersistAndStayImmutable(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "versions")
	firstBytes := []byte("first version\n知识库\x00\xff")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "  Personal notes  ", "photo.png", firstBytes)
	must(t, err)
	if first.Document.Title != "Personal notes" || first.Document.WorkspaceID != w.ID || first.Document.CreatedBy != u.ID || first.Document.CurrentVersionID != first.Version.ID || first.Version.DocumentID != first.Document.ID || first.Version.Number != 1 || first.Version.CreatedBy != u.ID || first.Version.Filename != "photo.png" {
		t.Fatalf("invalid initial document: %+v", first)
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, firstBytes)
	secondBytes := []byte("revised content\n第二版")
	second, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "notes.txt", secondBytes)
	must(t, err)
	if second.Document.ID != first.Document.ID || second.Document.CreatedAt != first.Document.CreatedAt || second.Document.Title != first.Document.Title || second.Document.CurrentVersionID != second.Version.ID || second.Version.Number != 2 || second.Version.ID == first.Version.ID {
		t.Fatalf("append did not preserve document identity: %+v", second)
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, firstBytes)
	versions, total, err := s.DocumentVersions(ctx, u.ID, w.ID, first.Document.ID, 1, 0)
	must(t, err)
	if total != 2 || len(versions) != 1 || versions[0].ID != second.Version.ID {
		t.Fatalf("version pagination/order: total=%d versions=%+v", total, versions)
	}
	versions, total, err = s.DocumentVersions(ctx, u.ID, w.ID, first.Document.ID, 1, 1)
	must(t, err)
	if total != 2 || len(versions) != 1 || !reflect.DeepEqual(versions[0], first.Version) {
		t.Fatalf("historical version metadata changed: %+v", versions)
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	must(t, reopened.Migrate(ctx))
	checkDocumentContent(t, reopened, u.ID, w.ID, first.Version, firstBytes)
	checkDocumentContent(t, reopened, u.ID, w.ID, second.Version, secondBytes)
	loaded, err := reopened.Document(ctx, u.ID, w.ID, first.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(loaded, second.Document) {
		t.Fatalf("document head lost after reconnect: %+v", loaded)
	}
}

func TestMySQLDocumentCASConcurrencyHasNoPartialVersions(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "cas")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Concurrent notes", "first.txt", []byte("original"))
	must(t, err)
	type outcome struct {
		write domain.DocumentWrite
		err   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, text := range []string{"left edit", "right edit"} {
		go func(content string) {
			<-start
			write, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "edited.txt", []byte(content))
			results <- outcome{write, err}
		}(text)
	}
	close(start)
	successes, conflicts := 0, 0
	var winner domain.DocumentWrite
	for range 2 {
		result := <-results
		if result.err == nil {
			successes++
			winner = result.write
		} else if errors.Is(result.err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(result.err)
		}
	}
	if successes != 1 || conflicts != 1 || winner.Version.Number != 2 {
		t.Fatalf("CAS did not serialize: success=%d conflict=%d winner=%+v", successes, conflicts, winner)
	}
	versions, total, err := s.DocumentVersions(ctx, u.ID, w.ID, first.Document.ID, 20, 0)
	must(t, err)
	if total != 2 || len(versions) != 2 || versions[0].ID != winner.Version.ID {
		t.Fatalf("losing CAS wrote a version: %+v total=%d", versions, total)
	}
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "stale.txt", []byte("stale"))
	wantError(t, err, domain.ErrConflict)
	current, err := s.Document(ctx, u.ID, w.ID, first.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, winner.Document) {
		t.Fatalf("stale CAS altered head or timestamp: %+v", current)
	}
	third, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, winner.Version.ID, "third.txt", nil)
	must(t, err)
	if third.Version.Number != 3 {
		t.Fatalf("failed CAS consumed a version number: %+v", third.Version)
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, []byte("original"))
	checkDocumentContent(t, s, u.ID, w.ID, third.Version, nil)
}

func TestMySQLDocumentAuthorizationAndRevocation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "docowner")
	admin, _ := testUser(t, s, "docadmin")
	contributor, _ := testUser(t, s, "doccontributor")
	viewer, _ := testUser(t, s, "docviewer")
	outsider, other := testUser(t, s, "docoutsider")
	for _, member := range []struct{ user, role string }{{admin.Email, "admin"}, {contributor.Email, "contributor"}, {viewer.Email, "viewer"}} {
		_, err := s.AddMember(ctx, owner.ID, w.ID, member.user, member.role)
		must(t, err)
	}
	first, err := s.CreateDocument(ctx, owner.ID, w.ID, "Shared document", "shared.txt", []byte("all members can read"))
	must(t, err)
	for _, user := range []domain.User{owner, admin, contributor, viewer} {
		checkDocumentContent(t, s, user.ID, w.ID, first.Version, []byte("all members can read"))
		docs, total, err := s.Documents(ctx, user.ID, w.ID, 20, 0)
		must(t, err)
		if total != 1 || len(docs) != 1 || docs[0].ID != first.Document.ID {
			t.Fatalf("workspace member cannot list shared documents: %+v", docs)
		}
	}
	for _, user := range []domain.User{owner, admin, contributor} {
		created, err := s.CreateDocument(ctx, user.ID, w.ID, "Writable", "one.txt", nil)
		must(t, err)
		_, err = s.AppendDocumentVersion(ctx, user.ID, w.ID, created.Document.ID, created.Version.ID, "two.txt", []byte("edit"))
		must(t, err)
		must(t, s.DeleteDocument(ctx, user.ID, w.ID, created.Document.ID))
	}
	_, err = s.CreateDocument(ctx, viewer.ID, w.ID, "Forbidden", "no.txt", nil)
	wantError(t, err, domain.ErrForbidden)
	_, err = s.AppendDocumentVersion(ctx, viewer.ID, w.ID, first.Document.ID, first.Version.ID, "no.txt", nil)
	wantError(t, err, domain.ErrForbidden)
	wantError(t, s.DeleteDocument(ctx, viewer.ID, w.ID, first.Document.ID), domain.ErrForbidden)
	checkDocumentUnavailable(t, s, outsider.ID, w.ID, first.Document.ID, first.Version.ID)
	_, _, err = s.Documents(ctx, outsider.ID, w.ID, 20, 0)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.CreateDocument(ctx, outsider.ID, w.ID, "Forbidden", "no.txt", nil)
	wantError(t, err, domain.ErrNotFound)
	// The user is authorized in this other workspace, but the document is not.
	checkDocumentUnavailable(t, s, outsider.ID, other.ID, first.Document.ID, first.Version.ID)
	second, err := s.CreateDocument(ctx, owner.ID, w.ID, "Other document", "other.txt", []byte("separate"))
	must(t, err)
	_, err = s.DocumentVersion(ctx, owner.ID, w.ID, second.Document.ID, first.Version.ID)
	wantError(t, err, domain.ErrNotFound)
	_, _, err = s.DocumentContent(ctx, owner.ID, w.ID, second.Document.ID, first.Version.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.AppendDocumentVersion(ctx, owner.ID, w.ID, second.Document.ID, first.Version.ID, "wrong-base.txt", nil)
	wantError(t, err, domain.ErrConflict)
	must(t, s.DeleteMember(ctx, owner.ID, w.ID, contributor.ID))
	checkDocumentUnavailable(t, s, contributor.ID, w.ID, first.Document.ID, first.Version.ID)
	_, err = s.AddMember(ctx, owner.ID, w.ID, contributor.Email, "viewer")
	must(t, err)
	checkDocumentContent(t, s, contributor.ID, w.ID, first.Version, []byte("all members can read"))
	_, err = s.AppendDocumentVersion(ctx, contributor.ID, w.ID, first.Document.ID, first.Version.ID, "downgraded.txt", nil)
	wantError(t, err, domain.ErrForbidden)
}

func TestMySQLDocumentDeleteAndWorkspaceCascade(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "documentdelete")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Retained history", "one.txt", []byte("retained"))
	must(t, err)
	second, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "two.txt", []byte("also retained"))
	must(t, err)
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, first.Document.ID))
	checkDocumentUnavailable(t, s, u.ID, w.ID, first.Document.ID, first.Version.ID)
	checkDocumentUnavailable(t, s, u.ID, w.ID, first.Document.ID, second.Version.ID)
	documents, total, err := s.Documents(ctx, u.ID, w.ID, 20, 0)
	must(t, err)
	if total != 0 || len(documents) != 0 {
		t.Fatalf("deleted document remained listed: %+v", documents)
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions WHERE document_id=?`, first.Document.ID).Scan(&count))
	if count != 0 {
		t.Fatal("document deletion retained source versions")
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents WHERE id=?`, first.Document.ID).Scan(&count))
	if count != 0 {
		t.Fatal("document deletion retained metadata")
	}
	_, err = s.CreateDocument(ctx, u.ID, w.ID, "Live document", "live.txt", nil)
	must(t, err)
	must(t, s.DeleteWorkspace(ctx, u.ID, w.ID))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&count))
	if count != 0 {
		t.Fatal("workspace cascade retained documents")
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions`).Scan(&count))
	if count != 0 {
		t.Fatal("workspace cascade retained content versions")
	}
}

func TestMySQLDocumentValidationAndAtomicCreation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "validation")
	// Force a failure after the document INSERT to verify rollback of both tables.
	_, err := s.pool.ExecContext(ctx, `ALTER TABLE document_versions ADD CONSTRAINT test_version_failure CHECK(version_number>1)`)
	must(t, err)
	_, err = s.CreateDocument(ctx, u.ID, w.ID, "Will roll back", "test.txt", []byte("valid"))
	wantError(t, err, domain.ErrInvalid)
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&count))
	if count != 0 {
		t.Fatal("failed version insert left a partial document")
	}
	_, err = s.pool.ExecContext(ctx, `ALTER TABLE document_versions DROP CHECK test_version_failure`)
	must(t, err)
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Empty document", "空文档.txt", nil)
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, nil)
	// Fail after the new version INSERT, at the head UPDATE, in this isolated
	// database only. Both the head and the inserted version must roll back.
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_head_failure BEFORE UPDATE ON documents FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced head failure'`)
	must(t, err)
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "rollback.txt", []byte("must be rolled back"))
	if err == nil {
		t.Fatal("expected head update failure")
	}
	unchanged, err := s.Document(ctx, u.ID, w.ID, first.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(unchanged, first.Document) {
		t.Fatalf("failed head update changed document metadata: %+v", unchanged)
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions WHERE document_id=?`, first.Document.ID).Scan(&count))
	if count != 1 {
		t.Fatalf("failed head update retained a newly inserted version: %d", count)
	}
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, nil)
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_head_failure`)
	must(t, err)
	for _, filename := range []string{"", "   ", ".", "..", " ../ ", "../outside", "folder/file", `folder\file`, "null\x00.txt", "line\n.txt", "delete\x7f.txt", "control\u0085.txt", string([]byte{0xff}), strings.Repeat("界", 86)} {
		_, err = s.CreateDocument(ctx, u.ID, w.ID, "Valid title", filename, nil)
		wantError(t, err, domain.ErrInvalid)
		_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, filename, nil)
		wantError(t, err, domain.ErrInvalid)
	}
	for _, title := range []string{"", " \t ", "con\ntrol", string([]byte{0xff}), strings.Repeat("界", 86)} {
		_, err = s.CreateDocument(ctx, u.ID, w.ID, title, "valid.txt", nil)
		wantError(t, err, domain.ErrInvalid)
	}
	must(t, s.SetUploadLimit(8<<20))
	tooLarge := make([]byte, (8<<20)+1)
	_, err = s.CreateDocument(ctx, u.ID, w.ID, "Too large", "large.bin", tooLarge)
	wantError(t, err, domain.ErrInvalid)
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "large.bin", tooLarge)
	wantError(t, err, domain.ErrInvalid)
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, "", "base.txt", nil)
	wantError(t, err, domain.ErrInvalid)
	_, _, err = s.Documents(ctx, u.ID, w.ID, 0, 0)
	wantError(t, err, domain.ErrInvalid)
	_, _, err = s.DocumentVersions(ctx, u.ID, w.ID, first.Document.ID, 20, -1)
	wantError(t, err, domain.ErrInvalid)
	versions, total, err := s.DocumentVersions(ctx, u.ID, w.ID, first.Document.ID, 20, 0)
	must(t, err)
	if total != 1 || len(versions) != 1 || versions[0].ID != first.Version.ID {
		t.Fatal("invalid input changed version history")
	}
	fullSize := tooLarge[:(8 << 20)]
	fullSize[0], fullSize[len(fullSize)-1] = 1, 255
	maximum, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, strings.Repeat("a", 255), fullSize)
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, maximum.Version, fullSize)
}

func TestMySQLDocumentMigrationUpgradeAndInterruptedRetry(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	// Seed the unchanged v1 user/session/message rows while the current runtime
	// has its required tables. Production only serves requests after Migrate;
	// current FinishRun must not be called against the reconstructed old schema.
	u, w := testUser(t, s, "upgrade")
	session, err := s.CreateSession(ctx, u.ID, w.ID, "Existing v1 session")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "old question", time.Minute)
	must(t, err)
	must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, "old answer", "demo"))
	// Only the test's own fresh empty derived/v2 tables are removed to reconstruct v1.
	_, err = s.pool.ExecContext(ctx, `DROP TABLE index_outbox`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE chunks`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE document_indexes`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE index_cleanup`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE document_versions`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `DROP TABLE documents`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=1 WHERE singleton=1`)
	must(t, err)
	// MySQL FK constraint names are schema-scoped. Reserve the v2 name so its
	// second CREATE fails after the documents table has already been committed.
	_, err = s.pool.ExecContext(ctx, `CREATE TABLE migration_blocker (workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT document_versions_document_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id)) ENGINE=InnoDB`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted v2 migration")
	}
	var version, count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != 1 {
		t.Fatalf("failed migration advanced schema marker: %d", version)
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&count))
	_, err = s.pool.ExecContext(ctx, `DROP TABLE migration_blocker`)
	must(t, err)
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- s.Migrate(ctx) }()
	}
	for range 2 {
		must(t, <-results)
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != len(migrationFiles) {
		t.Fatalf("retry did not reach latest schema: %d", version)
	}
	messages, total, err := s.Messages(ctx, u.ID, w.ID, session.ID, 20, 0)
	must(t, err)
	if total != 2 || len(messages) != 2 || messages[1].Content != "old answer" || messages[1].Status != "completed" {
		t.Fatalf("v1 session data changed during migration: %+v", messages)
	}
	created, err := s.CreateDocument(ctx, u.ID, w.ID, "After upgrade", "new.txt", []byte("v2 works"))
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, created.Version, []byte("v2 works"))
	must(t, s.Migrate(ctx))
}
