package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

// Only object I/O is in memory. Authorization, transactions, cleanup and
// migration assertions all exercise a real isolated MySQL database.
type memoryObjects struct {
	storageID  string
	mu         sync.Mutex
	data       map[string][]byte
	putHook    func(context.Context, string) error
	readHook   func(context.Context, string) error
	deleteHook func(context.Context, string) error
}

func newMemoryObjects() *memoryObjects {
	return &memoryObjects{storageID: domain.StorageBindingID("memory", "store-test", "objects"), data: map[string][]byte{}}
}
func (m *memoryObjects) StorageID() string { return m.storageID }
func (m *memoryObjects) Put(ctx context.Context, key string, content []byte) error {
	m.mu.Lock()
	if _, exists := m.data[key]; exists {
		m.mu.Unlock()
		return blob.ErrIntegrity
	}
	m.data[key] = append([]byte{}, content...)
	m.mu.Unlock()
	if m.putHook != nil {
		return m.putHook(ctx, key)
	}
	return ctx.Err()
}
func (m *memoryObjects) Read(ctx context.Context, key string, maximum int) ([]byte, error) {
	m.mu.Lock()
	data, exists := m.data[key]
	data = append([]byte{}, data...)
	m.mu.Unlock()
	if !exists {
		return nil, blob.ErrUnavailable
	}
	if m.readHook != nil {
		if err := m.readHook(ctx, key); err != nil {
			return nil, err
		}
	}
	if len(data) > maximum {
		return nil, blob.ErrIntegrity
	}
	return data, ctx.Err()
}
func (m *memoryObjects) Delete(ctx context.Context, key string) error {
	if m.deleteHook != nil {
		if err := m.deleteHook(ctx, key); err != nil {
			return err
		}
	}
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return ctx.Err()
}
func (m *memoryObjects) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

func assertObjectQueueCount(t *testing.T, s *Store, count int) {
	t.Helper()
	var actual int
	must(t, s.pool.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM object_cleanup`).Scan(&actual))
	if actual != count {
		t.Fatalf("object cleanup count = %d, want %d", actual, count)
	}
}

func TestMySQLObjectsPersistHistoryAndVerifyContent(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "objecthistory")
	legacy, err := s.CreateDocument(ctx, u.ID, w.ID, "Legacy", "inline.txt", []byte("old inline"))
	must(t, err)
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "Object", "source.txt", []byte("immutable original"))
	must(t, err)
	if first.Version.ObjectKey == "" {
		t.Fatal("new version did not reference object storage")
	}
	var inlineIsNull bool
	must(t, s.pool.QueryRowContext(ctx, `SELECT content IS NULL FROM document_versions WHERE id=?`, first.Version.ID).Scan(&inlineIsNull))
	if !inlineIsNull {
		t.Fatal("new object bytes remained in MySQL")
	}
	second, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, "empty.txt", nil)
	must(t, err)
	if second.Version.Number != 2 || second.Version.ObjectKey == first.Version.ObjectKey || second.Version.SizeBytes != 0 {
		t.Fatal("append did not create a distinct immutable empty object")
	}
	assertObjectQueueCount(t, s, 0)
	checkDocumentContent(t, s, u.ID, w.ID, first.Version, []byte("immutable original"))
	checkDocumentContent(t, s, u.ID, w.ID, second.Version, nil)
	checkDocumentContent(t, s, u.ID, w.ID, legacy.Version, []byte("old inline"))
	encoded, err := json.Marshal(first.Version)
	must(t, err)
	if strings.Contains(string(encoded), first.Version.ObjectKey) || strings.Contains(string(encoded), "object_key") {
		t.Fatal("private object key leaked through version metadata")
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(objects)
	checkDocumentContent(t, reopened, u.ID, w.ID, first.Version, []byte("immutable original"))
	checkDocumentContent(t, reopened, u.ID, w.ID, legacy.Version, []byte("old inline"))
	profile := testIndexProfile()
	_, _, err = reopened.EnqueueDocumentIndex(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID, profile)
	must(t, err)
	input, err := reopened.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil || input.Version.ObjectKey != first.Version.ObjectKey {
		t.Fatalf("claim did not preserve source metadata: %+v", input)
	}
	content, err := reopened.LoadIndexSource(ctx, *input)
	must(t, err)
	if string(content) != "immutable original" {
		t.Fatal("worker loaded a different source")
	}
	objects.mu.Lock()
	objects.data[first.Version.ObjectKey] = []byte("tampered original")
	objects.mu.Unlock()
	_, content, err = reopened.DocumentContent(ctx, u.ID, w.ID, first.Document.ID, first.Version.ID)
	wantError(t, err, blob.ErrIntegrity)
	if content != nil {
		t.Fatal("corrupt bytes were returned")
	}
	_, err = reopened.LoadIndexSource(ctx, *input)
	wantError(t, err, blob.ErrIntegrity)
}

func TestMySQLObjectsUploadFailuresRecheckPermissionsAndRollback(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "objectowner")
	member, _ := testUser(t, s, "objectmember")
	_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	objects.putHook = func(ctx context.Context, _ string) error {
		// This operation needs the same workspace lock. It also proves Put does
		// not hold the authorization transaction open during external I/O.
		return s.DeleteMember(ctx, owner.ID, w.ID, member.ID)
	}
	_, err = s.CreateDocument(ctx, member.ID, w.ID, "Revoked during upload", "source.txt", []byte("private"))
	wantError(t, err, domain.ErrNotFound)
	assertObjectQueueCount(t, s, 1)
	if objects.count() != 1 {
		t.Fatal("expected uploaded orphan before cleanup")
	}
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("revoked upload was not reclaimed")
	}
	objects.putHook = nil
	_, err = s.CreateDocument(ctx, member.ID, w.ID, "Unauthorized", "source.txt", []byte("private"))
	wantError(t, err, domain.ErrNotFound)
	if objects.count() != 0 {
		t.Fatal("unauthorized request reached object storage")
	}
	objects.putHook = func(context.Context, string) error { return blob.ErrUnavailable }
	_, err = s.CreateDocument(ctx, owner.ID, w.ID, "Lost upload acknowledgement", "source.txt", []byte("uncertain"))
	wantError(t, err, blob.ErrUnavailable)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("uncertain Put lost its protection window")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("uncertain upload was not reclaimed")
	}
	objects.putHook = nil
	write, err := s.CreateDocument(ctx, owner.ID, w.ID, "Stable", "source.txt", []byte("stable"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_object_head_failure BEFORE UPDATE ON documents FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced object head failure'`)
	must(t, err)
	_, err = s.AppendDocumentVersion(ctx, owner.ID, w.ID, write.Document.ID, write.Version.ID, "next.txt", []byte("must rollback"))
	if err == nil {
		t.Fatal("head write failure did not fail object append")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_object_head_failure`)
	must(t, err)
	current, err := s.Document(ctx, owner.ID, w.ID, write.Document.ID)
	must(t, err)
	if !reflect.DeepEqual(current, write.Document) {
		t.Fatal("failed object append changed head or timestamp")
	}
	_, count, err := s.DocumentVersions(ctx, owner.ID, w.ID, write.Document.ID, 20, 0)
	must(t, err)
	if count != 1 {
		t.Fatal("failed object append retained version metadata")
	}
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("rollback cleanup removed a referenced object or retained an orphan")
	}
	checkDocumentContent(t, s, owner.ID, w.ID, write.Version, []byte("stable"))
	profile := testIndexProfile()
	s.indexProfile = &profile
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER test_object_admission_failure BEFORE INSERT ON document_indexes FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced object admission failure'`)
	must(t, err)
	_, err = s.CreateDocument(ctx, owner.ID, w.ID, "Failed automatic index admission", "source.txt", []byte("rollback both"))
	if err == nil {
		t.Fatal("automatic admission failure did not roll back object metadata")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER test_object_admission_failure`)
	must(t, err)
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("failed automatic admission did not reclaim only the orphan")
	}
	_, total, err := s.Documents(ctx, owner.ID, w.ID, 20, 0)
	must(t, err)
	if total != 1 {
		t.Fatal("failed automatic admission retained document metadata")
	}
}

func TestMySQLObjectsConcurrentAppendAndCleanupFencing(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "objectcas")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "CAS", "source.txt", []byte("first"))
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
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, "next.txt", []byte("next"))
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent uploads did not proceed outside database locks")
		}
	}
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 3 {
		t.Fatal("cleanup removed an active upload")
	}
	close(release)
	var successes, conflicts int
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent object CAS: %d successes, %d conflicts", successes, conflicts)
	}
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 2 {
		t.Fatal("CAS loser was not reclaimed separately from historical versions")
	}
	objects.putHook = func(ctx context.Context, key string) error {
		// Simulate a delayed upload whose durable protection window expired.
		_, err := s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE object_key=?`, key)
		if err != nil {
			return err
		}
		return s.CleanupObjects(ctx)
	}
	_, err = s.CreateDocument(ctx, u.ID, w.ID, "Late upload", "late.txt", []byte("late"))
	wantError(t, err, domain.ErrConflict)
	if objects.count() != 2 {
		t.Fatal("late upload survived cleanup or damaged earlier versions")
	}
	// The rejected writer requeues its retired key in case its Put completed
	// after the first delete. Repeated deletion remains safe for live versions.
	must(t, s.CleanupObjects(ctx))
	assertObjectQueueCount(t, s, 0)
}

func TestMySQLObjectsCleanupSurvivesRestartAndWorkspaceDeletion(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "objectcleanup")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	_, err := s.CreateDocument(ctx, u.ID, w.ID, "Live document", "source.txt", []byte("retained"))
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("cleanup reclaimed a referenced source")
	}
	// A completed upload without its final transaction models a killed request.
	documentID, err := newID()
	must(t, err)
	versionID, err := newID()
	must(t, err)
	key, err := s.stageDocumentObject(ctx, u.ID, w.ID, documentID, versionID, "", []byte("orphan after crash"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE object_key=?`, key.key)
	must(t, err)
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(objects)
	objects.deleteHook = func(context.Context, string) error { return blob.ErrUnavailable }
	wantError(t, reopened.CleanupObjects(ctx), blob.ErrUnavailable)
	assertObjectQueueCount(t, reopened, 1)
	var attempts int
	var errorCode string
	must(t, reopened.pool.QueryRowContext(ctx, `SELECT attempts,last_error_code FROM object_cleanup WHERE storage_id=? AND object_key=?`, objects.StorageID(), key.key).Scan(&attempts, &errorCode))
	if attempts != 1 || errorCode != "object_storage_unavailable" {
		t.Fatal("failed cleanup did not retain a safe retry diagnosis")
	}
	objects.deleteHook = nil
	_, err = reopened.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND)`)
	must(t, err)
	must(t, reopened.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("restart did not recover orphan without touching live source")
	}
	must(t, reopened.DeleteWorkspace(ctx, u.ID, w.ID))
	assertObjectQueueCount(t, reopened, 1)
	var count int
	must(t, reopened.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions`).Scan(&count))
	if count != 0 {
		t.Fatal("workspace deletion retained version metadata")
	}
	must(t, reopened.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("workspace deletion lost durable source cleanup")
	}
	assertObjectQueueCount(t, reopened, 0)
}

func TestMySQLObjectsReadsReauthorizeAfterExternalIO(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	owner, w := testUser(t, s, "objectreadowner")
	member, _ := testUser(t, s, "objectreader")
	_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	write, err := s.CreateDocument(ctx, member.ID, w.ID, "Protected", "source.txt", []byte("private source"))
	must(t, err)
	objects.readHook = func(ctx context.Context, _ string) error { return s.DeleteMember(ctx, owner.ID, w.ID, member.ID) }
	_, content, err := s.DocumentContent(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID)
	wantError(t, err, domain.ErrNotFound)
	if content != nil {
		t.Fatal("revoked download returned source bytes")
	}
	_, err = s.AddMember(ctx, owner.ID, w.ID, member.Email, "contributor")
	must(t, err)
	profile := testIndexProfile()
	_, _, err = s.EnqueueDocumentIndex(ctx, member.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing index job")
	}
	content, err = s.LoadIndexSource(ctx, *input)
	wantError(t, err, domain.ErrForbidden)
	if content != nil {
		t.Fatal("revoked worker source read returned bytes")
	}
}

func TestMySQLObjectsMigrationPreservesInlineAndRetriesPartialDDL(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "objectupgrade")
	legacy, err := s.CreateDocument(ctx, u.ID, w.ID, "Legacy", "old.txt", []byte("legacy bytes"))
	must(t, err)
	// Reconstruct v3 only in this test's isolated database, retaining source bytes.
	for _, statement := range []string{
		`DROP TABLE object_cleanup`,
		`ALTER TABLE document_versions DROP CHECK document_versions_storage_v2`,
		`ALTER TABLE document_versions DROP CHECK document_versions_inline_binding`,
		`ALTER TABLE document_versions DROP COLUMN object_key, MODIFY COLUMN content mediumblob NOT NULL`,
		`ALTER TABLE document_versions ADD CONSTRAINT document_versions_content_size CHECK(size_bytes BETWEEN 0 AND 8388608 AND size_bytes=OCTET_LENGTH(content))`,
		`UPDATE schema_version SET version=3 WHERE singleton=1`,
		`CREATE TABLE migration_blocker (id int PRIMARY KEY, CONSTRAINT document_versions_storage CHECK(id>0)) ENGINE=InnoDB`,
	} {
		_, err = s.pool.ExecContext(ctx, statement)
		must(t, err)
	}
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted v4 migration")
	}
	var version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != 3 {
		t.Fatal("partial v4 migration advanced its marker")
	}
	var key string
	must(t, s.pool.QueryRowContext(ctx, `SELECT object_key FROM document_versions WHERE id=?`, legacy.Version.ID).Scan(&key))
	if key != "" {
		t.Fatal("partial migration rewrote inline storage")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TABLE migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	checkDocumentContent(t, s, u.ID, w.ID, legacy.Version, []byte("legacy bytes"))
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	current, err := s.AppendDocumentVersion(ctx, u.ID, w.ID, legacy.Document.ID, legacy.Version.ID, "new.txt", []byte("new object"))
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, current.Version, []byte("new object"))
	checkDocumentContent(t, s, u.ID, w.ID, legacy.Version, []byte("legacy bytes"))
	if objects.count() != 1 {
		t.Fatal("migration copied old bytes or failed to store new object")
	}
}
