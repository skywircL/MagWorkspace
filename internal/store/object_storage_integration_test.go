package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func namedMemoryObjects(name string) *memoryObjects {
	objects := newMemoryObjects()
	objects.storageID = domain.StorageBindingID("memory", "store-test", name)
	return objects
}

func expireObjectCleanup(t *testing.T, s *Store, binding, key string) {
	t.Helper()
	_, err := s.pool.Exec(`UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE storage_id=? AND object_key=?`, binding, key)
	must(t, err)
}

func TestMySQLObjectsStorageBindingSurvivesConfigurationChange(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "object-binding")
	a, b := namedMemoryObjects("a"), namedMemoryObjects("b")
	s.SetObjectStorage(a)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Original destination", "a.txt", []byte("original bytes"))
	must(t, err)
	if write.Version.StorageID != a.StorageID() {
		t.Fatal("version did not freeze the upload destination")
	}
	raw, err := json.Marshal(write.Version)
	must(t, err)
	if strings.Contains(string(raw), a.StorageID()) || strings.Contains(string(raw), "storage_id") {
		t.Fatal("private storage binding escaped public version metadata")
	}
	// The same key in the new destination must never be used as a fallback.
	must(t, b.Put(ctx, write.Version.ObjectKey, []byte("original bytes")))
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(b)
	_, content, err := reopened.DocumentContent(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID)
	wantError(t, err, blob.ErrUnavailable)
	if content != nil {
		t.Fatal("read fell back to the current destination")
	}
	reopened.SetObjectCleanupStorages([]blob.Storage{a})
	checkDocumentContent(t, reopened, u.ID, w.ID, write.Version, []byte("original bytes"))
	// Repeat a process restart without the old target configured before delete.
	reopened.Close()
	reopened, err = Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	reopened.SetObjectStorage(b)
	must(t, reopened.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
	wantError(t, reopened.CleanupObjects(ctx), blob.ErrUnavailable)
	var attempts int
	var code string
	var attempted sql.NullTime
	var deferred bool
	must(t, reopened.pool.QueryRow(`SELECT attempts,last_error_code,last_attempt_at,available_at>UTC_TIMESTAMP(6) FROM object_cleanup WHERE storage_id=? AND object_key=?`, a.StorageID(), write.Version.ObjectKey).Scan(&attempts, &code, &attempted, &deferred))
	if attempts != 1 || code != "storage_binding_unavailable" || !attempted.Valid || !deferred || a.count() != 1 || b.count() != 1 {
		t.Fatal("unknown destination lost its intent, diagnosis, or original bytes")
	}
	reopened.SetObjectCleanupStorages([]blob.Storage{a})
	expireObjectCleanup(t, reopened, a.StorageID(), write.Version.ObjectKey)
	must(t, reopened.CleanupObjects(ctx))
	assertObjectQueueCount(t, reopened, 0)
	if a.count() != 0 || b.count() != 1 {
		t.Fatal("cleanup did not delete exclusively from the original destination")
	}
}

func TestMySQLObjectsSameKeyReferencesAreScopedToStorage(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "object-key-scope")
	a, b := namedMemoryObjects("a"), namedMemoryObjects("b")
	s.SetObjectStorage(a)
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "A", "a.txt", []byte("same bytes"))
	must(t, err)
	s.SetObjectStorage(b)
	second, err := s.CreateDocument(ctx, u.ID, w.ID, "B", "b.txt", []byte("same bytes"))
	must(t, err)
	must(t, b.Put(ctx, first.Version.ObjectKey, []byte("same bytes")))
	must(t, b.Delete(ctx, second.Version.ObjectKey))
	_, err = s.pool.Exec(`UPDATE document_versions SET object_key=? WHERE id=?`, first.Version.ObjectKey, second.Version.ID)
	must(t, err)
	second.Version.ObjectKey = first.Version.ObjectKey
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, first.Document.ID))
	must(t, s.CleanupObjects(ctx))
	if a.count() != 0 || b.count() != 1 {
		t.Fatal("a reference in B protected A or caused deletion in B")
	}
	checkDocumentContent(t, s, u.ID, w.ID, second.Version, []byte("same bytes"))
	// A lost final commit acknowledgement must not retire a published object.
	s.abandonDocumentObject(stagedDocumentObject{key: second.Version.ObjectKey, storageID: b.StorageID()})
	assertObjectQueueCount(t, s, 0)
	checkDocumentContent(t, s, u.ID, w.ID, second.Version, []byte("same bytes"))
}

type writeAfterCleanupObjects struct {
	*memoryObjects
	beforeWrite func(context.Context, string) error
}

func (m *writeAfterCleanupObjects) Put(ctx context.Context, key string, content []byte) error {
	if err := m.beforeWrite(ctx, key); err != nil {
		return err
	}
	return m.memoryObjects.Put(ctx, key, content)
}

func TestMySQLObjectsRetirementReclaimsPutAfterFirstDelete(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "object-late-put")
	objects := &writeAfterCleanupObjects{memoryObjects: namedMemoryObjects("late")}
	var lateKey string
	objects.beforeWrite = func(ctx context.Context, key string) error {
		lateKey = key
		// This fake deliberately writes AFTER cleanup, unlike memoryObjects'
		// ordinary hook. Model a writer resuming after its reservation expired.
		_, err := s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE storage_id=? AND object_key=?`, objects.StorageID(), key)
		if err != nil {
			return err
		}
		return s.CleanupObjects(ctx)
	}
	s.SetObjectStorage(objects)
	_, err := s.CreateDocument(ctx, u.ID, w.ID, "Late upload", "late.txt", []byte("late bytes"))
	wantError(t, err, domain.ErrConflict)
	if objects.count() != 1 {
		t.Fatal("fixture did not complete its Put after the first delete")
	}
	assertObjectQueueCount(t, s, 1)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("failed publication did not requeue the late object")
	}
	assertObjectQueueCount(t, s, 0)
	// Also model an already-disconnected remote write completing after the
	// writer can no longer requeue: permanent retirement must recover it.
	must(t, objects.memoryObjects.Put(ctx, lateKey, []byte("late bytes")))
	_, err = s.pool.Exec(`UPDATE cleanup_retirements SET next_check_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE kind='object' AND storage_id=? AND resource_key=?`, objects.StorageID(), lateKey)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 0 {
		t.Fatal("permanent retirement lost responsibility after a successful delete")
	}
	var retired int
	must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM cleanup_retirements WHERE kind='object' AND storage_id=? AND resource_key=?`, objects.StorageID(), lateKey).Scan(&retired))
	if retired != 1 {
		t.Fatal("successful cleanup removed the permanent retirement")
	}
}

func TestMySQLObjectsOldCleanupAcknowledgementPreservesNewIntent(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "object-cleanup-token")
	objects := namedMemoryObjects("tokens")
	s.SetObjectStorage(objects)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Delete", "a.txt", []byte("bytes"))
	must(t, err)
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
	started, release := make(chan struct{}), make(chan struct{})
	objects.deleteHook = func(ctx context.Context, _ string) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- s.CleanupObjects(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach external deletion")
	}
	var oldToken string
	must(t, s.pool.QueryRow(`SELECT id FROM object_cleanup WHERE storage_id=? AND object_key=?`, objects.StorageID(), write.Version.ObjectKey).Scan(&oldToken))
	must(t, s.transaction(ctx, func(tx *sql.Tx) error {
		return queueObjectCleanupTx(ctx, tx, objects.StorageID(), write.Version.ObjectKey)
	}))
	close(release)
	must(t, <-finished)
	var newToken string
	must(t, s.pool.QueryRow(`SELECT id FROM object_cleanup WHERE storage_id=? AND object_key=?`, objects.StorageID(), write.Version.ObjectKey).Scan(&newToken))
	if oldToken == newToken {
		t.Fatal("requeue reused a live deletion acknowledgement")
	}
	objects.deleteHook = nil
	must(t, s.CleanupObjects(ctx))
	assertObjectQueueCount(t, s, 0)
}

func TestMySQLObjectsUnknownBindingsCannotBlockConfiguredCleanup(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	objects := namedMemoryObjects("configured")
	s.SetObjectStorage(objects)
	for i := range 101 {
		id, err := newID()
		must(t, err)
		binding := domain.StorageBindingID("memory", "store-test", "missing")
		if i == 0 {
			binding = "" // Unbound historical work must never use current storage.
		}
		_, err = s.pool.Exec(`INSERT INTO object_cleanup(id,storage_id,object_key,available_at) VALUES(?,?,?,DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 HOUR))`, id, binding, fmt.Sprintf("missing/%d", i))
		must(t, err)
	}
	key := "configured/later"
	must(t, objects.Put(ctx, key, []byte("cleanup")))
	must(t, s.transaction(ctx, func(tx *sql.Tx) error { return queueObjectCleanupTx(ctx, tx, objects.StorageID(), key) }))
	wantError(t, s.CleanupObjects(ctx), blob.ErrUnavailable)
	if objects.count() != 0 {
		t.Fatal("unknown bindings consumed the entire scan ahead of configured work")
	}
	var failed, pending int
	must(t, s.pool.QueryRow(`SELECT SUM(last_error_code='storage_binding_unavailable'),COUNT(*) FROM object_cleanup`).Scan(&failed, &pending))
	if failed != 99 || pending != 101 {
		t.Fatalf("unknown work was discarded or not diagnosed: failed=%d pending=%d", failed, pending)
	}
}
