package store

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func defaultKnowledgeBase(t *testing.T, s *Store, uid, wid string) domain.KnowledgeBase {
	t.Helper()
	items, err := s.KnowledgeBases(context.Background(), uid, wid)
	must(t, err)
	for _, kb := range items {
		if kb.IsDefault {
			return kb
		}
	}
	t.Fatal("missing default knowledge base")
	return domain.KnowledgeBase{}
}

func TestMySQLKnowledgeBasesDefaultElectionAndDatabaseScope(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "kbdefaults")
	_, foreign := testUser(t, s, "kbforeign")
	first := defaultKnowledgeBase(t, s, u.ID, w.ID)
	second, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Second", "independent")
	must(t, err)
	third, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Third", "")
	must(t, err)
	write, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, second.ID, "Kept document", "notes.txt", []byte("kept"))
	must(t, err)
	if write.Document.KnowledgeBaseID != second.ID {
		t.Fatal("explicit document lost its knowledge base")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE documents SET workspace_id=? WHERE id=?`, foreign.ID, write.Document.ID)
	wantError(t, dbError(err), domain.ErrNotFound)
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, first.ID))
	if got := defaultKnowledgeBase(t, s, u.ID, w.ID); got.ID != second.ID {
		t.Fatal("deleting default did not promote the oldest remaining knowledge base")
	}
	legacyUpload, err := s.CreateDocument(ctx, u.ID, w.ID, "Default upload", "default.txt", nil)
	must(t, err)
	if legacyUpload.Document.KnowledgeBaseID != second.ID {
		t.Fatal("legacy upload did not use the elected default")
	}
	documents, total, err := s.KnowledgeBaseDocuments(ctx, u.ID, w.ID, second.ID, 1, 0)
	must(t, err)
	if total != 2 || len(documents) != 1 || documents[0].KnowledgeBaseID != second.ID {
		t.Fatal("knowledge-base document pagination lost its scope")
	}
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, second.ID))
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, third.ID))
	_, err = s.CreateDocument(ctx, u.ID, w.ID, "No destination", "absent.txt", nil)
	wantError(t, err, domain.ErrConflict)
	// Concurrent first creations share the workspace lock and elect exactly one.
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Concurrent", "")
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		must(t, err)
	}
	items, err := s.KnowledgeBases(ctx, u.ID, w.ID)
	must(t, err)
	defaults := 0
	for _, kb := range items {
		if kb.IsDefault {
			defaults++
		}
	}
	if len(items) != 4 || defaults != 1 {
		t.Fatalf("concurrent creation default invariant: items=%d defaults=%d", len(items), defaults)
	}
}

func TestMySQLKnowledgeBaseDeletionFencesUploadsWithoutRedirect(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "explicit"}[explicit], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "kbdelayedupload")
			first := defaultKnowledgeBase(t, s, u.ID, w.ID)
			second, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Successor", "")
			must(t, err)
			objects := newMemoryObjects()
			s.SetObjectStorage(objects)
			objects.putHook = func(ctx context.Context, _ string) error {
				return s.DeleteKnowledgeBase(ctx, u.ID, w.ID, first.ID)
			}
			if explicit {
				_, err = s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, first.ID, "Late", "late.txt", []byte("late source"))
			} else {
				_, err = s.CreateDocument(ctx, u.ID, w.ID, "Late", "late.txt", []byte("late source"))
			}
			wantError(t, err, domain.ErrNotFound)
			if got := defaultKnowledgeBase(t, s, u.ID, w.ID); got.ID != second.ID {
				t.Fatal("deletion did not elect successor")
			}
			_, total, err := s.KnowledgeBaseDocuments(ctx, u.ID, w.ID, second.ID, 20, 0)
			must(t, err)
			if total != 0 {
				t.Fatal("late upload silently moved into the successor knowledge base")
			}
			assertObjectQueueCount(t, s, 1)
			must(t, s.CleanupObjects(ctx))
			if objects.count() != 0 {
				t.Fatal("late knowledge-base upload was not compensated")
			}
		})
	}
}

func TestMySQLKnowledgeBaseDeletionRollbackAndRetainedWorkspace(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	evidence := f.record(t, f.hit)[0]
	kb := defaultKnowledgeBase(t, f.s, f.u.ID, f.w.ID)
	kept, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Keep", "")
	must(t, err)
	other, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, kept.ID, "Keep source", "keep.txt", []byte("keep source"))
	must(t, err)
	memory, err := f.s.CreateMemory(ctx, f.u.ID, f.w.ID, memoryInput("Prefer concise replies", "reply"))
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `CREATE TRIGGER kb_delete_failure BEFORE DELETE ON knowledge_bases FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced kb delete failure'`)
	must(t, err)
	if err = f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, kb.ID); err == nil {
		t.Fatal("knowledge-base delete failure was ignored")
	}
	checkDocumentContent(t, f.s, f.u.ID, f.w.ID, f.write.Version, []byte("住宿费用每晚最高报销 500 元。"))
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_cleanup`).Scan(&count))
	if count != 0 {
		t.Fatal("failed knowledge-base deletion committed cleanup")
	}
	_, err = f.s.pool.ExecContext(ctx, `DROP TRIGGER kb_delete_failure`)
	must(t, err)
	// An active run is retained, but its now-invalid source cannot be delivered.
	must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, kb.ID))
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, evidence.ID)
	wantError(t, err, domain.ErrNotFound)
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE id=?`, evidence.ID).Scan(&count))
	if count != 1 {
		t.Fatal("knowledge-base deletion washed away source audit constraints")
	}
	for _, table := range []string{"workspaces", "workspace_members", "sessions", "runs", "user_memories"} {
		must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count == 0 {
			t.Fatalf("knowledge-base deletion removed %s", table)
		}
	}
	items, err := f.s.Memories(ctx, f.u.ID, f.w.ID, "active")
	must(t, err)
	if len(items) != 1 || items[0].ID != memory.ID {
		t.Fatal("knowledge-base deletion changed personal memory")
	}
	checkDocumentContent(t, f.s, f.u.ID, f.w.ID, other.Version, []byte("keep source"))
}

func TestMySQLKnowledgeBaseMigrationResumesAndPreservesChoices(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "kbmigration")
	secondOwner, empty := testUser(t, s, "kbempty")
	// Existing databases can have several owners. Backfill elects one creator
	// rather than inserting multiple conflicting default rows for that space.
	_, err := s.pool.ExecContext(ctx, `INSERT INTO workspace_members(workspace_id,user_id,role) VALUES(?,?,'owner')`, w.ID, secondOwner.ID)
	must(t, err)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Legacy source", "old.txt", []byte("immutable old source"))
	must(t, err)
	// Rewind dependent lineage constraints before removing the pre-v17 field.
	indexLineageLegacySchema(t, s)
	for _, query := range []string{
		`ALTER TABLE documents DROP FOREIGN KEY documents_knowledge_base_fk`,
		`ALTER TABLE documents DROP INDEX documents_knowledge_base_live, DROP COLUMN knowledge_base_id`,
		`DROP TABLE knowledge_bases`,
		`UPDATE schema_version SET version=16 WHERE singleton=1`,
		`CREATE TABLE kb_migration_blocker (workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT documents_knowledge_base_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id)) ENGINE=InnoDB`,
	} {
		_, err = s.pool.ExecContext(ctx, query)
		must(t, err)
	}
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted composite foreign-key migration")
	}
	var version, count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM knowledge_bases WHERE default_slot=1`).Scan(&count))
	if version != 16 || count != 2 {
		t.Fatal("partial migration lost default backfill or advanced its marker")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TABLE kb_migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	loaded, err := s.Document(ctx, u.ID, w.ID, write.Document.ID)
	must(t, err)
	if loaded.KnowledgeBaseID != defaultKnowledgeBase(t, s, u.ID, w.ID).ID {
		t.Fatal("legacy document did not receive its workspace default")
	}
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("immutable old source"))
	custom, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Explicit destination", "preserve on replay")
	must(t, err)
	current, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, custom.ID, "Current", "current.txt", []byte("current source"))
	must(t, err)
	var emptyOwner string
	must(t, s.pool.QueryRowContext(ctx, `SELECT user_id FROM workspace_members WHERE workspace_id=? AND role='owner'`, empty.ID).Scan(&emptyOwner))
	must(t, s.DeleteKnowledgeBase(ctx, emptyOwner, empty.ID, defaultKnowledgeBase(t, s, emptyOwner, empty.ID).ID))
	before, err := s.KnowledgeBases(ctx, u.ID, w.ID)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=16 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	after, err := s.KnowledgeBases(ctx, u.ID, w.ID)
	must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("lost-marker replay changed existing knowledge bases")
	}
	loaded, err = s.Document(ctx, u.ID, w.ID, current.Document.ID)
	must(t, err)
	if loaded.KnowledgeBaseID != custom.ID {
		t.Fatal("migration reassigned an explicit destination")
	}
	missing, err := s.KnowledgeBases(ctx, emptyOwner, empty.ID)
	must(t, err)
	if len(missing) != 0 {
		t.Fatal("migration recreated an intentionally deleted last knowledge base")
	}
}

func TestMySQLDocumentDeletionMigrationIsAtomicAndKeepsLeaseProtection(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "deletemigration")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	retired, err := s.CreateDocument(ctx, u.ID, w.ID, "Retired source", "old.txt", []byte("retired"))
	must(t, err)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, retired.Document.ID, retired.Version.ID, testIndexProfile())
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, testIndexProfile().ID, 10*time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing running retired index")
	}
	var lease time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT lease_expires_at FROM document_indexes WHERE id=?`, input.Job.ID).Scan(&lease))
	live, err := s.CreateDocument(ctx, u.ID, w.ID, "Live source", "live.txt", []byte("keep"))
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE documents SET deleted_at=UTC_TIMESTAMP(6) WHERE id=?`, retired.Document.ID)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=17 WHERE singleton=1`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER deletion_migration_blocker BEFORE DELETE ON documents FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced retirement failure'`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected retirement transaction to fail")
	}
	for _, table := range []string{"object_cleanup", "index_cleanup"} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
		if count != 0 {
			t.Fatalf("failed retirement committed %s", table)
		}
	}
	var version, retained int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions WHERE document_id=?`, retired.Document.ID).Scan(&retained))
	if version != 17 || retained != 1 {
		t.Fatal("failed retirement changed its marker or source metadata")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER deletion_migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	var token string
	var deadline time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,not_before FROM index_cleanup WHERE generation=?`, input.Job.Generation).Scan(&token, &deadline))
	if deadline.Before(lease.Add(30 * time.Second)) {
		t.Fatal("migration failed to copy the running lease protection")
	}
	assertObjectQueueCount(t, s, 1)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=17 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	var replayToken string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id FROM index_cleanup WHERE generation=?`, input.Job.Generation).Scan(&replayToken))
	if token != replayToken {
		t.Fatal("lost marker restarted an already completed retirement")
	}
	// The historical migration had no destination snapshot. The new worker
	// retains that responsibility without guessing the current store. This
	// isolated fixture can explicitly bind it after checking the original key.
	wantError(t, s.CleanupObjects(ctx), blob.ErrUnavailable)
	if objects.count() != 2 {
		t.Fatal("unresolved migration deleted bytes")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE object_cleanup SET storage_id=?,available_at=UTC_TIMESTAMP(6) WHERE object_key=? AND storage_id=''`, objects.StorageID(), retired.Version.ObjectKey)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE cleanup_retirements SET storage_id=? WHERE kind='object' AND resource_key=? AND storage_id=''`, objects.StorageID(), retired.Version.ObjectKey)
	must(t, err)
	must(t, s.CleanupObjects(ctx))
	if objects.count() != 1 {
		t.Fatal("migration cleanup did not preserve the live source")
	}
	checkDocumentContent(t, s, u.ID, w.ID, live.Version, []byte("keep"))
}
