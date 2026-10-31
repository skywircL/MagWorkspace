package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

func indexLineageForeignKeyError(t *testing.T, err error) {
	t.Helper()
	var mysqlError *mysql.MySQLError
	if !errors.As(err, &mysqlError) || mysqlError.Number != 1452 {
		t.Fatalf("write must fail its foreign key, got %v", err)
	}
}

func TestMySQLIndexLineageBindsJobsChunksAndCascades(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "index-lineage")
	otherUser, otherWorkspace := testUser(t, s, "index-lineage-other")
	first := defaultKnowledgeBase(t, s, u.ID, w.ID)
	second, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Second library", "")
	must(t, err)
	write, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, first.ID, "Indexed document", "source.txt", []byte("父子正文在归属更新与迁移后保持不变。"))
	must(t, err)
	otherDocument, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, first.ID, "Other document", "other.txt", []byte("other version"))
	must(t, err)
	foreign, err := s.CreateDocument(ctx, otherUser.ID, otherWorkspace.ID, "Foreign document", "foreign.txt", []byte("foreign source"))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, testIndexProfile())
	chunks, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if job.WorkspaceID != w.ID || job.KnowledgeBaseID != first.ID || len(chunks) != 2 {
		t.Fatalf("published job lost its lineage: %+v", job)
	}
	for _, chunk := range chunks {
		if chunk.WorkspaceID != w.ID || chunk.KnowledgeBaseID != first.ID || chunk.DocumentID != write.Document.ID || chunk.VersionID != write.Version.ID {
			t.Fatalf("published chunk lost its lineage: %+v", chunk)
		}
	}

	for _, mismatch := range []struct{ name, workspaceID, knowledgeBaseID, versionID string }{
		{"workspace", otherWorkspace.ID, first.ID, write.Version.ID},
		{"knowledge-base", w.ID, second.ID, write.Version.ID},
		{"document-version", w.ID, first.ID, otherDocument.Version.ID},
	} {
		t.Run("index-insert-"+mismatch.name, func(t *testing.T) {
			id, err := newID()
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `INSERT INTO document_indexes(id,workspace_id,knowledge_base_id,document_id,version_id,profile_id,embedding_model,dimensions,source_sha256,created_by) VALUES(?,?,?,?,?,?,?,3,?,?)`, id, mismatch.workspaceID, mismatch.knowledgeBaseID, write.Document.ID, mismatch.versionID, id+id, "test-embedding", job.SourceSHA256, u.ID)
			indexLineageForeignKeyError(t, err)
		})
		t.Run("index-update-"+mismatch.name, func(t *testing.T) {
			_, err := s.pool.ExecContext(ctx, `UPDATE document_indexes SET workspace_id=?,knowledge_base_id=?,version_id=? WHERE id=?`, mismatch.workspaceID, mismatch.knowledgeBaseID, mismatch.versionID, job.ID)
			indexLineageForeignKeyError(t, err)
		})
	}
	for _, mismatch := range []struct{ name, workspaceID, knowledgeBaseID string }{
		{"workspace", otherWorkspace.ID, first.ID},
		{"knowledge-base", w.ID, second.ID},
	} {
		t.Run("chunk-insert-"+mismatch.name, func(t *testing.T) {
			id, err := newID()
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `INSERT INTO chunks(index_id,generation,id,workspace_id,knowledge_base_id,kind,parent_id,ordinal,start_byte,end_byte,content,sha256,context_header) SELECT index_id,generation,?,?,?,kind,parent_id,100,start_byte,end_byte,content,sha256,context_header FROM chunks WHERE index_id=? AND id=?`, id, mismatch.workspaceID, mismatch.knowledgeBaseID, job.ID, chunks[0].ID)
			indexLineageForeignKeyError(t, err)
		})
		t.Run("chunk-update-"+mismatch.name, func(t *testing.T) {
			_, err := s.pool.ExecContext(ctx, `UPDATE chunks SET workspace_id=?,knowledge_base_id=? WHERE index_id=?`, mismatch.workspaceID, mismatch.knowledgeBaseID, job.ID)
			indexLineageForeignKeyError(t, err)
		})
	}

	// Existing scope-revocation tests simulate a document moving libraries by
	// updating its authoritative row. The database must cascade that ownership.
	_, err = s.pool.ExecContext(ctx, `UPDATE documents SET knowledge_base_id=? WHERE id=?`, second.ID, write.Document.ID)
	must(t, err)
	moved, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	want := job
	want.KnowledgeBaseID = second.ID
	if !reflect.DeepEqual(moved, want) {
		t.Fatalf("library move changed index state or missed lineage: %+v", moved)
	}
	for i := range chunks {
		chunks[i].KnowledgeBaseID = second.ID
	}
	loaded, err := s.ReadIndexChunks(ctx, u.ID, w.ID, moved, nil)
	must(t, err)
	if !reflect.DeepEqual(loaded, chunks) {
		t.Fatal("library move did not cascade chunk ownership while preserving bodies")
	}
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, second.ID))
	for _, query := range []string{
		`SELECT COUNT(*) FROM documents WHERE id=?`,
		`SELECT COUNT(*) FROM document_indexes WHERE document_id=?`,
		`SELECT COUNT(*) FROM chunks WHERE index_id=?`,
	} {
		id := write.Document.ID
		if strings.Contains(query, "FROM chunks") {
			id = job.ID
		}
		var count int
		must(t, s.pool.QueryRowContext(ctx, query, id).Scan(&count))
		if count != 0 {
			t.Fatalf("deleted library retained a dependent row: %s", query)
		}
	}
	_, err = s.Document(ctx, u.ID, w.ID, otherDocument.Document.ID)
	must(t, err)
	_, err = s.Document(ctx, otherUser.ID, otherWorkspace.ID, foreign.Document.ID)
	must(t, err)
}

func TestMySQLIndexLineageMovedRunningJobRejectsOldWorker(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "lineage-running-move")
	second, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Moved destination", "")
	must(t, err)
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Running document", "source.txt", []byte("source before the library move"))
	must(t, err)
	profile := testIndexProfile()
	queued, _, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJobByID(ctx, profile.ID, queued.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing claimed index")
	}
	chunks := testIndexChunks(input.Job, "source before the library move")
	_, err = s.pool.ExecContext(ctx, `UPDATE documents SET knowledge_base_id=? WHERE id=?`, second.ID, write.Document.ID)
	must(t, err)
	wantError(t, s.FinishDocumentIndex(ctx, input.Job, chunks), domain.ErrConflict)
	current, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, queued.ID)
	must(t, err)
	want := input.Job
	want.KnowledgeBaseID = second.ID
	if !reflect.DeepEqual(current, want) || current.Status != "running" || current.ChunkCount != 0 || current.ParentCount != 0 {
		t.Fatalf("old worker changed the moved running job: %+v", current)
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, queued.ID).Scan(&count))
	if count != 0 {
		t.Fatal("old worker published chunks under an obsolete library")
	}
	_, err = s.ReadIndexChunks(ctx, u.ID, w.ID, current, nil)
	wantError(t, err, domain.ErrConflict)
}

// Rewind only lineage DDL in this test's disposable database; v22's ready
// records, immutable bodies, generation, and original single-column FKs remain.
func indexLineageLegacySchema(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"chunks", "document_indexes"} {
		rows, err := s.pool.QueryContext(ctx, `SELECT constraint_name FROM information_schema.key_column_usage WHERE constraint_schema=DATABASE() AND table_name=? AND referenced_table_name IS NOT NULL GROUP BY constraint_name HAVING COUNT(*)>1`, table)
		must(t, err)
		var names []string
		for rows.Next() {
			var name string
			must(t, rows.Scan(&name))
			names = append(names, name)
		}
		must(t, rows.Err())
		must(t, rows.Close())
		for _, name := range names {
			_, err := s.pool.ExecContext(ctx, "ALTER TABLE `"+table+"` DROP FOREIGN KEY `"+name+"`")
			must(t, err)
		}
	}
	for _, legacy := range []struct{ table, name, column, parent string }{
		{"chunks", "chunks_index_fk", "index_id", "document_indexes"},
		{"document_indexes", "document_indexes_document_fk", "document_id", "documents"},
		{"document_indexes", "document_indexes_version_fk", "version_id", "document_versions"},
	} {
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name=? AND constraint_name=?`, legacy.table, legacy.name).Scan(&count))
		if count == 0 {
			_, err := s.pool.ExecContext(ctx, "ALTER TABLE `"+legacy.table+"` ADD CONSTRAINT `"+legacy.name+"` FOREIGN KEY(`"+legacy.column+"`) REFERENCES `"+legacy.parent+"`(id) ON DELETE CASCADE")
			must(t, err)
		}
	}
	for _, statement := range []string{
		`ALTER TABLE document_indexes ADD KEY index_lineage_legacy_workspace(workspace_id)`,
		`ALTER TABLE document_indexes ADD KEY index_lineage_legacy_document(document_id)`,
		`ALTER TABLE document_indexes ADD KEY index_lineage_legacy_version(version_id)`,
	} {
		_, err := s.pool.ExecContext(ctx, statement)
		must(t, err)
	}
	// DROP COLUMN shortens a composite index without renaming it. Remove new
	// indexes explicitly so migration guards cannot mistake that shortened
	// index for the complete new lineage key on replay.
	for _, table := range []string{"chunks", "document_indexes"} {
		rows, err := s.pool.QueryContext(ctx, `SELECT DISTINCT index_name FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND column_name='knowledge_base_id'`, table)
		must(t, err)
		var names []string
		for rows.Next() {
			var name string
			must(t, rows.Scan(&name))
			names = append(names, name)
		}
		must(t, rows.Err())
		must(t, rows.Close())
		for _, name := range names {
			_, err := s.pool.ExecContext(ctx, "ALTER TABLE `"+table+"` DROP INDEX `"+name+"`")
			must(t, err)
		}
	}
	for _, statement := range []string{
		`ALTER TABLE documents DROP INDEX documents_index_lineage`,
		`ALTER TABLE document_versions DROP INDEX document_versions_lineage`,
		`ALTER TABLE chunks DROP COLUMN workspace_id, DROP COLUMN knowledge_base_id`,
		`ALTER TABLE document_indexes DROP COLUMN knowledge_base_id`,
		`UPDATE schema_version SET version=22 WHERE singleton=1`,
	} {
		_, err := s.pool.ExecContext(ctx, statement)
		must(t, err)
	}
}

func indexLineageLegacyFixture(t *testing.T) (*Store, domain.User, domain.Workspace, domain.DocumentWrite, domain.IndexJob, []domain.IndexChunk) {
	t.Helper()
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "lineage-migration")
	kb, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Non-default source", "")
	must(t, err)
	write, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, kb.ID, "Historical ready index", "historical.txt", []byte("历史索引和父子分块正文不得重建或丢失。"))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, testIndexProfile())
	chunks, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	indexLineageLegacySchema(t, s)
	return s, u, w, write, job, chunks
}

func assertIndexLineageMigrationPreserved(t *testing.T, s *Store, u domain.User, w domain.Workspace, job domain.IndexJob, chunks []domain.IndexChunk) {
	t.Helper()
	ctx := context.Background()
	got, err := s.DocumentIndexByID(ctx, u.ID, w.ID, job.DocumentID, job.VersionID, job.ID)
	must(t, err)
	if !reflect.DeepEqual(got, job) {
		t.Fatalf("migration changed the ready job: got %+v, want %+v", got, job)
	}
	loaded, err := s.ReadIndexChunks(ctx, u.ID, w.ID, got, nil)
	must(t, err)
	if !reflect.DeepEqual(loaded, chunks) {
		t.Fatal("migration changed fixed chunk content, lineage, or generation")
	}
	var version, mandatoryColumns int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND is_nullable='NO' AND ((table_name='document_indexes' AND column_name='knowledge_base_id') OR (table_name='chunks' AND column_name IN ('workspace_id','knowledge_base_id')))`).Scan(&mandatoryColumns))
	if version != len(migrationFiles) || mandatoryColumns != 3 {
		t.Fatalf("migration incomplete: version=%d, mandatory columns=%d", version, mandatoryColumns)
	}
}

func TestMySQLIndexLineageMigrationBackfillsReadyRecordsAndReplays(t *testing.T) {
	s, u, w, write, job, chunks := indexLineageLegacyFixture(t)
	ctx := context.Background()
	must(t, s.Migrate(ctx))
	assertIndexLineageMigrationPreserved(t, s, u, w, job, chunks)
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("历史索引和父子分块正文不得重建或丢失。"))
	// A lost marker may replay both 023 and later migrations. Existing values
	// must remain authoritative, rather than being reset to a default library.
	_, err := s.pool.ExecContext(ctx, `UPDATE schema_version SET version=22 WHERE singleton=1`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	assertIndexLineageMigrationPreserved(t, s, u, w, job, chunks)
}

func TestMySQLIndexLineageMigrationResumesInterruptedBackfillAndMarker(t *testing.T) {
	for _, interruption := range []string{"backfill", "marker"} {
		t.Run(interruption, func(t *testing.T) {
			s, u, w, _, job, chunks := indexLineageLegacyFixture(t)
			ctx := context.Background()
			table := "document_indexes"
			if interruption == "marker" {
				table = "schema_version"
			}
			_, err := s.pool.ExecContext(ctx, "CREATE TRIGGER index_lineage_migration_failure BEFORE UPDATE ON "+table+" FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced lineage migration interruption'")
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("forced migration interruption did not fail")
			}
			var version, lineageColumns int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='knowledge_base_id'`).Scan(&lineageColumns))
			if version != 22 || lineageColumns != 1 {
				t.Fatalf("interrupted migration did not retain partial DDL and old marker: version=%d, columns=%d", version, lineageColumns)
			}
			_, err = s.pool.ExecContext(ctx, `DROP TRIGGER index_lineage_migration_failure`)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertIndexLineageMigrationPreserved(t, s, u, w, job, chunks)
		})
	}
}

func TestMySQLIndexLineageMigrationRejectsInconsistentHistory(t *testing.T) {
	for _, mismatch := range []string{"workspace", "document-version", "index-library", "chunk-library"} {
		t.Run(mismatch, func(t *testing.T) {
			s, u, w, write, job, chunks := indexLineageLegacyFixture(t)
			ctx := context.Background()
			otherUser, otherWorkspace := testUser(t, s, "lineage-invalid-history")
			other, err := s.CreateDocument(ctx, otherUser.ID, otherWorkspace.ID, "Another document", "other.txt", []byte("another source"))
			must(t, err)
			var statement, repair string
			var value, original string
			switch mismatch {
			case "workspace":
				statement = `UPDATE document_indexes SET workspace_id=? WHERE id=?`
				repair, value, original = statement, otherWorkspace.ID, w.ID
			case "document-version":
				statement = `UPDATE document_indexes SET version_id=? WHERE id=?`
				repair, value, original = statement, other.Version.ID, write.Version.ID
			case "index-library":
				_, err = s.pool.ExecContext(ctx, `ALTER TABLE document_indexes ADD COLUMN knowledge_base_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NULL`)
				must(t, err)
				statement = `UPDATE document_indexes SET knowledge_base_id=? WHERE id=?`
				repair, value, original = statement, other.Document.KnowledgeBaseID, write.Document.KnowledgeBaseID
			case "chunk-library":
				_, err = s.pool.ExecContext(ctx, `ALTER TABLE chunks ADD COLUMN workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NULL, ADD COLUMN knowledge_base_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NULL`)
				must(t, err)
				_, err = s.pool.ExecContext(ctx, `UPDATE chunks SET workspace_id=? WHERE index_id=?`, w.ID, job.ID)
				must(t, err)
				statement = `UPDATE chunks SET knowledge_base_id=? WHERE index_id=?`
				repair, value, original = statement, other.Document.KnowledgeBaseID, write.Document.KnowledgeBaseID
			}
			_, err = s.pool.ExecContext(ctx, statement, value, job.ID)
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("migration silently accepted inconsistent historical lineage")
			}
			var version, count int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, job.ID).Scan(&count))
			if version != 22 || count != len(chunks) {
				t.Fatalf("refused migration advanced marker or deleted bodies: version=%d, chunks=%d", version, count)
			}
			_, err = s.pool.ExecContext(ctx, repair, original, job.ID)
			must(t, err)
			// Backfill may have already committed a descendant's wrong scope
			// before adding the failing parent constraint. Forward repair must
			// restore every affected row before the operator retries migration.
			_, err = s.pool.ExecContext(ctx, `UPDATE chunks SET workspace_id=?,knowledge_base_id=? WHERE index_id=?`, w.ID, write.Document.KnowledgeBaseID, job.ID)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertIndexLineageMigrationPreserved(t, s, u, w, job, chunks)
		})
	}
}
