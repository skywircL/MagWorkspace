package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLIndexChunksTrustedBatchReadAndVersionIsolation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkreader")
	profile := testIndexProfile()
	content := "一份具有父子关系的可信测试文档。"
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Trusted title", "source.txt", []byte(content))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	expected := testIndexChunks(job, content)
	got, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("fixed tree changed: %+v", got)
	}
	got, err = s.ReadIndexChunks(ctx, u.ID, w.ID, job, []string{})
	must(t, err)
	if len(got) != 0 {
		t.Fatal("empty ID selection returned all chunks")
	}
	got, err = s.ReadIndexChunks(ctx, u.ID, w.ID, job, []string{expected[1].ID, expected[1].ID})
	must(t, err)
	if len(got) != 1 || got[0].ParentOrdinal != 0 {
		t.Fatalf("child selection lost parent binding: %+v", got)
	}
	outsider, _ := testUser(t, s, "chunkoutsider")
	_, err = s.ReadIndexChunks(ctx, outsider.ID, w.ID, job, nil)
	wantError(t, err, domain.ErrNotFound)
	hints := []domain.IndexCandidate{{ChunkID: expected[1].ID, IndexID: job.ID, Generation: job.Generation, Score: .8}, {ChunkID: expected[0].ID, IndexID: job.ID, Generation: job.Generation, Score: 1}, {ChunkID: expected[1].ID, IndexID: job.ID, Generation: strings.Repeat("f", 32), Score: 1}}
	hits, err := s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, hints)
	must(t, err)
	if len(hits) != 1 || hits[0].Title != write.Document.Title || hits[0].Score != .8 || hits[0].Parent == nil || !reflect.DeepEqual(*hits[0].Parent, expected[0]) || !reflect.DeepEqual(hits[0].Chunk, expected[1]) {
		t.Fatalf("untrusted hints changed hydration: %+v", hits)
	}
	hits, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, hints, []string{})
	must(t, err)
	if len(hits) != 0 {
		t.Fatal("empty document scope returned chunks")
	}
	_, err = s.ResolveIndexChunks(ctx, outsider.ID, w.ID, profile.ID, hints)
	wantError(t, err, domain.ErrNotFound)
	other, otherWorkspace := testUser(t, s, "chunkforeign")
	foreign, err := s.CreateDocument(ctx, other.ID, otherWorkspace.ID, "Private", "private.txt", []byte("foreign source"))
	must(t, err)
	foreignJob := publishTestIndex(t, s, other.ID, otherWorkspace.ID, foreign, profile)
	foreignChunks := testIndexChunks(foreignJob, "foreign source")
	hits, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, append(hints, domain.IndexCandidate{ChunkID: foreignChunks[1].ID, IndexID: foreignJob.ID, Generation: foreignJob.Generation, Score: 1}))
	must(t, err)
	if len(hits) != 1 {
		t.Fatal("cross-workspace hint leaked a chunk")
	}
	_, err = s.AppendDocumentVersion(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, "updated.txt", []byte("new head"))
	must(t, err)
	hits, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, hints)
	must(t, err)
	if len(hits) != 0 {
		t.Fatal("retired head participated in retrieval")
	}
	got, err = s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("head replacement changed immutable historical chunks")
	}
}

func TestMySQLIndexChunksTamperingFailsClosed(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunktamper")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Integrity", "source.txt", []byte("source"))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	chunks := testIndexChunks(job, "source")
	hints := []domain.IndexCandidate{{ChunkID: chunks[1].ID, IndexID: job.ID, Generation: job.Generation, Score: 1}}
	_, err = s.pool.ExecContext(ctx, `UPDATE chunks SET content='forged' WHERE index_id=? AND id=?`, job.ID, chunks[0].ID)
	must(t, err)
	_, err = s.ReadIndexChunks(ctx, u.ID, w.ID, job, []string{chunks[1].ID})
	wantError(t, err, domain.ErrConflict)
	_, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, hints)
	wantError(t, err, domain.ErrConflict)
	_, err = s.pool.ExecContext(ctx, `UPDATE chunks SET content='source' WHERE index_id=? AND id=?`, job.ID, chunks[0].ID)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE chunks SET parent_id=? WHERE index_id=? AND id=?`, strings.Repeat("f", 32), job.ID, chunks[1].ID)
	must(t, err)
	_, err = s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	wantError(t, err, domain.ErrConflict)
	_, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, profile.ID, hints)
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLIndexChunksPublicationBatchesRollBackTogether(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkbatches")
	profile := testIndexProfile()
	parts := make([]string, 140)
	for i := range parts {
		parts[i] = fmt.Sprintf("第 %d 项：", i) + strings.Repeat("内容", 50) + "\n"
	}
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Batch publication", "batch.txt", []byte(strings.Join(parts, "")))
	must(t, err)
	job, _, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJobByID(ctx, profile.ID, job.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing claim")
	}
	parents, children := []domain.IndexChunk{}, []domain.IndexChunk{}
	offset := 0
	for ordinal, part := range parts {
		pair := testIndexChunks(input.Job, part)
		for i := range pair {
			pair[i].Ordinal = ordinal
			pair[i].StartByte += offset
			pair[i].EndByte += offset
		}
		pair[1].ParentOrdinal = ordinal
		parents = append(parents, pair[0])
		children = append(children, pair[1])
		offset += len(part)
	}
	chunks := append(parents, children...)
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER chunk_batch_failure BEFORE INSERT ON chunks FOR EACH ROW BEGIN IF NEW.kind='child' AND NEW.ordinal=130 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced later batch failure'; END IF; END`)
	must(t, err)
	if err = s.FinishDocumentIndex(ctx, input.Job, chunks); err == nil {
		t.Fatal("expected later batch failure")
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks`).Scan(&count))
	if count != 0 {
		t.Fatal("later batch failure retained earlier chunk rows")
	}
	current, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	if current.Status != "running" || current.ParentCount != 0 || current.ChunkCount != 0 {
		t.Fatal("partial body became ready")
	}
	_, err = s.ReadIndexChunks(ctx, u.ID, w.ID, current, nil)
	wantError(t, err, domain.ErrConflict)
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER chunk_batch_failure`)
	must(t, err)
	must(t, s.FinishDocumentIndex(ctx, input.Job, chunks))
	current, err = s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	loaded, err := s.ReadIndexChunks(ctx, u.ID, w.ID, current, nil)
	must(t, err)
	if len(loaded) != 280 || current.ParentCount != 140 || current.ChunkCount != 140 {
		t.Fatal("complete chunk batches did not publish")
	}
	wantError(t, s.FinishDocumentIndex(ctx, input.Job, chunks), domain.ErrConflict)
}

func TestMySQLIndexChunksLeaseExpiryDuringInsertRollsBackPublication(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkleaseexpiry")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Lease expiry", "source.txt", []byte("source"))
	must(t, err)
	job, _, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJobByID(ctx, profile.ID, job.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("missing claim")
	}
	chunks := testIndexChunks(input.Job, "source")
	// Expire the lease only after a child has been inserted. This forces the
	// publication's final lease check without relying on elapsed wall time.
	_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER chunk_insert_expires_lease AFTER INSERT ON chunks FOR EACH ROW BEGIN IF NEW.kind='child' THEN UPDATE document_indexes SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=NEW.index_id; END IF; END`)
	must(t, err)
	wantError(t, s.FinishDocumentIndex(ctx, input.Job, chunks), domain.ErrConflict)
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, job.ID).Scan(&count))
	if count != 0 {
		t.Fatal("lease expiry during insertion retained chunk bodies")
	}
	current, err := s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	if !reflect.DeepEqual(current, input.Job) {
		t.Fatalf("lease expiry published or changed the running job: %+v", current)
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_outbox WHERE job_id=?`, job.ID).Scan(&count))
	if count != 1 {
		t.Fatal("rolled-back publication removed its recovery delivery")
	}
	_, err = s.ReadIndexChunks(ctx, u.ID, w.ID, current, nil)
	wantError(t, err, domain.ErrConflict)
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER chunk_insert_expires_lease`)
	must(t, err)
	must(t, s.FinishDocumentIndex(ctx, input.Job, chunks))
	current, err = s.DocumentIndexByID(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, job.ID)
	must(t, err)
	loaded, err := s.ReadIndexChunks(ctx, u.ID, w.ID, current, nil)
	must(t, err)
	if current.Status != "ready" || current.ParentCount != 1 || current.ChunkCount != 1 || !reflect.DeepEqual(loaded, chunks) {
		t.Fatal("publication could not recover after rolling back the expired insertion")
	}
}

func TestMySQLIndexChunksCleanupProtectsReadyAndRotatedTokens(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkcleanup")
	profile := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Cleanup", "source.txt", []byte("source"))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, profile)
	must(t, s.QueueIndexCleanup(ctx, profile.StorageID, profile.ID, job.Generation))
	expireTestIndexCleanup(t, s, job.Generation)
	var token string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id FROM index_cleanup WHERE generation=?`, job.Generation).Scan(&token))
	must(t, s.CompleteIndexCleanup(ctx, token, strings.Repeat("0", 32)))
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, job.ID).Scan(&count))
	if count != 2 {
		t.Fatal("ready cleanup deleted published bodies")
	}
	must(t, s.DeleteDocument(ctx, u.ID, w.ID, write.Document.ID))
	expireTestIndexCleanup(t, s, job.Generation)
	must(t, s.CompleteIndexCleanup(ctx, token, strings.Repeat("0", 32)))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, job.ID).Scan(&count))
	if count != 0 {
		t.Fatal("document deletion did not cascade chunk bodies")
	}
	pending, err := s.PendingIndexCleanup(ctx, 10)
	must(t, err)
	if len(pending) != 1 || pending[0].ID == token {
		t.Fatal("deletion lost rotated cleanup request")
	}
	claimed, err := s.ClaimIndexCleanup(ctx, []string{profile.StorageID}, 10, time.Minute)
	must(t, err)
	must(t, s.CompleteIndexCleanup(ctx, claimed[0].ID, claimed[0].ClaimToken))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE index_id=?`, job.ID).Scan(&count))
	if count != 0 {
		t.Fatal("eligible cleanup retained chunk bodies")
	}
}

func TestMySQLIndexChunksMigrationRejectsRetiredRowsBeforeDDLAndResumes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkmigration")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Old index", "old.txt", []byte("source"))
	must(t, err)
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, testIndexProfile())
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `ALTER TABLE document_indexes ADD COLUMN artifact_key varchar(512) NOT NULL DEFAULT ''`)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=14`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "clear retired indexes") {
		t.Fatalf("nonempty retired schema did not refuse early: %v", err)
	}
	var count, version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='artifact_key'`).Scan(&count))
	if count != 1 {
		t.Fatal("refused migration already removed artifact metadata")
	}
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version))
	if version != 14 {
		t.Fatal("refused migration advanced marker")
	}
	_, err = s.pool.ExecContext(ctx, `DELETE FROM document_indexes`)
	must(t, err)
	must(t, s.Migrate(ctx))
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=14`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name IN ('artifact_key','artifact_sha256')`).Scan(&count))
	if count != 0 {
		t.Fatal("retired artifact columns survived migration")
	}
	job := publishTestIndex(t, s, u.ID, w.ID, write, testIndexProfile())
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=14`)
	must(t, err)
	must(t, s.Migrate(ctx))
	chunks, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if len(chunks) != 2 {
		t.Fatal("lost-marker migration changed current chunk data")
	}
}

func TestMySQLIndexChunksMigrationResumesInterruptedDDL(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "chunkddlresume")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Existing document", "source.txt", []byte("source"))
	must(t, err)
	// Reconstruct v14 only in this test's isolated database. A dependent check
	// blocks the second retired column's removal after earlier DDL commits.
	for _, statement := range []string{
		`DROP TABLE chunks`,
		`ALTER TABLE document_indexes DROP CHECK document_indexes_chunks`,
		`ALTER TABLE document_indexes DROP COLUMN parent_count`,
		`ALTER TABLE document_indexes ADD COLUMN artifact_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '', ADD COLUMN artifact_sha256 varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''`,
		`ALTER TABLE document_indexes ADD CONSTRAINT document_indexes_manifest CHECK(chunk_count BETWEEN 0 AND 2048 AND (status<>'ready' OR (chunk_count>0 AND OCTET_LENGTH(artifact_key)>0 AND OCTET_LENGTH(artifact_sha256)=64)))`,
		`ALTER TABLE document_indexes ADD CONSTRAINT chunk_migration_blocker CHECK(OCTET_LENGTH(artifact_sha256)<=64 AND OCTET_LENGTH(id)=32)`,
		`UPDATE schema_version SET version=14 WHERE singleton=1`,
	} {
		_, err = s.pool.ExecContext(ctx, statement)
		must(t, err)
	}
	if err = s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "migration 015") {
		t.Fatalf("expected migration015 to stop during DDL: %v", err)
	}
	var version, parents, artifactKeys, artifactHashes, chunks int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks`).Scan(&chunks))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COALESCE(SUM(column_name='parent_count'),0),COALESCE(SUM(column_name='artifact_key'),0),COALESCE(SUM(column_name='artifact_sha256'),0) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes'`).Scan(&parents, &artifactKeys, &artifactHashes))
	if version != 14 || parents != 1 || artifactKeys != 0 || artifactHashes != 1 || chunks != 0 {
		t.Fatalf("unexpected partially committed schema: version=%d parent_count=%d artifact_key=%d artifact_sha256=%d chunks=%d", version, parents, artifactKeys, artifactHashes, chunks)
	}
	_, err = s.pool.ExecContext(ctx, `ALTER TABLE document_indexes DROP CHECK chunk_migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	var legacyColumns, chunkConstraint int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name IN ('artifact_key','artifact_sha256')`).Scan(&legacyColumns))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_indexes' AND constraint_name='document_indexes_chunks'`).Scan(&chunkConstraint))
	if version != len(migrationFiles) || legacyColumns != 0 || chunkConstraint != 1 {
		t.Fatalf("migration did not finish: version=%d legacy_columns=%d chunk_constraint=%d", version, legacyColumns, chunkConstraint)
	}
	checkDocumentContent(t, s, u.ID, w.ID, write.Version, []byte("source"))
	job := publishTestIndex(t, s, u.ID, w.ID, write, testIndexProfile())
	loaded, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if !reflect.DeepEqual(loaded, testIndexChunks(job, "source")) {
		t.Fatal("resumed schema did not support complete chunk publication")
	}
}
