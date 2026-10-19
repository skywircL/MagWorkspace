package store

import (
	"context"
	"magworkspace/internal/domain"
	"strings"
	"testing"
)

func TestMySQLUploadLimitMigrationPreservesHistoryAndResumes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "uploadmigration")
	legacy, err := s.CreateDocument(ctx, u.ID, w.ID, "Legacy", "old.txt", []byte("legacy"))
	must(t, err)
	s.SetObjectStorage(newMemoryObjects())
	original, err := s.CreateDocument(ctx, u.ID, w.ID, "Original", "old.txt", []byte("original"))
	must(t, err)
	// Reconstruct v10 guards around real inline and external version rows.
	for _, stmt := range []string{
		`ALTER TABLE document_versions DROP CHECK document_versions_storage_v2`,
		`ALTER TABLE document_versions ADD CONSTRAINT document_versions_storage CHECK((object_key='' AND content IS NOT NULL AND size_bytes BETWEEN 0 AND 8388608 AND size_bytes=OCTET_LENGTH(content)) OR (object_key<>'' AND content IS NULL AND size_bytes BETWEEN 0 AND 8388608))`,
		`UPDATE schema_version SET version=10 WHERE singleton=1`,
	} {
		_, err = s.pool.ExecContext(ctx, stmt)
		must(t, err)
	}
	// Crash after the replacement guard exists, before the old guard is removed.
	sql, err := migrations.ReadFile(migrationFiles[10])
	must(t, err)
	conn, err := s.pool.Conn(ctx)
	must(t, err)
	for _, stmt := range strings.Split(string(sql), ";")[:4] {
		_, err = conn.ExecContext(ctx, stmt)
		must(t, err)
	}
	must(t, conn.Close())
	_, err = s.pool.ExecContext(ctx, `UPDATE document_versions SET size_bytes=9437184 WHERE id=?`, original.Version.ID)
	if err == nil {
		t.Fatal("old guard disappeared before migration completed")
	}
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	checkDocumentContent(t, s, u.ID, w.ID, legacy.Version, []byte("legacy"))
	checkDocumentContent(t, s, u.ID, w.ID, original.Version, []byte("original"))
	_, err = s.pool.ExecContext(ctx, `UPDATE document_versions SET size_bytes=9437184 WHERE id=?`, original.Version.ID)
	must(t, err)
	for _, size := range []int{-1, domain.MaxStoredDocumentBytes + 1} {
		_, err = s.pool.ExecContext(ctx, `UPDATE document_versions SET size_bytes=? WHERE id=?`, size, original.Version.ID)
		if err == nil {
			t.Fatal("invalid external size accepted")
		}
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE document_versions SET size_bytes=8388609 WHERE id=?`, legacy.Version.ID)
	if err == nil {
		t.Fatal("invalid inline size accepted")
	}
}
