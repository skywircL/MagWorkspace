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

type documentSourceSnapshot struct {
	RunID, WorkspaceID, DocumentID, VersionID, SourceSHA256, IndexID, Generation string
}

type librarySourceSnapshot struct {
	RunID, WorkspaceID, KnowledgeBaseID, MetadataSHA256 string
}

type sourceIntegritySnapshot struct {
	Documents []documentSourceSnapshot
	Libraries []librarySourceSnapshot
}

func sourceIntegrityFixture(t *testing.T) evidenceFixture {
	t.Helper()
	f := newEvidenceFixture(t)
	ctx := context.Background()
	// The real tool entry points admit both forms of document provenance:
	// metadata without an index and a fixed ready index for the same version.
	for _, profile := range []string{"", f.job.ProfileID} {
		_, err := f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, f.write.Document.ID, "", "", profile)
		must(t, err)
	}
	page, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "", 0, 20)
	must(t, err)
	if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != f.write.Document.KnowledgeBaseID {
		t.Fatal("fixture did not acquire the document's library metadata")
	}
	return f
}

func readSourceIntegritySnapshot(t *testing.T, f evidenceFixture) sourceIntegritySnapshot {
	t.Helper()
	ctx := context.Background()
	out := sourceIntegritySnapshot{Documents: []documentSourceSnapshot{}, Libraries: []librarySourceSnapshot{}}
	rows, err := f.s.pool.QueryContext(ctx, `SELECT run_id,workspace_id,document_id,version_id,source_sha256,index_id,generation FROM run_document_sources WHERE run_id=? ORDER BY document_id,version_id,index_id`, f.run.ID)
	must(t, err)
	for rows.Next() {
		var row documentSourceSnapshot
		must(t, rows.Scan(&row.RunID, &row.WorkspaceID, &row.DocumentID, &row.VersionID, &row.SourceSHA256, &row.IndexID, &row.Generation))
		out.Documents = append(out.Documents, row)
	}
	must(t, rows.Err())
	must(t, rows.Close())
	rows, err = f.s.pool.QueryContext(ctx, `SELECT run_id,workspace_id,knowledge_base_id,metadata_sha256 FROM run_knowledge_base_sources WHERE run_id=? ORDER BY knowledge_base_id`, f.run.ID)
	must(t, err)
	for rows.Next() {
		var row librarySourceSnapshot
		must(t, rows.Scan(&row.RunID, &row.WorkspaceID, &row.KnowledgeBaseID, &row.MetadataSHA256))
		out.Libraries = append(out.Libraries, row)
	}
	must(t, rows.Err())
	must(t, rows.Close())
	return out
}

func sourceIntegrityCheckError(t *testing.T, err error) {
	t.Helper()
	var mysqlError *mysql.MySQLError
	if !errors.As(err, &mysqlError) || mysqlError.Number != 3819 {
		t.Fatalf("write must violate a CHECK constraint, got %v", err)
	}
}

func TestMySQLSourceIntegrityRejectsMalformedInsertAndUpdate(t *testing.T) {
	f := sourceIntegrityFixture(t)
	ctx := context.Background()
	before := readSourceIntegritySnapshot(t, f)
	if len(before.Documents) != 2 || before.Documents[0].IndexID != "" || before.Documents[0].Generation != "" ||
		before.Documents[1].IndexID != f.job.ID || before.Documents[1].Generation != f.job.Generation || len(before.Libraries) != 1 {
		t.Fatal("valid metadata and ready-index source forms were not preserved")
	}
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "constraint insertion target")
	must(t, err)
	run, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, ss.ID, "source shape", time.Minute)
	must(t, err)
	for _, tc := range []struct{ name, index, generation, digest string }{
		{"index-without-generation", f.job.ID, "", f.job.SourceSHA256},
		{"generation-without-index", "", f.job.Generation, f.job.SourceSHA256},
		{"blank-index", " ", "", f.job.SourceSHA256},
		{"blank-generation", "", " ", f.job.SourceSHA256},
		{"blank-index-and-generation", " ", " ", f.job.SourceSHA256},
		{"full-length-blank-pair", strings.Repeat(" ", 32), strings.Repeat(" ", 32), f.job.SourceSHA256},
		{"short-index", "abc", f.job.Generation, f.job.SourceSHA256},
		{"short-generation", f.job.ID, "abc", f.job.SourceSHA256},
		{"nonhex-index", strings.Repeat("g", 32), f.job.Generation, f.job.SourceSHA256},
		{"nonhex-generation", f.job.ID, strings.Repeat("g", 32), f.job.SourceSHA256},
		{"uppercase-index", strings.Repeat("A", 32), f.job.Generation, f.job.SourceSHA256},
		{"uppercase-generation", f.job.ID, strings.Repeat("B", 32), f.job.SourceSHA256},
		{"short-source-hash", f.job.ID, f.job.Generation, "abc"},
		{"nonhex-source-hash", f.job.ID, f.job.Generation, strings.Repeat("g", 64)},
		{"uppercase-source-hash", f.job.ID, f.job.Generation, strings.Repeat("A", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.s.pool.ExecContext(ctx, `INSERT INTO run_document_sources(run_id,workspace_id,document_id,version_id,source_sha256,index_id,generation) VALUES(?,?,?,?,?,?,?)`, run.ID, f.w.ID, f.write.Document.ID, f.write.Version.ID, tc.digest, tc.index, tc.generation)
			sourceIntegrityCheckError(t, err)
			// ascii_bin ignores trailing spaces during equality comparison.
			// Update the metadata row for blank IDs so its primary key cannot
			// mask a missing CHECK with a duplicate-key error.
			previousIndex := f.job.ID
			if strings.TrimSpace(tc.index) == "" {
				previousIndex = ""
			}
			_, err = f.s.pool.ExecContext(ctx, `UPDATE run_document_sources SET index_id=?,generation=?,source_sha256=? WHERE run_id=? AND index_id=?`, tc.index, tc.generation, tc.digest, f.run.ID, previousIndex)
			sourceIntegrityCheckError(t, err)
		})
	}
	for _, digest := range []string{"", "abc", strings.Repeat("g", 64), strings.Repeat("A", 64)} {
		t.Run("library-hash-"+digest, func(t *testing.T) {
			_, err := f.s.pool.ExecContext(ctx, `INSERT INTO run_knowledge_base_sources(run_id,workspace_id,knowledge_base_id,metadata_sha256) VALUES(?,?,?,?)`, run.ID, f.w.ID, f.write.Document.KnowledgeBaseID, digest)
			sourceIntegrityCheckError(t, err)
			_, err = f.s.pool.ExecContext(ctx, `UPDATE run_knowledge_base_sources SET metadata_sha256=? WHERE run_id=?`, digest, f.run.ID)
			sourceIntegrityCheckError(t, err)
		})
	}
	if !reflect.DeepEqual(before, readSourceIntegritySnapshot(t, f)) {
		t.Fatal("rejected source writes changed existing observations")
	}
	var added int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM run_document_sources WHERE run_id=?)+(SELECT COUNT(*) FROM run_knowledge_base_sources WHERE run_id=?)`, run.ID, run.ID).Scan(&added))
	if added != 0 {
		t.Fatal("rejected insert left a partial source")
	}
}

func sourceIntegrityLegacySchema(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE run_document_sources DROP CHECK run_document_sources_index_shape`,
		`ALTER TABLE run_document_sources DROP CHECK run_document_sources_hash_shape`,
		`ALTER TABLE run_knowledge_base_sources DROP CHECK run_knowledge_base_sources_hash_shape`,
		`UPDATE schema_version SET version=24 WHERE singleton=1`,
	} {
		_, err := s.pool.ExecContext(context.Background(), statement)
		must(t, err)
	}
}

func sourceIntegrityConstraints(t *testing.T, s *Store) (version, checks int) {
	t.Helper()
	must(t, s.pool.QueryRowContext(context.Background(), `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	must(t, s.pool.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND ((table_name='run_document_sources' AND constraint_name IN ('run_document_sources_index_shape','run_document_sources_hash_shape')) OR (table_name='run_knowledge_base_sources' AND constraint_name='run_knowledge_base_sources_hash_shape'))`).Scan(&checks))
	return version, checks
}

func TestMySQLSourceIntegrityMigrationPreservesAndResumes(t *testing.T) {
	for _, phase := range []string{"normal", "before-first-check", "after-first-check", "marker"} {
		t.Run(phase, func(t *testing.T) {
			f := sourceIntegrityFixture(t)
			ctx := context.Background()
			before := readSourceIntegritySnapshot(t, f)
			sourceIntegrityLegacySchema(t, f.s)
			wantChecks := 0
			switch phase {
			case "before-first-check", "after-first-check":
				name := "run_document_sources_index_shape"
				if phase == "after-first-check" {
					name, wantChecks = "run_document_sources_hash_shape", 1
				}
				// CHECK names are schema-wide. A different table can interrupt
				// the next ALTER after earlier DDL has already committed.
				_, err := f.s.pool.ExecContext(ctx, "CREATE TABLE source_integrity_blocker (n int, CONSTRAINT "+name+" CHECK(n>=0)) ENGINE=InnoDB")
				must(t, err)
			case "marker":
				_, err := f.s.pool.ExecContext(ctx, `CREATE TRIGGER source_integrity_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced source integrity marker failure'`)
				must(t, err)
				wantChecks = 3
			}
			if phase != "normal" {
				if err := f.s.Migrate(ctx); err == nil {
					t.Fatal("forced migration interruption did not fail")
				}
				version, checks := sourceIntegrityConstraints(t, f.s)
				if version != 24 || checks != wantChecks || !reflect.DeepEqual(before, readSourceIntegritySnapshot(t, f)) {
					t.Fatalf("interruption changed sources or marker: version=%d checks=%d", version, checks)
				}
				statement := `DROP TABLE source_integrity_blocker`
				if phase == "marker" {
					statement = `DROP TRIGGER source_integrity_marker_failure`
				}
				_, err := f.s.pool.ExecContext(ctx, statement)
				must(t, err)
			}
			must(t, f.s.Migrate(ctx))
			// Retry after a lost marker as well as an ordinary repeated startup.
			_, err := f.s.pool.ExecContext(ctx, `UPDATE schema_version SET version=24 WHERE singleton=1`)
			must(t, err)
			must(t, f.s.Migrate(ctx))
			must(t, f.s.Migrate(ctx))
			version, checks := sourceIntegrityConstraints(t, f.s)
			if version != len(migrationFiles) || checks != 3 || !reflect.DeepEqual(before, readSourceIntegritySnapshot(t, f)) {
				t.Fatalf("migration lost source identities or hashes: version=%d checks=%d", version, checks)
			}
			must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "valid observed metadata", "test"))
		})
	}
}

func TestMySQLSourceIntegrityMigrationRejectsBadHistoryWithoutRepairingIt(t *testing.T) {
	for _, kind := range []string{"index-pair", "blank-index", "blank-generation", "source-hash", "library-hash"} {
		t.Run(kind, func(t *testing.T) {
			f := sourceIntegrityFixture(t)
			ctx := context.Background()
			before := readSourceIntegritySnapshot(t, f)
			sourceIntegrityLegacySchema(t, f.s)
			statement, bad, original := `UPDATE run_document_sources SET generation=? WHERE run_id=? AND index_id<>''`, "", f.job.Generation
			if kind == "blank-index" {
				statement, bad, original = `UPDATE run_document_sources SET index_id=? WHERE run_id=? AND OCTET_LENGTH(index_id)<32`, " ", ""
			} else if kind == "blank-generation" {
				statement, bad, original = `UPDATE run_document_sources SET generation=? WHERE run_id=? AND OCTET_LENGTH(index_id)=0`, " ", ""
			} else if kind == "source-hash" {
				statement, bad, original = `UPDATE run_document_sources SET source_sha256=? WHERE run_id=?`, "short", f.job.SourceSHA256
			} else if kind == "library-hash" {
				statement, bad, original = `UPDATE run_knowledge_base_sources SET metadata_sha256=? WHERE run_id=?`, strings.Repeat("A", 64), before.Libraries[0].MetadataSHA256
			}
			_, err := f.s.pool.ExecContext(ctx, statement, bad, f.run.ID)
			must(t, err)
			corrupt := readSourceIntegritySnapshot(t, f)
			if err = f.s.Migrate(ctx); err == nil {
				t.Fatal("migration accepted malformed historical sources")
			}
			version, _ := sourceIntegrityConstraints(t, f.s)
			if version != 24 || !reflect.DeepEqual(corrupt, readSourceIntegritySnapshot(t, f)) {
				t.Fatal("failed migration changed bad history or advanced its marker")
			}
			// An explicit operator repair is separate from migration. Startup
			// resumes already committed CHECK additions after that repair.
			_, err = f.s.pool.ExecContext(ctx, statement, original, f.run.ID)
			must(t, err)
			must(t, f.s.Migrate(ctx))
			version, checks := sourceIntegrityConstraints(t, f.s)
			if version != len(migrationFiles) || checks != 3 || !reflect.DeepEqual(before, readSourceIntegritySnapshot(t, f)) {
				t.Fatal("forward repair failed to preserve original source observations")
			}
		})
	}
}

func TestMySQLSourceIntegrityRetainsDeletedResourceProvenance(t *testing.T) {
	for _, resource := range []string{"document", "knowledge-base"} {
		t.Run(resource, func(t *testing.T) {
			f := sourceIntegrityFixture(t)
			ctx := context.Background()
			evidence := f.record(t, f.hit)
			before := readSourceIntegritySnapshot(t, f)
			must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "observed source [E:"+evidence[0].ID+"]", "test"))
			if resource == "document" {
				must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
			} else {
				must(t, f.s.DeleteKnowledgeBase(ctx, f.u.ID, f.w.ID, f.write.Document.KnowledgeBaseID))
			}
			if !reflect.DeepEqual(before, readSourceIntegritySnapshot(t, f)) {
				t.Fatal("resource deletion erased provenance needed for denial")
			}
			allowed, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, nil)
			must(t, err)
			if allowed {
				t.Fatal("deleted resource remained eligible for model history")
			}
			_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, evidence[0].ID)
			wantError(t, err, domain.ErrNotFound)
			citations, err := f.s.EvidenceForRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, true)
			must(t, err)
			if len(citations) != 0 {
				t.Fatal("deleted source returned readable citations")
			}
		})
	}
}
