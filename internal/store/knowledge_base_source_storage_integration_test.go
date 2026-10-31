package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLKnowledgeBaseSourceMigrationRetriesDDLAndMarker(t *testing.T) {
	for _, interruption := range []string{"before-create", "after-create"} {
		t.Run(interruption, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "kb-source-migrate")
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "old history")
			must(t, err)
			old, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "ordinary question", time.Minute)
			must(t, err)
			must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, old.ID, "ordinary answer", "test"))
			_, err = s.pool.ExecContext(ctx, `DROP TABLE run_knowledge_base_sources`)
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=19 WHERE singleton=1`)
			must(t, err)
			if interruption == "before-create" {
				// Constraint names are schema-wide: fail CREATE without leaving
				// the source table or advancing its marker.
				_, err = s.pool.ExecContext(ctx, `CREATE TABLE kb_source_migration_blocker (run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT run_knowledge_base_sources_run_fk FOREIGN KEY(run_id) REFERENCES runs(id)) ENGINE=InnoDB`)
			} else {
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER kb_source_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced version marker failure'`)
			}
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("forced migration interruption did not fail")
			}
			var version, tables int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='run_knowledge_base_sources'`).Scan(&tables))
			wantTables := 0
			if interruption == "after-create" {
				wantTables = 1
			}
			if version != 19 || tables != wantTables {
				t.Fatalf("interruption persisted version=%d tables=%d, want 19/%d", version, tables, wantTables)
			}
			if interruption == "before-create" {
				_, err = s.pool.ExecContext(ctx, `DROP TABLE kb_source_migration_blocker`)
			} else {
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER kb_source_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			valid, err := s.CanHydrateRunHistory(ctx, u.ID, w.ID, ss.ID, old.ID, []string{})
			must(t, err)
			if !valid {
				t.Fatal("migration changed a completed run without library sources")
			}
			run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "read metadata", time.Minute)
			must(t, err)
			page, err := s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "", 0, 20)
			must(t, err)
			if len(page.KnowledgeBases) != 1 {
				t.Fatal("expected the default library")
			}
			var before, after string
			must(t, s.pool.QueryRowContext(ctx, `SELECT metadata_sha256 FROM run_knowledge_base_sources WHERE run_id=?`, run.ID).Scan(&before))
			// Replaying a lost marker must retain real observations and hashes.
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=19 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			must(t, s.Migrate(ctx))
			must(t, s.pool.QueryRowContext(ctx, `SELECT metadata_sha256 FROM run_knowledge_base_sources WHERE run_id=?`, run.ID).Scan(&after))
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if before != after || version != len(migrationFiles) {
				t.Fatal("migration replay reset a source hash or version")
			}
			must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "library description", "test"))
		})
	}
}

func TestMySQLKnowledgeBaseSourceBudgetSharedWithDocuments(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "kb-source-budget")
	first, err := s.CreateDocument(ctx, u.ID, w.ID, "first", "first.md", []byte("one"))
	must(t, err)
	second, err := s.CreateDocument(ctx, u.ID, w.ID, "second", "second.md", []byte("two"))
	must(t, err)
	// Create real library metadata efficiently; tool calls below acquire all
	// provenance through the public run-scoped listing entry point.
	must(t, s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, u.ID, w.ID); err != nil {
			return err
		}
		for i := range maxRunHistorySources {
			if _, err := createKnowledgeBaseTx(ctx, tx, u.ID, w.ID, fmt.Sprintf("budget-%03d", i), "目录来源"); err != nil {
				return err
			}
		}
		return nil
	}))
	count := func(runID string, wantKB, wantDocs int) {
		t.Helper()
		var libraries, documents int
		must(t, s.pool.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM run_knowledge_base_sources WHERE run_id=?),(SELECT COUNT(*) FROM run_document_sources WHERE run_id=?)`, runID, runID).Scan(&libraries, &documents))
		if libraries != wantKB || documents != wantDocs {
			t.Fatalf("sources=%d libraries/%d documents, want %d/%d", libraries, documents, wantKB, wantDocs)
		}
	}
	for _, documentsFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("documents-first-%t", documentsFirst), func(t *testing.T) {
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "budget")
			must(t, err)
			run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "read metadata", time.Minute)
			must(t, err)
			if documentsFirst {
				_, err = s.RunToolDocument(ctx, u.ID, w.ID, ss.ID, run.ID, first.Document.ID, "", "", "")
				must(t, err)
			}
			for offset := 0; offset < 240; offset += 20 {
				page, err := s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "budget-", offset, 20)
				must(t, err)
				if len(page.KnowledgeBases) != 20 || !page.HasMore {
					t.Fatal("expected a full page and its lookahead")
				}
			}
			if documentsFirst {
				count(run.ID, 241, 1)
				page, err := s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "budget-", 240, 20)
				wantError(t, err, domain.ErrInvalid)
				if len(page.KnowledgeBases) != 0 || page.HasMore || page.NextOffset != nil {
					t.Fatal("failed page exposed partially admitted library metadata")
				}
				count(run.ID, 241, 1)
				// Admit exactly the remaining capacity, leaving the final library
				// unknown, then prove both metadata types share the same limit.
				for offset := 241; offset < 255; offset++ {
					_, err = s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, fmt.Sprintf("budget-%03d", offset), 0, 1)
					must(t, err)
				}
				count(run.ID, 255, 1)
				_, err = s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "budget-255", 0, 1)
				wantError(t, err, domain.ErrInvalid)
				_, err = s.RunToolDocument(ctx, u.ID, w.ID, ss.ID, run.ID, second.Document.ID, "", "", "")
				wantError(t, err, domain.ErrInvalid)
				_, err = s.RunToolDocument(ctx, u.ID, w.ID, ss.ID, run.ID, first.Document.ID, "", "", "")
				must(t, err)
				count(run.ID, 255, 1)
			} else {
				_, err = s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "budget-", 240, 20)
				must(t, err)
				count(run.ID, 256, 0)
				_, err = s.RunToolDocument(ctx, u.ID, w.ID, ss.ID, run.ID, first.Document.ID, "", "", "")
				wantError(t, err, domain.ErrInvalid)
				count(run.ID, 256, 0)
			}
			// Re-reading observed identities remains idempotent at capacity.
			_, err = s.RunToolKnowledgeBases(ctx, u.ID, w.ID, ss.ID, run.ID, "budget-000", 0, 1)
			must(t, err)
			must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "metadata summary", "test"))
		})
	}
}
