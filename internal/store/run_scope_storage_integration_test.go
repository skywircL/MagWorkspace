package store

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLRunKnowledgeScopePersistsThreeStatesAcrossReconnect(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "run-kb-scope-storage")
	kb := defaultKnowledgeBase(t, s, u.ID, w.ID)
	type savedRun struct {
		sessionID, runID string
		knowledgeBases   []string
	}
	var saved []savedRun
	for _, ids := range [][]string{nil, {}, {kb.ID}} {
		ss, err := s.CreateSession(ctx, u.ID, w.ID, "persistent scope")
		must(t, err)
		run, _, _, err := s.BeginRunWithScope(ctx, u.ID, w.ID, ss.ID, "scope", time.Minute, domain.RetrievalScope{KnowledgeBaseIDs: ids})
		must(t, err)
		var count int
		must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_knowledge_base_scopes WHERE run_id=?`, run.ID).Scan(&count))
		if (count == 0) != (ids == nil) || count > 1 {
			t.Fatalf("scope rows=%d for selection %v", count, ids)
		}
		saved = append(saved, savedRun{ss.ID, run.ID, ids})
	}
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	for _, item := range saved {
		got, err := reopened.RunSearchScope(ctx, u.ID, w.ID, item.sessionID, item.runID, nil)
		must(t, err)
		if got.DocumentIDs != nil || (got.KnowledgeBaseIDs == nil) != (item.knowledgeBases == nil) || !slices.Equal(got.KnowledgeBaseIDs, item.knowledgeBases) {
			t.Fatalf("scope changed after reconnect: got=%+v want=%v", got, item.knowledgeBases)
		}
	}
}

func TestMySQLRunKnowledgeScopeBeginFailureRollsBack(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	other, foreign := testUser(t, f.s, "foreign-run-kb-scope")
	foreignKB := defaultKnowledgeBase(t, f.s, other.ID, foreign.ID)
	for _, scenario := range []struct {
		name  string
		scope domain.RetrievalScope
		want  error
	}{
		{"malformed-id", domain.RetrievalScope{KnowledgeBaseIDs: []string{"not-an-id"}}, domain.ErrInvalid},
		{"duplicate-id", domain.RetrievalScope{KnowledgeBaseIDs: []string{f.a.ID, f.a.ID}}, domain.ErrInvalid},
		{"foreign-library", domain.RetrievalScope{KnowledgeBaseIDs: []string{f.a.ID, foreignKB.ID}}, domain.ErrNotFound},
		{"document-outside-library", domain.RetrievalScope{DocumentIDs: []string{f.docB.Document.ID}, KnowledgeBaseIDs: []string{f.a.ID}}, domain.ErrForbidden},
		{"document-with-empty-libraries", domain.RetrievalScope{DocumentIDs: []string{f.docA.Document.ID}, KnowledgeBaseIDs: []string{}}, domain.ErrForbidden},
		{"duplicate-document", domain.RetrievalScope{DocumentIDs: []string{f.docA.Document.ID, f.docA.Document.ID}, KnowledgeBaseIDs: []string{f.a.ID}}, domain.ErrInvalid},
		{"scope-insert-failure", domain.RetrievalScope{DocumentIDs: []string{f.docA.Document.ID}, KnowledgeBaseIDs: []string{f.a.ID}}, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, scenario.name)
			must(t, err)
			if scenario.name == "scope-insert-failure" {
				_, err = f.s.pool.ExecContext(ctx, `CREATE TRIGGER run_scope_insert_failure BEFORE INSERT ON run_knowledge_base_scopes FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced scope insert failure'`)
				must(t, err)
				defer func() {
					_, err := f.s.pool.ExecContext(ctx, `DROP TRIGGER run_scope_insert_failure`)
					must(t, err)
				}()
			}
			_, _, _, err = f.s.BeginRunWithScope(ctx, f.u.ID, f.w.ID, ss.ID, "scope", time.Minute, scenario.scope)
			if scenario.want != nil {
				wantError(t, err, scenario.want)
			} else if err == nil {
				t.Fatal("forced scope insert failure succeeded")
			}
			// This fixture has no successful Runs. Failed selection must not leave
			// a Run, either scope, a user message, or memory queue intent behind.
			for _, table := range []string{"runs", "run_retrieval_scopes", "run_knowledge_base_scopes", "messages", "memory_capture_jobs", "memory_capture_outbox"} {
				var count int
				must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count))
				if count != 0 {
					t.Fatalf("failed run left %d rows in %s", count, table)
				}
			}
		})
	}
}

func TestMySQLRunKnowledgeScopeMigrationRetriesDDLAndMarker(t *testing.T) {
	for _, interruption := range []string{"before-create", "after-create"} {
		t.Run(interruption, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "run-kb-scope-migrate")
			kb := defaultKnowledgeBase(t, s, u.ID, w.ID)
			_, err := s.pool.ExecContext(ctx, `DROP TABLE run_knowledge_base_scopes`)
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=20 WHERE singleton=1`)
			must(t, err)
			if interruption == "before-create" {
				_, err = s.pool.ExecContext(ctx, `CREATE TABLE run_scope_migration_blocker (run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT run_knowledge_base_scopes_run_fk FOREIGN KEY(run_id) REFERENCES runs(id)) ENGINE=InnoDB`)
			} else {
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER run_scope_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced scope version marker failure'`)
			}
			must(t, err)
			if err = s.Migrate(ctx); err == nil {
				t.Fatal("forced scope migration interruption did not fail")
			}
			var version, tables int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='run_knowledge_base_scopes'`).Scan(&tables))
			wantTables := 0
			if interruption == "after-create" {
				wantTables = 1
			}
			if version != 20 || tables != wantTables {
				t.Fatalf("interruption persisted version=%d tables=%d, want 20/%d", version, tables, wantTables)
			}
			if interruption == "before-create" {
				_, err = s.pool.ExecContext(ctx, `DROP TABLE run_scope_migration_blocker`)
			} else {
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER run_scope_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "retained scope")
			must(t, err)
			run, _, _, err := s.BeginRunWithScope(ctx, u.ID, w.ID, ss.ID, "scope", time.Minute, domain.RetrievalScope{KnowledgeBaseIDs: []string{kb.ID}})
			must(t, err)
			var before, after string
			must(t, s.pool.QueryRowContext(ctx, `SELECT knowledge_base_ids FROM run_knowledge_base_scopes WHERE run_id=?`, run.ID).Scan(&before))
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=20 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			must(t, s.Migrate(ctx))
			must(t, s.pool.QueryRowContext(ctx, `SELECT knowledge_base_ids FROM run_knowledge_base_scopes WHERE run_id=?`, run.ID).Scan(&after))
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if before != after || version != len(migrationFiles) {
				t.Fatal("migration replay changed persisted scope or schema version")
			}
			got, err := s.RunSearchScope(ctx, u.ID, w.ID, ss.ID, run.ID, nil)
			must(t, err)
			if !slices.Equal(got.KnowledgeBaseIDs, []string{kb.ID}) {
				t.Fatal("replayed scope lost its restriction")
			}
		})
	}
}

func TestMySQLRunKnowledgeScopeMalformedStorageAndDeletionFailClosed(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "run-kb-scope-corruption")
	kb := defaultKnowledgeBase(t, s, u.ID, w.ID)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "scope")
	must(t, err)
	run, _, _, err := s.BeginRunWithScope(ctx, u.ID, w.ID, ss.ID, "scope", time.Minute, domain.RetrievalScope{KnowledgeBaseIDs: []string{kb.ID}})
	must(t, err)
	original, err := json.Marshal([]string{kb.ID})
	must(t, err)
	for _, invalid := range []string{"null", `{}`, `"all"`} {
		if _, err = s.pool.ExecContext(ctx, `UPDATE run_knowledge_base_scopes SET knowledge_base_ids=? WHERE run_id=?`, invalid, run.ID); err == nil {
			t.Fatalf("database accepted non-array scope %s", invalid)
		}
		got, err := s.RunSearchScope(ctx, u.ID, w.ID, ss.ID, run.ID, nil)
		must(t, err)
		if !slices.Equal(got.KnowledgeBaseIDs, []string{kb.ID}) {
			t.Fatal("rejected invalid JSON changed the scope")
		}
	}
	for _, corrupt := range []any{[]string{kb.ID, kb.ID}, []string{"invalid"}, []string{strings.Repeat("x", 3000)}, []any{nil}} {
		raw, err := json.Marshal(corrupt)
		must(t, err)
		_, err = s.pool.ExecContext(ctx, `UPDATE run_knowledge_base_scopes SET knowledge_base_ids=? WHERE run_id=?`, raw, run.ID)
		must(t, err)
		_, err = s.RunSearchScope(ctx, u.ID, w.ID, ss.ID, run.ID, nil)
		wantError(t, err, domain.ErrInvalid)
		_, err = s.pool.ExecContext(ctx, `UPDATE run_knowledge_base_scopes SET knowledge_base_ids=? WHERE run_id=?`, original, run.ID)
		must(t, err)
	}
	// Deleting a library must retain the selection as a denied capability,
	// including for an empty model selection that never read any source.
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, kb.ID))
	var retained string
	must(t, s.pool.QueryRowContext(ctx, `SELECT knowledge_base_ids FROM run_knowledge_base_scopes WHERE run_id=?`, run.ID).Scan(&retained))
	var ids []string
	must(t, json.Unmarshal([]byte(retained), &ids))
	if !slices.Equal(ids, []string{kb.ID}) {
		t.Fatal("library deletion erased the durable run scope")
	}
	for _, selection := range [][]string{nil, {}} {
		_, err = s.RunSearchScope(ctx, u.ID, w.ID, ss.ID, run.ID, selection)
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("deleted scoped library widened selection %v: %v", selection, err)
		}
	}
}
