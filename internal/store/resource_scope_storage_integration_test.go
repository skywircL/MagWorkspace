package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLResourceScopeNormalizationPersistenceAndLegacy(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		selection     domain.ResourceSelection
		all           bool
		targets       int
		projectionNil bool
	}{
		{"all", domain.ResourceSelection{}, true, 0, true},
		{"none", domain.ResourceSelection{KnowledgeBaseIDs: []string{}}, false, 0, false},
		{"document-only", domain.ResourceSelection{DocumentIDs: []string{f.docB.Document.ID}}, false, 1, false},
		{"full-covers-doc", domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docA.Document.ID}}, false, 1, true},
		{"mixed", domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docB.Document.ID}}, false, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, tc.name)
			must(t, err)
			run, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "scope", time.Minute, tc.selection)
			must(t, err)
			scope, err := f.s.RunResourceScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil)
			must(t, err)
			if scope.All != tc.all || len(scope.Targets) != tc.targets {
				t.Fatalf("normalization: %+v", scope)
			}
			projection, err := f.s.RunDocumentScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID)
			must(t, err)
			if (projection == nil) != tc.projectionNil {
				t.Fatalf("unsafe Harness projection: %#v", projection)
			}
			var raw []byte
			must(t, f.s.pool.QueryRowContext(ctx, `SELECT scope FROM run_search_targets WHERE run_id=?`, run.ID).Scan(&raw))
			var persisted domain.ResourceScope
			must(t, json.Unmarshal(raw, &persisted))
			if !reflect.DeepEqual(scope, persisted) {
				t.Fatal("durable scope differs")
			}
		})
	}
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "old intersection")
	must(t, err)
	run, _, _, err := f.s.BeginRunWithScope(ctx, f.u.ID, f.w.ID, ss.ID, "old", time.Minute, domain.RetrievalScope{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docA.Document.ID}})
	must(t, err)
	scope, err := f.s.RunResourceScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil)
	must(t, err)
	if scope.AllowsCreate(f.a.ID) || scope.AllowsDocument(f.a.ID, f.docB.Document.ID) || !scope.AllowsDocument(f.a.ID, f.docA.Document.ID) {
		t.Fatal("legacy intersection widened")
	}
}

func TestMySQLResourceScopeRollbackMalformedAndDeleted(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	for _, selection := range []domain.ResourceSelection{
		{DocumentIDs: []string{f.docA.Document.ID, f.docA.Document.ID}},
		{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{"bad"}},
	} {
		ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "invalid")
		must(t, err)
		_, _, _, err = f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "invalid", time.Minute, selection)
		wantError(t, err, domain.ErrInvalid)
		var n int
		must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE session_id=?`, ss.ID).Scan(&n))
		if n != 0 {
			t.Fatal("invalid input left Run")
		}
	}
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "insert rollback")
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `CREATE TRIGGER resource_scope_insert_failure BEFORE INSERT ON run_search_targets FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced scope insert failure'`)
	must(t, err)
	_, _, _, err = f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "rollback", time.Minute, domain.ResourceSelection{})
	if err == nil {
		t.Fatal("expected insertion failure")
	}
	_, err = f.s.pool.ExecContext(ctx, `DROP TRIGGER resource_scope_insert_failure`)
	must(t, err)
	var n int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE session_id=?`, ss.ID).Scan(&n))
	if n != 0 {
		t.Fatal("scope insertion failure left Run")
	}
	run, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "valid", time.Minute, domain.ResourceSelection{DocumentIDs: []string{f.docB.Document.ID}})
	must(t, err)
	for _, raw := range []string{`{"all":false,"targets":[{"knowledge_base_id":"` + f.b.ID + `"}]}`, `{"all":true}`, `{"all":false,"targets":[{"knowledge_base_id":"` + f.b.ID + `","document_ids":null,"unexpected":1}]}`} {
		_, err = f.s.pool.ExecContext(ctx, `UPDATE run_search_targets SET scope=? WHERE run_id=?`, raw, run.ID)
		must(t, err)
		_, err = f.s.RunResourceScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil)
		wantError(t, err, domain.ErrInvalid)
	}
	raw, _ := json.Marshal(domain.ResourceScope{Targets: []domain.SearchTarget{{KnowledgeBaseID: f.b.ID, DocumentIDs: []string{f.docB.Document.ID}}}})
	_, err = f.s.pool.ExecContext(ctx, `UPDATE run_search_targets SET scope=? WHERE run_id=?`, raw, run.ID)
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `DELETE FROM run_search_targets WHERE run_id=?`, run.ID)
	must(t, err)
	_, err = f.s.RunResourceScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil)
	wantError(t, err, domain.ErrInvalid)
	_, err = f.s.pool.ExecContext(ctx, `INSERT INTO run_search_targets(run_id,scope) VALUES(?,?)`, run.ID, raw)
	must(t, err)
	_, err = f.s.RunSearchScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, nil)
	wantError(t, err, domain.ErrInvalid)
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.docB.Document.ID))
	_, err = f.s.RunResourceScope(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, []string{})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted selection accepted via empty tool filter: %v", err)
	}
}

func TestMySQLResourceScopeMigrationReentry(t *testing.T) {
	for _, phase := range []string{"before-create", "after-create"} {
		t.Run(phase, func(t *testing.T) {
			s, dsn := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "resource-migration")
			_, err := s.pool.ExecContext(ctx, `DROP TABLE run_search_targets`)
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=21 WHERE singleton=1`)
			must(t, err)
			if phase == "before-create" {
				_, err = s.pool.ExecContext(ctx, `CREATE TABLE resource_migration_blocker (run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, CONSTRAINT run_search_targets_run_fk FOREIGN KEY(run_id) REFERENCES runs(id)) ENGINE=InnoDB`)
			} else {
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER resource_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced marker failure'`)
			}
			must(t, err)
			if s.Migrate(ctx) == nil {
				t.Fatal("interrupted migration succeeded")
			}
			var version int
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if version != 21 {
				t.Fatal("advanced failed migration")
			}
			if phase == "before-create" {
				_, err = s.pool.ExecContext(ctx, `DROP TABLE resource_migration_blocker`)
			} else {
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER resource_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "persist")
			must(t, err)
			run, _, _, err := s.BeginRunWithResources(ctx, u.ID, w.ID, ss.ID, "scope", time.Minute, domain.ResourceSelection{DocumentIDs: []string{}})
			must(t, err)
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=21 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			reopened, err := Open(ctx, dsn)
			must(t, err)
			defer reopened.Close()
			scope, err := reopened.RunResourceScope(ctx, u.ID, w.ID, ss.ID, run.ID, nil)
			must(t, err)
			if scope.All || len(scope.Targets) != 0 {
				t.Fatal("reentry or reconnect widened scope")
			}
		})
	}
}
