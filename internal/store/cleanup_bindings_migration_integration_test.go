package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

type cleanupBindingFixture struct {
	write                              domain.DocumentWrite
	job                                domain.IndexJob
	objectID, indexID, key, generation string
	storageID                          string
}

func newCleanupBindingFixture(t *testing.T, s *Store) cleanupBindingFixture {
	t.Helper()
	ctx := context.Background()
	u, w := testUser(t, s, "cleanup-migration")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "retained original", "original.txt", []byte("unchanged inline original"))
	must(t, err)
	profile := testIndexProfile()
	job, _, err := s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	objectID, err := newID()
	must(t, err)
	indexID, err := newID()
	must(t, err)
	f := cleanupBindingFixture{write: write, job: job, objectID: objectID, indexID: indexID, key: "documents/retired/original", generation: strings.Repeat("c", 32), storageID: profile.StorageID}
	_, err = s.pool.Exec(`INSERT INTO object_cleanup(id,storage_id,object_key,state,available_at) VALUES(?,?,?,'deleting',UTC_TIMESTAMP(6))`, f.objectID, f.storageID, f.key)
	must(t, err)
	_, err = s.pool.Exec(`INSERT INTO index_cleanup(id,storage_id,profile_id,generation,not_before) VALUES(?,?,?,?,UTC_TIMESTAMP(6))`, f.indexID, f.storageID, f.job.ProfileID, f.generation)
	must(t, err)
	return f
}

func cleanupBindingSQLError(t *testing.T, err error, number uint16) {
	t.Helper()
	var dbErr *mysql.MySQLError
	if !errors.As(err, &dbErr) || dbErr.Number != number {
		t.Fatalf("expected MySQL %d, got %v", number, err)
	}
}

func TestMySQLCleanupBindingsRejectInvalidSQL(t *testing.T) {
	s, _ := testStore(t)
	f := newCleanupBindingFixture(t, s)
	_, err := s.pool.Exec(`UPDATE document_versions SET storage_id=? WHERE id=?`, f.storageID, f.write.Version.ID)
	cleanupBindingSQLError(t, err, 3819)
	// Bind an external version for these direct-SQL boundary checks. Inline
	// versions intentionally have no storage target.
	_, err = s.pool.Exec(`UPDATE document_versions SET content=NULL,object_key='documents/current/original',storage_id=? WHERE id=?`, f.storageID, f.write.Version.ID)
	must(t, err)
	_, err = s.pool.Exec(`INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at) VALUES('object',?,'',?,UTC_TIMESTAMP(6))`, f.storageID, f.key)
	must(t, err)
	for _, row := range []struct {
		table, condition string
		id               any
	}{
		{"document_versions", "id=?", f.write.Version.ID},
		{"document_indexes", "id=?", f.job.ID},
		{"object_cleanup", "id=?", f.objectID},
		{"index_cleanup", "id=?", f.indexID},
		{"cleanup_retirements", "kind='object' AND resource_key=?", f.key},
	} {
		for _, bad := range []string{" ", "\t", strings.Repeat("A", 64), strings.Repeat("a", 63), strings.Repeat("a", 63) + "g", strings.Repeat("a", 63) + " "} {
			_, err = s.pool.Exec(`UPDATE `+row.table+` SET storage_id=? WHERE `+row.condition, bad, row.id)
			cleanupBindingSQLError(t, err, 3819)
		}
		// Blank is reserved for unresolved rows, not an inferred current target.
		_, err = s.pool.Exec(`UPDATE `+row.table+` SET storage_id='' WHERE `+row.condition, row.id)
		must(t, err)
		_, err = s.pool.Exec(`UPDATE `+row.table+` SET storage_id=? WHERE `+row.condition, f.storageID, row.id)
		must(t, err)
	}
	for _, token := range []string{" ", strings.Repeat("A", 32), strings.Repeat("a", 31), strings.Repeat("a", 31) + " "} {
		_, err = s.pool.Exec(`UPDATE index_cleanup SET claim_token=?,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE) WHERE id=?`, token, f.indexID)
		cleanupBindingSQLError(t, err, 3819)
	}
	for _, statement := range []string{
		`UPDATE index_cleanup SET claim_token='',lease_expires_at=UTC_TIMESTAMP(6) WHERE id=?`,
		`UPDATE index_cleanup SET claim_token=REPEAT('a',32),lease_expires_at=NULL WHERE id=?`,
	} {
		_, err = s.pool.Exec(statement, f.indexID)
		cleanupBindingSQLError(t, err, 3819)
	}
	_, err = s.pool.Exec(`UPDATE index_cleanup SET claim_token=REPEAT('a',32),lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE) WHERE id=?`, f.indexID)
	must(t, err)
	for _, table := range []string{"object_cleanup", "index_cleanup"} {
		_, err = s.pool.Exec(`UPDATE ` + table + ` SET attempts=-1`)
		cleanupBindingSQLError(t, err, 1264)
	}
	for _, row := range []struct{ kind, profile, key string }{
		{"object ", "", "invalid-kind"},
		{"index ", f.job.ProfileID, f.generation},
		{"object", f.job.ProfileID, "invalid-profile"},
		{"object", "", ""},
		{"index", "", f.generation},
		{"index", f.job.ProfileID, "short"},
	} {
		_, err = s.pool.Exec(`INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))`, row.kind, f.storageID, row.profile, row.key)
		cleanupBindingSQLError(t, err, 3819)
	}
}

func TestMySQLCleanupBindingsUniquePerStorage(t *testing.T) {
	s, _ := testStore(t)
	f := newCleanupBindingFixture(t, s)
	otherStorage := strings.Repeat("d", 64)
	for _, storageID := range []string{otherStorage, f.storageID} {
		objectID, err := newID()
		must(t, err)
		_, err = s.pool.Exec(`INSERT INTO object_cleanup(id,storage_id,object_key,state,available_at) VALUES(?,?,?,'deleting',UTC_TIMESTAMP(6))`, objectID, storageID, f.key)
		if storageID == f.storageID {
			cleanupBindingSQLError(t, err, 1062)
		} else {
			must(t, err)
		}
		indexID, err := newID()
		must(t, err)
		_, err = s.pool.Exec(`INSERT INTO index_cleanup(id,storage_id,profile_id,generation,not_before) VALUES(?,?,?,?,UTC_TIMESTAMP(6))`, indexID, storageID, f.job.ProfileID, f.generation)
		if storageID == f.storageID {
			cleanupBindingSQLError(t, err, 1062)
		} else {
			must(t, err)
		}
	}
	for _, kind := range []string{"object", "index"} {
		profile, key := "", f.key
		if kind == "index" {
			profile, key = f.job.ProfileID, f.generation
		}
		for i, storageID := range []string{f.storageID, otherStorage, f.storageID} {
			_, err := s.pool.Exec(`INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))`, kind, storageID, profile, key)
			if i == 2 {
				cleanupBindingSQLError(t, err, 1062)
			} else {
				must(t, err)
			}
		}
	}
}

func cleanupBindingLegacySchema(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{
		`DROP TABLE cleanup_retirements`,
		`ALTER TABLE document_versions DROP CHECK document_versions_inline_binding`,
		`ALTER TABLE document_versions DROP CHECK document_versions_binding`,
		`ALTER TABLE document_indexes DROP CHECK document_indexes_binding`,
		`ALTER TABLE object_cleanup DROP CHECK object_cleanup_binding`,
		`ALTER TABLE index_cleanup DROP CHECK index_cleanup_binding`,
		`ALTER TABLE index_cleanup DROP CHECK index_cleanup_lease`,
		`ALTER TABLE object_cleanup DROP INDEX object_cleanup_binding_key,ADD UNIQUE KEY object_cleanup_key(object_key)`,
		`ALTER TABLE index_cleanup DROP INDEX index_cleanup_binding_generation,ADD UNIQUE KEY index_cleanup_generation(profile_id,generation)`,
		`ALTER TABLE document_versions DROP COLUMN storage_id`,
		`ALTER TABLE document_indexes DROP COLUMN storage_id`,
		`ALTER TABLE object_cleanup DROP COLUMN storage_id,DROP COLUMN attempts,DROP COLUMN last_error_code,DROP COLUMN last_attempt_at`,
		`ALTER TABLE index_cleanup DROP COLUMN storage_id,DROP COLUMN attempts,DROP COLUMN last_error_code,DROP COLUMN last_attempt_at,DROP COLUMN claim_token,DROP COLUMN lease_expires_at`,
		`UPDATE schema_version SET version=28 WHERE singleton=1`,
	} {
		_, err := s.pool.Exec(statement)
		must(t, err)
	}
}

func assertCleanupBindingHistory(t *testing.T, s *Store, f cleanupBindingFixture) {
	t.Helper()
	for _, row := range []struct{ table, id string }{
		{"document_versions", f.write.Version.ID}, {"document_indexes", f.job.ID}, {"object_cleanup", f.objectID}, {"index_cleanup", f.indexID},
	} {
		var binding string
		must(t, s.pool.QueryRow(`SELECT storage_id FROM `+row.table+` WHERE id=?`, row.id).Scan(&binding))
		if binding != "" {
			t.Fatalf("migration guessed %s target: %q", row.table, binding)
		}
	}
	var original, hash, key string
	var bytes int
	must(t, s.pool.QueryRow(`SELECT content,sha256,size_bytes,object_key FROM document_versions WHERE id=?`, f.write.Version.ID).Scan(&original, &hash, &bytes, &key))
	if original != "unchanged inline original" || hash != f.write.Version.SHA256 || bytes != f.write.Version.SizeBytes || key != "" {
		t.Fatal("migration changed existing original bytes or metadata")
	}
	var count int
	must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM cleanup_retirements WHERE storage_id=''`).Scan(&count))
	if count != 2 {
		t.Fatalf("migration lost unresolved retirement responsibility: %d", count)
	}
}

func TestMySQLCleanupBindingsMigrationPreservesUnknownAndReenters(t *testing.T) {
	for _, failure := range []string{"binding-check", "claim-check", "marker"} {
		t.Run(failure, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			f := newCleanupBindingFixture(t, s)
			cleanupBindingLegacySchema(t, s)
			var err error
			switch failure {
			case "binding-check":
				_, err = s.pool.Exec(`ALTER TABLE object_cleanup ADD COLUMN storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''`)
				must(t, err)
				_, err = s.pool.Exec(`UPDATE object_cleanup SET storage_id=' '`)
			case "claim-check":
				_, err = s.pool.Exec(`ALTER TABLE index_cleanup ADD COLUMN claim_token varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',ADD COLUMN lease_expires_at datetime(6) NULL`)
				must(t, err)
				_, err = s.pool.Exec(`UPDATE index_cleanup SET claim_token=REPEAT('a',32)`)
			case "marker":
				_, err = s.pool.Exec(`CREATE TRIGGER cleanup_binding_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced marker failure'`)
			}
			must(t, err)
			for range 2 {
				if err = s.Migrate(ctx); err == nil {
					t.Fatal("partial or invalid cleanup migration succeeded")
				}
				var version int
				must(t, s.pool.QueryRow(`SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
				if version != 28 {
					t.Fatalf("failed migration advanced marker: %d", version)
				}
			}
			// Only repair the deliberately malformed fixture. Migration must not
			// overwrite an unknown target or invent a valid worker claim.
			switch failure {
			case "binding-check":
				var binding string
				must(t, s.pool.QueryRow(`SELECT storage_id FROM object_cleanup WHERE id=?`, f.objectID).Scan(&binding))
				if binding != " " {
					t.Fatal("migration rewrote malformed binding")
				}
				_, err = s.pool.Exec(`UPDATE object_cleanup SET storage_id=''`)
			case "claim-check":
				var token string
				must(t, s.pool.QueryRow(`SELECT claim_token FROM index_cleanup WHERE id=?`, f.indexID).Scan(&token))
				if token != strings.Repeat("a", 32) {
					t.Fatal("migration rewrote malformed claim")
				}
				_, err = s.pool.Exec(`UPDATE index_cleanup SET claim_token=''`)
			case "marker":
				_, err = s.pool.Exec(`DROP TRIGGER cleanup_binding_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			assertCleanupBindingHistory(t, s, f)
			_, err = s.pool.Exec(`UPDATE schema_version SET version=28 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			assertCleanupBindingHistory(t, s, f)
			var version int
			must(t, s.pool.QueryRow(`SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if version != len(migrationFiles) {
				t.Fatal("cleanup migration did not finish")
			}
			objects := newMemoryObjects()
			objects.data[f.key] = []byte("same key at the current target must survive")
			var deletes int
			objects.deleteHook = func(context.Context, string) error { deletes++; return nil }
			s.SetObjectStorage(objects)
			wantError(t, s.CleanupObjects(ctx), blob.ErrUnavailable)
			if deletes != 0 || objects.count() != 1 {
				t.Fatal("unresolved historical cleanup reached the current storage")
			}
			var binding, code string
			must(t, s.pool.QueryRow(`SELECT storage_id,last_error_code FROM object_cleanup WHERE object_key=?`, f.key).Scan(&binding, &code))
			if binding != "" || code != "storage_binding_unavailable" {
				t.Fatal("unknown historical target lost its diagnosable cleanup responsibility")
			}
		})
	}
}
