package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

func memoryTimeCheckError(t *testing.T, err error) {
	t.Helper()
	var detail *mysql.MySQLError
	if !errors.As(err, &detail) || detail.Number != 3819 {
		t.Fatalf("expected time CHECK rejection, got %v", err)
	}
}

func TestMySQLMemoryTimeBoundsRejectInvalidStorage(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "memory-time")
	in := memoryInput("retain the valid memory", "reminder")
	item, err := s.CreateMemory(ctx, u.ID, w.ID, in)
	must(t, err)
	for _, column := range []string{"created_at", "expires_at", "last_used_at"} {
		for _, invalid := range []float64{-1, domain.MaxMemoryTimeSeconds, 1e300} {
			_, err = s.pool.ExecContext(ctx, `UPDATE user_memories SET `+column+`=? WHERE id=?`, invalid, item.ID)
			memoryTimeCheckError(t, err)
		}
	}
	loaded, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(loaded) != 1 || !reflect.DeepEqual(loaded[0], item) {
		t.Fatal("rejected SQL time update changed memory")
	}
	must(t, s.ForgetMemory(ctx, u.ID, w.ID, item.ID))
	for _, invalid := range []float64{-1, domain.MaxMemoryTimeSeconds, 1e300} {
		_, err = s.pool.ExecContext(ctx, `UPDATE memory_tombstones SET forgotten_at=? WHERE workspace_id=? AND user_id=?`, invalid, w.ID, u.ID)
		memoryTimeCheckError(t, err)
	}
}

func TestMySQLMemoryTimeBoundsMigrationPreservesAndRejectsHistory(t *testing.T) {
	for _, failure := range []string{"invalid-memory", "invalid-tombstone", "marker"} {
		t.Run(failure, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "memory-time-migrate")
			in := memoryInput("historical private content", "history")
			expiry := float64(domain.MaxMemoryTimeSeconds - 1)
			in.ExpiresAt = &expiry
			item, err := s.CreateMemory(ctx, u.ID, w.ID, in)
			must(t, err)
			must(t, s.ForgetMemory(ctx, u.ID, w.ID, item.ID))
			before, err := s.Memories(ctx, u.ID, w.ID, "all")
			must(t, err)
			var forgotten float64
			must(t, s.pool.QueryRowContext(ctx, `SELECT forgotten_at FROM memory_tombstones WHERE workspace_id=? AND user_id=?`, w.ID, u.ID).Scan(&forgotten))
			for _, q := range []string{`ALTER TABLE user_memories DROP CHECK memories_time_range`, `ALTER TABLE memory_tombstones DROP CHECK memory_tombstones_time_range`, `UPDATE schema_version SET version=27 WHERE singleton=1`} {
				_, err = s.pool.ExecContext(ctx, q)
				must(t, err)
			}
			switch failure {
			case "invalid-memory":
				_, err = s.pool.ExecContext(ctx, `UPDATE user_memories SET expires_at=1e300 WHERE id=?`, item.ID)
			case "invalid-tombstone":
				_, err = s.pool.ExecContext(ctx, `UPDATE memory_tombstones SET forgotten_at=1e300 WHERE workspace_id=? AND user_id=?`, w.ID, u.ID)
			case "marker":
				_, err = s.pool.ExecContext(ctx, `CREATE TRIGGER memory_time_marker_failure BEFORE UPDATE ON schema_version FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced marker failure'`)
			}
			must(t, err)
			for range 2 {
				if err = s.Migrate(ctx); err == nil {
					t.Fatal("invalid history or marker failure accepted")
				}
				var version int
				must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
				if version != 27 {
					t.Fatal("failed time migration advanced version")
				}
			}
			// Repair only the known test corruption; migration itself never changes
			// unsupported historical dates to a guessed value.
			switch failure {
			case "invalid-memory":
				var current float64
				must(t, s.pool.QueryRowContext(ctx, `SELECT expires_at FROM user_memories WHERE id=?`, item.ID).Scan(&current))
				if current != 1e300 {
					t.Fatal("migration rewrote unsupported historical expiry")
				}
				_, err = s.pool.ExecContext(ctx, `UPDATE user_memories SET expires_at=? WHERE id=?`, expiry, item.ID)
			case "invalid-tombstone":
				var current float64
				must(t, s.pool.QueryRowContext(ctx, `SELECT forgotten_at FROM memory_tombstones WHERE workspace_id=? AND user_id=?`, w.ID, u.ID).Scan(&current))
				if current != 1e300 {
					t.Fatal("migration rewrote historical tombstone time")
				}
				_, err = s.pool.ExecContext(ctx, `UPDATE memory_tombstones SET forgotten_at=? WHERE workspace_id=? AND user_id=?`, forgotten, w.ID, u.ID)
			case "marker":
				_, err = s.pool.ExecContext(ctx, `DROP TRIGGER memory_time_marker_failure`)
			}
			must(t, err)
			must(t, s.Migrate(ctx))
			_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=27 WHERE singleton=1`)
			must(t, err)
			must(t, s.Migrate(ctx))
			after, err := s.Memories(ctx, u.ID, w.ID, "all")
			must(t, err)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("migration changed archived content, identity or supported times")
			}
			var actual float64
			var version int
			must(t, s.pool.QueryRowContext(ctx, `SELECT forgotten_at FROM memory_tombstones WHERE workspace_id=? AND user_id=?`, w.ID, u.ID).Scan(&actual))
			must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
			if actual != forgotten || version != len(migrationFiles) {
				t.Fatal("migration lost tombstone time or version")
			}
		})
	}
}
