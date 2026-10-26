// Package store owns MySQL persistence and transactional authorization.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

var migrationFiles = []string{"migrations/001_initial.sql", "migrations/002_documents.sql", "migrations/003_indexes.sql", "migrations/004_document_objects.sql", "migrations/005_evidence.sql", "migrations/006_tool_sources.sql", "migrations/007_run_prompts.sql", "migrations/008_run_journals.sql", "migrations/009_memories.sql", "migrations/010_document_approvals.sql", "migrations/011_document_upload_limits.sql", "migrations/012_memory_settings.sql", "migrations/013_memory_capture.sql", "migrations/014_index_outbox.sql", "migrations/015_parent_child_chunks.sql", "migrations/016_agent_state_content.sql", "migrations/017_knowledge_bases.sql", "migrations/018_document_deletion.sql", "migrations/019_memory_capture_outbox.sql", "migrations/020_knowledge_base_sources.sql", "migrations/021_run_knowledge_base_scopes.sql", "migrations/022_run_search_targets.sql", "migrations/023_index_lineage.sql", "migrations/024_run_relationships.sql", "migrations/025_source_integrity.sql", "migrations/026_prompt_integrity.sql", "migrations/027_memory_capture_lineage.sql", "migrations/028_memory_time_bounds.sql", "migrations/029_cleanup_bindings.sql"}

type Store struct {
	pool           *sql.DB
	maxUploadBytes int
	indexProfile   *domain.IndexProfile
	objects        blob.Storage
	objectStores   map[string]blob.Storage
}
type rowScanner interface{ Scan(...any) error }

func Open(ctx context.Context, dsn string, profiles ...domain.IndexProfile) (*Store, error) {
	if len(profiles) > 1 || (len(profiles) == 1 && !validIndexProfile(profiles[0])) {
		return nil, domain.ErrInvalid
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("configure database: %w", err)
	}
	config.ParseTime = true
	config.Loc = time.UTC
	if config.Params == nil {
		config.Params = map[string]string{}
	}
	config.Params["time_zone"] = "'+00:00'"
	pool, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("configure database: %w", err)
	}
	pool.SetMaxOpenConns(20)
	pool.SetMaxIdleConns(5)
	pool.SetConnMaxLifetime(5 * time.Minute)
	if err = pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}
	store := &Store{pool: pool}
	if len(profiles) == 1 {
		profile := profiles[0]
		store.indexProfile = &profile
	}
	return store, nil
}
func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.PingContext(ctx) }

// Migrate serializes startup migrations on one MySQL connection. MySQL DDL
// implicitly commits, so each statement is resumable and the version advances
// only after all statements in that version succeed. Each migration is retried
// after interruption; MySQL DDL is not an atomic or reversible schema change.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var database string
	if err = conn.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&database); err != nil {
		return err
	}
	databaseHash := sha256.Sum256([]byte(database))
	lock := fmt.Sprintf("magworkspace:%x", databaseHash[:20])
	var acquired int
	if err = conn.QueryRowContext(ctx, `SELECT GET_LOCK(?,10)`, lock).Scan(&acquired); err != nil {
		return err
	}
	if acquired != 1 {
		return fmt.Errorf("database migration lock unavailable")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn.ExecContext(releaseCtx, `DO RELEASE_LOCK(?)`, lock)
	}()
	if _, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (singleton tinyint PRIMARY KEY, version int NOT NULL, CONSTRAINT schema_version_singleton CHECK(singleton=1)) ENGINE=InnoDB`); err != nil {
		return err
	}
	var version int
	err = conn.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		version = 0
	} else if err != nil {
		return err
	}
	if version < 0 || version > len(migrationFiles) {
		return fmt.Errorf("unsupported database schema version %d", version)
	}
	for next := version + 1; next <= len(migrationFiles); next++ {
		if next == 15 {
			// Retired manifests cannot become trusted database chunks. Refuse
			// non-empty old indexes before any destructive DDL. A fully
			// applied v15 with only a lost marker remains safely resumable.
			var count, legacyColumns, readySchema int
			if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_indexes`).Scan(&count); err != nil {
				return err
			}
			if count > 0 {
				if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name IN ('artifact_key','artifact_sha256')`).Scan(&legacyColumns); err != nil {
					return err
				}
				if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_indexes' AND constraint_name='document_indexes_chunks'`).Scan(&readySchema); err != nil {
					return err
				}
				if legacyColumns > 0 || readySchema != 1 {
					return fmt.Errorf("clear retired indexes before parent_child migration")
				}
			}
		}
		content, err := migrations.ReadFile(migrationFiles[next-1])
		if err != nil {
			return err
		}
		if next == 18 {
			// This data-only retirement must atomically retain external cleanup
			// intents and remove their sources, including its version marker.
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			if err = executeMigrationStatements(ctx, tx, content); err == nil {
				_, err = tx.ExecContext(ctx, `INSERT INTO schema_version(singleton,version) VALUES(1,?) ON DUPLICATE KEY UPDATE version=?`, next, next)
			}
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("migration %03d: %w", next, err)
			}
			if err = tx.Commit(); err != nil {
				return fmt.Errorf("migration %03d: %w", next, err)
			}
			continue
		}
		if err = executeMigrationStatements(ctx, conn, content); err != nil {
			return fmt.Errorf("migration %03d: %w", next, err)
		}
		if _, err = conn.ExecContext(ctx, `INSERT INTO schema_version(singleton,version) VALUES(1,?) ON DUPLICATE KEY UPDATE version=?`, next, next); err != nil {
			return err
		}
	}
	return nil
}

func executeMigrationStatements(ctx context.Context, target interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, content []byte) error {
	for _, statement := range strings.Split(string(content), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := target.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) transaction(ctx context.Context, f func(*sql.Tx) error) error {
	// Read committed avoids stale membership snapshots after waiting for a
	// workspace row lock and unnecessary range locks for independent sessions.
	tx, err := s.pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return dbError(err)
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return dbError(err)
	}
	return dbError(tx.Commit())
}
func dbError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrNotFound
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1062, 1213, 1205:
			return domain.ErrConflict
		case 1451, 1452:
			return domain.ErrNotFound
		case 1406, 3819, 1366:
			return domain.ErrInvalid
		}
	}
	return err
}
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func validText(v string, max int) bool { return strings.TrimSpace(v) != "" && len(v) <= max }
func normalizedEmail(v string) (string, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	a, err := mail.ParseAddress(v)
	if err != nil || a.Address != v || len(v) > 254 {
		return "", domain.ErrInvalid
	}
	return v, nil
}

// lockWorkspace is acquired before a session/document lock, then an index job
// lock. Membership changes, document changes and publication share this boundary.
func lockWorkspace(ctx context.Context, tx *sql.Tx, userID, workspaceID string) (string, error) {
	var id, role string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE id=? FOR UPDATE`, workspaceID).Scan(&id); err != nil {
		return "", dbError(err)
	}
	err := tx.QueryRowContext(ctx, `SELECT role FROM workspace_members WHERE workspace_id=? AND user_id=?`, workspaceID, userID).Scan(&role)
	return role, dbError(err)
}
func expireRuns(ctx context.Context, tx *sql.Tx, workspaceID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs r JOIN sessions s ON s.id=r.session_id SET r.status='failed',r.error_code='lease_expired',r.finished_at=UTC_TIMESTAMP(6) WHERE s.workspace_id=? AND r.status='running' AND r.lease_expires_at<=UTC_TIMESTAMP(6)`, workspaceID)
	return err
}
func activeForUser(ctx context.Context, tx *sql.Tx, workspaceID, userID string) (bool, error) {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN sessions s ON s.id=r.session_id WHERE s.workspace_id=? AND s.user_id=? AND r.status='running' AND r.lease_expires_at>UTC_TIMESTAMP(6))`, workspaceID, userID).Scan(&active)
	return active, err
}
