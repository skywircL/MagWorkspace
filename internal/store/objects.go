package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

// SetObjectStorage is startup configuration, before serving requests or workers.
// A nil store supports inline file fixtures and migrations; Agent database
// persistence does not depend on object storage.
func (s *Store) SetObjectStorage(objects blob.Storage) { s.objects = objects }

// stageDocumentObject commits an authorized cleanup record before any external
// write. Its five-minute publication window exceeds the bounded 30-second Put.
// A killed process leaves a durable, eventually eligible cleanup record.
func (s *Store) stageDocumentObject(ctx context.Context, userID, workspaceID, documentID, versionID, baseVersionID string, content []byte) (string, error) {
	if s.objects == nil {
		return "", nil
	}
	key := "documents/" + documentID + "/" + versionID + "/original"
	id, err := newID()
	if err != nil {
		return "", err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if baseVersionID != "" {
			document, err := documentInWorkspace(ctx, tx, workspaceID, documentID)
			if err != nil {
				return err
			}
			if !canWriteDocument(role) {
				return domain.ErrForbidden
			}
			if document.CurrentVersionID != baseVersionID {
				return domain.ErrConflict
			}
		} else if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO object_cleanup(id,object_key,available_at) VALUES(?,?,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 MINUTE))`, id, key)
		return err
	})
	if err != nil {
		return "", err
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = s.objects.Put(uploadCtx, key, content)
	cancel()
	if err != nil {
		// A failed/timeout response may hide a completed remote Put. Preserve
		// the grace period before cleanup so an uncertain upload can settle.
		return "", err
	}
	return key, nil
}

// Do not insert on failure: an uncertain commit may already have linked the
// object and removed its cleanup row. Existing records remain safe to retry.
func (s *Store) abandonDocumentObject(key string) {
	if key == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.pool.ExecContext(ctx, `UPDATE object_cleanup SET available_at=UTC_TIMESTAMP(6) WHERE object_key=? AND state='pending'`, key)
}

// The workspace/document lock precedes this row lock. Cleanup can irrevocably
// claim an expired upload; a late final transaction then cannot publish it.
func consumeDocumentObjectTx(ctx context.Context, tx *sql.Tx, key string) error {
	if key == "" {
		return nil
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM object_cleanup WHERE object_key=? AND state='pending' AND available_at>UTC_TIMESTAMP(6)`, key)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return domain.ErrConflict
	}
	return err
}

func (s *Store) loadDocumentBytes(ctx context.Context, version domain.DocumentVersion, inline []byte) ([]byte, error) {
	// Metadata is authenticated before I/O. Use its exact byte count so reducing
	// the deployment upload cap cannot break downloads or queued indexing.
	if version.SizeBytes < 0 || version.SizeBytes > domain.MaxStoredDocumentBytes {
		return nil, blob.ErrIntegrity
	}
	content := inline
	if version.ObjectKey != "" {
		if s.objects == nil {
			return nil, blob.ErrUnavailable
		}
		var err error
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		content, err = s.objects.Read(readCtx, version.ObjectKey, max(1, version.SizeBytes))
		cancel()
		if err != nil {
			return nil, err
		}
	}
	digest := sha256.Sum256(content)
	if len(content) != version.SizeBytes || len(content) > domain.MaxStoredDocumentBytes || hex.EncodeToString(digest[:]) != version.SHA256 {
		return nil, blob.ErrIntegrity
	}
	return content, nil
}

// LoadIndexSource performs external I/O outside all database transactions, and
// checks the current lease and creator authorization on both sides of that I/O.
func (s *Store) LoadIndexSource(ctx context.Context, input domain.IndexInput) ([]byte, error) {
	var version domain.DocumentVersion
	var inline []byte
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := validateIndexSourceTx(ctx, tx, input.Job); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+`,content FROM document_versions WHERE id=? AND document_id=?`, input.Job.VersionID, input.Job.DocumentID).Scan(append(documentVersionDest(&version), &inline)...)
	})
	if err != nil {
		return nil, err
	}
	if version.SHA256 != input.Job.SourceSHA256 {
		return nil, blob.ErrIntegrity
	}
	content, err := s.loadDocumentBytes(ctx, version, inline)
	if err != nil {
		return nil, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error { return validateIndexSourceTx(ctx, tx, input.Job) })
	if err != nil {
		return nil, err
	}
	return content, nil
}

func validateIndexSourceTx(ctx context.Context, tx *sql.Tx, expected domain.IndexJob) error {
	job, deleted, active, err := lockIndex(ctx, tx, expected)
	if err != nil {
		return err
	}
	if deleted {
		return domain.ErrNotFound
	}
	if job.Status != "running" || job.Generation != expected.Generation || !active || job.SourceSHA256 != expected.SourceSHA256 {
		return domain.ErrConflict
	}
	allowed, err := currentIndexWriter(ctx, tx, job)
	if err != nil {
		return err
	}
	if !allowed {
		return domain.ErrForbidden
	}
	return nil
}

func queueWorkspaceObjectsTx(ctx context.Context, tx *sql.Tx, workspaceID string) error {
	return queueDocumentObjectsTx(ctx, tx, workspaceID, "")
}

// queueDocumentObjectsTx must run before deleting version rows, while the
// caller holds the workspace lock. An empty documentID covers the workspace.
func queueDocumentObjectsTx(ctx context.Context, tx *sql.Tx, workspaceID, documentID string) error {
	query := `SELECT v.object_key FROM document_versions v JOIN documents d ON d.id=v.document_id WHERE d.workspace_id=? AND v.object_key<>''`
	args := []any{workspaceID}
	if documentID != "" {
		query += ` AND d.id=?`
		args = append(args, documentID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, key := range keys {
		id, err := newID()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO object_cleanup(id,object_key,state,available_at) VALUES(?,?,'deleting',UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id=?,state='deleting',available_at=UTC_TIMESTAMP(6)`, id, key, id); err != nil {
			return err
		}
	}
	return nil
}

// CleanupObjects drains a bounded batch. The queue has no foreign keys, so it
// survives workspace removal. External deletion never holds a database lock.
func (s *Store) CleanupObjects(ctx context.Context) error {
	if s.objects == nil {
		return blob.ErrUnavailable
	}
	rows, err := s.pool.QueryContext(ctx, `SELECT id FROM object_cleanup WHERE available_at<=UTC_TIMESTAMP(6) ORDER BY available_at,id LIMIT 100`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var cleanupErrors []error
	for _, id := range ids {
		if err = s.cleanupObject(ctx, id); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(cleanupErrors...)
}

func (s *Store) cleanupObject(ctx context.Context, id string) error {
	var key, token string
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var eligible bool
		if err := tx.QueryRowContext(ctx, `SELECT object_key,available_at<=UTC_TIMESTAMP(6) FROM object_cleanup WHERE id=? FOR UPDATE`, id).Scan(&key, &eligible); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if !eligible {
			return nil
		}
		var referenced bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM document_versions WHERE object_key=?)`, key).Scan(&referenced); err != nil {
			return err
		}
		if referenced {
			_, err := tx.ExecContext(ctx, `DELETE FROM object_cleanup WHERE id=?`, id)
			return err
		}
		var err error
		token, err = newID()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE object_cleanup SET id=?,state='deleting',available_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 2 MINUTE) WHERE id=?`, token, id)
		return err
	})
	if err != nil || token == "" {
		return err
	}
	deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = s.objects.Delete(deleteCtx, key)
	cancel()
	if err != nil {
		return err
	}
	_, err = s.pool.ExecContext(ctx, `DELETE FROM object_cleanup WHERE id=? AND state='deleting'`, token)
	return err
}
