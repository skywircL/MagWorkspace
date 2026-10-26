package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

// SetObjectStorage is startup configuration, before serving requests or workers.
// A nil store supports inline file fixtures and migrations; Agent database
// persistence does not depend on object storage.
func (s *Store) SetObjectStorage(objects blob.Storage) {
	s.objects = objects
	s.SetObjectCleanupStorages([]blob.Storage{objects})
}

// Register historical destinations before starting requests or cleanup workers.
// They can serve old versions and deletion intents, but do not receive new writes.
func (s *Store) SetObjectCleanupStorages(stores []blob.Storage) {
	if s.objectStores == nil {
		s.objectStores = make(map[string]blob.Storage)
	}
	for _, objects := range stores {
		if objects != nil && validArtifactSHA(objects.StorageID()) {
			// An old-target configuration may repeat the current destination
			// with retired credentials. Keep the current client's credentials.
			if s.objects != nil && objects.StorageID() == s.objects.StorageID() {
				objects = s.objects
			}
			s.objectStores[objects.StorageID()] = objects
		}
	}
}

type stagedDocumentObject struct {
	key, storageID string
}

// stageDocumentObject commits an authorized cleanup record before any external
// write. Its five-minute publication window exceeds the bounded 30-second Put.
// A killed process leaves a durable, eventually eligible cleanup record.
func (s *Store) stageDocumentObject(ctx context.Context, userID, workspaceID, documentID, versionID, baseVersionID string, content []byte) (stagedDocumentObject, error) {
	var staged stagedDocumentObject
	if s.objects == nil {
		return staged, nil
	}
	objects := s.objects
	staged = stagedDocumentObject{key: "documents/" + documentID + "/" + versionID + "/original", storageID: objects.StorageID()}
	if !validArtifactSHA(staged.storageID) {
		return stagedDocumentObject{}, blob.ErrIntegrity
	}
	id, err := newID()
	if err != nil {
		return stagedDocumentObject{}, err
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
		_, err = tx.ExecContext(ctx, `INSERT INTO object_cleanup(id,storage_id,object_key,available_at) VALUES(?,?,?,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 MINUTE))`, id, staged.storageID, staged.key)
		return err
	})
	if err != nil {
		return stagedDocumentObject{}, err
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = objects.Put(uploadCtx, staged.key, content)
	cancel()
	if err != nil {
		// A failed/timeout response may hide a completed remote Put. Preserve
		// the grace period before cleanup so an uncertain upload can settle.
		return stagedDocumentObject{}, err
	}
	return staged, nil
}

// A late successful Put may follow cleanup of its original reservation. Requeue
// it durably; both here and in the cleaner, committed references protect an
// upload whose final database commit succeeded but lost its acknowledgement.
func (s *Store) abandonDocumentObject(staged stagedDocumentObject) {
	if staged.key == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.transaction(ctx, func(tx *sql.Tx) error {
		var referenced bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM document_versions WHERE storage_id=? AND object_key=?)`, staged.storageID, staged.key).Scan(&referenced); err != nil || referenced {
			return err
		}
		return queueObjectCleanupTx(ctx, tx, staged.storageID, staged.key)
	})
}

// The workspace/document lock precedes this row lock. Cleanup can irrevocably
// claim an expired upload; a late final transaction then cannot publish it.
func consumeDocumentObjectTx(ctx context.Context, tx *sql.Tx, staged stagedDocumentObject) error {
	if staged.key == "" {
		return nil
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM object_cleanup WHERE storage_id=? AND object_key=? AND state='pending' AND available_at>UTC_TIMESTAMP(6)`, staged.storageID, staged.key)
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
		objects := s.objectStores[version.StorageID]
		if version.StorageID == "" || objects == nil {
			return nil, blob.ErrUnavailable
		}
		var err error
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		content, err = objects.Read(readCtx, version.ObjectKey, max(1, version.SizeBytes))
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
	if job.Status != "running" || job.Generation != expected.Generation || !active || job.SourceSHA256 != expected.SourceSHA256 || job.StorageID != expected.StorageID {
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
	query := `SELECT v.storage_id,v.object_key FROM document_versions v JOIN documents d ON d.id=v.document_id WHERE d.workspace_id=? AND v.object_key<>''`
	args := []any{workspaceID}
	if documentID != "" {
		query += ` AND d.id=?`
		args = append(args, documentID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var objects []stagedDocumentObject
	for rows.Next() {
		var object stagedDocumentObject
		if err = rows.Scan(&object.storageID, &object.key); err != nil {
			rows.Close()
			return err
		}
		objects = append(objects, object)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err = queueObjectCleanupTx(ctx, tx, object.storageID, object.key); err != nil {
			return err
		}
	}
	return nil
}

func queueObjectCleanupTx(ctx context.Context, tx *sql.Tx, storageID, key string) error {
	id, err := newID()
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO object_cleanup(id,storage_id,object_key,state,available_at) VALUES(?,?,?,'deleting',UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id=?,state='deleting',available_at=UTC_TIMESTAMP(6)`, id, storageID, key, id); err != nil {
		return err
	}
	return queueCleanupRetirementTx(ctx, tx, "object", storageID, "", key)
}

// CleanupObjects drains a bounded batch. The queue has no foreign keys, so it
// survives workspace removal. External deletion never holds a database lock.
func (s *Store) CleanupObjects(ctx context.Context) error {
	var storageIDs []string
	var bindings []any
	var placeholders []string
	for id := range s.objectStores {
		storageIDs = append(storageIDs, id)
		bindings = append(bindings, id)
		placeholders = append(placeholders, "?")
	}
	if err := s.RequeueRetiredCleanup(ctx, "object", storageIDs...); err != nil {
		return err
	}
	query := `SELECT id FROM object_cleanup WHERE available_at<=UTC_TIMESTAMP(6) ORDER BY `
	if len(bindings) > 0 {
		query += `CASE WHEN storage_id IN (` + strings.Join(placeholders, ",") + `) THEN 0 ELSE 1 END,`
	}
	query += `available_at,id LIMIT 100`
	rows, err := s.pool.QueryContext(ctx, query, bindings...)
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
	var key, storageID, token string
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var eligible bool
		if err := tx.QueryRowContext(ctx, `SELECT storage_id,object_key,available_at<=UTC_TIMESTAMP(6) FROM object_cleanup WHERE id=? FOR UPDATE`, id).Scan(&storageID, &key, &eligible); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if !eligible {
			return nil
		}
		var referenced bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM document_versions WHERE storage_id=? AND object_key=?)`, storageID, key).Scan(&referenced); err != nil {
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
		if _, err = tx.ExecContext(ctx, `UPDATE object_cleanup SET id=?,state='deleting',available_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 2 MINUTE),attempts=LEAST(attempts+1,4294967295),last_attempt_at=UTC_TIMESTAMP(6),last_error_code='' WHERE id=?`, token, id); err != nil {
			return err
		}
		return queueCleanupRetirementTx(ctx, tx, "object", storageID, "", key)
	})
	if err != nil || token == "" {
		return err
	}
	objects := s.objectStores[storageID]
	if storageID == "" || objects == nil {
		return s.recordObjectCleanupFailure(token, "storage_binding_unavailable", blob.ErrUnavailable)
	}
	deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = objects.Delete(deleteCtx, key)
	cancel()
	if err != nil {
		code := "object_storage_unavailable"
		if errors.Is(err, blob.ErrIntegrity) {
			code = "object_integrity_failed"
		} else if ctx.Err() != nil {
			code = "cleanup_interrupted"
		}
		return s.recordObjectCleanupFailure(token, code, err)
	}
	_, err = s.pool.ExecContext(ctx, `DELETE FROM object_cleanup WHERE id=? AND state='deleting'`, token)
	return err
}

func (s *Store) recordObjectCleanupFailure(token, code string, cause error) error {
	// Persist a safe diagnosis even when the deletion's own deadline expired.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.pool.ExecContext(ctx, `UPDATE object_cleanup SET last_error_code=? WHERE id=? AND state='deleting'`, code, token)
	return errors.Join(cause, err)
}
