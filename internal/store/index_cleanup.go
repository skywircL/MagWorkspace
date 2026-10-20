package store

import (
	"context"
	"database/sql"
	"time"

	"magworkspace/internal/domain"
)

func queueIndexCleanupTx(ctx context.Context, tx *sql.Tx, profileID, generation string) error {
	if generation == "" {
		return nil
	}
	id, err := newID()
	if err != nil {
		return err
	}
	// Copy the lease before failure clears it or a workspace cascade removes
	// it. Database time defines the grace period even after that source row is
	// gone. A timeout cannot prove remote processing stopped instantaneously.
	var notBefore time.Time
	if err = tx.QueryRowContext(ctx, `SELECT CAST(GREATEST(DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 MINUTE),COALESCE(MAX(DATE_ADD(lease_expires_at,INTERVAL 30 SECOND)),UTC_TIMESTAMP(6))) AS DATETIME(6)) FROM document_indexes WHERE profile_id=? AND generation=?`, profileID, generation).Scan(&notBefore); err != nil {
		return err
	}
	// Keep one pending row, but rotate its identity on reenqueuing. A cleanup
	// already in flight must not acknowledge a later request for the same
	// artifact generation, or shorten a previously copied lease/grace period.
	_, err = tx.ExecContext(ctx, `INSERT INTO index_cleanup(id,profile_id,generation,not_before) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE id=?,not_before=GREATEST(not_before,?),created_at=UTC_TIMESTAMP(6)`, id, profileID, generation, notBefore, id, notBefore)
	return err
}

func queueDocumentCleanupTx(ctx context.Context, tx *sql.Tx, workspaceID, documentID string) error {
	query := `SELECT profile_id,generation FROM document_indexes WHERE workspace_id=? AND generation<>''`
	args := []any{workspaceID}
	if documentID != "" {
		query += ` AND document_id=?`
		args = append(args, documentID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var pending []domain.IndexCleanup
	for rows.Next() {
		var cleanup domain.IndexCleanup
		if err = rows.Scan(&cleanup.ProfileID, &cleanup.Generation); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, cleanup)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, cleanup := range pending {
		if err = queueIndexCleanupTx(ctx, tx, cleanup.ProfileID, cleanup.Generation); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) QueueIndexCleanup(ctx context.Context, profileID, generation string) error {
	if !validProfileID(profileID) || !validHexID(generation) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		return queueIndexCleanupTx(ctx, tx, profileID, generation)
	})
}

func (s *Store) PendingIndexCleanup(ctx context.Context, limit int) ([]domain.IndexCleanup, error) {
	result := []domain.IndexCleanup{}
	if limit <= 0 || limit > 200 {
		return result, domain.ErrInvalid
	}
	// Publication and live lease protection apply in addition to the copied
	// grace period. They also protect a Finish whose acknowledgement was lost.
	rows, err := s.pool.QueryContext(ctx, `SELECT c.id,c.profile_id,c.generation FROM index_cleanup c WHERE c.not_before<=UTC_TIMESTAMP(6) AND NOT EXISTS(SELECT 1 FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id WHERE i.profile_id=c.profile_id AND i.generation=c.generation AND d.deleted_at IS NULL AND (i.status='ready' OR (i.status='running' AND i.lease_expires_at>UTC_TIMESTAMP(6)))) ORDER BY c.not_before,c.id LIMIT ?`, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cleanup domain.IndexCleanup
		if err = rows.Scan(&cleanup.ID, &cleanup.ProfileID, &cleanup.Generation); err != nil {
			return nil, dbError(err)
		}
		result = append(result, cleanup)
	}
	return result, dbError(rows.Err())
}

func (s *Store) CompleteIndexCleanup(ctx context.Context, id string) error {
	if !validHexID(id) {
		return domain.ErrInvalid
	}
	_, err := s.pool.ExecContext(ctx, `DELETE FROM index_cleanup WHERE id=?`, id)
	return dbError(err)
}

// DeferIndexCleanup lets other eligible generations progress after a failed
// cleanup. The identity is an acknowledgement token: a late failure must not
// postpone a newer request that rotated it, nor shorten its protection window.
func (s *Store) DeferIndexCleanup(ctx context.Context, id string, delay time.Duration) error {
	if !validHexID(id) || delay.Microseconds() <= 0 {
		return domain.ErrInvalid
	}
	_, err := s.pool.ExecContext(ctx, `UPDATE index_cleanup SET not_before=GREATEST(not_before,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND)) WHERE id=?`, delay.Microseconds(), id)
	return dbError(err)
}
