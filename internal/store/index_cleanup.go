package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"magworkspace/internal/domain"
)

func queueIndexCleanupTx(ctx context.Context, tx *sql.Tx, storageID, profileID, generation string) error {
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
	if err = tx.QueryRowContext(ctx, `SELECT CAST(GREATEST(DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 MINUTE),COALESCE(MAX(DATE_ADD(lease_expires_at,INTERVAL 30 SECOND)),UTC_TIMESTAMP(6))) AS DATETIME(6)) FROM document_indexes WHERE storage_id=? AND profile_id=? AND generation=?`, storageID, profileID, generation).Scan(&notBefore); err != nil {
		return err
	}
	// Keep one pending row, but rotate its identity on reenqueuing. A cleanup
	// already in flight must not acknowledge a later request for the same
	// artifact generation, or shorten a previously copied lease/grace period.
	_, err = tx.ExecContext(ctx, `INSERT INTO index_cleanup(id,storage_id,profile_id,generation,not_before) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE id=?,not_before=GREATEST(not_before,?),claim_token='',lease_expires_at=NULL,created_at=UTC_TIMESTAMP(6)`, id, storageID, profileID, generation, notBefore, id, notBefore)
	if err != nil {
		return err
	}
	return queueCleanupRetirementTx(ctx, tx, "index", storageID, profileID, generation)
}

func queueDocumentCleanupTx(ctx context.Context, tx *sql.Tx, workspaceID, documentID string) error {
	query := `SELECT storage_id,profile_id,generation FROM document_indexes WHERE workspace_id=? AND generation<>''`
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
		if err = rows.Scan(&cleanup.StorageID, &cleanup.ProfileID, &cleanup.Generation); err != nil {
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
		if err = queueIndexCleanupTx(ctx, tx, cleanup.StorageID, cleanup.ProfileID, cleanup.Generation); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) QueueIndexCleanup(ctx context.Context, storageID, profileID, generation string) error {
	if (storageID != "" && !validArtifactSHA(storageID)) || !validProfileID(profileID) || !validHexID(generation) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		return queueIndexCleanupTx(ctx, tx, storageID, profileID, generation)
	})
}

// A live publication or writer cannot be retired. Claims also exclude another
// cleaner's active lease; the SQL row, not a Redis delivery, owns admission.
const indexCleanupEligible = `c.not_before<=UTC_TIMESTAMP(6) AND (c.lease_expires_at IS NULL OR c.lease_expires_at<=UTC_TIMESTAMP(6)) AND NOT EXISTS(SELECT 1 FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id WHERE i.storage_id=c.storage_id AND i.profile_id=c.profile_id AND i.generation=c.generation AND d.deleted_at IS NULL AND (i.status='ready' OR (i.status='running' AND i.lease_expires_at>UTC_TIMESTAMP(6))))`

func (s *Store) ClaimIndexCleanup(ctx context.Context, storageIDs []string, limit int, lease time.Duration) ([]domain.IndexCleanup, error) {
	if len(storageIDs) > 64 || limit <= 0 || limit > 200 || lease.Microseconds() <= 0 {
		return nil, domain.ErrInvalid
	}
	order := "c.not_before,c.id"
	args := []any{}
	if len(storageIDs) > 0 {
		for _, id := range storageIDs {
			if !validArtifactSHA(id) {
				return nil, domain.ErrInvalid
			}
			args = append(args, id)
		}
		order = "(c.storage_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(storageIDs)), ",") + ")) DESC," + order
	}
	args = append(args, limit)
	items := []domain.IndexCleanup{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT c.id,c.storage_id,c.profile_id,c.generation FROM index_cleanup c WHERE `+indexCleanupEligible+` ORDER BY `+order+` LIMIT ? FOR UPDATE SKIP LOCKED`, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var item domain.IndexCleanup
			if err = rows.Scan(&item.ID, &item.StorageID, &item.ProfileID, &item.Generation); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for i := range items {
			items[i].ClaimToken, err = newID()
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE index_cleanup SET claim_token=?,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND),attempts=attempts+1,last_attempt_at=UTC_TIMESTAMP(6) WHERE id=?`, items[i].ClaimToken, lease.Microseconds(), items[i].ID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) PendingIndexCleanup(ctx context.Context, limit int) ([]domain.IndexCleanup, error) {
	result := []domain.IndexCleanup{}
	if limit <= 0 || limit > 200 {
		return result, domain.ErrInvalid
	}
	// Publication and live lease protection apply in addition to the copied
	// grace period. They also protect a Finish whose acknowledgement was lost.
	rows, err := s.pool.QueryContext(ctx, `SELECT c.id,c.storage_id,c.profile_id,c.generation FROM index_cleanup c WHERE `+indexCleanupEligible+` ORDER BY c.not_before,c.id LIMIT ?`, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cleanup domain.IndexCleanup
		if err = rows.Scan(&cleanup.ID, &cleanup.StorageID, &cleanup.ProfileID, &cleanup.Generation); err != nil {
			return nil, dbError(err)
		}
		result = append(result, cleanup)
	}
	return result, dbError(rows.Err())
}

func (s *Store) CompleteIndexCleanup(ctx context.Context, id, token string) error {
	if !validHexID(id) || !validHexID(token) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		var storageID, profileID, generation string
		var due bool
		err := tx.QueryRowContext(ctx, `SELECT storage_id,profile_id,generation,not_before<=UTC_TIMESTAMP(6) FROM index_cleanup WHERE id=? AND claim_token=? AND lease_expires_at>UTC_TIMESTAMP(6) FOR UPDATE`, id, token).Scan(&storageID, &profileID, &generation, &due)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !due {
			return nil
		}
		var protected bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id WHERE i.storage_id=? AND i.profile_id=? AND i.generation=? AND d.deleted_at IS NULL AND (i.status='ready' OR (i.status='running' AND i.lease_expires_at>UTC_TIMESTAMP(6))))`, storageID, profileID, generation).Scan(&protected); err != nil {
			return err
		}
		if protected {
			return nil
		}
		if _, err = tx.ExecContext(ctx, `DELETE c FROM chunks c JOIN document_indexes i ON i.id=c.index_id WHERE i.storage_id=? AND i.profile_id=? AND c.generation=?`, storageID, profileID, generation); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM index_cleanup WHERE id=?`, id)
		return err
	})
}

// DeferIndexCleanup lets other eligible generations progress after a failed
// cleanup. The identity is an acknowledgement token: a late failure must not
// postpone a newer request that rotated it, nor shorten its protection window.
func (s *Store) DeferIndexCleanup(ctx context.Context, id, token, code string, delay time.Duration) error {
	if !validHexID(id) || !validHexID(token) || !validIndexFailureCode(code) || len(code) > 64 || delay.Microseconds() <= 0 {
		return domain.ErrInvalid
	}
	_, err := s.pool.ExecContext(ctx, `UPDATE index_cleanup SET not_before=GREATEST(not_before,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND)),claim_token='',lease_expires_at=NULL,last_error_code=? WHERE id=? AND claim_token=? AND lease_expires_at>UTC_TIMESTAMP(6)`, delay.Microseconds(), code, id, token)
	return dbError(err)
}
