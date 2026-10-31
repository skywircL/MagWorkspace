package store

import (
	"context"
	"database/sql"
	"strings"

	"magworkspace/internal/domain"
)

// A successful remote delete does not prove an earlier timed-out write has
// stopped. Keep small, permanent retirement records and periodically repeat the
// idempotent delete, even if the original writer no longer exists.
func queueCleanupRetirementTx(ctx context.Context, tx *sql.Tx, kind, storageID, profileID, key string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at) VALUES(?,?,?,?,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 DAY)) ON DUPLICATE KEY UPDATE next_check_at=GREATEST(next_check_at,VALUES(next_check_at))`, kind, storageID, profileID, key)
	return err
}

// RequeueRetiredCleanup advances a bounded batch, each item atomically. Its
// active-row -> retirement-row lock order matches admission and object claims.
// Concurrent scans may repeat an idempotent enqueue, but never rotate an active
// claim or shorten a new upload/publication protection window. Normal cleanup
// still rechecks live references before external I/O.
func (s *Store) RequeueRetiredCleanup(ctx context.Context, kind string, storageIDs ...string) error {
	if (kind != "index" && kind != "object") || len(storageIDs) > 17 {
		return domain.ErrInvalid
	}
	order := ""
	args := []any{kind}
	if len(storageIDs) > 0 {
		order = "storage_id IN (" + strings.TrimRight(strings.Repeat("?,", len(storageIDs)), ",") + ") DESC,"
		for _, id := range storageIDs {
			if !validProfileID(id) {
				return domain.ErrInvalid
			}
			args = append(args, id)
		}
	}
	rows, err := s.pool.QueryContext(ctx, `SELECT storage_id,profile_id,resource_key FROM cleanup_retirements WHERE kind=? AND next_check_at<=UTC_TIMESTAMP(6) ORDER BY `+order+`next_check_at,storage_id,profile_id,resource_key LIMIT 100`, args...)
	if err != nil {
		return dbError(err)
	}
	type retired struct{ binding, profile, key string }
	var pending []retired
	for rows.Next() {
		var r retired
		if err = rows.Scan(&r.binding, &r.profile, &r.key); err != nil {
			rows.Close()
			return dbError(err)
		}
		pending = append(pending, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return dbError(err)
	}
	for _, r := range pending {
		err = s.transaction(ctx, func(tx *sql.Tx) error {
			id, err := newID()
			if err != nil {
				return err
			}
			if kind == "index" {
				_, err = tx.ExecContext(ctx, `INSERT INTO index_cleanup(id,storage_id,profile_id,generation,not_before) VALUES(?,?,?,?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id=index_cleanup.id`, id, r.binding, r.profile, r.key)
			} else {
				_, err = tx.ExecContext(ctx, `INSERT INTO object_cleanup(id,storage_id,object_key,state,available_at) VALUES(?,?,?,'deleting',UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id=object_cleanup.id`, id, r.binding, r.key)
			}
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE cleanup_retirements SET next_check_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 DAY) WHERE kind=? AND storage_id=? AND profile_id=? AND resource_key=? AND next_check_at<=UTC_TIMESTAMP(6)`, kind, r.binding, r.profile, r.key)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
