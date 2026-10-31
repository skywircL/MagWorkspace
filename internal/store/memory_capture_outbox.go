package store

import (
	"context"
	"database/sql"
	"time"

	"magworkspace/internal/domain"
)

// MemoryCaptureDelivery authorizes one dispatch epoch. Execution authority and
// its generation remain in the job; sending to Redis never consumes this row.
type MemoryCaptureDelivery struct {
	ID    string
	JobID string
}

func resetMemoryCaptureDeliveryTx(ctx context.Context, tx *sql.Tx, jobID string) error {
	id, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO memory_capture_outbox(run_id,id) VALUES(?,?) ON DUPLICATE KEY UPDATE id=?,next_dispatch_at=UTC_TIMESTAMP(6)`, jobID, id, id)
	return err
}

func deleteMemoryCaptureDeliveryTx(ctx context.Context, tx *sql.Tx, jobID string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM memory_capture_outbox WHERE run_id=?`, jobID)
	return err
}

// queueMemoryCaptureTx is part of successful answer publication. Only waiting
// or failed-Run recovery sources may enter queued; acquisition still rechecks
// the begin-time epoch, live settings and membership before extraction.
func queueMemoryCaptureTx(ctx context.Context, tx *sql.Tx, jobID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='queued',error_code='',updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND (status='waiting' OR (status='cancelled' AND error_code='run_failed'))`, jobID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed == 0 {
		return err
	}
	return resetMemoryCaptureDeliveryTx(ctx, tx, jobID)
}

func (s *Store) PendingMemoryCaptureDeliveries(ctx context.Context, limit int) ([]MemoryCaptureDelivery, error) {
	if limit < 1 || limit > 200 {
		return nil, domain.ErrInvalid
	}
	if err := s.cancelFailedWaitingCaptures(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.QueryContext(ctx, `SELECT o.id,o.run_id FROM memory_capture_outbox o JOIN memory_capture_jobs j ON j.run_id=o.run_id WHERE o.next_dispatch_at<=UTC_TIMESTAMP(6) AND ((j.status='queued' AND j.next_attempt_at<=UTC_TIMESTAMP(6)) OR (j.status='running' AND j.lease_expires_at<=UTC_TIMESTAMP(6))) ORDER BY o.next_dispatch_at,o.run_id LIMIT ?`, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	result := []MemoryCaptureDelivery{}
	for rows.Next() {
		var delivery MemoryCaptureDelivery
		if err := rows.Scan(&delivery.ID, &delivery.JobID); err != nil {
			return nil, dbError(err)
		}
		result = append(result, delivery)
	}
	return result, dbError(rows.Err())
}

func (s *Store) MarkMemoryCaptureDeliverySent(ctx context.Context, delivery MemoryCaptureDelivery) error {
	if !validHexID(delivery.ID) || !validHexID(delivery.JobID) {
		return domain.ErrInvalid
	}
	// The token is checked in the same statement as the update. An old queue
	// acknowledgement cannot postpone a new retry or recreate a terminal job.
	_, err := s.pool.ExecContext(ctx, `UPDATE memory_capture_outbox SET next_dispatch_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 15 SECOND) WHERE run_id=? AND id=?`, delivery.JobID, delivery.ID)
	return dbError(err)
}

func (s *Store) ClaimMemoryCaptureDelivery(ctx context.Context, jobID, deliveryID string, lease time.Duration) (domain.MemoryCaptureJob, bool, error) {
	if !validHexID(deliveryID) {
		return domain.MemoryCaptureJob{}, false, domain.ErrInvalid
	}
	return s.claimMemoryCapture(ctx, jobID, deliveryID, lease)
}
