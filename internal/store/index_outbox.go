package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"magworkspace/internal/domain"
)

// IndexDelivery is a revocable dispatch token, not an execution lease. The
// durable job owns authorization, publication and generation fencing.
type IndexDelivery struct {
	ID        string
	JobID     string
	ProfileID string
}

const maxIndexLeaseRecoveries = 3

func resetIndexDeliveryTx(ctx context.Context, tx *sql.Tx, jobID string) error {
	id, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO index_outbox(job_id,id) VALUES(?,?) ON DUPLICATE KEY UPDATE id=?,failures=0,lease_recoveries=0,next_attempt_at=UTC_TIMESTAMP(6),next_dispatch_at=UTC_TIMESTAMP(6)`, jobID, id, id)
	return err
}

func deleteIndexDeliveryTx(ctx context.Context, tx *sql.Tx, jobID string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM index_outbox WHERE job_id=?`, jobID)
	return err
}

// PendingIndexDeliveries reads committed work. Sending to Redis never consumes
// the intent: an unclaimed delivery is periodically sent again, including after
// a Redis flush or a dispatcher crash between enqueue and acknowledgement.
func (s *Store) PendingIndexDeliveries(ctx context.Context, profileID string, limit int) ([]IndexDelivery, error) {
	if !validProfileID(profileID) || limit <= 0 || limit > 1000 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.pool.QueryContext(ctx, `SELECT o.id,o.job_id,i.profile_id FROM index_outbox o JOIN document_indexes i ON i.id=o.job_id WHERE i.profile_id=? AND o.next_dispatch_at<=UTC_TIMESTAMP(6) AND ((i.status='queued' AND o.next_attempt_at<=UTC_TIMESTAMP(6)) OR (i.status='running' AND i.lease_expires_at<=UTC_TIMESTAMP(6))) ORDER BY o.next_dispatch_at,o.job_id LIMIT ?`, profileID, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	deliveries := []IndexDelivery{}
	for rows.Next() {
		var delivery IndexDelivery
		if err := rows.Scan(&delivery.ID, &delivery.JobID, &delivery.ProfileID); err != nil {
			return nil, dbError(err)
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, dbError(rows.Err())
}

func (s *Store) MarkIndexDeliverySent(ctx context.Context, delivery IndexDelivery) error {
	if !validHexID(delivery.ID) || !validHexID(delivery.JobID) || !validProfileID(delivery.ProfileID) {
		return domain.ErrInvalid
	}
	// Only the outbox row is locked here. A late acknowledgement cannot postpone
	// a newer retry token, and no reverse Job -> Workspace lock path is added.
	_, err := s.pool.ExecContext(ctx, `UPDATE index_outbox SET next_dispatch_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 15 SECOND) WHERE job_id=? AND id=?`, delivery.JobID, delivery.ID)
	return dbError(err)
}

func (s *Store) ClaimIndexDelivery(ctx context.Context, profileID, jobID, deliveryID string, lease time.Duration) (*domain.IndexInput, error) {
	if !validProfileID(profileID) || !validHexID(jobID) || !validHexID(deliveryID) || lease.Microseconds() <= 0 {
		return nil, domain.ErrInvalid
	}
	job, err := scanIndex(s.pool.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=? AND i.profile_id=?`, jobID, profileID))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.claimIndexCandidates(ctx, []domain.IndexJob{job}, lease, deliveryID)
}

func validIndexFailureCode(code string) bool {
	if len(code) == 0 || len(code) > 100 {
		return false
	}
	for _, c := range code {
		if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '_' {
			return false
		}
	}
	return true
}

// RetryDocumentIndex atomically retires the partial generation and schedules a
// fresh token. Redis retry state is never the source of the retry budget.
func (s *Store) RetryDocumentIndex(ctx context.Context, expected domain.IndexJob, code string, maxRetries int, delay time.Duration) (bool, error) {
	if !validHexID(expected.Generation) || !validIndexFailureCode(code) || maxRetries < 0 || delay < 0 {
		return false, domain.ErrInvalid
	}
	retried := false
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		job, deleted, _, err := lockIndex(ctx, tx, expected)
		if err != nil {
			return err
		}
		if job.Status != "running" || job.Generation != expected.Generation {
			return domain.ErrConflict
		}
		if deleted {
			return failIndexTx(ctx, tx, job, "document_deleted")
		}
		allowed, err := currentIndexWriter(ctx, tx, job)
		if err != nil {
			return err
		}
		if !allowed {
			return failIndexTx(ctx, tx, job, "permission_revoked")
		}
		var failures int
		if err := tx.QueryRowContext(ctx, `SELECT failures FROM index_outbox WHERE job_id=? FOR UPDATE`, job.ID).Scan(&failures); err != nil {
			return err
		}
		if failures >= maxRetries {
			return failIndexTx(ctx, tx, job, code)
		}
		if err := queueIndexCleanupTx(ctx, tx, job.StorageID, job.ProfileID, job.Generation); err != nil {
			return err
		}
		id, err := newID()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE document_indexes SET status='queued',generation='',lease_expires_at=NULL,error_code=?,chunk_count=0,parent_count=0,finished_at=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, code, job.ID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE index_outbox SET id=?,failures=failures+1,next_attempt_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND),next_dispatch_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND) WHERE job_id=?`, id, delay.Microseconds(), delay.Microseconds(), job.ID); err != nil {
			return err
		}
		retried = true
		return nil
	})
	return retried && err == nil, err
}
