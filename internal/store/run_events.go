package store

import (
	"context"
	"database/sql"

	"magworkspace/internal/domain"
)

// RunEventState authorizes transient cached content using current private
// session ownership, workspace membership, provenance and execution generation.
func (s *Store) RunEventState(ctx context.Context, uid, wid, sid, rid string) (domain.Run, uint64, error) {
	var run domain.Run
	var generation uint64
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, false); err != nil {
			return err
		}
		if err := expireSessionRuns(ctx, tx, sid); err != nil {
			return err
		}
		var err error
		run, err = scanRun(tx.QueryRowContext(ctx, `SELECT id,session_id,status,error_code,mode,created_at,finished_at FROM runs WHERE id=? AND session_id=?`, rid, sid))
		if err != nil {
			return err
		}
		if err = tx.QueryRowContext(ctx, `SELECT execution_generation FROM run_journal_heads WHERE run_id=?`, rid).Scan(&generation); err != nil {
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		valid, err := runSourcesValidTx(ctx, tx, wid, rid, scope)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrForbidden
		}
		return nil
	})
	return run, generation, err
}
