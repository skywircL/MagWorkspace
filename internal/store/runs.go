package store

import (
	"context"
	"time"

	"database/sql"
	"magworkspace/internal/domain"
)

func expireSessionRuns(ctx context.Context, tx *sql.Tx, sessionID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET status='failed',error_code='lease_expired',finished_at=UTC_TIMESTAMP(6) WHERE session_id=? AND status='running' AND lease_expires_at<=UTC_TIMESTAMP(6)`, sessionID)
	if err != nil {
		return err
	}
	return expirePromptsTx(ctx, tx, sessionID)
}

func (s *Store) BeginRun(ctx context.Context, userID, workspaceID, sessionID, query string, lease time.Duration, documentIDs ...[]string) (domain.Run, []domain.Message, string, error) {
	var run domain.Run
	history := []domain.Message{}
	var role string
	if !validText(query, 64000) || lease <= 0 || len(documentIDs) > 1 || (len(documentIDs) == 1 && len(documentIDs[0]) > 100) {
		return run, history, role, domain.ErrInvalid
	}
	runID, err := newID()
	if err != nil {
		return run, history, role, err
	}
	messageID, err := newID()
	if err != nil {
		return run, history, role, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		role, err = lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := expireSessionRuns(ctx, tx, sessionID); err != nil {
			return err
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=? AND status IN ('running','suspended'))`, sessionID).Scan(&active); err != nil {
			return err
		}
		if active {
			return domain.ErrConflict
		}
		// Bound hydration without splitting a completed user/assistant pair. Persisted
		// history remains intact and the HTTP layer applies the smaller model window.
		rows, err := tx.QueryContext(ctx, `SELECT m.id,m.session_id,m.run_id,m.role,m.content,r.status,m.created_at FROM (SELECT * FROM runs WHERE session_id=? AND status='completed' ORDER BY created_at DESC,id DESC LIMIT 100) r JOIN messages m ON m.run_id=r.id ORDER BY r.created_at,r.id,CASE m.role WHEN 'user' THEN 0 ELSE 1 END`, sessionID)
		if err != nil {
			return err
		}
		for rows.Next() {
			m, err := scanMessage(rows)
			if err != nil {
				rows.Close()
				return err
			}
			history = append(history, m)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO runs(id,session_id,status,lease_expires_at) VALUES(?,?,'running',DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND))`, runID, sessionID, lease.Microseconds())
		if err != nil {
			return err
		}
		if len(documentIDs) == 1 {
			if err = storeRunDocumentScopeTx(ctx, tx, workspaceID, runID, documentIDs[0]); err != nil {
				return err
			}
		}
		run, err = scanRun(tx.QueryRowContext(ctx, `SELECT id,session_id,status,error_code,mode,created_at,finished_at FROM runs WHERE id=? AND session_id=?`, runID, sessionID))
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO messages(id,session_id,run_id,role,content) VALUES(?,?,?,'user',?)`, messageID, sessionID, runID, query); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE sessions SET updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=? AND user_id=?`, sessionID, workspaceID, userID)
		return err
	})
	return run, history, role, err
}
func (s *Store) FinishRun(ctx context.Context, userID, workspaceID, sessionID, runID, answer, mode string) error {
	if len(answer) > 64000 || len(mode) > 100 {
		return domain.ErrInvalid
	}
	citationIDs, err := domain.CitationIDs(answer)
	if err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	expired := false
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := assertRunExecutionTx(ctx, tx, runID); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=? AND session_id=? FOR UPDATE`, runID, sessionID).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return domain.ErrConflict
		}
		updated, err := tx.ExecContext(ctx, `UPDATE runs SET status='completed',mode=?,finished_at=UTC_TIMESTAMP(6) WHERE id=? AND lease_expires_at>UTC_TIMESTAMP(6)`, mode, runID)
		if err != nil {
			return err
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if changed == 0 {
			expired = true
			_, err := tx.ExecContext(ctx, `UPDATE runs SET status='failed',error_code='lease_expired',finished_at=UTC_TIMESTAMP(6) WHERE id=?`, runID)
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		validSources, err := runSourcesValidTx(ctx, tx, workspaceID, runID, scope)
		if err != nil {
			return err
		}
		if !validSources {
			return domain.ErrInvalid
		}
		if err := citeEvidenceTx(ctx, tx, workspaceID, runID, citationIDs); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_prompts SET status='done' WHERE run_id=? AND status='resuming'`, runID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO messages(id,session_id,run_id,role,content) VALUES(?,?,?,'assistant',?)`, id, sessionID, runID, answer); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE sessions SET updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=? AND user_id=?`, sessionID, workspaceID, userID)
		return err
	})
	if err == nil && expired {
		return domain.ErrConflict
	}
	return err
}
func (s *Store) FailRun(ctx context.Context, userID, workspaceID, sessionID, runID, code string) error {
	if !validText(code, 100) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := assertRunExecutionTx(ctx, tx, runID); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=? AND session_id=? FOR UPDATE`, runID, sessionID).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return domain.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_prompts SET status='failed' WHERE run_id=? AND status='resuming'`, runID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE runs SET status='failed',error_code=?,finished_at=UTC_TIMESTAMP(6) WHERE id=?`, code, runID)
		return err
	})
}
