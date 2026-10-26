package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func scanSession(row rowScanner) (domain.Session, error) {
	var v domain.Session
	err := row.Scan(&v.ID, &v.WorkspaceID, &v.UserID, &v.Title, &v.CreatedAt, &v.UpdatedAt)
	return v, dbError(err)
}
func privateSession(ctx context.Context, tx *sql.Tx, userID, workspaceID, sessionID string) (domain.Session, error) {
	return scanSession(tx.QueryRowContext(ctx, `SELECT id,workspace_id,user_id,title,created_at,updated_at FROM sessions WHERE id=? AND workspace_id=? AND user_id=? FOR UPDATE`, sessionID, workspaceID, userID))
}
func (s *Store) CreateSession(ctx context.Context, userID, workspaceID, title string) (domain.Session, error) {
	var v domain.Session
	title = strings.TrimSpace(title)
	if !validText(title, 200) {
		return v, domain.ErrInvalid
	}
	id, err := newID()
	if err != nil {
		return v, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sessions(id,workspace_id,user_id,title) VALUES(?,?,?,?)`, id, workspaceID, userID, title); err != nil {
			return err
		}
		var err error
		v, err = privateSession(ctx, tx, userID, workspaceID, id)
		return err
	})
	return v, err
}
func validPage(limit, offset int) bool { return limit > 0 && limit <= 200 && offset >= 0 }
func (s *Store) Sessions(ctx context.Context, userID, workspaceID string, limit, offset int) ([]domain.Session, int, error) {
	result := []domain.Session{}
	total := 0
	if !validPage(limit, offset) {
		return result, total, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sessions WHERE workspace_id=? AND user_id=?`, workspaceID, userID).Scan(&total); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT id,workspace_id,user_id,title,created_at,updated_at FROM sessions WHERE workspace_id=? AND user_id=? ORDER BY updated_at DESC,id DESC LIMIT ? OFFSET ?`, workspaceID, userID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			v, err := scanSession(rows)
			if err != nil {
				return err
			}
			result = append(result, v)
		}
		return rows.Err()
	})
	return result, total, err
}
func (s *Store) Session(ctx context.Context, userID, workspaceID, sessionID string) (domain.Session, error) {
	return scanSession(s.pool.QueryRowContext(ctx, `SELECT s.id,s.workspace_id,s.user_id,s.title,s.created_at,s.updated_at FROM sessions s JOIN workspace_members m ON m.workspace_id=s.workspace_id AND m.user_id=? WHERE s.id=? AND s.workspace_id=? AND s.user_id=?`, userID, sessionID, workspaceID, userID))
}
func (s *Store) UpdateSession(ctx context.Context, userID, workspaceID, sessionID, title string) (domain.Session, error) {
	var v domain.Session
	title = strings.TrimSpace(title)
	if !validText(title, 200) {
		return v, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET title=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=? AND user_id=?`, title, sessionID, workspaceID, userID); err != nil {
			return err
		}
		var err error
		v, err = privateSession(ctx, tx, userID, workspaceID, sessionID)
		return err
	})
	return v, err
}
func (s *Store) DeleteSession(ctx context.Context, userID, workspaceID, sessionID string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := expireRuns(ctx, tx, workspaceID); err != nil {
			return err
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=? AND status='running')`, sessionID).Scan(&active); err != nil {
			return err
		}
		if active {
			return domain.ErrConflict
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE id=? AND workspace_id=? AND user_id=?`, sessionID, workspaceID, userID)
		return err
	})
}
func scanMessage(row rowScanner) (domain.Message, error) {
	var m domain.Message
	err := row.Scan(&m.ID, &m.SessionID, &m.RunID, &m.Role, &m.Content, &m.Status, &m.CreatedAt)
	return m, dbError(err)
}
func (s *Store) Messages(ctx context.Context, userID, workspaceID, sessionID string, limit, offset int) ([]domain.Message, int, error) {
	result := []domain.Message{}
	total := 0
	if !validPage(limit, offset) {
		return result, total, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := expireSessionRuns(ctx, tx, sessionID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM messages WHERE session_id=?`, sessionID).Scan(&total); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT m.id,m.session_id,m.run_id,m.role,m.content,r.status,m.created_at FROM messages m JOIN runs r ON r.id=m.run_id WHERE m.session_id=? ORDER BY r.created_at,r.id,CASE m.role WHEN 'user' THEN 0 ELSE 1 END LIMIT ? OFFSET ?`, sessionID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			m, err := scanMessage(rows)
			if err != nil {
				return err
			}
			result = append(result, m)
		}
		return rows.Err()
	})
	return result, total, err
}
func scanRun(row rowScanner) (domain.Run, error) {
	var r domain.Run
	err := row.Scan(&r.ID, &r.SessionID, &r.Status, &r.ErrorCode, &r.Mode, &r.CreatedAt, &r.FinishedAt)
	return r, dbError(err)
}
func (s *Store) Run(ctx context.Context, userID, workspaceID, sessionID, runID string) (domain.Run, error) {
	var run domain.Run
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
			return err
		}
		if err := expireSessionRuns(ctx, tx, sessionID); err != nil {
			return err
		}
		var err error
		run, err = scanRun(tx.QueryRowContext(ctx, `SELECT id,session_id,status,error_code,mode,created_at,finished_at FROM runs WHERE id=? AND session_id=?`, runID, sessionID))
		if err != nil {
			return err
		}
		_, err = prepareRunRecoveryTx(ctx, tx, userID, workspaceID, sessionID, runID, run.Status, run.ErrorCode)
		run.CanRecover = err == nil
		// An unavailable recovery does not hide the durable run's failure state.
		if errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalid) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, blob.ErrIntegrity) {
			return nil
		}
		return err
	})
	return run, err
}
