package store

import (
	"context"
	"strings"

	"database/sql"
	"magworkspace/internal/domain"
)

func scanWorkspace(row rowScanner) (domain.Workspace, error) {
	var w domain.Workspace
	err := row.Scan(&w.ID, &w.Name, &w.Description, &w.Role, &w.CreatedAt, &w.UpdatedAt)
	return w, dbError(err)
}
func (s *Store) CreateWorkspace(ctx context.Context, userID, name, description string) (domain.Workspace, error) {
	var w domain.Workspace
	name = strings.TrimSpace(name)
	if !validText(name, 100) || len(description) > 2000 {
		return w, domain.ErrInvalid
	}
	id, err := newID()
	if err != nil {
		return w, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		_, e := tx.ExecContext(ctx, `INSERT INTO workspaces(id,name,description) VALUES(?,?,?)`, id, name, description)
		if e != nil {
			return e
		}
		w, e = scanWorkspace(tx.QueryRowContext(ctx, `SELECT id,name,description,'owner',created_at,updated_at FROM workspaces WHERE id=?`, id))
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO workspace_members(workspace_id,user_id,role) VALUES(?,?,'owner')`, id, userID)
		return e
	})
	return w, err
}
func (s *Store) Workspaces(ctx context.Context, userID string) ([]domain.Workspace, error) {
	result := []domain.Workspace{}
	rows, err := s.pool.QueryContext(ctx, `SELECT w.id,w.name,w.description,m.role,w.created_at,w.updated_at FROM workspaces w JOIN workspace_members m ON m.workspace_id=w.id AND m.user_id=? ORDER BY w.created_at,w.id`, userID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		w, err := scanWorkspace(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, w)
	}
	return result, dbError(rows.Err())
}
func (s *Store) Workspace(ctx context.Context, userID, workspaceID string) (domain.Workspace, error) {
	return scanWorkspace(s.pool.QueryRowContext(ctx, `SELECT w.id,w.name,w.description,m.role,w.created_at,w.updated_at FROM workspaces w JOIN workspace_members m ON m.workspace_id=w.id AND m.user_id=? WHERE w.id=?`, userID, workspaceID))
}
func (s *Store) UpdateWorkspace(ctx context.Context, userID, workspaceID, name, description string) (domain.Workspace, error) {
	return s.PatchWorkspace(ctx, userID, workspaceID, &name, &description)
}

// PatchWorkspace merges only supplied fields while holding the same row lock
// that authorizes the write, so disjoint concurrent edits cannot clobber each other.
func (s *Store) PatchWorkspace(ctx context.Context, userID, workspaceID string, name, description *string) (domain.Workspace, error) {
	var w domain.Workspace
	if (name != nil && !validText(*name, 100)) || (description != nil && len(*description) > 2000) {
		return w, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if role != "owner" && role != "admin" {
			return domain.ErrForbidden
		}
		w, err = scanWorkspace(tx.QueryRowContext(ctx, `SELECT id,name,description,?,created_at,updated_at FROM workspaces WHERE id=?`, role, workspaceID))
		if err != nil {
			return err
		}
		if name == nil && description == nil {
			return nil
		}
		if name != nil {
			w.Name = strings.TrimSpace(*name)
		}
		if description != nil {
			w.Description = *description
		}
		if _, err = tx.ExecContext(ctx, `UPDATE workspaces SET name=?,description=?,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, w.Name, w.Description, workspaceID); err != nil {
			return err
		}
		w, err = scanWorkspace(tx.QueryRowContext(ctx, `SELECT id,name,description,?,created_at,updated_at FROM workspaces WHERE id=?`, role, workspaceID))
		return err
	})
	return w, err
}
func (s *Store) DeleteWorkspace(ctx context.Context, userID, workspaceID string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if role != "owner" {
			return domain.ErrForbidden
		}
		if err := expireRuns(ctx, tx, workspaceID); err != nil {
			return err
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN sessions s ON s.id=r.session_id WHERE s.workspace_id=? AND r.status='running')`, workspaceID).Scan(&active); err != nil {
			return err
		}
		if active {
			return domain.ErrConflict
		}
		if err = queueDocumentCleanupTx(ctx, tx, workspaceID, ""); err != nil {
			return err
		}
		if err = queueWorkspaceObjectsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM workspaces WHERE id=?`, workspaceID)
		return err
	})
}
func scanMember(row rowScanner) (domain.Member, error) {
	var m domain.Member
	err := row.Scan(&m.UserID, &m.Email, &m.Name, &m.Role, &m.CreatedAt)
	return m, dbError(err)
}
func (s *Store) Members(ctx context.Context, userID, workspaceID string) ([]domain.Member, error) {
	result := []domain.Member{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT u.id,u.email,u.name,m.role,m.created_at FROM workspace_members m JOIN users u ON u.id=m.user_id WHERE m.workspace_id=? ORDER BY m.created_at,u.id`, workspaceID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			m, err := scanMember(rows)
			if err != nil {
				return err
			}
			result = append(result, m)
		}
		return rows.Err()
	})
	return result, err
}
func canManage(actor, target, next string) bool {
	if actor == "owner" {
		return true
	}
	return actor == "admin" && target != "owner" && target != "admin" && next != "owner" && next != "admin"
}
func (s *Store) AddMember(ctx context.Context, actorID, workspaceID, email, role string) (domain.Member, error) {
	var m domain.Member
	if !domain.ValidRole(role) {
		return m, domain.ErrInvalid
	}
	email, err := normalizedEmail(email)
	if err != nil {
		return m, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		actor, err := lockWorkspace(ctx, tx, actorID, workspaceID)
		if err != nil {
			return err
		}
		if !canManage(actor, "", role) {
			return domain.ErrForbidden
		}
		var id string
		if err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE email=?`, email).Scan(&id); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO workspace_members(workspace_id,user_id,role) VALUES(?,?,?)`, workspaceID, id, role); err != nil {
			return err
		}
		m, err = scanMember(tx.QueryRowContext(ctx, `SELECT u.id,u.email,u.name,m.role,m.created_at FROM workspace_members m JOIN users u ON u.id=m.user_id WHERE m.workspace_id=? AND m.user_id=?`, workspaceID, id))
		return err
	})
	return m, err
}
func (s *Store) UpdateMember(ctx context.Context, actorID, workspaceID, targetUserID, role string) error {
	if !domain.ValidRole(role) {
		return domain.ErrInvalid
	}
	return s.changeMember(ctx, actorID, workspaceID, targetUserID, role, false)
}
func (s *Store) DeleteMember(ctx context.Context, actorID, workspaceID, targetUserID string) error {
	return s.changeMember(ctx, actorID, workspaceID, targetUserID, "", true)
}
func (s *Store) changeMember(ctx context.Context, actorID, workspaceID, targetUserID, next string, remove bool) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		actor, err := lockWorkspace(ctx, tx, actorID, workspaceID)
		if err != nil {
			return err
		}
		var current string
		if err = tx.QueryRowContext(ctx, `SELECT role FROM workspace_members WHERE workspace_id=? AND user_id=?`, workspaceID, targetUserID).Scan(&current); err != nil {
			return err
		}
		if !canManage(actor, current, next) {
			return domain.ErrForbidden
		}
		if !remove && current == next {
			return nil
		}
		if current == "owner" && next != "owner" {
			var owners int
			if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM workspace_members WHERE workspace_id=? AND role='owner'`, workspaceID).Scan(&owners); err != nil {
				return err
			}
			if owners <= 1 {
				return domain.ErrConflict
			}
		}
		active, err := activeForUser(ctx, tx, workspaceID, targetUserID)
		if err != nil {
			return err
		}
		if active {
			return domain.ErrConflict
		}
		if remove {
			// Revocation invalidates in-flight capture even if membership is later
			// restored before its model response arrives.
			if err = invalidateMemoryCapturesTx(ctx, tx, targetUserID, workspaceID); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, workspaceID, targetUserID)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE workspace_members SET role=? WHERE workspace_id=? AND user_id=?`, next, workspaceID, targetUserID)
		}
		return err
	})
}
