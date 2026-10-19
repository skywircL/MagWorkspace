package store

import (
	"context"
	"errors"
	"strings"

	"database/sql"
	"magworkspace/internal/domain"
)

func (s *Store) CreateUser(ctx context.Context, name, email, passwordHash string) (domain.User, domain.Workspace, error) {
	var u domain.User
	var w domain.Workspace
	email, err := normalizedEmail(email)
	if err != nil {
		return u, w, err
	}
	name = strings.TrimSpace(name)
	if !validText(name, 100) || passwordHash == "" {
		return u, w, domain.ErrInvalid
	}
	u.ID, err = newID()
	if err != nil {
		return u, w, err
	}
	w.ID, err = newID()
	if err != nil {
		return u, w, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO users(id,name,email,password_hash) VALUES(?,?,?,?)`, u.ID, name, email, passwordHash); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT id,email,name,created_at FROM users WHERE id=?`, u.ID).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO workspaces(id,name,description) VALUES(?,?,'')`, w.ID, name+"'s Workspace"); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT id,name,description,created_at,updated_at FROM workspaces WHERE id=?`, w.ID).Scan(&w.ID, &w.Name, &w.Description, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return err
		}
		w.Role = "owner"
		_, err := tx.ExecContext(ctx, `INSERT INTO workspace_members(workspace_id,user_id,role) VALUES(?,?,'owner')`, w.ID, u.ID)
		return err
	})
	return u, w, err
}
func (s *Store) UserByEmail(ctx context.Context, email string) (domain.User, string, error) {
	var u domain.User
	var hash string
	email, err := normalizedEmail(email)
	if err != nil {
		return u, hash, err
	}
	err = s.pool.QueryRowContext(ctx, `SELECT id,email,name,created_at,password_hash FROM users WHERE email=?`, email).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt, &hash)
	return u, hash, dbError(err)
}
func (s *Store) User(ctx context.Context, userID string) (domain.User, error) {
	var u domain.User
	err := s.pool.QueryRowContext(ctx, `SELECT id,email,name,created_at FROM users WHERE id=?`, userID).Scan(&u.ID, &u.Email, &u.Name, &u.CreatedAt)
	return u, dbError(err)
}
func (s *Store) CreateAuth(ctx context.Context, a domain.AuthSession) error {
	if a.ID == "" || a.AccessHash == "" || a.RefreshHash == "" || !a.AccessExpiresAt.Before(a.RefreshExpiresAt) {
		return domain.ErrInvalid
	}
	_, err := s.pool.ExecContext(ctx, `INSERT INTO auth_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES(?,?,?,?,?,?)`, a.ID, a.UserID, a.AccessHash, a.RefreshHash, a.AccessExpiresAt, a.RefreshExpiresAt)
	return dbError(err)
}
func scanAuth(row rowScanner) (domain.AuthSession, error) {
	var a domain.AuthSession
	err := row.Scan(&a.ID, &a.UserID, &a.AccessHash, &a.RefreshHash, &a.AccessExpiresAt, &a.RefreshExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return a, domain.ErrUnauthorized
	}
	return a, err
}
func (s *Store) Authenticate(ctx context.Context, accessHash string) (domain.AuthSession, error) {
	return scanAuth(s.pool.QueryRowContext(ctx, `SELECT id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at FROM auth_sessions WHERE access_hash=? AND access_expires_at>UTC_TIMESTAMP(6)`, accessHash))
}
func (s *Store) RotateAuth(ctx context.Context, refreshHash string, next domain.AuthSession) (domain.AuthSession, error) {
	var result domain.AuthSession
	if next.AccessHash == "" || next.RefreshHash == "" || !next.AccessExpiresAt.Before(next.RefreshExpiresAt) {
		return result, domain.ErrInvalid
	}
	// Lock the matched refresh session before rotation so concurrent refreshes
	// cannot both succeed. Keep the authoritative user and session identifiers.
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = scanAuth(tx.QueryRowContext(ctx, `SELECT id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at FROM auth_sessions WHERE refresh_hash=? AND refresh_expires_at>UTC_TIMESTAMP(6) FOR UPDATE`, refreshHash))
		if err != nil {
			return err
		}
		updated, err := tx.ExecContext(ctx, `UPDATE auth_sessions SET access_hash=?,refresh_hash=?,access_expires_at=?,refresh_expires_at=? WHERE id=? AND refresh_expires_at>UTC_TIMESTAMP(6)`, next.AccessHash, next.RefreshHash, next.AccessExpiresAt, next.RefreshExpiresAt, result.ID)
		if err != nil {
			return err
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return domain.ErrUnauthorized
		}
		result.AccessHash = next.AccessHash
		result.RefreshHash = next.RefreshHash
		result.AccessExpiresAt = next.AccessExpiresAt
		result.RefreshExpiresAt = next.RefreshExpiresAt
		return nil
	})
	return result, err
}
func (s *Store) DeleteAuth(ctx context.Context, authID string) error {
	_, err := s.pool.ExecContext(ctx, `DELETE FROM auth_sessions WHERE id=?`, authID)
	return dbError(err)
}

// DeleteAuthByRefresh revokes the whole session even after its access token
// expires. Deleting a rotated, expired, or already revoked credential is safe.
func (s *Store) DeleteAuthByRefresh(ctx context.Context, refreshHash string) error {
	_, err := s.pool.ExecContext(ctx, `DELETE FROM auth_sessions WHERE refresh_hash=?`, refreshHash)
	return dbError(err)
}
