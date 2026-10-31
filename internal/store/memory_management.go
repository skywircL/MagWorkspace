package store

import (
	"context"
	"database/sql"
	"magworkspace/internal/domain"
)

func memoryEnabledTx(ctx context.Context, tx *sql.Tx, uid, wid string) (bool, error) {
	var enabled bool
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT enabled FROM user_memory_settings WHERE workspace_id=? AND user_id=?),TRUE)`, wid, uid).Scan(&enabled)
	return enabled, err
}

func (s *Store) MemorySettings(ctx context.Context, uid, wid string, enabled *bool) (domain.MemorySettings, error) {
	var out domain.MemorySettings
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		if enabled != nil {
			if err := invalidateMemoryCapturesTx(ctx, tx, uid, wid); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO user_memory_settings(workspace_id,user_id,enabled) VALUES(?,?,?) ON DUPLICATE KEY UPDATE enabled=?`, wid, uid, *enabled, *enabled); err != nil {
				return err
			}
		}
		var err error
		out.Enabled, err = memoryEnabledTx(ctx, tx, uid, wid)
		return err
	})
	return out, err
}

// Export includes inactive/expired history, but never another member's data.
func (s *Store) ExportMemories(ctx context.Context, uid, wid string) ([]domain.Memory, error) {
	out := []domain.Memory{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		var err error
		out, err = memoryRows(ctx, tx, `SELECT `+memoryColumns+` FROM user_memories WHERE workspace_id=? AND user_id=? ORDER BY created_at DESC,id DESC LIMIT ?`, wid, uid, maxMemories)
		return err
	})
	return out, err
}

func (s *Store) ClearMemories(ctx context.Context, uid, wid string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		if err := invalidateMemoryCapturesTx(ctx, tx, uid, wid); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO memory_tombstones(workspace_id,user_id,fingerprint,forgotten_at) SELECT workspace_id,user_id,fingerprint,UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) FROM user_memories WHERE workspace_id=? AND user_id=?`, wid, uid); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE user_memories SET status='archived' WHERE workspace_id=? AND user_id=?`, wid, uid)
		return err
	})
}
