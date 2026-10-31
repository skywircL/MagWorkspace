package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const knowledgeBaseColumns = `id,workspace_id,name,description,COALESCE(default_slot=1,FALSE),created_by,created_at,updated_at`

func scanKnowledgeBase(row rowScanner) (domain.KnowledgeBase, error) {
	var kb domain.KnowledgeBase
	err := row.Scan(&kb.ID, &kb.WorkspaceID, &kb.Name, &kb.Description, &kb.IsDefault, &kb.CreatedBy, &kb.CreatedAt, &kb.UpdatedAt)
	return kb, dbError(err)
}

func canManageKnowledgeBase(role string) bool { return role == "owner" || role == "admin" }

func knowledgeBaseTx(ctx context.Context, tx *sql.Tx, workspaceID, knowledgeBaseID string) (domain.KnowledgeBase, error) {
	return scanKnowledgeBase(tx.QueryRowContext(ctx, `SELECT `+knowledgeBaseColumns+` FROM knowledge_bases WHERE id=? AND workspace_id=? FOR UPDATE`, knowledgeBaseID, workspaceID))
}

// The caller holds the workspace lock (or is creating that workspace). Creating
// the first knowledge base elects it as default without recreating deleted ones.
func createKnowledgeBaseTx(ctx context.Context, tx *sql.Tx, userID, workspaceID, name, description string) (domain.KnowledgeBase, error) {
	id, err := newID()
	if err != nil {
		return domain.KnowledgeBase{}, err
	}
	var existing bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge_bases WHERE workspace_id=?)`, workspaceID).Scan(&existing); err != nil {
		return domain.KnowledgeBase{}, err
	}
	var slot any
	if !existing {
		slot = 1
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO knowledge_bases(id,workspace_id,name,description,default_slot,created_by) VALUES(?,?,?,?,?,?)`, id, workspaceID, name, description, slot, userID); err != nil {
		return domain.KnowledgeBase{}, err
	}
	return knowledgeBaseTx(ctx, tx, workspaceID, id)
}

func (s *Store) KnowledgeBases(ctx context.Context, userID, workspaceID string) ([]domain.KnowledgeBase, error) {
	result := []domain.KnowledgeBase{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+knowledgeBaseColumns+` FROM knowledge_bases WHERE workspace_id=? ORDER BY created_at,id`, workspaceID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			kb, err := scanKnowledgeBase(rows)
			if err != nil {
				return err
			}
			result = append(result, kb)
		}
		return rows.Err()
	})
	return result, err
}

func (s *Store) KnowledgeBase(ctx context.Context, userID, workspaceID, knowledgeBaseID string) (domain.KnowledgeBase, error) {
	var result domain.KnowledgeBase
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		var err error
		result, err = knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID)
		return err
	})
	return result, err
}

func validKnowledgeBase(name, description string) bool {
	return validDocumentText(name) && len(description) <= 2000 && utf8.ValidString(description)
}

func (s *Store) CreateKnowledgeBase(ctx context.Context, userID, workspaceID, name, description string) (domain.KnowledgeBase, error) {
	var result domain.KnowledgeBase
	name = strings.TrimSpace(name)
	if !validKnowledgeBase(name, description) {
		return result, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if !canManageKnowledgeBase(role) {
			return domain.ErrForbidden
		}
		result, err = createKnowledgeBaseTx(ctx, tx, userID, workspaceID, name, description)
		return err
	})
	return result, err
}

func (s *Store) UpdateKnowledgeBase(ctx context.Context, userID, workspaceID, knowledgeBaseID, name, description string) (domain.KnowledgeBase, error) {
	var result domain.KnowledgeBase
	name = strings.TrimSpace(name)
	if !validKnowledgeBase(name, description) {
		return result, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if _, err = knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID); err != nil {
			return err
		}
		if !canManageKnowledgeBase(role) {
			return domain.ErrForbidden
		}
		if _, err = tx.ExecContext(ctx, `UPDATE knowledge_bases SET name=?,description=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=?`, name, description, knowledgeBaseID, workspaceID); err != nil {
			return err
		}
		result, err = knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID)
		return err
	})
	return result, err
}

func (s *Store) DeleteKnowledgeBase(ctx context.Context, userID, workspaceID, knowledgeBaseID string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		kb, err := knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID)
		if err != nil {
			return err
		}
		if !canManageKnowledgeBase(role) {
			return domain.ErrForbidden
		}
		rows, err := tx.QueryContext(ctx, `SELECT id FROM documents WHERE workspace_id=? AND knowledge_base_id=? ORDER BY id`, workspaceID, knowledgeBaseID)
		if err != nil {
			return err
		}
		var documentIDs []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			documentIDs = append(documentIDs, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range documentIDs {
			if err = queueDocumentCleanupTx(ctx, tx, workspaceID, id); err != nil {
				return err
			}
			if err = queueDocumentObjectsTx(ctx, tx, workspaceID, id); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM knowledge_bases WHERE id=? AND workspace_id=?`, knowledgeBaseID, workspaceID); err != nil {
			return err
		}
		if kb.IsDefault {
			// Workspace serialization makes this election atomic with deletion,
			// new uploads, and concurrent knowledge-base creation.
			_, err = tx.ExecContext(ctx, `UPDATE knowledge_bases SET default_slot=1,updated_at=UTC_TIMESTAMP(6) WHERE workspace_id=? ORDER BY created_at,id LIMIT 1`, workspaceID)
		}
		return err
	})
}

// Resolve and freeze the destination before external upload. A deleted default
// must never silently redirect that upload into its newly elected successor.
func (s *Store) documentKnowledgeBase(ctx context.Context, userID, workspaceID, knowledgeBaseID string) (string, error) {
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		if knowledgeBaseID == "" {
			err = tx.QueryRowContext(ctx, `SELECT id FROM knowledge_bases WHERE workspace_id=? AND default_slot=1`, workspaceID).Scan(&knowledgeBaseID)
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrConflict
			}
			return err
		}
		_, err = knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID)
		return err
	})
	return knowledgeBaseID, err
}

func (s *Store) KnowledgeBaseDocuments(ctx context.Context, userID, workspaceID, knowledgeBaseID string, limit, offset int) ([]domain.Document, int, error) {
	result := []domain.Document{}
	total := 0
	if knowledgeBaseID == "" || !validPage(limit, offset) {
		return result, total, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := knowledgeBaseTx(ctx, tx, workspaceID, knowledgeBaseID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents WHERE workspace_id=? AND knowledge_base_id=? AND deleted_at IS NULL`, workspaceID, knowledgeBaseID).Scan(&total); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+documentColumns+` FROM documents WHERE workspace_id=? AND knowledge_base_id=? AND deleted_at IS NULL ORDER BY updated_at DESC,id DESC LIMIT ? OFFSET ?`, workspaceID, knowledgeBaseID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			document, err := scanDocument(rows)
			if err != nil {
				return err
			}
			result = append(result, document)
		}
		return rows.Err()
	})
	return result, total, err
}
