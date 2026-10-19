package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// UpdateDocument changes presentation metadata without replacing immutable
// source bytes, versions, or published index artifacts.
func (s *Store) UpdateDocument(ctx context.Context, userID, workspaceID, documentID, title string) (domain.Document, error) {
	var document domain.Document
	title = strings.TrimSpace(title)
	if !validDocumentText(title) {
		return document, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if _, err = documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		if _, err = tx.ExecContext(ctx, `UPDATE documents SET title=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=?`, title, documentID, workspaceID); err != nil {
			return err
		}
		document, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
		return err
	})
	return document, err
}

func validEditableDocumentContent(content string) bool {
	if len(content) > domain.MaxEditableDocumentBytes || !utf8.ValidString(content) {
		return false
	}
	for _, char := range content {
		if unicode.IsControl(char) && char != '\n' && char != '\r' && char != '\t' && char != '\f' {
			return false
		}
	}
	return true
}

// UpdateDocumentText preserves the current version's filename and appends a
// new immutable version through the existing object/CAS/index admission path.
func (s *Store) UpdateDocumentText(ctx context.Context, userID, workspaceID, documentID, baseVersionID, content string) (domain.DocumentWrite, error) {
	if baseVersionID == "" || !validEditableDocumentContent(content) {
		return domain.DocumentWrite{}, domain.ErrInvalid
	}
	var filename string
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		document, err := documentInWorkspace(ctx, tx, workspaceID, documentID)
		if err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		// Resolve the supplied version within this document before comparing
		// heads, so a foreign version never becomes a usable update reference.
		if err = tx.QueryRowContext(ctx, `SELECT filename FROM document_versions WHERE id=? AND document_id=?`, baseVersionID, documentID).Scan(&filename); err != nil {
			return err
		}
		if document.CurrentVersionID != baseVersionID {
			return domain.ErrConflict
		}
		switch strings.ToLower(filepath.Ext(filename)) {
		case ".txt", ".md", ".markdown":
			return nil
		default:
			return domain.ErrInvalid
		}
	})
	if err != nil {
		return domain.DocumentWrite{}, err
	}
	// External upload happens after releasing the preflight locks. Append
	// reauthorizes and repeats CAS before linking the object and queuing its job.
	return s.AppendDocumentVersion(ctx, userID, workspaceID, documentID, baseVersionID, filename, []byte(content))
}
