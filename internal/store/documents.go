package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"math"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const documentColumns = `id,workspace_id,title,current_version_id,created_by,created_at,updated_at`
const documentVersionColumns = `id,document_id,version_number,filename,media_type,size_bytes,sha256,created_by,created_at,object_key`

func validDocumentText(value string) bool {
	if !utf8.ValidString(value) || !validText(value, 255) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func validDocumentFilename(filename string) bool {
	trimmed := strings.TrimSpace(filename)
	return validDocumentText(filename) && trimmed != "." && trimmed != ".." && !strings.ContainsAny(filename, `/\`)
}

func canWriteDocument(role string) bool {
	return role == "owner" || role == "admin" || role == "contributor"
}

func scanDocument(row rowScanner) (domain.Document, error) {
	var document domain.Document
	err := row.Scan(&document.ID, &document.WorkspaceID, &document.Title, &document.CurrentVersionID, &document.CreatedBy, &document.CreatedAt, &document.UpdatedAt)
	return document, dbError(err)
}

func scanDocumentVersion(row rowScanner) (domain.DocumentVersion, error) {
	var version domain.DocumentVersion
	err := row.Scan(documentVersionDest(&version)...)
	return version, dbError(err)
}

func documentVersionDest(version *domain.DocumentVersion) []any {
	return []any{&version.ID, &version.DocumentID, &version.Number, &version.Filename, &version.MediaType, &version.SizeBytes, &version.SHA256, &version.CreatedBy, &version.CreatedAt, &version.ObjectKey}
}

// documentInWorkspace runs only after lockWorkspace. Including workspace and
// deleted_at here makes every version/content lookup inherit the same boundary.
func documentInWorkspace(ctx context.Context, tx *sql.Tx, workspaceID, documentID string) (domain.Document, error) {
	return scanDocument(tx.QueryRowContext(ctx, `SELECT `+documentColumns+` FROM documents WHERE id=? AND workspace_id=? AND deleted_at IS NULL FOR UPDATE`, documentID, workspaceID))
}

// insertDocumentVersion is the only application write to document_versions.
// Metadata and its storage reference are inserted together; old versions are
// never updated. Inline bytes are only retained for legacy fixtures.
func insertDocumentVersion(ctx context.Context, tx *sql.Tx, userID, documentID, versionID string, number int, filename string, content []byte, objectKey string) (domain.DocumentVersion, error) {
	if content == nil {
		content = []byte{}
	}
	digest := sha256.Sum256(content)
	var inline any = content
	if objectKey != "" {
		inline = nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO document_versions(id,document_id,version_number,filename,media_type,size_bytes,sha256,content,created_by,object_key) VALUES(?,?,?,?,?,?,?,?,?,?)`, versionID, documentID, number, filename, http.DetectContentType(content), len(content), hex.EncodeToString(digest[:]), inline, userID, objectKey)
	if err != nil {
		return domain.DocumentVersion{}, err
	}
	return scanDocumentVersion(tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+` FROM document_versions WHERE id=? AND document_id=?`, versionID, documentID))
}

func (s *Store) CreateDocument(ctx context.Context, userID, workspaceID, title, filename string, content []byte) (domain.DocumentWrite, error) {
	return s.createDocument(ctx, userID, workspaceID, title, filename, content, nil)
}

func (s *Store) createDocument(ctx context.Context, userID, workspaceID, title, filename string, content []byte, gate *documentWriteGate) (domain.DocumentWrite, error) {
	var result domain.DocumentWrite
	title = strings.TrimSpace(title)
	if !validDocumentText(title) || !validDocumentFilename(filename) || !s.validUploadSize(len(content)) {
		return result, domain.ErrInvalid
	}
	documentID, err := newID()
	if err != nil {
		return result, err
	}
	versionID, err := newID()
	if err != nil {
		return result, err
	}
	objectKey, err := s.stageDocumentObject(ctx, userID, workspaceID, documentID, versionID, "", content)
	if err != nil {
		return result, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if gate != nil {
			done, checkErr := gate.check(ctx, tx, &result)
			if checkErr != nil || done {
				return checkErr
			}
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		if err = consumeDocumentObjectTx(ctx, tx, objectKey); err != nil {
			return err
		}
		// The head is assigned with its first immutable version in this transaction.
		// Avoid a cyclic FK: versions own the document FK; all head writes live here.
		if _, err = tx.ExecContext(ctx, `INSERT INTO documents(id,workspace_id,title,current_version_id,created_by) VALUES(?,?,?,?,?)`, documentID, workspaceID, title, versionID, userID); err != nil {
			return err
		}
		result.Version, err = insertDocumentVersion(ctx, tx, userID, documentID, versionID, 1, filename, content, objectKey)
		if err != nil {
			return err
		}
		result.Document, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
		if err == nil && s.indexProfile != nil {
			_, _, err = enqueueIndexTx(ctx, tx, userID, workspaceID, documentID, versionID, *s.indexProfile)
		}
		if err == nil && gate != nil {
			err = gate.commit(ctx, tx, result)
		}
		return err
	})
	if err != nil || result.Version.ID != versionID {
		s.abandonDocumentObject(objectKey)
	}
	return result, err
}

func (s *Store) AppendDocumentVersion(ctx context.Context, userID, workspaceID, documentID, baseVersionID, filename string, content []byte) (domain.DocumentWrite, error) {
	return s.appendDocumentVersion(ctx, userID, workspaceID, documentID, baseVersionID, filename, content, nil)
}

func (s *Store) appendDocumentVersion(ctx context.Context, userID, workspaceID, documentID, baseVersionID, filename string, content []byte, gate *documentWriteGate) (domain.DocumentWrite, error) {
	var result domain.DocumentWrite
	if baseVersionID == "" || !validDocumentFilename(filename) || !s.validUploadSize(len(content)) {
		return result, domain.ErrInvalid
	}
	versionID, err := newID()
	if err != nil {
		return result, err
	}
	objectKey, err := s.stageDocumentObject(ctx, userID, workspaceID, documentID, versionID, baseVersionID, content)
	if err != nil {
		return result, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if gate != nil {
			done, checkErr := gate.check(ctx, tx, &result)
			if checkErr != nil || done {
				return checkErr
			}
		}
		result.Document, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
		if err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		if result.Document.CurrentVersionID != baseVersionID {
			return domain.ErrConflict
		}
		if err = consumeDocumentObjectTx(ctx, tx, objectKey); err != nil {
			return err
		}
		var currentNumber int
		if err = tx.QueryRowContext(ctx, `SELECT version_number FROM document_versions WHERE id=? AND document_id=?`, baseVersionID, documentID).Scan(&currentNumber); err != nil {
			return err
		}
		if currentNumber == math.MaxInt32 {
			return domain.ErrConflict
		}
		result.Version, err = insertDocumentVersion(ctx, tx, userID, documentID, versionID, currentNumber+1, filename, content, objectKey)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE documents SET current_version_id=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=?`, versionID, documentID, workspaceID); err != nil {
			return err
		}
		result.Document, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
		if err == nil && s.indexProfile != nil {
			_, _, err = enqueueIndexTx(ctx, tx, userID, workspaceID, documentID, versionID, *s.indexProfile)
		}
		if err == nil && gate != nil {
			err = gate.commit(ctx, tx, result)
		}
		return err
	})
	if err != nil || result.Version.ID != versionID {
		s.abandonDocumentObject(objectKey)
	}
	return result, err
}

func (s *Store) Documents(ctx context.Context, userID, workspaceID string, limit, offset int) ([]domain.Document, int, error) {
	result := []domain.Document{}
	total := 0
	if !validPage(limit, offset) {
		return result, total, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents WHERE workspace_id=? AND deleted_at IS NULL`, workspaceID).Scan(&total); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+documentColumns+` FROM documents WHERE workspace_id=? AND deleted_at IS NULL ORDER BY updated_at DESC,id DESC LIMIT ? OFFSET ?`, workspaceID, limit, offset)
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

func (s *Store) Document(ctx context.Context, userID, workspaceID, documentID string) (domain.Document, error) {
	var result domain.Document
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		var err error
		result, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
		return err
	})
	return result, err
}

func (s *Store) DocumentVersions(ctx context.Context, userID, workspaceID, documentID string, limit, offset int) ([]domain.DocumentVersion, int, error) {
	result := []domain.DocumentVersion{}
	total := 0
	if !validPage(limit, offset) {
		return result, total, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM document_versions WHERE document_id=?`, documentID).Scan(&total); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+documentVersionColumns+` FROM document_versions WHERE document_id=? ORDER BY version_number DESC LIMIT ? OFFSET ?`, documentID, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			version, err := scanDocumentVersion(rows)
			if err != nil {
				return err
			}
			result = append(result, version)
		}
		return rows.Err()
	})
	return result, total, err
}

func (s *Store) DocumentVersion(ctx context.Context, userID, workspaceID, documentID, versionID string) (domain.DocumentVersion, error) {
	var result domain.DocumentVersion
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		var err error
		result, err = scanDocumentVersion(tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+` FROM document_versions WHERE id=? AND document_id=?`, versionID, documentID))
		return err
	})
	return result, err
}

func (s *Store) DocumentContent(ctx context.Context, userID, workspaceID, documentID, versionID string) (domain.DocumentVersion, []byte, error) {
	var version domain.DocumentVersion
	var content []byte
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+`,content FROM document_versions WHERE id=? AND document_id=?`, versionID, documentID).Scan(append(documentVersionDest(&version), &content)...)
	})
	if err != nil {
		return version, nil, err
	}
	content, err = s.loadDocumentBytes(ctx, version, content)
	if err != nil {
		return version, nil, err
	}
	// A download may wait on remote I/O while access is removed or the document
	// is deleted. Reauthorize before releasing any bytes to its caller.
	_, err = s.DocumentVersion(ctx, userID, workspaceID, documentID, versionID)
	if err != nil {
		return version, nil, err
	}
	return version, content, nil
}

func (s *Store) DeleteDocument(ctx context.Context, userID, workspaceID, documentID string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		if err = queueDocumentCleanupTx(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE documents SET deleted_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=? AND workspace_id=?`, documentID, workspaceID)
		return err
	})
}
