package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// ToolDocument exposes only authorized source metadata, never object keys.
type ToolDocument struct {
	Document domain.Document        `json:"document"`
	Version  domain.DocumentVersion `json:"version"`
	Index    *domain.IndexJob       `json:"index,omitempty"`
}

type ToolDocumentPage struct {
	Documents  []ToolDocument `json:"documents"`
	Offset     int            `json:"offset"`
	Limit      int            `json:"limit"`
	HasMore    bool           `json:"has_more"`
	NextOffset *int           `json:"next_offset,omitempty"`
}

func toolDocumentTx(ctx context.Context, tx *sql.Tx, workspaceID, documentID, versionID, indexID, profileID string) (ToolDocument, error) {
	var out ToolDocument
	var err error
	out.Document, err = documentInWorkspace(ctx, tx, workspaceID, documentID)
	if err != nil {
		return out, err
	}
	if versionID == "" {
		versionID = out.Document.CurrentVersionID
	}
	out.Version, err = scanDocumentVersion(tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+` FROM document_versions WHERE document_id=? AND id=?`, documentID, versionID))
	if err != nil {
		return out, err
	}
	var job domain.IndexJob
	if indexID != "" {
		job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=? AND i.workspace_id=? AND i.document_id=? AND i.version_id=?`, indexID, workspaceID, documentID, versionID))
	} else if profileID != "" {
		job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.workspace_id=? AND i.document_id=? AND i.version_id=? AND i.profile_id=?`, workspaceID, documentID, versionID, profileID))
	} else {
		return out, nil
	}
	if errors.Is(err, domain.ErrNotFound) && indexID == "" {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	if job.SourceSHA256 != out.Version.SHA256 {
		return out, domain.ErrConflict
	}
	out.Index = &job
	return out, nil
}

func pinToolDocumentTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, doc ToolDocument) error {
	indexID, generation := "", ""
	if doc.Index != nil && doc.Index.Status == "ready" {
		indexID, generation = doc.Index.ID, doc.Index.Generation
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_document_sources WHERE run_id=? AND document_id=? AND version_id=? AND index_id=?)`, runID, doc.Document.ID, doc.Version.ID, indexID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_document_sources WHERE run_id=?`, runID).Scan(&count); err != nil {
			return err
		}
		if count >= maxRunHistorySources {
			return domain.ErrInvalid
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT IGNORE INTO run_document_sources(run_id,workspace_id,document_id,version_id,source_sha256,index_id,generation) VALUES(?,?,?,?,?,?,?)`, runID, workspaceID, doc.Document.ID, doc.Version.ID, doc.Version.SHA256, indexID, generation)
	return err
}

func runIndexPinnedTx(ctx context.Context, tx *sql.Tx, runID string, job domain.IndexJob) (bool, error) {
	var pinned bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_document_sources WHERE run_id=? AND document_id=? AND version_id=? AND source_sha256=? AND index_id=? AND generation=?) OR EXISTS(SELECT 1 FROM run_evidence WHERE run_id=? AND document_id=? AND version_id=? AND source_sha256=? AND index_id=? AND generation=?)`, runID, job.DocumentID, job.VersionID, job.SourceSHA256, job.ID, job.Generation, runID, job.DocumentID, job.VersionID, job.SourceSHA256, job.ID, job.Generation).Scan(&pinned)
	return pinned, err
}

func runAllowsDocumentTx(ctx context.Context, tx *sql.Tx, runID, documentID string) error {
	scope, err := runDocumentScopeTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	if scope == nil {
		return nil
	}
	for _, id := range scope {
		if id == documentID {
			return nil
		}
	}
	return domain.ErrForbidden
}

// RunToolDocument pins a current source before remote I/O. Explicit historical
// reads require a matching source already acquired by this private run.
func (s *Store) RunToolDocument(ctx context.Context, userID, workspaceID, sessionID, runID, documentID, versionID, indexID, profileID string) (ToolDocument, error) {
	var out ToolDocument
	if !validHexID(documentID) || (versionID != "" && !validHexID(versionID)) || (indexID != "" && !validHexID(indexID)) || (versionID == "") != (indexID == "") || (profileID != "" && !validProfileID(profileID)) {
		return out, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		if err := runAllowsDocumentTx(ctx, tx, runID, documentID); err != nil {
			return err
		}
		var err error
		out, err = toolDocumentTx(ctx, tx, workspaceID, documentID, versionID, indexID, profileID)
		if err != nil {
			return err
		}
		if indexID != "" && out.Index.Status != "ready" {
			return domain.ErrConflict
		}
		if out.Version.ID != out.Document.CurrentVersionID {
			if out.Index == nil {
				return domain.ErrConflict
			}
			pinned, err := runIndexPinnedTx(ctx, tx, runID, *out.Index)
			if err != nil {
				return err
			}
			if !pinned {
				return domain.ErrConflict
			}
		}
		return pinToolDocumentTx(ctx, tx, workspaceID, runID, out)
	})
	return out, err
}

// RunToolDocuments applies scope before filtering/pagination. Recording even
// the lookahead row keeps the has_more observation within history provenance.
func (s *Store) RunToolDocuments(ctx context.Context, userID, workspaceID, sessionID, runID, keyword, profileID string, offset, limit int) (ToolDocumentPage, error) {
	out := ToolDocumentPage{Documents: []ToolDocument{}, Offset: offset, Limit: limit}
	keyword = strings.TrimSpace(keyword)
	if !utf8.ValidString(keyword) || len(keyword) > 256 || offset < 0 || offset > 10000 || limit < 1 || limit > 20 || (profileID != "" && !validProfileID(profileID)) {
		return out, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		filter, args, err := documentScopeFilter([][]string{scope})
		if err != nil {
			return err
		}
		values := append([]any{workspaceID, keyword, keyword}, args...)
		values = append(values, limit+1, offset)
		rows, err := tx.QueryContext(ctx, `SELECT d.id FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.id=d.current_version_id WHERE d.workspace_id=? AND d.deleted_at IS NULL AND (LOCATE(?,d.title)>0 OR LOCATE(?,v.filename)>0)`+filter+` ORDER BY d.updated_at DESC,d.id DESC LIMIT ? OFFSET ?`, values...)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range ids {
			doc, err := toolDocumentTx(ctx, tx, workspaceID, id, "", "", profileID)
			if err != nil {
				return err
			}
			if err = pinToolDocumentTx(ctx, tx, workspaceID, runID, doc); err != nil {
				return err
			}
			if len(out.Documents) < limit {
				out.Documents = append(out.Documents, doc)
			}
		}
		out.HasMore = len(ids) > limit
		if out.HasMore {
			next := offset + len(out.Documents)
			out.NextOffset = &next
		}
		return nil
	})
	return out, err
}
