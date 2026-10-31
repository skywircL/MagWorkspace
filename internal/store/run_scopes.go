package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"

	"magworkspace/internal/domain"
)

// No row means no additional KB restriction. A stored [] explicitly denies all
// libraries. IDs are retained after deletion so a removed scope cannot widen.
func storeRunKnowledgeBaseScopeTx(ctx context.Context, tx *sql.Tx, wid, rid string, ids []string) error {
	if err := checkIndexKnowledgeBases(ctx, tx, wid, ids); err != nil {
		return err
	}
	if ids == nil {
		return nil
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_knowledge_base_scopes(run_id,knowledge_base_ids) VALUES(?,?)`, rid, raw)
	return err
}

func runKnowledgeBaseScopeTx(ctx context.Context, tx *sql.Tx, rid string) ([]string, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT LEFT(knowledge_base_ids,2048) FROM run_knowledge_base_scopes WHERE run_id=?`, rid).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	if json.Unmarshal(raw, &ids) != nil || ids == nil || domain.ValidateKnowledgeBaseIDs(ids) != nil {
		return nil, domain.ErrInvalid
	}
	return ids, nil
}

// The caller holds Workspace -> Session -> Run locks. Model arguments may only
// narrow the stored upper bound; reject an entire mixed selection on escape.
func resolveRunKnowledgeBasesTx(ctx context.Context, tx *sql.Tx, wid, rid string, selection []string) ([]string, error) {
	if err := domain.ValidateKnowledgeBaseIDs(selection); err != nil {
		return nil, err
	}
	upper, err := runKnowledgeBaseScopeTx(ctx, tx, rid)
	if err != nil {
		return nil, err
	}
	if upper != nil && selection != nil {
		for _, id := range selection {
			if !slices.Contains(upper, id) {
				return nil, domain.ErrForbidden
			}
		}
	}
	// A deleted selected library invalidates the Run even if this call happens
	// to request another subset, or has no acquired sources yet.
	if err = checkIndexKnowledgeBases(ctx, tx, wid, upper); err != nil {
		return nil, err
	}
	if selection == nil {
		return upper, nil
	}
	if err = checkIndexKnowledgeBases(ctx, tx, wid, selection); err != nil {
		return nil, err
	}
	return selection, nil
}

func checkRunDocumentKnowledgeBaseTx(ctx context.Context, tx *sql.Tx, wid, rid, did string) error {
	ids, err := resolveRunKnowledgeBasesTx(ctx, tx, wid, rid, nil)
	if err != nil {
		return err
	}
	var kb string
	if err = tx.QueryRowContext(ctx, `SELECT knowledge_base_id FROM documents WHERE id=? AND workspace_id=? AND deleted_at IS NULL`, did, wid).Scan(&kb); err != nil {
		return err
	}
	if ids != nil && !slices.Contains(ids, kb) {
		return domain.ErrForbidden
	}
	return nil
}

// RunSearchScope reads legacy intersection snapshots only. Current Run tools
// must use RunResourceScope; projecting a mixed union into two dimensions loses
// the per-library document boundary.
func (s *Store) RunSearchScope(ctx context.Context, uid, wid, sid, rid string, selection []string) (domain.RetrievalScope, error) {
	var out domain.RetrievalScope
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT resource_scope_version FROM runs WHERE id=?`, rid).Scan(&version); err != nil {
			return err
		}
		if version != 1 {
			return domain.ErrInvalid
		}
		var err error
		out.DocumentIDs, err = runDocumentScopeTx(ctx, tx, rid)
		if err == nil {
			out.KnowledgeBaseIDs, err = resolveRunKnowledgeBasesTx(ctx, tx, wid, rid, selection)
		}
		return err
	})
	return out, err
}

func validateRunScopeForResumeTx(ctx context.Context, tx *sql.Tx, wid, rid string, documentIDs []string) error {
	_, err := resolveRunResourceScopeTx(ctx, tx, wid, rid, nil)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) {
		return domain.ErrConflict
	}
	return err
}
