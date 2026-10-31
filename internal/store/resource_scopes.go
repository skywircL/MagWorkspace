package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"sort"
	"strings"

	"magworkspace/internal/domain"
)

// Resolve requested resources while the caller holds the workspace lock. Every
// submitted ID is checked, including documents covered by a whole-library pick.
func resourceSelectionTx(ctx context.Context, tx *sql.Tx, wid string, in domain.ResourceSelection) (domain.ResourceScope, error) {
	out := domain.ResourceScope{Targets: []domain.SearchTarget{}}
	if domain.ValidateKnowledgeBaseIDs(in.KnowledgeBaseIDs) != nil || len(in.DocumentIDs) > 100 {
		return out, domain.ErrInvalid
	}
	if in.KnowledgeBaseIDs == nil && in.DocumentIDs == nil {
		out.All = true
		return out, nil
	}
	if err := checkIndexKnowledgeBases(ctx, tx, wid, in.KnowledgeBaseIDs); err != nil {
		return out, err
	}
	targets := map[string][]string{}
	for _, id := range in.KnowledgeBaseIDs {
		targets[id] = nil
	}
	seen := map[string]bool{}
	for _, id := range in.DocumentIDs {
		if !validHexID(id) || seen[id] {
			return out, domain.ErrInvalid
		}
		seen[id] = true
		var kb string
		if err := tx.QueryRowContext(ctx, `SELECT knowledge_base_id FROM documents WHERE id=? AND workspace_id=? AND deleted_at IS NULL`, id, wid).Scan(&kb); err != nil {
			return out, err
		}
		if docs, exists := targets[kb]; !exists || docs != nil {
			targets[kb] = append(docs, id)
		}
	}
	for kb, docs := range targets {
		sort.Strings(docs)
		out.Targets = append(out.Targets, domain.SearchTarget{KnowledgeBaseID: kb, DocumentIDs: docs})
	}
	sort.Slice(out.Targets, func(i, j int) bool { return out.Targets[i].KnowledgeBaseID < out.Targets[j].KnowledgeBaseID })
	return out, out.Validate()
}

// Old snapshots retain their intersection semantics. No old selected library
// is ever reinterpreted as a full-library union target during recovery.
func legacyResourceScopeTx(ctx context.Context, tx *sql.Tx, wid string, documents, libraries []string) (domain.ResourceScope, error) {
	if err := checkIndexKnowledgeBases(ctx, tx, wid, libraries); err != nil {
		return domain.ResourceScope{}, err
	}
	if documents == nil {
		return resourceSelectionTx(ctx, tx, wid, domain.ResourceSelection{KnowledgeBaseIDs: libraries})
	}
	scope, err := resourceSelectionTx(ctx, tx, wid, domain.ResourceSelection{DocumentIDs: documents})
	if err != nil {
		return scope, err
	}
	if libraries != nil {
		for _, target := range scope.Targets {
			if !slices.Contains(libraries, target.KnowledgeBaseID) {
				return domain.ResourceScope{}, domain.ErrForbidden
			}
		}
	}
	return scope, nil
}

func storeRunResourceScopeTx(ctx context.Context, tx *sql.Tx, wid, rid string, in domain.ResourceSelection) error {
	scope, err := resourceSelectionTx(ctx, tx, wid, in)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_search_targets(run_id,scope) VALUES(?,?)`, rid, raw)
	if err == nil {
		err = storeRunDocumentScopeTx(ctx, tx, wid, rid, scope.DocumentProjection())
	}
	return err
}

func runResourceScopeTx(ctx context.Context, tx *sql.Tx, wid, rid string) (domain.ResourceScope, error) {
	var raw []byte
	var version int
	err := tx.QueryRowContext(ctx, `SELECT r.resource_scope_version,LEFT(t.scope,65537) FROM runs r LEFT JOIN run_search_targets t ON t.run_id=r.id WHERE r.id=?`, rid).Scan(&version, &raw)
	if err != nil {
		return domain.ResourceScope{}, err
	}
	if version == 1 && raw == nil {
		documents, err := runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return domain.ResourceScope{}, err
		}
		libraries, err := runKnowledgeBaseScopeTx(ctx, tx, rid)
		if err != nil {
			return domain.ResourceScope{}, err
		}
		return legacyResourceScopeTx(ctx, tx, wid, documents, libraries)
	}
	if version != 2 || raw == nil {
		return domain.ResourceScope{}, domain.ErrInvalid
	}
	var scope domain.ResourceScope
	var fields map[string]json.RawMessage
	if len(raw) > 65536 || json.Unmarshal(raw, &fields) != nil || len(fields) != 2 || fields["all"] == nil || fields["targets"] == nil || string(fields["all"]) == "null" || json.Unmarshal(raw, &scope) != nil || scope.Validate() != nil {
		return scope, domain.ErrInvalid
	}
	var targets []map[string]json.RawMessage
	if json.Unmarshal(fields["targets"], &targets) != nil {
		return scope, domain.ErrInvalid
	}
	for _, target := range targets {
		// Missing document_ids must not silently become nil/full-library.
		if len(target) != 2 || target["knowledge_base_id"] == nil || target["document_ids"] == nil {
			return scope, domain.ErrInvalid
		}
	}
	return scope, nil
}

func checkResourceScopeTx(ctx context.Context, tx *sql.Tx, wid string, scope domain.ResourceScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	for _, target := range scope.Targets {
		if err := checkIndexKnowledgeBases(ctx, tx, wid, []string{target.KnowledgeBaseID}); err != nil {
			return err
		}
		for _, id := range target.DocumentIDs {
			var kb string
			if err := tx.QueryRowContext(ctx, `SELECT knowledge_base_id FROM documents WHERE id=? AND workspace_id=? AND deleted_at IS NULL`, id, wid).Scan(&kb); err != nil {
				return err
			}
			if kb != target.KnowledgeBaseID {
				return domain.ErrForbidden
			}
		}
	}
	return nil
}

func resolveRunResourceScopeTx(ctx context.Context, tx *sql.Tx, wid, rid string, selection []string) (domain.ResourceScope, error) {
	upper, err := runResourceScopeTx(ctx, tx, wid, rid)
	if err != nil {
		return upper, err
	}
	if err = checkResourceScopeTx(ctx, tx, wid, upper); err != nil {
		return upper, err
	}
	out, err := upper.NarrowKnowledgeBases(selection)
	if err == nil {
		err = checkResourceScopeTx(ctx, tx, wid, out)
	}
	return out, err
}

func resourceScopeFilter(scope domain.ResourceScope) (string, []any, error) {
	if err := scope.Validate(); err != nil {
		return "", nil, err
	}
	if scope.All {
		return "", nil, nil
	}
	if len(scope.Targets) == 0 {
		return " AND 1=0", nil, nil
	}
	clauses, args := []string{}, []any{}
	for _, target := range scope.Targets {
		clause := "d.knowledge_base_id=?"
		args = append(args, target.KnowledgeBaseID)
		if target.DocumentIDs != nil {
			clause += " AND d.id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(target.DocumentIDs)), ",") + ")"
			for _, id := range target.DocumentIDs {
				args = append(args, id)
			}
		}
		clauses = append(clauses, "("+clause+")")
	}
	return " AND (" + strings.Join(clauses, " OR ") + ")", args, nil
}

// RunResourceScope is read from MySQL for every tool call, independently of
// model arguments or the Harness snapshot. The latter never grants authority.
func (s *Store) RunResourceScope(ctx context.Context, uid, wid, sid, rid string, selection []string) (domain.ResourceScope, error) {
	var out domain.ResourceScope
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		var err error
		out, err = resolveRunResourceScopeTx(ctx, tx, wid, rid, selection)
		return err
	})
	return out, err
}
