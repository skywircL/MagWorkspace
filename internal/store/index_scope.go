package store

import (
	"context"
	"database/sql"
	"magworkspace/internal/domain"
	"strings"
)

func singleKnowledgeBase(id string) []string {
	if id == "" {
		return nil
	}
	return []string{id}
}

// Both filters are intersections with workspace authorization, including []=none.
func indexKnowledgeBasesScopeFilter(ids []string, selections [][]string) (string, []any, error) {
	filter, args, err := documentScopeFilter(selections)
	if err != nil {
		return "", nil, err
	}
	if err = domain.ValidateKnowledgeBaseIDs(ids); err != nil {
		return "", nil, err
	}
	if ids == nil {
		return filter, args, nil
	}
	if len(ids) == 0 {
		return filter + " AND 1=0", args, nil
	}
	for _, id := range ids {
		args = append(args, id)
	}
	return filter + " AND i.knowledge_base_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")", args, nil
}

func indexDocumentScopeFilter(id string, selections [][]string) (string, []any, error) {
	return indexKnowledgeBasesScopeFilter(singleKnowledgeBase(id), selections)
}

// The workspace lock serializes this check with deletion. Every search stage,
// including empty hydration and post-reranking checks, validates all selected IDs.
func checkIndexKnowledgeBases(ctx context.Context, tx *sql.Tx, workspaceID string, ids []string) error {
	if err := domain.ValidateKnowledgeBaseIDs(ids); err != nil {
		return err
	}
	for _, id := range ids {
		var found string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM knowledge_bases WHERE id=? AND workspace_id=?`, id, workspaceID).Scan(&found); err != nil {
			return err
		}
	}
	return nil
}

func checkIndexKnowledgeBase(ctx context.Context, tx *sql.Tx, workspaceID, id string) error {
	return checkIndexKnowledgeBases(ctx, tx, workspaceID, singleKnowledgeBase(id))
}
