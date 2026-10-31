package store

import (
	"context"
	"database/sql"
	"magworkspace/internal/domain"
	"sort"
	"strings"
	"unicode/utf8"
)

type ToolKnowledgeBase struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsDefault   bool   `json:"is_default"`
}

type ToolKnowledgeBasePage struct {
	KnowledgeBases []ToolKnowledgeBase `json:"knowledge_bases"`
	Offset         int                 `json:"offset"`
	Limit          int                 `json:"limit"`
	HasMore        bool                `json:"has_more"`
	NextOffset     *int                `json:"next_offset,omitempty"`
}

func (s *Store) RunToolKnowledgeBases(ctx context.Context, uid, wid, sid, rid, keyword string, offset, limit int) (ToolKnowledgeBasePage, error) {
	out := ToolKnowledgeBasePage{KnowledgeBases: []ToolKnowledgeBase{}, Offset: offset, Limit: limit}
	keyword = strings.TrimSpace(keyword)
	if !utf8.ValidString(keyword) || len(keyword) > 256 || offset < 0 || offset > 10000 || limit < 1 || limit > 20 {
		return out, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		scope, err := resolveRunResourceScopeTx(ctx, tx, wid, rid, nil)
		if err != nil {
			return err
		}
		clause, args, err := knowledgeBaseResourceScopeFilter(scope)
		if err != nil {
			return err
		}
		query := `SELECT k.id,k.name,k.description,COALESCE(k.default_slot=1,FALSE) FROM knowledge_bases k WHERE k.workspace_id=? AND LOCATE(?,k.name)>0` + clause
		values := append([]any{wid, keyword}, args...)
		query += ` ORDER BY k.created_at,k.id LIMIT ? OFFSET ?`
		values = append(values, limit+1, offset)
		rows, err := tx.QueryContext(ctx, query, values...)
		if err != nil {
			return err
		}
		defer rows.Close()
		observed := []ToolKnowledgeBase{}
		for rows.Next() {
			var item ToolKnowledgeBase
			if err := rows.Scan(&item.ID, &item.Name, &item.Description, &item.IsDefault); err != nil {
				return err
			}
			observed = append(observed, item)
			if len(out.KnowledgeBases) < limit {
				out.KnowledgeBases = append(out.KnowledgeBases, item)
			} else {
				out.HasMore = true
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		// The lookahead is observable through has_more and carries provenance.
		for _, item := range observed {
			if err := pinToolKnowledgeBaseTx(ctx, tx, wid, rid, item); err != nil {
				return err
			}
		}
		if out.HasMore {
			next := offset + len(out.KnowledgeBases)
			out.NextOffset = &next
		}
		return nil
	})
	if err != nil {
		return ToolKnowledgeBasePage{}, err
	}
	return out, nil
}

// Capability discovery is private deployment metadata. It never replaces the
// operation's own transaction-time authorization, approval or generation gate.
func (s *Store) RunToolCapabilities(ctx context.Context, uid, wid, sid, rid string, indexing bool) ([]string, error) {
	names := []string{"ask_user", "search_conversations", "list_knowledge_bases"}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		role, err := lockWorkspace(ctx, tx, uid, wid)
		if err != nil {
			return err
		}
		scope, err := resolveRunResourceScopeTx(ctx, tx, wid, rid, nil)
		if err != nil {
			return err
		}
		var libraries bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM knowledge_bases WHERE workspace_id=?)`, wid).Scan(&libraries); err != nil {
			return err
		}
		if !libraries || (!scope.All && len(scope.Targets) == 0) {
			return nil
		}
		names = append(names, "list_documents", "read_document")
		if indexing {
			names = append(names, "search_knowledge", "search_documents", "read_evidence")
		}
		if s.objects != nil && canWriteDocument(role) {
			names = append(names, "update_document")
			canCreate := scope.All
			for _, target := range scope.Targets {
				canCreate = canCreate || scope.AllowsCreate(target.KnowledgeBaseID)
			}
			if canCreate {
				names = append(names, "create_document")
			}
		}
		return nil
	})
	sort.Strings(names)
	return names, err
}
