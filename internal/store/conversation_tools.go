package store

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const (
	maxConversationResults   = 8
	maxConversationOffset    = 200
	maxConversationScan      = 1000
	conversationSnippetRunes = 400
)

// SearchRunConversations recalls completed exchanges belonging to the current
// user in this workspace. Literal substring matching needs no additional search
// infrastructure. A bounded recent candidate window limits provenance work.
func (s *Store) SearchRunConversations(ctx context.Context, userID, workspaceID, sessionID, runID, query string, limit, offset int) (domain.ConversationSearchResult, error) {
	result := domain.ConversationSearchResult{Items: []domain.ConversationMatch{}}
	query = strings.TrimSpace(query)
	if limit == 0 {
		limit = 5
	}
	if query == "" || len(query) > 512 || !utf8.ValidString(query) || limit < 1 || limit > maxConversationResults || offset < 0 || offset > maxConversationOffset {
		return result, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		scope, err := resolveRunResourceScopeTx(ctx, tx, workspaceID, runID, nil)
		if err != nil {
			return err
		}
		// Only IDs are buffered before permission filtering. MySQL may scan chat
		// text, but neither unrelated text nor source titles leave this boundary.
		rows, err := tx.QueryContext(ctx, `SELECT r.id FROM sessions s JOIN runs r ON r.session_id=s.id JOIN messages u ON u.run_id=r.id AND u.session_id=s.id AND u.role='user' JOIN messages a ON a.run_id=r.id AND a.session_id=s.id AND a.role='assistant' WHERE s.workspace_id=? AND s.user_id=? AND r.status='completed' AND r.id<>? AND (LOCATE(CAST(? AS BINARY),CAST(s.title AS BINARY))>0 OR LOCATE(CAST(? AS BINARY),CAST(u.content AS BINARY))>0 OR LOCATE(CAST(? AS BINARY),CAST(a.content AS BINARY))>0) ORDER BY r.finished_at DESC,r.id DESC LIMIT ?`, workspaceID, userID, runID, query, query, query, maxConversationScan)
		if err != nil {
			return err
		}
		candidates := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		selected := []string{}
		acquired := []string{}
		visible := 0
		for _, id := range candidates {
			allowed, err := runSourcesValidInScopeTx(ctx, tx, workspaceID, id, scope)
			if err != nil {
				return err
			}
			if !allowed {
				continue
			}
			if visible < offset {
				visible++
				continue
			}
			// The extra match is observable through HasMore/NextOffset even
			// without an excerpt, so it carries the same source constraints.
			acquired = append(acquired, id)
			if len(selected) == limit {
				result.HasMore = true
				next := offset + len(selected)
				if next <= maxConversationOffset {
					result.NextOffset = &next
				}
				break
			}
			selected = append(selected, id)
		}
		if err := bindConversationSearchTx(ctx, tx, runID, acquired); err != nil {
			return err
		}
		allowed, err := runSourcesValidInScopeTx(ctx, tx, workspaceID, runID, scope)
		if err != nil {
			return err
		}
		if !allowed {
			return domain.ErrInvalid
		}
		for _, id := range selected {
			var item domain.ConversationMatch
			if err := tx.QueryRowContext(ctx, `SELECT s.id,r.id,s.title,u.id,a.id,u.content,a.content,r.created_at,r.finished_at FROM runs r JOIN sessions s ON s.id=r.session_id JOIN messages u ON u.run_id=r.id AND u.session_id=s.id AND u.role='user' JOIN messages a ON a.run_id=r.id AND a.session_id=s.id AND a.role='assistant' WHERE r.id=? AND r.status='completed' AND s.workspace_id=? AND s.user_id=?`, id, workspaceID, userID).Scan(&item.SessionID, &item.RunID, &item.Title, &item.UserMessageID, &item.AssistantMessageID, &item.Question, &item.Answer, &item.CreatedAt, &item.FinishedAt); err != nil {
				return err
			}
			item.Question = conversationSnippet(item.Question, query)
			item.Answer = conversationSnippet(item.Answer, query)
			result.Items = append(result.Items, item)
		}
		return nil
	})
	if err != nil {
		return domain.ConversationSearchResult{Items: []domain.ConversationMatch{}}, err
	}
	return result, nil
}

func conversationSnippet(content, query string) string {
	content = strings.TrimSpace(content)
	runes := []rune(content)
	if len(runes) <= conversationSnippetRunes {
		return content
	}
	start := 0
	if match := strings.Index(content, query); match >= 0 {
		start = max(0, utf8.RuneCountInString(content[:match])-80)
	}
	// Ellipses also count towards the public limit.
	budget := conversationSnippetRunes
	if start > 0 {
		budget--
	}
	end := min(start+budget, len(runes))
	if end < len(runes) {
		end--
	}
	snippet := string(runes[start:end])
	if start > 0 {
		snippet = "…" + snippet
	}
	if end < len(runes) {
		snippet += "…"
	}
	return snippet
}

// Conversation source IDs intentionally have no cascading source foreign key.
// Deleting a recalled session must invalidate its descendants, not erase the
// provenance that would otherwise prevent reuse of its now-unavailable content.
func runConversationSourcesValidTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, _ []string) (bool, error) {
	var userID string
	if err := tx.QueryRowContext(ctx, `SELECT s.user_id FROM runs r JOIN sessions s ON s.id=r.session_id WHERE r.id=? AND s.workspace_id=?`, runID, workspaceID).Scan(&userID); err != nil {
		return false, err
	}
	var total, invalid int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(r.id IS NULL OR r.status<>'completed' OR s.user_id<>? OR s.workspace_id<>? OR c.source_run_id=c.run_id),0) FROM run_conversation_sources c LEFT JOIN runs r ON r.id=c.source_run_id LEFT JOIN sessions s ON s.id=r.session_id WHERE c.run_id=?`, userID, workspaceID, runID).Scan(&total, &invalid); err != nil {
		return false, err
	}
	return invalid == 0 && total <= maxRunHistorySources, nil
}

// bindConversationHistoryTx propagates recalled provenance through the actual
// completed turns that are subsequently sent as model history.
func bindConversationHistoryTx(ctx context.Context, tx *sql.Tx, runID string, historyRunIDs []string) error {
	return bindConversationSourcesTx(ctx, tx, runID, historyRunIDs, false)
}

func bindConversationSearchTx(ctx context.Context, tx *sql.Tx, runID string, sourceRunIDs []string) error {
	return bindConversationSourcesTx(ctx, tx, runID, sourceRunIDs, true)
}

func bindConversationSourcesTx(ctx context.Context, tx *sql.Tx, runID string, sourceRunIDs []string, includeDirect bool) error {
	sources := map[string]bool{}
	collect := func(id string, includeHistory bool) error {
		query := `SELECT source_run_id FROM run_conversation_sources WHERE run_id=?`
		args := []any{id}
		if includeHistory {
			query += ` UNION SELECT source_run_id FROM run_history_sources WHERE run_id=?`
			args = append(args, id)
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var source string
			if err := rows.Scan(&source); err != nil {
				return err
			}
			sources[source] = true
			if len(sources) > maxRunHistorySources {
				return domain.ErrInvalid
			}
		}
		return rows.Err()
	}
	if err := collect(runID, false); err != nil {
		return err
	}
	for _, id := range sourceRunIDs {
		if includeDirect {
			sources[id] = true
		}
		if err := collect(id, includeDirect); err != nil {
			return err
		}
	}
	if len(sources) > maxRunHistorySources {
		return domain.ErrInvalid
	}
	ordered := make([]string, 0, len(sources))
	for id := range sources {
		if id == runID || !validHexID(id) {
			return domain.ErrInvalid
		}
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO run_conversation_sources(run_id,source_run_id) VALUES(?,?)`, runID, id); err != nil {
			return err
		}
	}
	return nil
}
