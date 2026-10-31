package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"magworkspace/internal/domain"
)

// Fixed-width component digests make the concatenation unambiguous and match
// the SQL expression below without depending on a JSON serializer's spelling.
func knowledgeBaseMetadataDigest(item ToolKnowledgeBase) string {
	name := sha256.Sum256([]byte(item.Name))
	description := sha256.Sum256([]byte(item.Description))
	combined := hex.EncodeToString(name[:]) + hex.EncodeToString(description[:]) + strconv.FormatBool(item.IsDefault)
	digest := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(digest[:])
}

// Document and library metadata share the existing per-run source budget.
// Callers hold the workspace/session/run locks and only count new identities.
func requireMetadataSourceCapacityTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM run_document_sources WHERE run_id=?)+(SELECT COUNT(*) FROM run_knowledge_base_sources WHERE run_id=?)`, runID, runID).Scan(&count); err != nil {
		return err
	}
	if count >= maxRunHistorySources {
		return domain.ErrInvalid
	}
	return nil
}

func pinToolKnowledgeBaseTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, item ToolKnowledgeBase) error {
	digest := knowledgeBaseMetadataDigest(item)
	var priorWorkspace, priorDigest string
	err := tx.QueryRowContext(ctx, `SELECT workspace_id,metadata_sha256 FROM run_knowledge_base_sources WHERE run_id=? AND knowledge_base_id=?`, runID, item.ID).Scan(&priorWorkspace, &priorDigest)
	if err == nil {
		if priorWorkspace != workspaceID || priorDigest != digest {
			return domain.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := requireMetadataSourceCapacityTx(ctx, tx, runID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_knowledge_base_sources(run_id,workspace_id,knowledge_base_id,metadata_sha256) VALUES(?,?,?,?)`, runID, workspaceID, item.ID, digest)
	return err
}

// Library source rows intentionally survive library deletion. An absent or
// edited library must invalidate derived text instead of erasing its boundary.
// The same flattened ancestry covers local history and recalled conversations.
func runKnowledgeBaseSourcesValidTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, scope domain.ResourceScope) (bool, error) {
	filter, scopeArgs, err := knowledgeBaseResourceScopeFilter(scope)
	if err != nil {
		return false, err
	}
	query := `SELECT COUNT(*) FROM run_knowledge_base_sources e WHERE (e.run_id=? OR e.run_id IN (SELECT source_run_id FROM run_history_sources WHERE run_id=?) OR e.run_id IN (SELECT source_run_id FROM run_conversation_sources WHERE run_id=?)) AND (e.workspace_id<>? OR NOT EXISTS(SELECT 1 FROM knowledge_bases k WHERE k.id=e.knowledge_base_id AND k.workspace_id=e.workspace_id AND SHA2(CONCAT(SHA2(k.name,256),SHA2(k.description,256),IF(k.default_slot=1,'true','false')),256)=e.metadata_sha256` + filter + `))`
	args := append([]any{runID, runID, runID, workspaceID}, scopeArgs...)
	var invalid int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&invalid); err != nil {
		return false, err
	}
	return invalid == 0, nil
}

// Library metadata is visible for either a whole-library target or an explicit
// document target in that library. Empty whole-library targets remain visible.
func knowledgeBaseResourceScopeFilter(scope domain.ResourceScope) (string, []any, error) {
	if err := scope.Validate(); err != nil {
		return "", nil, err
	}
	if scope.All {
		return "", nil, nil
	}
	if len(scope.Targets) == 0 {
		return " AND 1=0", nil, nil
	}
	args := make([]any, 0, len(scope.Targets))
	for _, target := range scope.Targets {
		args = append(args, target.KnowledgeBaseID)
	}
	return ` AND k.id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + `)`, args, nil
}
