package store

import (
	"context"
	"database/sql"
	"errors"
	"magworkspace/internal/domain"
)

// ResolveRunCitationAliases never acquires evidence. It maps a cited marker from
// bound, completed history in this private session to the exact immutable passage
// already acquired by the active run. It does not make historical IDs valid IDs.
func (s *Store) ResolveRunCitationAliases(ctx context.Context, uid, wid, sid, rid string, ids []string) (map[string]string, error) {
	if len(ids) == 0 || len(ids) > maxRunEvidence {
		return nil, domain.ErrInvalid
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !validHexID(id) || seen[id] {
			return nil, domain.ErrInvalid
		}
		seen[id] = true
	}
	aliases := map[string]string{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		allowed, err := runSourcesValidTx(ctx, tx, wid, rid, scope)
		if err != nil {
			return err
		}
		if !allowed {
			return domain.ErrForbidden
		}
		for _, id := range ids {
			current, err := scanEvidence(tx.QueryRowContext(ctx, `SELECT `+evidenceColumns+` FROM run_evidence e
    JOIN run_evidence old ON old.id=? AND old.workspace_id=e.workspace_id
     AND old.document_id=e.document_id AND old.version_id=e.version_id
     AND old.index_id=e.index_id AND old.generation=e.generation AND old.chunk_id=e.chunk_id
     AND old.source_sha256=e.source_sha256 AND old.sha256=e.sha256
     AND old.start_byte=e.start_byte AND old.end_byte=e.end_byte
    JOIN run_history_sources h ON h.run_id=e.run_id AND h.source_run_id=old.run_id
    JOIN runs prior ON prior.id=old.run_id AND prior.session_id=? AND prior.status='completed'
    WHERE e.run_id=? AND e.workspace_id=? AND old.cited=TRUE`, id, sid, rid, wid))
			if errors.Is(err, domain.ErrNotFound) {
				return domain.ErrInvalid
			}
			if err != nil {
				return err
			}
			if err = evidenceSourceValidTx(ctx, tx, current); err != nil {
				return err
			}
			aliases[id] = current.ID
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return aliases, nil
}
