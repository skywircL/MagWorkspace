package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"magworkspace/internal/domain"
)

const maxRunEvidence = 64
const maxRunHistorySources = 256

const evidenceColumns = `e.id,e.run_id,e.workspace_id,e.document_id,e.version_id,e.index_id,e.generation,e.chunk_id,e.source_sha256,e.sha256,e.title,e.start_byte,e.end_byte,e.cited,e.created_at`

func scanEvidence(row rowScanner) (domain.Evidence, error) {
	var e domain.Evidence
	err := row.Scan(&e.ID, &e.RunID, &e.WorkspaceID, &e.DocumentID, &e.VersionID, &e.IndexID, &e.Generation, &e.ChunkID, &e.SourceSHA256, &e.SHA256, &e.Title, &e.StartByte, &e.EndByte, &e.Cited, &e.CreatedAt)
	return e, dbError(err)
}

// authorizeEvidenceRunTx takes the same Workspace -> Session -> Run locks as
// finalization. Acquisition additionally requires a live, unexpired execution.
func authorizeEvidenceRunTx(ctx context.Context, tx *sql.Tx, userID, workspaceID, sessionID, runID string, active bool) error {
	if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
		return err
	}
	if _, err := privateSession(ctx, tx, userID, workspaceID, sessionID); err != nil {
		return err
	}
	var status string
	var unexpired bool
	if err := tx.QueryRowContext(ctx, `SELECT status,lease_expires_at>UTC_TIMESTAMP(6) FROM runs WHERE id=? AND session_id=? FOR UPDATE`, runID, sessionID).Scan(&status, &unexpired); err != nil {
		return err
	}
	if err := assertRunExecutionTx(ctx, tx, runID); err != nil {
		return err
	}
	if active && (status != "running" || !unexpired) {
		return domain.ErrConflict
	}
	return nil
}

// storeRunDocumentScopeTx is called while BeginRun holds the authorization
// locks. No row represents all documents; an explicit empty array means none.
func storeRunDocumentScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, documentIDs []string) error {
	if documentIDs == nil {
		return nil
	}
	if len(documentIDs) > 100 {
		return domain.ErrInvalid
	}
	ids := make([]string, 0, len(documentIDs))
	seen := make(map[string]bool, len(documentIDs))
	for _, id := range documentIDs {
		if !validHexID(id) || seen[id] {
			return domain.ErrInvalid
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, id); err != nil {
			return err
		}
		seen[id] = true
		ids = append(ids, id)
	}
	data, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_retrieval_scopes(run_id,document_ids) VALUES(?,?)`, runID, data)
	return err
}

func runDocumentScopeTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	var data []byte
	err := tx.QueryRowContext(ctx, `SELECT document_ids FROM run_retrieval_scopes WHERE run_id=?`, runID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := []string{}
	if json.Unmarshal(data, &ids) != nil || ids == nil || len(ids) > 100 {
		return nil, domain.ErrInvalid
	}
	for _, id := range ids {
		if !validHexID(id) {
			return nil, domain.ErrInvalid
		}
	}
	return ids, nil
}

func (s *Store) RunDocumentScope(ctx context.Context, userID, workspaceID, sessionID, runID string) ([]string, error) {
	var result []string
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		var err error
		result, err = runDocumentScopeTx(ctx, tx, runID)
		return err
	})
	return result, err
}

// runSourcesValidTx retains the legacy call shape for existing run lifecycle
// consumers. Without explicit dimensions the persisted canonical scope wins.
func runSourcesValidTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, documentIDs []string, knowledgeBaseSelections ...[]string) (bool, error) {
	if len(knowledgeBaseSelections) > 1 {
		return false, domain.ErrInvalid
	}
	var scope domain.ResourceScope
	var err error
	if len(knowledgeBaseSelections) == 1 {
		scope, err = legacyResourceScopeTx(ctx, tx, workspaceID, documentIDs, knowledgeBaseSelections[0])
	} else {
		scope, err = runResourceScopeTx(ctx, tx, workspaceID, runID)
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return runSourcesValidInScopeTx(ctx, tx, workspaceID, runID, scope)
}

// Sources include flattened history and recalled conversations. Checking their
// actual document/library relationship preserves each mixed target's boundary.
func runSourcesValidInScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, scope domain.ResourceScope) (bool, error) {
	if err := checkResourceScopeTx(ctx, tx, workspaceID, scope); err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) {
			return false, nil
		}
		return false, err
	}
	allowed, err := runConversationSourcesValidTx(ctx, tx, workspaceID, runID, nil)
	if err != nil || !allowed {
		return false, err
	}
	allowed, err = runKnowledgeBaseSourcesValidTx(ctx, tx, workspaceID, runID, scope)
	if err != nil || !allowed {
		return false, err
	}
	filter, scopeArgs, err := resourceScopeFilter(scope)
	if err != nil {
		return false, err
	}
	// Both metadata and acquired text constrain later history, including runs
	// recalled from another private conversation.
	const sources = `(e.run_id=? OR e.run_id IN (SELECT source_run_id FROM run_history_sources WHERE run_id=?) OR e.run_id IN (SELECT source_run_id FROM run_conversation_sources WHERE run_id=?))`
	args := append([]any{runID, runID, runID, workspaceID}, scopeArgs...)
	var invalid int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_document_sources e WHERE `+sources+` AND (e.workspace_id<>? OR NOT EXISTS(SELECT 1 FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.id=e.version_id AND v.sha256=e.source_sha256 WHERE d.id=e.document_id AND d.workspace_id=e.workspace_id AND d.deleted_at IS NULL`+filter+`))`, args...).Scan(&invalid); err != nil {
		return false, err
	}
	if invalid != 0 {
		return false, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence e WHERE `+sources+` AND (e.workspace_id<>? OR NOT EXISTS (SELECT 1 FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.id=e.version_id AND v.sha256=e.source_sha256 JOIN document_indexes i ON i.id=e.index_id AND i.document_id=d.id AND i.workspace_id=d.workspace_id AND i.version_id=v.id AND i.source_sha256=v.sha256 AND i.generation=e.generation AND i.status='ready' JOIN chunks c ON c.index_id=i.id AND c.generation=i.generation AND c.id=e.chunk_id AND c.sha256=e.sha256 AND c.start_byte=e.start_byte AND c.end_byte=e.end_byte WHERE d.id=e.document_id AND d.workspace_id=e.workspace_id AND d.deleted_at IS NULL`+filter+`))`, args...).Scan(&invalid); err != nil {
		return false, err
	}
	return invalid == 0, nil
}

// CanHydrateRunHistory checks acquired and inherited sources, not merely the
// sources the model chose to cite. A narrower scope or deleted source drops the
// whole source-derived turn, including tool-free paraphrases in later turns.
func (s *Store) CanHydrateRunHistory(ctx context.Context, userID, workspaceID, sessionID, runID string, documentIDs []string, knowledgeBaseSelections ...[]string) (bool, error) {
	if len(knowledgeBaseSelections) > 1 {
		return false, domain.ErrInvalid
	}
	// This API evaluates a previous run for a new request. An omitted selection
	// means that request has no extra KB restriction, not the previous run's.
	var knowledgeBaseIDs []string
	if len(knowledgeBaseSelections) == 1 {
		knowledgeBaseIDs = knowledgeBaseSelections[0]
	}
	allowed := false
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, false); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=? AND session_id=?`, runID, sessionID).Scan(&status); err != nil {
			return err
		}
		if status != "completed" {
			return domain.ErrInvalid
		}
		var err error
		allowed, err = runSourcesValidTx(ctx, tx, workspaceID, runID, documentIDs, knowledgeBaseIDs)
		return err
	})
	return allowed, err
}

// CanHydrateRunHistoryForRun evaluates a completed turn against the immutable
// resource selection of the new run, including mixed whole-library/doc targets.
func (s *Store) CanHydrateRunHistoryForRun(ctx context.Context, userID, workspaceID, sessionID, sourceRunID, targetRunID string) (bool, error) {
	allowed := false
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, targetRunID, true); err != nil {
			return err
		}
		// The target authorization already locks this private session. Match the
		// source within that session without applying the target generation to it.
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=? AND session_id=?`, sourceRunID, sessionID).Scan(&status); err != nil {
			return err
		}
		if status != "completed" {
			return domain.ErrInvalid
		}
		scope, err := runResourceScopeTx(ctx, tx, workspaceID, targetRunID)
		if err != nil {
			return err
		}
		allowed, err = runSourcesValidInScopeTx(ctx, tx, workspaceID, sourceRunID, scope)
		return err
	})
	return allowed, err
}

// BindRunHistory records only the source runs behind the actual model history,
// before any model call. Ancestors are flattened so source checks do not depend
// on a recursive-query depth or the original turn remaining in the window.
// Repeating a binding is idempotent; additional bindings only add constraints.
func (s *Store) BindRunHistory(ctx context.Context, userID, workspaceID, sessionID, runID string, historyRunIDs []string) error {
	if len(historyRunIDs) > 100 {
		return domain.ErrInvalid
	}
	for _, id := range historyRunIDs {
		if !validHexID(id) || id == runID {
			return domain.ErrInvalid
		}
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		scope, err := runResourceScopeTx(ctx, tx, workspaceID, runID)
		if err != nil {
			return err
		}
		sources := make(map[string]bool)
		collect := func(id string) error {
			rows, err := tx.QueryContext(ctx, `SELECT source_run_id FROM run_history_sources WHERE run_id=?`, id)
			if err != nil {
				return err
			}
			for rows.Next() {
				var source string
				if err = rows.Scan(&source); err != nil {
					rows.Close()
					return err
				}
				sources[source] = true
			}
			err = rows.Err()
			rows.Close()
			return err
		}
		if err = collect(runID); err != nil {
			return err
		}
		seen := make(map[string]bool, len(historyRunIDs))
		for _, id := range historyRunIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			var status string
			var acquired bool
			if err = tx.QueryRowContext(ctx, `SELECT r.status,(EXISTS(SELECT 1 FROM run_evidence e WHERE e.run_id=r.id) OR EXISTS(SELECT 1 FROM run_document_sources d WHERE d.run_id=r.id) OR EXISTS(SELECT 1 FROM run_knowledge_base_sources k WHERE k.run_id=r.id)) FROM runs r WHERE r.id=? AND r.session_id=? FOR UPDATE`, id, sessionID).Scan(&status, &acquired); err != nil {
				return err
			}
			if status != "completed" {
				return domain.ErrInvalid
			}
			if acquired {
				sources[id] = true
			}
			if err = collect(id); err != nil {
				return err
			}
			if len(sources) > maxRunHistorySources {
				return domain.ErrInvalid
			}
		}
		if len(sources) > maxRunHistorySources {
			return domain.ErrInvalid
		}
		for source := range sources {
			// Flattened rows remain metadata only and must themselves be completed
			// runs from this private session with actual acquired source evidence.
			var valid bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r WHERE r.id=? AND r.session_id=? AND r.status='completed' AND (EXISTS(SELECT 1 FROM run_evidence e WHERE e.run_id=r.id) OR EXISTS(SELECT 1 FROM run_document_sources d WHERE d.run_id=r.id) OR EXISTS(SELECT 1 FROM run_knowledge_base_sources k WHERE k.run_id=r.id)))`, source, sessionID).Scan(&valid); err != nil {
				return err
			}
			if !valid || source == runID {
				return domain.ErrInvalid
			}
			allowed, err := runSourcesValidInScopeTx(ctx, tx, workspaceID, source, scope)
			if err != nil {
				return err
			}
			if !allowed {
				return domain.ErrInvalid
			}
			if _, err = tx.ExecContext(ctx, `INSERT IGNORE INTO run_history_sources(run_id,source_run_id) VALUES(?,?)`, runID, source); err != nil {
				return err
			}
		}
		if err = bindConversationHistoryTx(ctx, tx, runID, historyRunIDs); err != nil {
			return err
		}
		allowed, err := runSourcesValidInScopeTx(ctx, tx, workspaceID, runID, scope)
		if err != nil {
			return err
		}
		if !allowed {
			return domain.ErrInvalid
		}
		return nil
	})
}

func validEvidenceChunk(chunk domain.IndexChunk) bool {
	return validStoredChunk(chunk)
}

func sameEvidenceChunk(e domain.Evidence, chunk domain.IndexChunk, generation string) bool {
	return e.IndexID == chunk.IndexID && e.ChunkID == chunk.ID && e.DocumentID == chunk.DocumentID && e.VersionID == chunk.VersionID && e.Generation == generation && e.SourceSHA256 == chunk.SourceSHA256 && e.SHA256 == chunk.SHA256 && e.StartByte == chunk.StartByte && e.EndByte == chunk.EndByte
}

// RecordRunEvidence binds only exact, published database chunks. Authorization,
// generation and immutable content are checked again at acquisition. Evidence
// stores a stable reference; chunk plaintext remains in the chunks table.
func (s *Store) RecordRunEvidence(ctx context.Context, userID, workspaceID, sessionID, runID string, hits []domain.SearchHit) ([]domain.Evidence, error) {
	return s.recordRunEvidence(ctx, userID, workspaceID, sessionID, runID, hits, false, nil)
}

// RecordRunDocumentEvidence also accepts an immutable index already pinned by
// this run, so reading adjacent passages cannot silently switch document heads.
func (s *Store) RecordRunDocumentEvidence(ctx context.Context, userID, workspaceID, sessionID, runID string, hits []domain.SearchHit) ([]domain.Evidence, error) {
	return s.recordRunEvidence(ctx, userID, workspaceID, sessionID, runID, hits, true, nil)
}

func (s *Store) recordRunEvidence(ctx context.Context, userID, workspaceID, sessionID, runID string, hits []domain.SearchHit, allowPinned bool, knowledgeBaseIDs []string) ([]domain.Evidence, error) {
	result := []domain.Evidence{}
	if len(hits) > maxRunEvidence || domain.ValidateKnowledgeBaseIDs(knowledgeBaseIDs) != nil {
		return result, domain.ErrInvalid
	}
	for _, hit := range hits {
		if !validEvidenceChunk(hit.Chunk) {
			return result, domain.ErrInvalid
		}
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, true); err != nil {
			return err
		}
		scope, err := resolveRunResourceScopeTx(ctx, tx, workspaceID, runID, knowledgeBaseIDs)
		if err != nil {
			return err
		}
		var count int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, runID).Scan(&count); err != nil {
			return err
		}
		seen := make(map[string]bool, len(hits))
		for _, hit := range hits {
			chunk := hit.Chunk
			var generation, title, knowledgeBaseID string
			var chunkCount, parentCount int
			err = tx.QueryRowContext(ctx, `SELECT i.generation,d.title,i.chunk_count,i.parent_count,d.knowledge_base_id FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id JOIN document_versions v ON v.id=i.version_id AND v.document_id=i.document_id AND v.sha256=i.source_sha256 WHERE i.id=? AND i.workspace_id=? AND i.document_id=? AND i.version_id=? AND i.source_sha256=? AND i.status='ready' AND d.deleted_at IS NULL AND (d.current_version_id=i.version_id OR (? AND (EXISTS(SELECT 1 FROM run_document_sources ds WHERE ds.run_id=? AND ds.document_id=d.id AND ds.version_id=i.version_id AND ds.source_sha256=i.source_sha256 AND ds.index_id=i.id AND ds.generation=i.generation) OR EXISTS(SELECT 1 FROM run_evidence re WHERE re.run_id=? AND re.document_id=d.id AND re.version_id=i.version_id AND re.source_sha256=i.source_sha256 AND re.index_id=i.id AND re.generation=i.generation))))`, chunk.IndexID, workspaceID, chunk.DocumentID, chunk.VersionID, chunk.SourceSHA256, allowPinned, runID, runID).Scan(&generation, &title, &chunkCount, &parentCount, &knowledgeBaseID)
			if err != nil {
				return err
			}
			if !scope.AllowsDocument(knowledgeBaseID, chunk.DocumentID) {
				return domain.ErrForbidden
			}
			if chunk.Kind == "parent" {
				chunkCount = parentCount
			}
			if !validHexID(generation) || chunk.Generation != generation || chunk.Ordinal >= chunkCount {
				return domain.ErrInvalid
			}
			// Source metadata and a plausible digest alone do not prove that a
			// passage belongs to this publication. Compare against the row that
			// was committed atomically with its ready generation.
			var stored domain.IndexChunk
			err = tx.QueryRowContext(ctx, `SELECT kind,parent_id,ordinal,start_byte,end_byte,content,sha256,context_header FROM chunks WHERE index_id=? AND generation=? AND id=?`, chunk.IndexID, generation, chunk.ID).Scan(&stored.Kind, &stored.ParentID, &stored.Ordinal, &stored.StartByte, &stored.EndByte, &stored.Content, &stored.SHA256, &stored.ContextHeader)
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrInvalid
			}
			if err != nil {
				return err
			}
			if stored.Kind != chunk.Kind || stored.ParentID != chunk.ParentID || stored.Ordinal != chunk.Ordinal || stored.StartByte != chunk.StartByte || stored.EndByte != chunk.EndByte || stored.Content != chunk.Content || stored.SHA256 != chunk.SHA256 || stored.ContextHeader != chunk.ContextHeader {
				return domain.ErrInvalid
			}
			e, err := scanEvidence(tx.QueryRowContext(ctx, `SELECT `+evidenceColumns+` FROM run_evidence e WHERE e.run_id=? AND e.index_id=? AND e.chunk_id=?`, runID, chunk.IndexID, chunk.ID))
			if errors.Is(err, domain.ErrNotFound) {
				if count >= maxRunEvidence {
					return domain.ErrInvalid
				}
				id, err := newID()
				if err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO run_evidence(id,run_id,workspace_id,document_id,version_id,index_id,generation,chunk_id,source_sha256,sha256,title,start_byte,end_byte) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, runID, workspaceID, chunk.DocumentID, chunk.VersionID, chunk.IndexID, generation, chunk.ID, chunk.SourceSHA256, chunk.SHA256, title, chunk.StartByte, chunk.EndByte); err != nil {
					return err
				}
				e, err = scanEvidence(tx.QueryRowContext(ctx, `SELECT `+evidenceColumns+` FROM run_evidence e WHERE e.id=?`, id))
				if err != nil {
					return err
				}
				count++
			} else if err != nil {
				return err
			} else if !sameEvidenceChunk(e, chunk, generation) {
				return domain.ErrInvalid
			}
			if !seen[e.ID] {
				result = append(result, e)
				seen[e.ID] = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// evidenceSourceValidTx deliberately permits an old document head: a citation
// resolves the exact version acquired by its run, while deletion or revocation
// still makes it inaccessible. Generation/source changes invalidate it.
func evidenceSourceValidTx(ctx context.Context, tx *sql.Tx, e domain.Evidence) error {
	if err := runAllowsDocumentTx(ctx, tx, e.WorkspaceID, e.RunID, e.DocumentID); err != nil {
		return dbError(err)
	}
	var valid bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.id=? AND v.sha256=? JOIN document_indexes i ON i.id=? AND i.document_id=d.id AND i.workspace_id=d.workspace_id AND i.version_id=v.id AND i.source_sha256=v.sha256 AND i.generation=? AND i.status='ready' JOIN chunks c ON c.index_id=i.id AND c.generation=i.generation AND c.id=? AND c.sha256=? AND c.start_byte=? AND c.end_byte=? WHERE d.id=? AND d.workspace_id=? AND d.deleted_at IS NULL)`, e.VersionID, e.SourceSHA256, e.IndexID, e.Generation, e.ChunkID, e.SHA256, e.StartByte, e.EndByte, e.DocumentID, e.WorkspaceID).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return domain.ErrNotFound
	}
	return nil
}

func evidenceByIDTx(ctx context.Context, tx *sql.Tx, workspaceID, runID, evidenceID string) (domain.Evidence, error) {
	e, err := scanEvidence(tx.QueryRowContext(ctx, `SELECT `+evidenceColumns+` FROM run_evidence e WHERE e.id=? AND e.run_id=? AND e.workspace_id=?`, evidenceID, runID, workspaceID))
	if err == nil {
		err = evidenceSourceValidTx(ctx, tx, e)
	}
	return e, err
}

func (s *Store) RunEvidence(ctx context.Context, userID, workspaceID, sessionID, runID, evidenceID string) (domain.Evidence, error) {
	var result domain.Evidence
	if !validHexID(evidenceID) {
		return result, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, false); err != nil {
			return err
		}
		var err error
		result, err = evidenceByIDTx(ctx, tx, workspaceID, runID, evidenceID)
		return err
	})
	return result, err
}

func (s *Store) EvidenceForRun(ctx context.Context, userID, workspaceID, sessionID, runID string, citedOnly bool) ([]domain.Evidence, error) {
	result := []domain.Evidence{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, userID, workspaceID, sessionID, runID, false); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT `+evidenceColumns+` FROM run_evidence e WHERE e.run_id=? AND e.workspace_id=? AND (?=FALSE OR e.cited=TRUE) ORDER BY e.created_at,e.id`, runID, workspaceID, citedOnly)
		if err != nil {
			return err
		}
		all := []domain.Evidence{}
		for rows.Next() {
			e, err := scanEvidence(rows)
			if err != nil {
				rows.Close()
				return err
			}
			all = append(all, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, e := range all {
			if err = evidenceSourceValidTx(ctx, tx, e); errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden) {
				continue
			} else if err != nil {
				return err
			}
			result = append(result, e)
		}
		return nil
	})
	return result, err
}

func citeEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, runID string, ids []string) error {
	for _, id := range ids {
		if _, err := evidenceByIDTx(ctx, tx, workspaceID, runID, id); errors.Is(err, domain.ErrNotFound) {
			return domain.ErrInvalid
		} else if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_evidence SET cited=TRUE WHERE id=? AND run_id=? AND workspace_id=?`, id, runID, workspaceID); err != nil {
			return err
		}
	}
	return nil
}

// Bind evidence to the selected libraries in the final authorization transaction,
// even when search produced no hits. Tool selections only narrow the Run scope.
func (s *Store) RecordRunKnowledgeEvidence(ctx context.Context, userID, workspaceID, sessionID, runID string, hits []domain.SearchHit, knowledgeBaseIDs []string) ([]domain.Evidence, error) {
	return s.recordRunEvidence(ctx, userID, workspaceID, sessionID, runID, hits, false, knowledgeBaseIDs)
}
