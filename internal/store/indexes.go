package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"

	"magworkspace/internal/domain"
)

const indexColumns = `i.id,i.workspace_id,i.knowledge_base_id,i.document_id,i.version_id,i.profile_id,i.storage_id,i.source_sha256,i.status,i.error_code,i.attempts,i.generation,i.created_by,i.created_at,i.updated_at,i.finished_at,i.chunk_count,i.parent_count`

func validProfileID(id string) bool {
	if len(id) != 32 && len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func validHexID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'f') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func validIndexProfile(profile domain.IndexProfile) bool {
	return validProfileID(profile.ID) && validArtifactSHA(profile.StorageID) && validDocumentText(profile.Model) && profile.Dimensions > 0 && profile.Dimensions <= 65536
}

func indexDest(job *domain.IndexJob) []any {
	return []any{&job.ID, &job.WorkspaceID, &job.KnowledgeBaseID, &job.DocumentID, &job.VersionID, &job.ProfileID, &job.StorageID, &job.SourceSHA256, &job.Status, &job.ErrorCode, &job.Attempts, &job.Generation, &job.CreatedBy, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt, &job.ChunkCount, &job.ParentCount}
}

func scanIndex(row rowScanner) (domain.IndexJob, error) {
	var job domain.IndexJob
	err := row.Scan(indexDest(&job)...)
	return job, dbError(err)
}

// enqueueIndexTx requires the workspace/document authorization locks. It is
// also used by upload, making source-version creation and admission atomic.
func enqueueIndexTx(ctx context.Context, tx *sql.Tx, userID, workspaceID, documentID, versionID string, profile domain.IndexProfile) (domain.IndexJob, bool, error) {
	var source, knowledgeBaseID string
	if err := tx.QueryRowContext(ctx, `SELECT v.sha256,d.knowledge_base_id FROM document_versions v JOIN documents d ON d.id=v.document_id WHERE v.id=? AND v.document_id=? AND d.workspace_id=?`, versionID, documentID, workspaceID).Scan(&source, &knowledgeBaseID); err != nil {
		return domain.IndexJob{}, false, err
	}
	job, err := scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.version_id=? AND i.profile_id=? AND i.document_id=? AND i.workspace_id=? FOR UPDATE`, versionID, profile.ID, documentID, workspaceID))
	if err == nil {
		var model string
		var dimensions int
		if err = tx.QueryRowContext(ctx, `SELECT embedding_model,dimensions FROM document_indexes WHERE id=?`, job.ID).Scan(&model, &dimensions); err != nil {
			return job, false, err
		}
		if model != profile.Model || dimensions != profile.Dimensions || job.SourceSHA256 != source || job.StorageID != profile.StorageID {
			return job, false, domain.ErrConflict
		}
		if job.Status != "failed" {
			return job, false, nil
		}
		if err = queueIndexCleanupTx(ctx, tx, job.StorageID, job.ProfileID, job.Generation); err != nil {
			return job, false, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE document_indexes SET status='queued',error_code='',generation='',lease_expires_at=NULL,chunk_count=0,parent_count=0,created_by=?,finished_at=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, userID, job.ID)
	} else if errors.Is(err, domain.ErrNotFound) {
		job.ID, err = newID()
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO document_indexes(id,workspace_id,knowledge_base_id,document_id,version_id,profile_id,storage_id,embedding_model,dimensions,source_sha256,created_by) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, job.ID, workspaceID, knowledgeBaseID, documentID, versionID, profile.ID, profile.StorageID, profile.Model, profile.Dimensions, source, userID)
		}
	}
	if err != nil {
		return domain.IndexJob{}, false, err
	}
	if err = resetIndexDeliveryTx(ctx, tx, job.ID); err != nil {
		return domain.IndexJob{}, false, err
	}
	job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=?`, job.ID))
	return job, true, err
}

func (s *Store) EnqueueDocumentIndex(ctx context.Context, userID, workspaceID, documentID, versionID string, profile domain.IndexProfile) (domain.IndexJob, bool, error) {
	var job domain.IndexJob
	var queued bool
	if !validIndexProfile(profile) {
		return job, false, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		role, err := lockWorkspace(ctx, tx, userID, workspaceID)
		if err != nil {
			return err
		}
		if _, err = documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		if !canWriteDocument(role) {
			return domain.ErrForbidden
		}
		job, queued, err = enqueueIndexTx(ctx, tx, userID, workspaceID, documentID, versionID, profile)
		return err
	})
	return job, queued, err
}

func (s *Store) DocumentIndex(ctx context.Context, userID, workspaceID, documentID, versionID, profileID string) (domain.IndexJob, error) {
	var job domain.IndexJob
	if !validProfileID(profileID) {
		return job, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		var err error
		job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.workspace_id=? AND i.document_id=? AND i.version_id=? AND i.profile_id=?`, workspaceID, documentID, versionID, profileID))
		return err
	})
	return job, err
}

// lockIndex allows a deleted document or revoked creator to be inspected by a
// worker, so it can fail the job and schedule cleanup instead of publishing it.
func lockIndex(ctx context.Context, tx *sql.Tx, expected domain.IndexJob) (domain.IndexJob, bool, bool, error) {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE id=? FOR UPDATE`, expected.WorkspaceID).Scan(&id); err != nil {
		return domain.IndexJob{}, false, false, err
	}
	var deleted bool
	if err := tx.QueryRowContext(ctx, `SELECT deleted_at IS NOT NULL FROM documents WHERE id=? AND workspace_id=? FOR UPDATE`, expected.DocumentID, expected.WorkspaceID).Scan(&deleted); err != nil {
		return domain.IndexJob{}, false, false, err
	}
	var job domain.IndexJob
	var active bool
	dest := append(indexDest(&job), &active)
	err := tx.QueryRowContext(ctx, `SELECT `+indexColumns+`,COALESCE(i.lease_expires_at>UTC_TIMESTAMP(6),FALSE) FROM document_indexes i WHERE i.id=? AND i.workspace_id=? AND i.document_id=? AND i.version_id=? AND i.profile_id=? FOR UPDATE`, expected.ID, expected.WorkspaceID, expected.DocumentID, expected.VersionID, expected.ProfileID).Scan(dest...)
	return job, deleted, active, err
}

func currentIndexWriter(ctx context.Context, tx *sql.Tx, job domain.IndexJob) (bool, error) {
	var role string
	err := tx.QueryRowContext(ctx, `SELECT role FROM workspace_members WHERE workspace_id=? AND user_id=?`, job.WorkspaceID, job.CreatedBy).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return canWriteDocument(role), err
}

func failIndexTx(ctx context.Context, tx *sql.Tx, job domain.IndexJob, code string) error {
	if err := queueIndexCleanupTx(ctx, tx, job.StorageID, job.ProfileID, job.Generation); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE document_indexes SET status='failed',error_code=?,lease_expires_at=NULL,finished_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=?`, code, job.ID)
	if err != nil {
		return err
	}
	return deleteIndexDeliveryTx(ctx, tx, job.ID)
}

func (s *Store) ClaimIndexJob(ctx context.Context, profileID string, lease time.Duration) (*domain.IndexInput, error) {
	if !validProfileID(profileID) || lease.Microseconds() <= 0 {
		return nil, domain.ErrInvalid
	}
	candidates, err := s.PendingIndexJobs(ctx, profileID)
	if err != nil {
		return nil, err
	}
	return s.claimIndexCandidates(ctx, candidates, lease, "")
}

// PendingIndexJobs is a read of durable transactional intents, not a claim.
func (s *Store) PendingIndexJobs(ctx context.Context, profileID string) ([]domain.IndexJob, error) {
	// Candidates are intentionally read without row locks: every claim then takes
	// Workspace -> Document -> Job, matching upload, membership and deletion.
	rows, err := s.pool.QueryContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i JOIN index_outbox o ON o.job_id=i.id WHERE i.profile_id=? AND ((i.status='queued' AND o.next_attempt_at<=UTC_TIMESTAMP(6)) OR (i.status='running' AND i.lease_expires_at<=UTC_TIMESTAMP(6))) ORDER BY i.created_at,i.id LIMIT 256`, profileID)
	if err != nil {
		return nil, dbError(err)
	}
	var candidates []domain.IndexJob
	for rows.Next() {
		job, err := scanIndex(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, job)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError(err)
	}
	return candidates, nil
}

func (s *Store) ClaimIndexJobByID(ctx context.Context, profileID, id string, lease time.Duration) (*domain.IndexInput, error) {
	if !validProfileID(profileID) || lease <= 0 {
		return nil, domain.ErrInvalid
	}
	job, err := scanIndex(s.pool.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=? AND i.profile_id=?`, id, profileID))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.claimIndexCandidates(ctx, []domain.IndexJob{job}, lease, "")
}
func (s *Store) claimIndexCandidates(ctx context.Context, candidates []domain.IndexJob, lease time.Duration, deliveryID string) (*domain.IndexInput, error) {
	var err error
	for _, candidate := range candidates {
		var input *domain.IndexInput
		err = s.transaction(ctx, func(tx *sql.Tx) error {
			job, deleted, active, err := lockIndex(ctx, tx, candidate)
			if err != nil {
				return err
			}
			if job.Status != "queued" && (job.Status != "running" || active) {
				return nil
			}
			if job.StorageID == "" || (s.indexProfile != nil && (job.ProfileID != s.indexProfile.ID || job.StorageID != s.indexProfile.StorageID)) {
				return nil
			}
			var currentDelivery string
			var due bool
			var recoveries int
			if err = tx.QueryRowContext(ctx, `SELECT id,next_attempt_at<=UTC_TIMESTAMP(6),lease_recoveries FROM index_outbox WHERE job_id=? FOR UPDATE`, job.ID).Scan(&currentDelivery, &due, &recoveries); errors.Is(err, sql.ErrNoRows) {
				return nil
			} else if err != nil {
				return err
			}
			if (deliveryID != "" && currentDelivery != deliveryID) || (job.Status == "queued" && !due) {
				return nil
			}
			if deleted {
				return failIndexTx(ctx, tx, job, "document_deleted")
			}
			allowed, err := currentIndexWriter(ctx, tx, job)
			if err != nil {
				return err
			}
			if !allowed {
				return failIndexTx(ctx, tx, job, "permission_revoked")
			}
			if job.Attempts == math.MaxInt32 {
				return failIndexTx(ctx, tx, job, "attempt_limit")
			}
			if job.Status == "running" {
				if recoveries >= maxIndexLeaseRecoveries {
					return failIndexTx(ctx, tx, job, "lease_recovery_exhausted")
				}
				if _, err = tx.ExecContext(ctx, `UPDATE index_outbox SET lease_recoveries=lease_recoveries+1 WHERE job_id=?`, job.ID); err != nil {
					return err
				}
			}
			if err = queueIndexCleanupTx(ctx, tx, job.StorageID, job.ProfileID, job.Generation); err != nil {
				return err
			}
			generation, err := newID()
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE document_indexes SET status='running',generation=?,attempts=attempts+1,error_code='',finished_at=NULL,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND),updated_at=UTC_TIMESTAMP(6) WHERE id=?`, generation, lease.Microseconds(), job.ID); err != nil {
				return err
			}
			claimed := &domain.IndexInput{}
			claimed.Job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=?`, job.ID))
			if err != nil {
				return err
			}
			v := &claimed.Version
			err = tx.QueryRowContext(ctx, `SELECT `+documentVersionColumns+` FROM document_versions WHERE id=? AND document_id=?`, job.VersionID, job.DocumentID).Scan(documentVersionDest(v)...)
			if err == nil && v.SHA256 != job.SourceSHA256 {
				return domain.ErrConflict
			}
			if err == nil {
				input = claimed
			}
			return err
		})
		if errors.Is(err, domain.ErrNotFound) {
			continue // A workspace may have been deleted after candidate selection.
		}
		if err != nil || input != nil {
			return input, err
		}
	}
	return nil, nil
}

func validArtifactSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'f') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// FinishDocumentIndex publishes trusted chunk bodies and the ready state in one
// transaction, after the worker has persisted and verified the search engine.
func (s *Store) FinishDocumentIndex(ctx context.Context, expected domain.IndexJob, chunks []domain.IndexChunk) error {
	if !validArtifactSHA(expected.StorageID) {
		return domain.ErrInvalid
	}
	parents, children, err := validateIndexChunks(expected, chunks)
	if err != nil {
		return err
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		job, deleted, active, err := lockIndex(ctx, tx, expected)
		if err != nil {
			return err
		}
		if deleted {
			return domain.ErrNotFound
		}
		if job.Status != "running" || job.Generation != expected.Generation || !active || job.SourceSHA256 != expected.SourceSHA256 || job.KnowledgeBaseID != expected.KnowledgeBaseID || job.StorageID != expected.StorageID {
			return domain.ErrConflict
		}
		allowed, err := currentIndexWriter(ctx, tx, job)
		if err != nil {
			return err
		}
		if !allowed {
			return domain.ErrForbidden
		}
		var source string
		if err = tx.QueryRowContext(ctx, `SELECT sha256 FROM document_versions WHERE id=? AND document_id=?`, job.VersionID, job.DocumentID).Scan(&source); err != nil {
			return err
		}
		if source != job.SourceSHA256 {
			return domain.ErrConflict
		}
		if err = insertIndexChunksTx(ctx, tx, chunks); err != nil {
			return err
		}
		// Recheck the lease after the bounded inserts: expiry while persisting
		// must roll back the chunk rows as well as publication.
		result, err := tx.ExecContext(ctx, `UPDATE document_indexes SET status='ready',chunk_count=?,parent_count=?,lease_expires_at=NULL,error_code='',finished_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=? AND generation=? AND status='running' AND lease_expires_at>UTC_TIMESTAMP(6)`, children, parents, job.ID, job.Generation)
		if err != nil {
			return err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if updated != 1 {
			return domain.ErrConflict
		}
		return deleteIndexDeliveryTx(ctx, tx, job.ID)
	})
}

func (s *Store) FailDocumentIndex(ctx context.Context, expected domain.IndexJob, code string) error {
	if !validHexID(expected.Generation) || !validIndexFailureCode(code) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		job, _, _, err := lockIndex(ctx, tx, expected)
		if err != nil {
			return err
		}
		if job.Status != "running" || job.Generation != expected.Generation {
			return domain.ErrConflict
		}
		return failIndexTx(ctx, tx, job, code)
	})
}

func (s *Store) DocumentIndexByID(ctx context.Context, userID, workspaceID, documentID, versionID, indexID string) (domain.IndexJob, error) {
	var job domain.IndexJob
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, documentID); err != nil {
			return err
		}
		var err error
		job, err = scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=? AND i.workspace_id=? AND i.document_id=? AND i.version_id=?`, indexID, workspaceID, documentID, versionID))
		return err
	})
	return job, err
}

func (s *Store) SearchIndexScope(ctx context.Context, userID, workspaceID, profileID string, documentIDs ...[]string) ([]string, error) {
	return s.SearchIndexScopeInKnowledgeBase(ctx, userID, workspaceID, profileID, "", documentIDs...)
}

func (s *Store) SearchIndexScopeInKnowledgeBase(ctx context.Context, userID, workspaceID, profileID, knowledgeBaseID string, documentIDs ...[]string) ([]string, error) {
	return s.SearchIndexScopeInKnowledgeBases(ctx, userID, workspaceID, profileID, singleKnowledgeBase(knowledgeBaseID), documentIDs...)
}

func (s *Store) SearchIndexScopeInKnowledgeBases(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, documentIDs ...[]string) ([]string, error) {
	return s.searchIndexScopeScoped(ctx, userID, workspaceID, profileID, knowledgeBaseIDs, nil, documentIDs...)
}

func (s *Store) SearchIndexScopeForResources(ctx context.Context, userID, workspaceID, profileID string, scope domain.ResourceScope) ([]string, error) {
	return s.searchIndexScopeScoped(ctx, userID, workspaceID, profileID, nil, &scope)
}

func (s *Store) searchIndexScopeScoped(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, resources *domain.ResourceScope, documentIDs ...[]string) ([]string, error) {
	result := []string{}
	if !validProfileID(profileID) {
		return result, domain.ErrInvalid
	}
	filter, scopeArgs, scopeErr := indexKnowledgeBasesScopeFilter(knowledgeBaseIDs, documentIDs)
	if resources != nil {
		filter, scopeArgs, scopeErr = resourceScopeFilter(*resources)
	}
	if scopeErr != nil {
		return result, scopeErr
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if resources != nil {
			if err := checkResourceScopeTx(ctx, tx, workspaceID, *resources); err != nil {
				return err
			}
		} else if err := checkIndexKnowledgeBases(ctx, tx, workspaceID, knowledgeBaseIDs); err != nil {
			return err
		}
		args := append([]any{workspaceID, profileID}, scopeArgs...)
		rows, err := tx.QueryContext(ctx, `SELECT i.generation FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id AND d.current_version_id=i.version_id WHERE i.workspace_id=? AND i.profile_id=? AND i.storage_id<>'' AND i.status='ready' AND d.deleted_at IS NULL`+filter+` ORDER BY i.id LIMIT 10001`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var generation string
			if err = rows.Scan(&generation); err != nil {
				return err
			}
			result = append(result, generation)
		}
		if len(result) > 10000 {
			return domain.ErrConflict
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ResolveIndexes(ctx context.Context, userID, workspaceID, profileID string, ids []string, documentIDs ...[]string) ([]domain.IndexReference, error) {
	return s.ResolveIndexesInKnowledgeBase(ctx, userID, workspaceID, profileID, "", ids, documentIDs...)
}

func (s *Store) ResolveIndexesInKnowledgeBase(ctx context.Context, userID, workspaceID, profileID, knowledgeBaseID string, ids []string, documentIDs ...[]string) ([]domain.IndexReference, error) {
	return s.ResolveIndexesInKnowledgeBases(ctx, userID, workspaceID, profileID, singleKnowledgeBase(knowledgeBaseID), ids, documentIDs...)
}

func (s *Store) ResolveIndexesInKnowledgeBases(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, ids []string, documentIDs ...[]string) ([]domain.IndexReference, error) {
	return s.resolveIndexesScoped(ctx, userID, workspaceID, profileID, knowledgeBaseIDs, ids, nil, documentIDs...)
}

func (s *Store) ResolveIndexesForResources(ctx context.Context, userID, workspaceID, profileID string, ids []string, scope domain.ResourceScope) ([]domain.IndexReference, error) {
	return s.resolveIndexesScoped(ctx, userID, workspaceID, profileID, nil, ids, &scope)
}

func (s *Store) resolveIndexesScoped(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, ids []string, resources *domain.ResourceScope, documentIDs ...[]string) ([]domain.IndexReference, error) {
	result := []domain.IndexReference{}
	if !validProfileID(profileID) || len(ids) > 200 {
		return result, domain.ErrInvalid
	}
	filter, scopeArgs, scopeErr := indexKnowledgeBasesScopeFilter(knowledgeBaseIDs, documentIDs)
	if resources != nil {
		filter, scopeArgs, scopeErr = resourceScopeFilter(*resources)
	}
	if scopeErr != nil {
		return result, scopeErr
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if resources != nil {
			if err := checkResourceScopeTx(ctx, tx, workspaceID, *resources); err != nil {
				return err
			}
		} else if err := checkIndexKnowledgeBases(ctx, tx, workspaceID, knowledgeBaseIDs); err != nil {
			return err
		}
		// Treat external IDs only as hints. Invalid, duplicate, foreign and stale
		// IDs do not grant access or cause a response containing external payloads.
		valid := []string{}
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if validHexID(id) && !seen[id] {
				valid = append(valid, id)
				seen[id] = true
			}
		}
		if len(valid) == 0 {
			return nil
		}
		args := []any{workspaceID, profileID}
		for _, id := range valid {
			args = append(args, id)
		}
		args = append(args, scopeArgs...)
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(valid)), ",")
		rows, err := tx.QueryContext(ctx, `SELECT `+indexColumns+`,d.title FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id AND d.current_version_id=i.version_id WHERE i.workspace_id=? AND i.profile_id=? AND i.storage_id<>'' AND i.status='ready' AND d.deleted_at IS NULL AND i.id IN (`+placeholders+`)`+filter, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		byID := make(map[string]domain.IndexReference, len(valid))
		for rows.Next() {
			var hit domain.IndexReference
			if err = rows.Scan(append(indexDest(&hit.Job), &hit.Title)...); err != nil {
				return err
			}
			byID[hit.Job.ID] = hit
		}
		for _, id := range valid {
			if hit, ok := byID[id]; ok {
				result = append(result, hit)
			}
		}
		return rows.Err()
	})
	return result, err
}

// nil means the authorized workspace; an explicit empty selection means none.
// Apply the same boundary before external search and before chunk hydration.
func documentScopeFilter(selections [][]string) (string, []any, error) {
	if len(selections) > 1 {
		return "", nil, domain.ErrInvalid
	}
	if len(selections) == 0 || selections[0] == nil {
		return "", nil, nil
	}
	ids := selections[0]
	if len(ids) > 100 {
		return "", nil, domain.ErrInvalid
	}
	if len(ids) == 0 {
		return " AND 1=0", nil, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		if !validHexID(id) {
			return "", nil, domain.ErrInvalid
		}
		args = append(args, id)
	}
	return " AND d.id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")", args, nil
}
