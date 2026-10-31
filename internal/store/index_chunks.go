package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const chunkColumns = `c.id,c.index_id,c.generation,c.workspace_id,c.knowledge_base_id,i.document_id,i.version_id,i.source_sha256,c.kind,c.parent_id,c.ordinal,c.start_byte,c.end_byte,c.content,c.sha256,c.context_header`

func chunkDest(chunk *domain.IndexChunk) []any {
	return []any{&chunk.ID, &chunk.IndexID, &chunk.Generation, &chunk.WorkspaceID, &chunk.KnowledgeBaseID, &chunk.DocumentID, &chunk.VersionID, &chunk.SourceSHA256, &chunk.Kind, &chunk.ParentID, &chunk.Ordinal, &chunk.StartByte, &chunk.EndByte, &chunk.Content, &chunk.SHA256, &chunk.ContextHeader}
}

func chunkKey(indexID, generation, id string) string { return indexID + ":" + generation + ":" + id }

func validStoredChunk(chunk domain.IndexChunk) bool {
	if !validHexID(chunk.WorkspaceID) || !validHexID(chunk.KnowledgeBaseID) || !validHexID(chunk.ID) || !validHexID(chunk.IndexID) || !validHexID(chunk.Generation) || !validHexID(chunk.DocumentID) || !validHexID(chunk.VersionID) || !validArtifactSHA(chunk.SourceSHA256) || !validArtifactSHA(chunk.SHA256) || chunk.Ordinal < 0 || chunk.Ordinal >= domain.MaxIndexChunks || chunk.StartByte < 0 || chunk.EndByte <= chunk.StartByte || chunk.EndByte > domain.MaxExtractedBytes || chunk.EndByte-chunk.StartByte != len(chunk.Content) || !utf8.ValidString(chunk.Content) || !utf8.ValidString(chunk.ContextHeader) || len(chunk.ContextHeader) > 8192 {
		return false
	}
	limit := domain.ChunkRunes
	switch chunk.Kind {
	case "parent":
		if chunk.ParentID != "" {
			return false
		}
		limit = domain.ParentChunkRunes
	case "child":
		if !validHexID(chunk.ParentID) || chunk.ParentID == chunk.ID {
			return false
		}
	default:
		return false
	}
	if utf8.RuneCountInString(chunk.Content) > limit {
		return false
	}
	sum := sha256.Sum256([]byte(chunk.Content))
	return hex.EncodeToString(sum[:]) == chunk.SHA256
}

func chunkBelongsToParent(child, parent domain.IndexChunk) bool {
	if child.Kind != "child" || parent.Kind != "parent" || child.ParentID != parent.ID || child.IndexID != parent.IndexID || child.Generation != parent.Generation || child.WorkspaceID != parent.WorkspaceID || child.KnowledgeBaseID != parent.KnowledgeBaseID || child.StartByte < parent.StartByte || child.EndByte > parent.EndByte {
		return false
	}
	return child.Content == parent.Content[child.StartByte-parent.StartByte:child.EndByte-parent.StartByte]
}

// validateIndexChunks checks the complete immutable tree before publication.
// Text is never normalized here: offsets and hashes describe exact UTF-8 bytes.
func validateIndexChunks(job domain.IndexJob, chunks []domain.IndexChunk) (int, int, error) {
	if !validProfileID(job.ProfileID) || !validHexID(job.Generation) || len(chunks) < 2 || len(chunks) > domain.MaxIndexChunks {
		return 0, 0, domain.ErrInvalid
	}
	parents := make(map[string]domain.IndexChunk)
	seen := make(map[string]bool, len(chunks))
	parentOrder, childOrder := []domain.IndexChunk{}, []domain.IndexChunk{}
	totalBytes := 0
	for _, chunk := range chunks {
		if !validStoredChunk(chunk) || seen[chunk.ID] || chunk.WorkspaceID != job.WorkspaceID || chunk.KnowledgeBaseID != job.KnowledgeBaseID || chunk.IndexID != job.ID || chunk.Generation != job.Generation || chunk.DocumentID != job.DocumentID || chunk.VersionID != job.VersionID || chunk.SourceSHA256 != job.SourceSHA256 {
			return 0, 0, domain.ErrInvalid
		}
		seen[chunk.ID] = true
		totalBytes += len(chunk.Content)
		if totalBytes > 4*domain.MaxExtractedBytes {
			return 0, 0, domain.ErrInvalid
		}
		if chunk.Kind == "parent" {
			parents[chunk.ID] = chunk
			parentOrder = append(parentOrder, chunk)
		} else {
			childOrder = append(childOrder, chunk)
		}
	}
	if len(parentOrder) == 0 || len(childOrder) == 0 {
		return 0, 0, domain.ErrInvalid
	}
	sort.Slice(parentOrder, func(i, j int) bool { return parentOrder[i].Ordinal < parentOrder[j].Ordinal })
	sort.Slice(childOrder, func(i, j int) bool { return childOrder[i].Ordinal < childOrder[j].Ordinal })
	previousEnd := 0
	for ordinal, parent := range parentOrder {
		if parent.Ordinal != ordinal || parent.StartByte != previousEnd {
			return 0, 0, domain.ErrInvalid
		}
		previousEnd = parent.EndByte
	}
	covered := make(map[string]int, len(parents))
	lastParent := -1
	lastStart := -1
	for ordinal, child := range childOrder {
		parent, ok := parents[child.ParentID]
		if !ok || child.Ordinal != ordinal || !chunkBelongsToParent(child, parent) || parent.Ordinal < lastParent {
			return 0, 0, domain.ErrInvalid
		}
		end, exists := covered[parent.ID]
		if (!exists && child.StartByte != parent.StartByte) || (exists && (child.StartByte > end || child.EndByte <= end || child.StartByte <= lastStart)) {
			return 0, 0, domain.ErrInvalid
		}
		covered[parent.ID] = child.EndByte
		lastParent, lastStart = parent.Ordinal, child.StartByte
	}
	for _, parent := range parentOrder {
		if covered[parent.ID] != parent.EndByte {
			return 0, 0, domain.ErrInvalid
		}
	}
	return len(parentOrder), len(childOrder), nil
}

func insertIndexChunksTx(ctx context.Context, tx *sql.Tx, chunks []domain.IndexChunk) error {
	// Bound each statement well below MySQL packet/placeholder limits. The
	// surrounding publication transaction owns all batches and the ready state.
	for start := 0; start < len(chunks); start += 128 {
		batch := chunks[start:min(start+128, len(chunks))]
		args := make([]any, 0, len(batch)*13)
		for _, c := range batch {
			args = append(args, c.IndexID, c.Generation, c.WorkspaceID, c.KnowledgeBaseID, c.ID, c.Kind, c.ParentID, c.Ordinal, c.StartByte, c.EndByte, c.Content, c.SHA256, c.ContextHeader)
		}
		query := `INSERT INTO chunks(index_id,generation,workspace_id,knowledge_base_id,id,kind,parent_id,ordinal,start_byte,end_byte,content,sha256,context_header) VALUES ` + strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func readChunksTx(ctx context.Context, tx *sql.Tx, clause string, args []any) ([]domain.IndexChunk, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+chunkColumns+` FROM chunks c JOIN document_indexes i ON i.id=c.index_id WHERE `+clause+` ORDER BY c.index_id,c.generation,FIELD(c.kind,'parent','child'),c.ordinal`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	chunks := []domain.IndexChunk{}
	for rows.Next() {
		var chunk domain.IndexChunk
		if err := rows.Scan(chunkDest(&chunk)...); err != nil {
			return nil, err
		}
		chunk.ParentOrdinal = -1
		if !validStoredChunk(chunk) {
			return nil, domain.ErrConflict
		}
		chunks = append(chunks, chunk)
	}
	return chunks, rows.Err()
}

// attachChunkParents validates each returned child against the actual same-
// generation parent. It never trusts the external index's copy of any text.
func attachChunkParents(ctx context.Context, tx *sql.Tx, chunks []domain.IndexChunk) (map[string]domain.IndexChunk, error) {
	parents := map[string]domain.IndexChunk{}
	clauses, args := []string{}, []any{}
	seen := map[string]bool{}
	for _, chunk := range chunks {
		if chunk.Kind == "parent" {
			parents[chunkKey(chunk.IndexID, chunk.Generation, chunk.ID)] = chunk
		}
		if chunk.Kind != "child" {
			continue
		}
		key := chunkKey(chunk.IndexID, chunk.Generation, chunk.ParentID)
		if _, present := parents[key]; present {
			continue
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		clauses = append(clauses, `(c.index_id=? AND c.generation=? AND c.id=?)`)
		args = append(args, chunk.IndexID, chunk.Generation, chunk.ParentID)
	}
	if len(clauses) > 0 {
		loaded, err := readChunksTx(ctx, tx, "("+strings.Join(clauses, " OR ")+")", args)
		if err != nil {
			return nil, err
		}
		for _, parent := range loaded {
			parents[chunkKey(parent.IndexID, parent.Generation, parent.ID)] = parent
		}
	}
	for i := range chunks {
		if chunks[i].Kind != "child" {
			continue
		}
		parent, ok := parents[chunkKey(chunks[i].IndexID, chunks[i].Generation, chunks[i].ParentID)]
		if !ok || !chunkBelongsToParent(chunks[i], parent) {
			return nil, domain.ErrConflict
		}
		chunks[i].ParentOrdinal = parent.Ordinal
	}
	return parents, nil
}

// ReadIndexChunks reads a fixed published version (including historical heads).
// A nil ID list returns the full tree; an explicit empty list returns no chunks.
func (s *Store) ReadIndexChunks(ctx context.Context, userID, workspaceID string, expected domain.IndexJob, ids []string) ([]domain.IndexChunk, error) {
	if len(ids) > domain.MaxIndexChunks || !validHexID(expected.ID) {
		return nil, domain.ErrInvalid
	}
	for _, id := range ids {
		if !validHexID(id) {
			return nil, domain.ErrInvalid
		}
	}
	result := []domain.IndexChunk{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, userID, workspaceID); err != nil {
			return err
		}
		if _, err := documentInWorkspace(ctx, tx, workspaceID, expected.DocumentID); err != nil {
			return err
		}
		job, err := scanIndex(tx.QueryRowContext(ctx, `SELECT `+indexColumns+` FROM document_indexes i WHERE i.id=? AND i.workspace_id=? AND i.document_id=? AND i.version_id=?`, expected.ID, workspaceID, expected.DocumentID, expected.VersionID))
		if err != nil {
			return err
		}
		if job.Status != "ready" || job.Generation != expected.Generation || job.SourceSHA256 != expected.SourceSHA256 || job.ProfileID != expected.ProfileID {
			return domain.ErrConflict
		}
		if ids != nil && len(ids) == 0 {
			return nil
		}
		clause, args := `c.index_id=? AND c.generation=?`, []any{job.ID, job.Generation}
		if ids != nil {
			clause += ` AND c.id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `)`
			for _, id := range ids {
				args = append(args, id)
			}
		}
		result, err = readChunksTx(ctx, tx, clause, args)
		if err != nil {
			return err
		}
		if ids == nil {
			parents, children, err := validateIndexChunks(job, result)
			if err != nil || parents != job.ParentCount || children != job.ChunkCount {
				return domain.ErrConflict
			}
		}
		_, err = attachChunkParents(ctx, tx, result)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ResolveIndexChunks batch-hydrates current authorized candidates. IDs and
// scores from the search engine are hints; all returned bodies come from MySQL.
func (s *Store) ResolveIndexChunks(ctx context.Context, userID, workspaceID, profileID string, candidates []domain.IndexCandidate, documentIDs ...[]string) ([]domain.SearchHit, error) {
	return s.ResolveIndexChunksInKnowledgeBase(ctx, userID, workspaceID, profileID, "", candidates, documentIDs...)
}

func (s *Store) ResolveIndexChunksInKnowledgeBase(ctx context.Context, userID, workspaceID, profileID, knowledgeBaseID string, candidates []domain.IndexCandidate, documentIDs ...[]string) ([]domain.SearchHit, error) {
	return s.ResolveIndexChunksInKnowledgeBases(ctx, userID, workspaceID, profileID, singleKnowledgeBase(knowledgeBaseID), candidates, documentIDs...)
}

func (s *Store) ResolveIndexChunksInKnowledgeBases(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, candidates []domain.IndexCandidate, documentIDs ...[]string) ([]domain.SearchHit, error) {
	return s.resolveIndexChunksScoped(ctx, userID, workspaceID, profileID, knowledgeBaseIDs, candidates, nil, documentIDs...)
}

func (s *Store) ResolveIndexChunksForResources(ctx context.Context, userID, workspaceID, profileID string, candidates []domain.IndexCandidate, scope domain.ResourceScope) ([]domain.SearchHit, error) {
	return s.resolveIndexChunksScoped(ctx, userID, workspaceID, profileID, nil, candidates, &scope)
}

func (s *Store) resolveIndexChunksScoped(ctx context.Context, userID, workspaceID, profileID string, knowledgeBaseIDs []string, candidates []domain.IndexCandidate, resources *domain.ResourceScope, documentIDs ...[]string) ([]domain.SearchHit, error) {
	if !validProfileID(profileID) || len(candidates) > 200 {
		return nil, domain.ErrInvalid
	}
	filter, scopeArgs, err := indexKnowledgeBasesScopeFilter(knowledgeBaseIDs, documentIDs)
	if resources != nil {
		filter, scopeArgs, err = resourceScopeFilter(*resources)
	}
	if err != nil {
		return nil, err
	}
	hits := []domain.SearchHit{}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
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
		clauses, args := []string{}, []any{workspaceID, profileID}
		valid := []domain.IndexCandidate{}
		seen := map[string]bool{}
		for _, candidate := range candidates {
			key := chunkKey(candidate.IndexID, candidate.Generation, candidate.ChunkID)
			if !validHexID(candidate.IndexID) || !validHexID(candidate.Generation) || !validHexID(candidate.ChunkID) || math.IsNaN(candidate.Score) || math.IsInf(candidate.Score, 0) || seen[key] {
				continue
			}
			seen[key] = true
			valid = append(valid, candidate)
			clauses = append(clauses, `(c.index_id=? AND c.generation=? AND c.id=?)`)
			args = append(args, candidate.IndexID, candidate.Generation, candidate.ChunkID)
		}
		if len(valid) == 0 {
			return nil
		}
		args = append(args, scopeArgs...)
		rows, err := tx.QueryContext(ctx, `SELECT `+chunkColumns+`,d.title FROM chunks c JOIN document_indexes i ON i.id=c.index_id AND i.generation=c.generation JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id AND d.current_version_id=i.version_id WHERE i.workspace_id=? AND i.profile_id=? AND i.storage_id<>'' AND i.status='ready' AND d.deleted_at IS NULL AND c.kind='child' AND (`+strings.Join(clauses, " OR ")+`)`+filter, args...)
		if err != nil {
			return err
		}
		byID := map[string]domain.SearchHit{}
		chunks := []domain.IndexChunk{}
		for rows.Next() {
			var hit domain.SearchHit
			if err = rows.Scan(append(chunkDest(&hit.Chunk), &hit.Title)...); err != nil {
				rows.Close()
				return err
			}
			if !validStoredChunk(hit.Chunk) {
				rows.Close()
				return domain.ErrConflict
			}
			byID[chunkKey(hit.Chunk.IndexID, hit.Chunk.Generation, hit.Chunk.ID)] = hit
			chunks = append(chunks, hit.Chunk)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		parents, err := attachChunkParents(ctx, tx, chunks)
		if err != nil {
			return err
		}
		for _, candidate := range valid {
			hit, ok := byID[chunkKey(candidate.IndexID, candidate.Generation, candidate.ChunkID)]
			if !ok {
				continue
			}
			parent := parents[chunkKey(hit.Chunk.IndexID, hit.Chunk.Generation, hit.Chunk.ParentID)]
			hit.Chunk.ParentOrdinal = parent.Ordinal
			hit.Parent = &parent
			hit.Score = candidate.Score
			hits = append(hits, hit)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return hits, nil
}
