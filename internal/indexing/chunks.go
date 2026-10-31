package indexing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"magworkspace/internal/domain"
)

func digestBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// Chunks provides the reading view: ordered immutable parents. Retrieval scores
// children separately, then returns these exact parent identities as evidence.
func (s *Service) Chunks(ctx context.Context, userID, workspaceID, documentID, versionID, indexID string) ([]domain.IndexChunk, error) {
	return s.ChunksByKind(ctx, userID, workspaceID, documentID, versionID, indexID, "parent")
}

func (s *Service) ChunksByKind(ctx context.Context, userID, workspaceID, documentID, versionID, indexID, kind string) ([]domain.IndexChunk, error) {
	if kind != "parent" && kind != "child" {
		return nil, domain.ErrInvalid
	}
	job, err := s.db.DocumentIndexByID(ctx, userID, workspaceID, documentID, versionID, indexID)
	if err != nil {
		return nil, err
	}
	chunks, err := s.db.ReadIndexChunks(ctx, userID, workspaceID, job, nil)
	if err != nil {
		return nil, err
	}
	result := make([]domain.IndexChunk, 0)
	for _, chunk := range chunks {
		if chunk.Kind == kind {
			result = append(result, chunk)
		}
	}
	return result, nil
}

func (s *Service) Chunk(ctx context.Context, userID, workspaceID, documentID, versionID, indexID, chunkID string) (domain.IndexChunk, error) {
	job, err := s.db.DocumentIndexByID(ctx, userID, workspaceID, documentID, versionID, indexID)
	if err != nil {
		return domain.IndexChunk{}, err
	}
	chunks, err := s.db.ReadIndexChunks(ctx, userID, workspaceID, job, []string{chunkID})
	if err != nil {
		return domain.IndexChunk{}, err
	}
	if len(chunks) != 1 {
		return domain.IndexChunk{}, domain.ErrNotFound
	}
	return chunks[0], nil
}

// Child scores determine order. Siblings share one parent evidence item, whose
// bytes, offsets and digest are never replaced with a synthetic concatenation.
func expandParentHits(hits []domain.SearchHit, limit int) []domain.SearchHit {
	result := make([]domain.SearchHit, 0, min(limit, len(hits)))
	seen := make(map[string]int)
	for _, hit := range hits {
		if hit.Parent == nil {
			continue
		}
		if i, ok := seen[hit.Parent.ID]; ok {
			result[i].MatchedChildIDs = append(result[i].MatchedChildIDs, hit.Chunk.ID)
			continue
		}
		if len(result) >= limit {
			continue
		}
		childID := hit.Chunk.ID
		hit.Focus = &domain.ChunkSpan{StartByte: hit.Chunk.StartByte, EndByte: hit.Chunk.EndByte}
		hit.Chunk, hit.Parent = *hit.Parent, nil
		hit.MatchedChildIDs = []string{childID}
		seen[hit.Chunk.ID] = len(result)
		result = append(result, hit)
	}
	return result
}
