package indexing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"magworkspace/internal/domain"
)

const maxManifestBytes = 8 << 20

type manifestChunk struct {
	ID        string `json:"id"`
	Ordinal   int    `json:"ordinal"`
	StartByte int    `json:"start_byte"`
	EndByte   int    `json:"end_byte"`
	SHA256    string `json:"sha256"`
}
type manifest struct {
	Format       int             `json:"format"`
	IndexID      string          `json:"index_id"`
	Generation   string          `json:"generation"`
	DocumentID   string          `json:"document_id"`
	VersionID    string          `json:"version_id"`
	SourceSHA256 string          `json:"source_sha256"`
	Text         string          `json:"text"`
	Chunks       []manifestChunk `json:"chunks"`
}

func artifactKey(profile, generation string) string {
	return "indexes/" + profile + "/" + generation + "/manifest.json"
}
func digestBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func encodeManifest(job domain.IndexJob, text string, chunks []domain.IndexChunk) ([]byte, error) {
	m := manifest{Format: 1, IndexID: job.ID, Generation: job.Generation, DocumentID: job.DocumentID, VersionID: job.VersionID, SourceSHA256: job.SourceSHA256, Text: text}
	for _, c := range chunks {
		m.Chunks = append(m.Chunks, manifestChunk{c.ID, c.Ordinal, c.StartByte, c.EndByte, c.SHA256})
	}
	data, err := json.Marshal(m)
	if err != nil || len(data) > maxManifestBytes {
		return nil, &Failure{Code: "artifact_invalid"}
	}
	return data, nil
}

func (s *Service) loadManifest(ctx context.Context, job domain.IndexJob) ([]domain.IndexChunk, error) {
	if job.Status != "ready" {
		return nil, domain.ErrConflict
	}
	if job.ArtifactKey != artifactKey(job.ProfileID, job.Generation) {
		return nil, &Failure{Code: "artifact_integrity_failed"}
	}
	data, err := s.objects.Read(ctx, job.ArtifactKey, maxManifestBytes)
	if err != nil {
		return nil, &Failure{Code: "artifact_unavailable"}
	}
	if digestBytes(data) != job.ArtifactSHA256 {
		return nil, &Failure{Code: "artifact_integrity_failed"}
	}
	var m manifest
	if json.Unmarshal(data, &m) != nil || m.Format != 1 || m.IndexID != job.ID || m.Generation != job.Generation || m.DocumentID != job.DocumentID || m.VersionID != job.VersionID || m.SourceSHA256 != job.SourceSHA256 || len(m.Chunks) != job.ChunkCount {
		return nil, &Failure{Code: "artifact_integrity_failed"}
	}
	chunks, err := Split(m.Text)
	if err != nil || len(chunks) != len(m.Chunks) {
		return nil, &Failure{Code: "artifact_integrity_failed"}
	}
	seen := make(map[string]bool, len(chunks))
	for i := range chunks {
		c := m.Chunks[i]
		if len(c.ID) != 32 || seen[c.ID] || c.Ordinal != chunks[i].Ordinal || c.StartByte != chunks[i].StartByte || c.EndByte != chunks[i].EndByte || c.SHA256 != chunks[i].SHA256 {
			return nil, &Failure{Code: "artifact_integrity_failed"}
		}
		if _, err = hex.DecodeString(c.ID); err != nil {
			return nil, &Failure{Code: "artifact_integrity_failed"}
		}
		seen[c.ID] = true
		chunks[i].ID = c.ID
		chunks[i].IndexID = job.ID
		chunks[i].DocumentID = job.DocumentID
		chunks[i].VersionID = job.VersionID
		chunks[i].SourceSHA256 = job.SourceSHA256
	}
	return chunks, nil
}

// Chunks reads a fixed published index, including an older immutable version.
func (s *Service) Chunks(ctx context.Context, userID, workspaceID, documentID, versionID, indexID string) ([]domain.IndexChunk, error) {
	job, err := s.db.DocumentIndexByID(ctx, userID, workspaceID, documentID, versionID, indexID)
	if err != nil {
		return nil, err
	}
	chunks, err := s.loadManifest(ctx, job)
	if err != nil {
		return nil, err
	}
	current, err := s.db.DocumentIndexByID(ctx, userID, workspaceID, documentID, versionID, indexID)
	if err != nil {
		return nil, err
	}
	if current.Status != "ready" || current.Generation != job.Generation || current.ArtifactSHA256 != job.ArtifactSHA256 {
		return nil, domain.ErrConflict
	}
	return chunks, nil
}
