package domain

import "time"

const (
	MaxExtractedBytes = 1 << 20
	MaxIndexChunks    = 2048
	ChunkRunes        = 800
	ChunkOverlapRunes = 100
)

// IndexProfile identifies every input that can affect derived text or vectors.
type IndexProfile struct {
	ID               string `json:"id"`
	Model            string `json:"model"`
	Dimensions       int    `json:"dimensions"`
	ChunkingStrategy string `json:"chunking_strategy,omitempty"`
}

type IndexProgress struct {
	Stage     string    `json:"stage"`
	Chunks    int       `json:"chunks,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type IndexJob struct {
	Progress       *IndexProgress `json:"progress,omitempty"`
	ID             string         `json:"id"`
	WorkspaceID    string         `json:"workspace_id"`
	DocumentID     string         `json:"document_id"`
	VersionID      string         `json:"version_id"`
	ProfileID      string         `json:"profile_id"`
	SourceSHA256   string         `json:"source_sha256"`
	Status         string         `json:"status"`
	ErrorCode      string         `json:"error_code,omitempty"`
	Attempts       int            `json:"attempts"`
	Generation     string         `json:"generation,omitempty"`
	CreatedBy      string         `json:"created_by"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	ArtifactKey    string         `json:"-"`
	ArtifactSHA256 string         `json:"-"`
	ChunkCount     int            `json:"chunk_count"`
}

type IndexChunk struct {
	ID           string `json:"id"`
	IndexID      string `json:"index_id"`
	DocumentID   string `json:"document_id"`
	VersionID    string `json:"version_id"`
	SourceSHA256 string `json:"source_sha256"`
	Ordinal      int    `json:"ordinal"`
	StartByte    int    `json:"start_byte"`
	EndByte      int    `json:"end_byte"`
	Content      string `json:"content"`
	SHA256       string `json:"sha256"`
}

type IndexInput struct {
	Job     IndexJob
	Version DocumentVersion
}

type IndexCandidate struct {
	ChunkID    string
	IndexID    string
	Generation string
	Score      float64
}

type IndexReference struct {
	Job   IndexJob
	Title string
}

type SearchHit struct {
	Chunk                    IndexChunk `json:"chunk"`
	Title                    string     `json:"title"`
	Score                    float64    `json:"score"`
	RetrievalScore           *float64   `json:"retrieval_score,omitempty"`
	RerankScore              *float64   `json:"rerank_score,omitempty"`
	NormalizedRetrievalScore *float64   `json:"normalized_retrieval_score,omitempty"`
}

type IndexCleanup struct {
	ID         string
	ProfileID  string
	Generation string
}
