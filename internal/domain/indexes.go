package domain

import "time"

const (
	MaxExtractedBytes = 1 << 20
	MaxIndexChunks    = 8192
	ChunkRunes        = 384
	ChunkOverlapRunes = 76
	ParentChunkRunes  = 4096
)

// IndexProfile identifies every input that can affect derived text or vectors.
type IndexProfile struct {
	StorageID        string `json:"-"`
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
	StorageID       string         `json:"-"`
	Progress        *IndexProgress `json:"progress,omitempty"`
	ID              string         `json:"id"`
	WorkspaceID     string         `json:"workspace_id"`
	KnowledgeBaseID string         `json:"knowledge_base_id"`
	DocumentID      string         `json:"document_id"`
	VersionID       string         `json:"version_id"`
	ProfileID       string         `json:"profile_id"`
	SourceSHA256    string         `json:"source_sha256"`
	Status          string         `json:"status"`
	ErrorCode       string         `json:"error_code,omitempty"`
	Attempts        int            `json:"attempts"`
	Generation      string         `json:"generation,omitempty"`
	CreatedBy       string         `json:"created_by"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	FinishedAt      *time.Time     `json:"finished_at,omitempty"`
	ChunkCount      int            `json:"chunk_count"`
	ParentCount     int            `json:"parent_count"`
}

type IndexChunk struct {
	WorkspaceID     string `json:"workspace_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
	Kind            string `json:"kind"`
	ParentID        string `json:"parent_id,omitempty"`
	ParentOrdinal   int    `json:"-"`
	Generation      string `json:"generation"`
	ContextHeader   string `json:"context_header,omitempty"`
	ID              string `json:"id"`
	IndexID         string `json:"index_id"`
	DocumentID      string `json:"document_id"`
	VersionID       string `json:"version_id"`
	SourceSHA256    string `json:"source_sha256"`
	Ordinal         int    `json:"ordinal"`
	StartByte       int    `json:"start_byte"`
	EndByte         int    `json:"end_byte"`
	Content         string `json:"content"`
	SHA256          string `json:"sha256"`
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
	Focus                    *ChunkSpan  `json:"-"`
	Parent                   *IndexChunk `json:"-"`
	MatchedChildIDs          []string    `json:"matched_child_ids,omitempty"`
	Chunk                    IndexChunk  `json:"chunk"`
	Title                    string      `json:"title"`
	Score                    float64     `json:"score"`
	RetrievalScore           *float64    `json:"retrieval_score,omitempty"`
	RerankScore              *float64    `json:"rerank_score,omitempty"`
	NormalizedRetrievalScore *float64    `json:"normalized_retrieval_score,omitempty"`
}

// ChunkSpan is an exact UTF-8 byte range within canonical parsed text.
type ChunkSpan struct {
	StartByte int `json:"start_byte"`
	EndByte   int `json:"end_byte"`
}

type IndexCleanup struct {
	ID         string
	StorageID  string `json:"-"`
	ProfileID  string
	Generation string
	ClaimToken string `json:"-"`
}
