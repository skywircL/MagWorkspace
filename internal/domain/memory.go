package domain

import "time"

// MaxMemoryTimeSeconds is the exclusive UTC year-10000 boundary. Keep persisted
// and public Unix seconds representable by database and browser calendar types.
const MaxMemoryTimeSeconds = 253402300800

// Memory is a personal statement, independent of Session history.
// Public timestamps use Unix seconds to match the Harness MemoryItem contract.
type Memory struct {
	Origin          string   `json:"origin"`
	SourceRunID     string   `json:"source_run_id"`
	ID              string   `json:"id"`
	Kind            string   `json:"kind"`
	Content         string   `json:"content"`
	Topic           string   `json:"topic"`
	NormalizedKey   string   `json:"normalized_key"`
	Importance      int      `json:"importance"`
	Status          string   `json:"status"`
	CreatedAt       float64  `json:"created_at"`
	ExpiresAt       *float64 `json:"expires_at"`
	SupersededBy    string   `json:"superseded_by"`
	SourceSessionID string   `json:"source_session_id"`
	UseCount        uint64   `json:"use_count"`
	LastUsedAt      *float64 `json:"last_used_at"`
}

type CreateMemory struct {
	Kind            string
	Content         string
	Topic           string
	Importance      int
	ExpiresAt       *float64
	SourceSessionID string
}

type MemorySettings struct {
	Enabled bool `json:"enabled"`
}

// MemoryCandidate contains no identity or persistence fields from the model.
type MemoryCandidate struct {
	Kind       string `json:"kind"`
	Content    string `json:"content"`
	Topic      string `json:"topic"`
	Importance int    `json:"importance"`
	Evidence   string `json:"evidence"`
}

type MemoryCaptureJob struct {
	ID          string
	RunID       string
	UserID      string
	WorkspaceID string
	SessionID   string
	Query       string
	Mode        string
	Explicit    *MemoryCandidate
	Attempt     int
	Generation  uint64
	Epoch       uint64
	CreatedAt   time.Time
}

type MemoryCaptureStatus struct {
	Status     string `json:"status"`
	SavedCount int    `json:"saved_count"`
	ErrorCode  string `json:"error_code,omitempty"`
}
