package domain

// Memory is an explicit personal statement, independent of Session history.
// Timestamps use Unix seconds to match the Harness MemoryItem contract.
type Memory struct {
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
