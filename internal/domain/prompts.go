package domain

import (
	"encoding/json"
	"time"
)

const MaxRunSnapshotBytes = 512 << 10

// RunPrompt exposes a question or reviewable approval, never the private snapshot.
type RunPrompt struct {
	Kind       string            `json:"kind"`
	Approval   *DocumentApproval `json:"approval,omitempty"`
	ID         string            `json:"id"`
	RunID      string            `json:"run_id"`
	SessionID  string            `json:"session_id"`
	ToolCallID string            `json:"tool_call_id"`
	Question   string            `json:"question"`
	Options    []string          `json:"options"`
	Status     string            `json:"status"`
	Answer     string            `json:"answer,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	ExpiresAt  time.Time         `json:"expires_at"`
}

type SuspendedPrompt struct {
	ID, ToolCallID, Question, Snapshot string
	Approval                           *DocumentApproval
	Options                            []string
	ExpiresAt                          time.Time
}

type ResumedRun struct {
	Run         Run
	Role, Query string
	DocumentIDs []string
	Prompt      json.RawMessage
	Generation  uint64
}
