package domain

import "encoding/json"

// RecoveredRun is private execution input after an authenticated recovery claim.
// Prompt is an already-owned answer, never an API response or a model message.
type RecoveredRun struct {
	Run         Run
	Role, Query string
	DocumentIDs []string
	Prompt      json.RawMessage
	Generation  uint64
}
