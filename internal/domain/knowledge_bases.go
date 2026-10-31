package domain

import (
	"strings"
	"time"
)

// KnowledgeBase groups documents within a Workspace's permission boundary.
type KnowledgeBase struct {
	ID          string    `json:"id"`
	WorkspaceID string    `json:"workspace_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	IsDefault   bool      `json:"is_default"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// A tool's optional knowledge-base selection narrows its current document scope.
// nil keeps that scope; an allocated empty list explicitly selects no sources.
func ValidateKnowledgeBaseIDs(ids []string) error {
	if len(ids) > 20 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" || seen[id] {
			return ErrInvalid
		}
		seen[id] = true
	}
	return nil
}
