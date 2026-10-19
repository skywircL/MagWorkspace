package domain

import "time"

// ConversationMatch is a bounded excerpt of one completed, private exchange.
// Its original citations are historical text, not evidence acquired by this run.
type ConversationMatch struct {
	SessionID          string    `json:"session_id"`
	RunID              string    `json:"run_id"`
	Title              string    `json:"title"`
	UserMessageID      string    `json:"user_message_id"`
	AssistantMessageID string    `json:"assistant_message_id"`
	Question           string    `json:"question"`
	Answer             string    `json:"answer"`
	CreatedAt          time.Time `json:"created_at"`
	FinishedAt         time.Time `json:"finished_at"`
}

type ConversationSearchResult struct {
	Items      []ConversationMatch `json:"items"`
	HasMore    bool                `json:"has_more"`
	NextOffset *int                `json:"next_offset,omitempty"`
}
