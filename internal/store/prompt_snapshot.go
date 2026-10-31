package store

import (
	"encoding/json"
	"slices"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// Validate the native header that Go owns before claiming an answer or execution.
// Harness remains responsible for the full model/tool state schema. Accept only
// the current version; a partial native header cannot become an opaque payload
// by dropping its version.
func validatePromptSnapshot(raw []byte, uid, wid, sid, rid, query string, scope []string, prompt domain.RunPrompt) error {
	var fields map[string]json.RawMessage
	if !utf8.Valid(raw) || !uniqueJournalJSON(raw) || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return domain.ErrInvalid
	}
	if !requiredSnapshotFields(raw, "version", "run_id", "session_id", "caller", "scope", "policy", "query", "pending") || !requiredSnapshotFields(fields["caller"], "user_id", "workspace_id") || !requiredSnapshotFields(fields["scope"], "workspace_id", "document_ids") || !requiredSnapshotFields(fields["policy"], "access") || !requiredSnapshotFields(fields["pending"], "blocked_index", "tool_calls") {
		return domain.ErrInvalid
	}
	var header struct {
		Version   int    `json:"version"`
		RunID     string `json:"run_id"`
		SessionID string `json:"session_id"`
		Query     string `json:"query"`
		Caller    struct {
			UserID      string `json:"user_id"`
			WorkspaceID string `json:"workspace_id"`
		} `json:"caller"`
		Scope struct {
			WorkspaceID string          `json:"workspace_id"`
			DocumentIDs json.RawMessage `json:"document_ids"`
		} `json:"scope"`
		Policy struct {
			Access string `json:"access"`
		} `json:"policy"`
		Pending struct {
			Blocked *int `json:"blocked_index"`
			Calls   []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"tool_calls"`
		} `json:"pending"`
	}
	if json.Unmarshal(raw, &header) != nil || header.Version != 2 || header.RunID != rid || header.SessionID != sid || header.Query != query || header.Caller.UserID != uid || header.Caller.WorkspaceID != wid || header.Scope.WorkspaceID != wid || (header.Policy.Access != "read_only" && header.Policy.Access != "confirm") {
		return domain.ErrInvalid
	}
	var snapshotScope []string
	if len(header.Scope.DocumentIDs) == 0 || json.Unmarshal(header.Scope.DocumentIDs, &snapshotScope) != nil || (scope == nil) != (snapshotScope == nil) {
		return domain.ErrInvalid
	}
	expectedScope := slices.Clone(scope)
	slices.Sort(expectedScope)
	slices.Sort(snapshotScope)
	if !slices.Equal(expectedScope, snapshotScope) || header.Pending.Blocked == nil || *header.Pending.Blocked < 0 || *header.Pending.Blocked >= len(header.Pending.Calls) {
		return domain.ErrInvalid
	}
	call := header.Pending.Calls[*header.Pending.Blocked]
	var pending struct {
		Calls []json.RawMessage `json:"tool_calls"`
	}
	if json.Unmarshal(fields["pending"], &pending) != nil || !requiredSnapshotFields(pending.Calls[*header.Pending.Blocked], "id", "name", "arguments") {
		return domain.ErrInvalid
	}
	if call.ID != prompt.ToolCallID || !uniqueJournalJSON([]byte(call.Arguments)) {
		return domain.ErrInvalid
	}
	if prompt.Approval != nil {
		if header.Policy.Access != "confirm" || call.Name != prompt.Approval.Tool {
			return domain.ErrInvalid
		}
		_, hash, err := domain.ParseDocumentMutation(call.Name, []byte(call.Arguments))
		if err != nil || (prompt.Approval.ArgumentsSHA256 != "" && hash != prompt.Approval.ArgumentsSHA256) {
			return domain.ErrInvalid
		}
		return nil
	}
	var arguments map[string]json.RawMessage
	if call.Name != "ask_user" || json.Unmarshal([]byte(call.Arguments), &arguments) != nil || arguments == nil {
		return domain.ErrInvalid
	}
	for key := range arguments {
		if key != "question" && key != "options" {
			return domain.ErrInvalid
		}
	}
	var question string
	if json.Unmarshal(arguments["question"], &question) != nil || question != prompt.Question {
		return domain.ErrInvalid
	}
	var options []string
	if rawOptions, exists := arguments["options"]; exists {
		if json.Unmarshal(rawOptions, &options) != nil || options == nil {
			return domain.ErrInvalid
		}
	}
	if !slices.Equal(options, prompt.Options) {
		return domain.ErrInvalid
	}
	return nil
}

// encoding/json matches struct fields without case sensitivity, whereas native
// snapshots use exact keys. Reject aliases so validation cannot inspect values
// different from those the Harness will read.
func requiredSnapshotFields(raw []byte, required ...string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	for _, name := range required {
		if _, exists := fields[name]; !exists {
			return false
		}
		for key := range fields {
			if key != name && strings.EqualFold(key, name) {
				return false
			}
		}
	}
	return true
}
