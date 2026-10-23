package agent

import (
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const maxSnapshotBytes = 512 << 10

var promptIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func validUserPrompt(prompt *UserPrompt, request Request) bool {
	if prompt == nil || !promptIDPattern.MatchString(prompt.ID) || strings.TrimSpace(prompt.ToolCallID) == "" || len(prompt.ToolCallID) > 256 || !utf8.ValidString(prompt.Question) || strings.TrimSpace(prompt.Question) == "" || len(prompt.Question) > 4096 || len(prompt.Options) > 10 || len(prompt.Snapshot) == 0 || len(prompt.Snapshot) > maxSnapshotBytes || !utf8.ValidString(prompt.Snapshot) || !uniqueJSON([]byte(prompt.Snapshot)) || math.IsNaN(prompt.ExpiresAt) || math.IsInf(prompt.ExpiresAt, 0) || prompt.ExpiresAt <= float64(time.Now().Unix()) {
		return false
	}
	for _, option := range prompt.Options {
		if !utf8.ValidString(option) || strings.TrimSpace(option) == "" || len(option) > 512 {
			return false
		}
	}
	var snapshot struct {
		RunID     string `json:"run_id"`
		SessionID string `json:"session_id"`
		Query     string `json:"query"`
		Caller    struct {
			WorkspaceID string `json:"workspace_id"`
			UserID      string `json:"user_id"`
		} `json:"caller"`
		Scope struct {
			WorkspaceID string    `json:"workspace_id"`
			DocumentIDs *[]string `json:"document_ids"`
		} `json:"scope"`
		Policy struct {
			Access       string   `json:"access"`
			AllowedTools []string `json:"allowed_tools"`
		} `json:"policy"`
		Pending struct {
			BlockedIndex int `json:"blocked_index"`
			ToolCalls    []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"tool_calls"`
		} `json:"pending"`
	}
	if json.Unmarshal([]byte(prompt.Snapshot), &snapshot) != nil || snapshot.RunID != request.RunID || snapshot.SessionID != request.SessionID || snapshot.Query != request.Query || snapshot.Caller.WorkspaceID != request.WorkspaceID || snapshot.Caller.UserID != request.UserID || snapshot.Scope.WorkspaceID != request.WorkspaceID || (snapshot.Policy.Access != "read_only" && snapshot.Policy.Access != "confirm") {
		return false
	}
	// The capability's scope must survive suspension exactly, including empty vs all.
	if (snapshot.Scope.DocumentIDs == nil) != (request.DocumentIDs == nil) {
		return false
	}
	if request.DocumentIDs != nil {
		wanted := map[string]bool{}
		for _, id := range *request.DocumentIDs {
			wanted[id] = true
		}
		got := map[string]bool{}
		for _, id := range *snapshot.Scope.DocumentIDs {
			got[id] = true
		}
		if len(got) != len(wanted) {
			return false
		}
		for id := range got {
			if !wanted[id] {
				return false
			}
		}
	}
	allowed := map[string]bool{"create_document": true, "update_document": true, "search_knowledge": true, "read_document": true, "list_documents": true, "list_knowledge_bases": true, "search_conversations": true, "search_memory": true, "ask_user": true, "search_documents": true, "read_evidence": true}
	for _, name := range snapshot.Policy.AllowedTools {
		if !allowed[name] {
			return false
		}
	}
	if snapshot.Policy.AllowedTools == nil {
		return false
	}
	i := snapshot.Pending.BlockedIndex
	if i < 0 || i >= len(snapshot.Pending.ToolCalls) {
		return false
	}
	call := snapshot.Pending.ToolCalls[i]
	if prompt.Kind == "tool_approval" {
		var a domain.DocumentApproval
		if snapshot.Policy.Access != "confirm" || !uniqueJSON(prompt.Request) || json.Unmarshal(prompt.Request, &a) != nil || a.Risk != "write" || call.ID != prompt.ToolCallID || call.Name != a.Tool {
			return false
		}
		_, hash, err := domain.ParseDocumentMutation(a.Tool, []byte(call.Arguments))
		if err != nil {
			return false
		}
		_, presented, err := domain.ParseDocumentMutation(a.Tool, a.Arguments)
		return err == nil && hash == presented
	}
	if prompt.Kind != "" && prompt.Kind != "ask_user" {
		return false
	}
	var question struct {
		Question string   `json:"question"`
		Options  []string `json:"options"`
	}
	if call.ID != prompt.ToolCallID || call.Name != "ask_user" || json.Unmarshal([]byte(call.Arguments), &question) != nil || question.Question != prompt.Question || len(question.Options) != len(prompt.Options) {
		return false
	}
	for i := range question.Options {
		if question.Options[i] != prompt.Options[i] {
			return false
		}
	}
	return true
}
