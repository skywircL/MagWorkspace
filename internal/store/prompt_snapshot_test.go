package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"magworkspace/internal/domain"
)

// A native header fixture intentionally omits the model transcript: Go verifies
// its ownership contract, while the Harness decoder owns the complete format.
func nativePromptSnapshot(t *testing.T, uid, wid, sid, rid, query string, scope []string, in domain.SuspendedPrompt) map[string]any {
	t.Helper()
	args, err := json.Marshal(map[string]any{"question": in.Question, "options": in.Options})
	must(t, err)
	return map[string]any{
		"version": 2, "run_id": rid, "session_id": sid, "query": query,
		"caller":  map[string]any{"user_id": uid, "workspace_id": wid},
		"scope":   map[string]any{"workspace_id": wid, "document_ids": scope},
		"policy":  map[string]any{"access": "read_only"},
		"pending": map[string]any{"blocked_index": 0, "tool_calls": []any{map[string]any{"id": in.ToolCallID, "name": "ask_user", "arguments": string(args)}}},
	}
}

func encodePromptSnapshot(t *testing.T, snapshot map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	must(t, err)
	return raw
}

func TestPromptSnapshotNativeHeaderContract(t *testing.T) {
	in := promptInput(t)
	p := domain.RunPrompt{ToolCallID: in.ToolCallID, Question: in.Question, Options: in.Options}
	validate := func(raw []byte) error {
		return validatePromptSnapshot(raw, "user", "workspace", "session", "run", "query", []string{}, p)
	}
	must(t, validate(encodePromptSnapshot(t, nativePromptSnapshot(t, "user", "workspace", "session", "run", "query", []string{}, in))))
	for _, raw := range []string{`{"opaque":"historical adapter"}`, `{}`, `null`, `[]`, `1`, `{"opaque":1,"opaque":2}`, `{"opaque":{"x":1,"x":2}}`} {
		wantError(t, validate([]byte(raw)), domain.ErrInvalid)
	}
	for name, change := range map[string]func(map[string]any){
		"old version":           func(m map[string]any) { m["version"] = 1 },
		"unknown version":       func(m map[string]any) { m["version"] = 3 },
		"fractional version":    func(m map[string]any) { m["version"] = 1.5 },
		"missing version":       func(m map[string]any) { delete(m, "version") },
		"case aliased version":  func(m map[string]any) { m["VERSION"] = 2 },
		"case aliased caller":   func(m map[string]any) { m["caller"].(map[string]any)["USER_ID"] = "user" },
		"foreign run":           func(m map[string]any) { m["run_id"] = "other" },
		"foreign session":       func(m map[string]any) { m["session_id"] = "other" },
		"foreign caller":        func(m map[string]any) { m["caller"].(map[string]any)["user_id"] = "other" },
		"foreign workspace":     func(m map[string]any) { m["scope"].(map[string]any)["workspace_id"] = "other" },
		"changed query":         func(m map[string]any) { m["query"] = "other" },
		"empty scope widened":   func(m map[string]any) { m["scope"].(map[string]any)["document_ids"] = nil },
		"missing scope":         func(m map[string]any) { delete(m["scope"].(map[string]any), "document_ids") },
		"missing blocked index": func(m map[string]any) { delete(m["pending"].(map[string]any), "blocked_index") },
		"invalid blocked index": func(m map[string]any) { m["pending"].(map[string]any)["blocked_index"] = 1 },
		"wrong call":            func(m map[string]any) { snapshotBlockedCall(m)["id"] = "other" },
		"wrong tool":            func(m map[string]any) { snapshotBlockedCall(m)["name"] = "other" },
		"wrong question":        func(m map[string]any) { snapshotBlockedCall(m)["arguments"] = `{"question":"other"}` },
		"wrong options": func(m map[string]any) {
			snapshotBlockedCall(m)["arguments"] = `{"question":"国内还是海外差旅？","options":["国内"]}`
		},
		"duplicate arguments": func(m map[string]any) {
			snapshotBlockedCall(m)["arguments"] = `{"question":"国内还是海外差旅？","question":"国内还是海外差旅？","options":["国内","海外"]}`
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := nativePromptSnapshot(t, "user", "workspace", "session", "run", "query", []string{}, in)
			change(snapshot)
			if err := validate(encodePromptSnapshot(t, snapshot)); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("invalid native header accepted: %v", err)
			}
		})
	}
	raw := string(encodePromptSnapshot(t, nativePromptSnapshot(t, "user", "workspace", "session", "run", "query", []string{}, in)))
	wantError(t, validate([]byte(strings.Replace(raw, `"version":2`, `"version":2,"version":2`, 1))), domain.ErrInvalid)
	wantError(t, validate([]byte(strings.Replace(raw, `"version":2`, "\"memory_prompt\":\"\xff\",\"version\":2", 1))), domain.ErrInvalid)
}

func snapshotBlockedCall(m map[string]any) map[string]any {
	return m["pending"].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)
}
