package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestToolTransportKnowledgeBaseClosedArguments(t *testing.T) {
	id := strings.Repeat("a", 32)
	cases := []struct {
		name, args string
		accepted   bool
	}{
		{"tool_capabilities", `{}`, true},
		{"tool_capabilities", `{"user_id":"forged"}`, false},
		{"list_knowledge_bases", `{"keyword":"research","offset":1,"limit":2}`, true},
		{"list_knowledge_bases", `{"knowledge_base_ids":[]}`, false},
		{"search_knowledge", `{"query":"research","knowledge_base_ids":[]}`, true},
		{"search_knowledge", `{"query":"research","knowledge_base_ids":["` + id + `"]}`, true},
		{"list_documents", `{"knowledge_base_ids":["` + id + `","` + id + `"]}`, false},
		{"list_documents", `{"knowledge_base_ids":null}`, false},
		{"list_documents", `{"knowledge_base_ids":[null]}`, false},
		{"search_documents", `{"query":"research","knowledge_base_ids":[]}`, false},
		{"create_document", `{"title":"A","filename":"a.md","content":"x","knowledge_base_id":"` + id + `"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name+tc.args, func(t *testing.T) {
			calls := 0
			handler := toolHTTPHandler("token", func(context.Context, string, json.RawMessage) (any, error) {
				calls++
				return map[string]bool{"ok": true}, nil
			})
			payload := map[string]any{"tool": tc.name, "arguments": json.RawMessage(tc.args)}
			if tc.name == "create_document" {
				payload["tool_call_id"] = "proposal"
			}
			raw, _ := json.Marshal(payload)
			request := httptest.NewRequest("POST", "http://127.0.0.1/tools", bytes.NewReader(raw))
			request.Header.Set("Authorization", "Bearer token")
			request.Header.Set("Content-Type", "application/json")
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, request)
			if (reply.Code == 200) != tc.accepted || (calls == 1) != tc.accepted {
				t.Fatalf("accepted=%v status=%d calls=%d", tc.accepted, reply.Code, calls)
			}
		})
	}
}
