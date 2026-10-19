package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestToolTransportDocumentApprovalCallBinding(t *testing.T) {
	calls := 0
	handler := toolHTTPHandler("secret", func(ctx context.Context, name string, raw json.RawMessage) (any, error) {
		calls++
		if ToolCallID(ctx) != "bound-call" || name != "create_document" {
			t.Fatal("missing private call binding")
		}
		return map[string]bool{"received": true}, nil
	})
	args, _ := json.Marshal(map[string]string{"title": "x", "filename": "x.md", "content": strings.Repeat("正文", 5000)})
	good := `{"tool":"create_document","tool_call_id":"bound-call","arguments":` + string(args) + `}`
	for _, tc := range []struct {
		raw    string
		status int
	}{
		{good, 200},
		{strings.Replace(good, `"tool_call_id":"bound-call",`, "", 1), 400},
		{strings.Replace(good, `"tool_call_id":"bound-call"`, `"tool_call_id":null`, 1), 400},
		{strings.Replace(good, `"title":"x"`, `"user_id":"spoof","title":"x"`, 1), 400},
		{strings.Replace(good, `"title":"x"`, `"title":"other","title":"x"`, 1), 400},
		{`{"tool":"list_documents","tool_call_id":"bound-call","arguments":{}}`, 400},
	} {
		r := httptest.NewRequest(http.MethodPost, "/tools", strings.NewReader(tc.raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status=%d want=%d", w.Code, tc.status)
		}
	}
	if calls != 1 {
		t.Fatalf("invalid writes reached handler: %d", calls)
	}
	for _, raw := range []string{`{"document_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":true}`, `{"document_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":true,"version_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","source_offset":2048}`} {
		if !validToolArguments("read_document", []byte(raw)) {
			t.Fatal("source schema rejected")
		}
	}
	for _, raw := range []string{`{"document_id":"a","source":true,"source_offset":1}`, `{"document_id":"a","source":true,"index_id":"b"}`, `{"document_id":"a","source":null}`, `{"document_id":"a","source_offset":0}`} {
		if validToolArguments("read_document", []byte(raw)) {
			t.Fatal("invalid source schema accepted")
		}
	}
}
