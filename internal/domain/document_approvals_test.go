package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDocumentMutationStrictApprovalPayload(t *testing.T) {
	good := `{"title":"稿件","filename":"a.md","content":"正文\n"}`
	_, hash, err := ParseDocumentMutation("create_document", []byte(good))
	if err != nil {
		t.Fatal(err)
	}
	_, same, err := ParseDocumentMutation("create_document", []byte(`{"content":"正文\u000a", "filename":"a.md", "title":"稿件"}`))
	if err != nil || hash != same {
		t.Fatal("equivalent argument encodings changed approval")
	}
	for _, raw := range []string{`null`, `{}`, strings.Replace(good, `"content":`, `"content":"first","content":`, 1), strings.Replace(good, `"正文\n"`, `null`, 1), strings.Replace(good, `a.md`, `../a.md`, 1), strings.Replace(good, `a.md`, `a.pdf`, 1), strings.Replace(good, `"title":`, `"user_id":"spoof","title":`, 1), strings.Replace(good, `正文\n`, `\u0000`, 1), string([]byte{'{', '"', 0xff, '"', ':', '"', 'x', '"', '}'})} {
		if _, _, err := ParseDocumentMutation("create_document", []byte(raw)); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	big, _ := json.Marshal(DocumentMutation{Title: "x", Filename: "a.txt", Content: strings.Repeat("x", MaxAgentDocumentBytes+1)})
	if _, _, err := ParseDocumentMutation("create_document", big); err == nil {
		t.Fatal("oversized body")
	}
}
