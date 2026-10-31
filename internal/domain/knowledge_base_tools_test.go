package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestDocumentMutationKnowledgeBaseApprovalBinding(t *testing.T) {
	original := `{"content":"reviewed","filename":"a.md","title":"Draft"}`
	_, old, err := ParseDocumentMutation("create_document", []byte(original))
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte(original))
	if old != hex.EncodeToString(expected[:]) {
		t.Fatal("legacy approval hash changed")
	}
	id := strings.Repeat("a", 32)
	selected := strings.TrimSuffix(original, "}") + `,"knowledge_base_id":"` + id + `"}`
	mutation, bound, err := ParseDocumentMutation("create_document", []byte(selected))
	if err != nil || mutation.KnowledgeBaseID != id || old == bound {
		t.Fatal("destination not bound to approval")
	}
	_, changed, err := ParseDocumentMutation("create_document", []byte(strings.Replace(selected, id, strings.Repeat("b", 32), 1)))
	if err != nil || changed == bound {
		t.Fatal("destination change reused approval hash")
	}
	for _, value := range []string{`null`, `""`, `"wrong"`, `123`, `[]`} {
		raw := strings.TrimSuffix(original, "}") + `,"knowledge_base_id":` + value + `}`
		if _, _, err := ParseDocumentMutation("create_document", []byte(raw)); err == nil {
			t.Errorf("accepted %s", value)
		}
	}
	update := `{"document_id":"` + id + `","base_version_id":"` + id + `","content":"x","knowledge_base_id":"` + id + `"}`
	if _, _, err := ParseDocumentMutation("update_document", []byte(update)); err == nil {
		t.Fatal("update changed destination")
	}
}
