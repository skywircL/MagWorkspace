package store

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"magworkspace/internal/domain"
)

func TestEvidenceParentAndChildContentIdentity(t *testing.T) {
	body := strings.Repeat("父段上下文。", 200)
	digest := sha256.Sum256([]byte(body))
	parent := domain.IndexChunk{
		WorkspaceID: strings.Repeat("1", 32), KnowledgeBaseID: strings.Repeat("2", 32),
		ID: strings.Repeat("a", 32), IndexID: strings.Repeat("b", 32),
		DocumentID: strings.Repeat("c", 32), VersionID: strings.Repeat("d", 32),
		Generation: strings.Repeat("e", 32), SourceSHA256: strings.Repeat("f", 64),
		Kind: "parent", Content: body, EndByte: len(body), SHA256: hex.EncodeToString(digest[:]),
	}
	if !validEvidenceChunk(parent) {
		t.Fatal("parent context longer than a searchable child must remain valid evidence")
	}
	child := parent
	child.ID = strings.Repeat("1", 32)
	child.Kind, child.ParentID = "child", parent.ID
	if validEvidenceChunk(child) {
		t.Fatal("a parent-sized body must not be accepted under child identity")
	}
	child.Content = "独立命中的子片段。"
	child.EndByte = len(child.Content)
	digest = sha256.Sum256([]byte(child.Content))
	child.SHA256 = hex.EncodeToString(digest[:])
	if !validEvidenceChunk(child) {
		t.Fatal("bounded child with its parent binding was rejected")
	}
	child.Content = parent.Content
	if validEvidenceChunk(child) {
		t.Fatal("parent expansion under unchanged child offsets and digest was accepted")
	}
	parent.Content = strings.Repeat("字", domain.ParentChunkRunes+1)
	parent.EndByte = len(parent.Content)
	digest = sha256.Sum256([]byte(parent.Content))
	parent.SHA256 = hex.EncodeToString(digest[:])
	if validEvidenceChunk(parent) {
		t.Fatal("oversized parent evidence exceeded the bounded tool contract")
	}
}
