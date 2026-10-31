package indexing

import "testing"

func TestChunkingProfileUsesOnlyParentChild(t *testing.T) {
	current := Profile("model", 3, "parser/", "embed/", "qdrant/", 256)
	if current.ChunkingStrategy != ChunkingParentChild {
		t.Fatal("default profile does not select parent-child chunking")
	}
	defaultProfile, err := ProfileWithChunking("model", 3, "parser/", "embed/", "qdrant/", 256, "")
	if err != nil || current != defaultProfile {
		t.Fatal("default strategy differs")
	}
	for _, strategy := range []string{"legacy_v1", "structure_v2", "typo"} {
		if _, err := ProfileWithChunking("model", 3, "parser", "embed", "qdrant", 256, strategy); err == nil {
			t.Fatalf("obsolete or unknown profile strategy %q accepted", strategy)
		}
		if _, err := SplitWithStrategy("content", strategy); err == nil {
			t.Fatalf("obsolete or unknown splitter strategy %q accepted", strategy)
		}
	}
}
