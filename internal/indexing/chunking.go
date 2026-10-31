package indexing

import "magworkspace/internal/domain"

const ChunkingParentChild = "parent_child_v1"

// SplitWithStrategy accepts only the active parent-child strategy. Existing
// test data is rebuilt when the profile changes; obsolete strategies fail closed.
func SplitWithStrategy(text, strategy string) ([]domain.IndexChunk, error) {
	switch strategy {
	case "", ChunkingParentChild:
		return Split(text)
	default:
		return nil, &Failure{Code: "artifact_invalid"}
	}
}
