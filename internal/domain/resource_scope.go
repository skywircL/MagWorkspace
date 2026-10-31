package domain

import "slices"

// ResourceSelection is user intent. Omitted dimensions select the workspace;
// otherwise full libraries and individual documents form a union.
type ResourceSelection struct {
	KnowledgeBaseIDs []string
	DocumentIDs      []string
}

// SearchTarget belongs to one library. nil DocumentIDs means the whole library;
// a nonempty list keeps that target restricted even when a tool selects its KB.
type SearchTarget struct {
	KnowledgeBaseID string   `json:"knowledge_base_id"`
	DocumentIDs     []string `json:"document_ids"`
}

// ResourceScope is the normalized, immutable backend authority for a Run.
// All=false with no targets is an explicit empty scope, never a default.
type ResourceScope struct {
	All     bool           `json:"all"`
	Targets []SearchTarget `json:"targets"`
}

// DocumentProjection is an additional Harness bound when the full scope can
// be represented as a flat document set. A whole-library target requires nil;
// Go still enforces its precise targets on every operation.
func (s ResourceScope) DocumentProjection() []string {
	if s.All {
		return nil
	}
	ids := []string{}
	for _, target := range s.Targets {
		if target.DocumentIDs == nil {
			return nil
		}
		ids = append(ids, target.DocumentIDs...)
	}
	return ids
}

func (s ResourceScope) Validate() error {
	if (s.All && len(s.Targets) != 0) || len(s.Targets) > 120 {
		return ErrInvalid
	}
	seenKB, seenDoc := map[string]bool{}, map[string]bool{}
	full := 0
	for _, target := range s.Targets {
		if ValidateKnowledgeBaseIDs([]string{target.KnowledgeBaseID}) != nil || seenKB[target.KnowledgeBaseID] {
			return ErrInvalid
		}
		seenKB[target.KnowledgeBaseID] = true
		if target.DocumentIDs == nil {
			full++
		} else if len(target.DocumentIDs) == 0 {
			return ErrInvalid
		}
		for _, id := range target.DocumentIDs {
			if ValidateKnowledgeBaseIDs([]string{id}) != nil || seenDoc[id] {
				return ErrInvalid
			}
			seenDoc[id] = true
		}
	}
	if full > 20 || len(seenDoc) > 100 {
		return ErrInvalid
	}
	return nil
}

func (s ResourceScope) AllowsKnowledgeBase(kb string) bool {
	return s.All || slices.ContainsFunc(s.Targets, func(t SearchTarget) bool { return t.KnowledgeBaseID == kb })
}

func (s ResourceScope) AllowsDocument(kb, document string) bool {
	if s.All {
		return true
	}
	for _, target := range s.Targets {
		if target.KnowledgeBaseID == kb && (target.DocumentIDs == nil || slices.Contains(target.DocumentIDs, document)) {
			return true
		}
	}
	return false
}

func (s ResourceScope) AllowsCreate(kb string) bool {
	return s.All || slices.ContainsFunc(s.Targets, func(t SearchTarget) bool { return t.KnowledgeBaseID == kb && t.DocumentIDs == nil })
}

func (s ResourceScope) NarrowKnowledgeBases(ids []string) (ResourceScope, error) {
	if s.Validate() != nil || ValidateKnowledgeBaseIDs(ids) != nil {
		return ResourceScope{}, ErrInvalid
	}
	if ids == nil {
		return s, nil
	}
	out := ResourceScope{Targets: []SearchTarget{}}
	for _, id := range ids {
		if !s.AllowsKnowledgeBase(id) {
			return ResourceScope{}, ErrForbidden
		}
		if s.All {
			out.Targets = append(out.Targets, SearchTarget{KnowledgeBaseID: id})
		} else {
			for _, target := range s.Targets {
				if target.KnowledgeBaseID == id {
					out.Targets = append(out.Targets, target)
				}
			}
		}
	}
	return out, nil
}
