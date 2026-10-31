package domain

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestResourceScopeUnionAndNarrowing(t *testing.T) {
	a, b, c, d, other := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32), strings.Repeat("e", 32)
	s := ResourceScope{Targets: []SearchTarget{{KnowledgeBaseID: a}, {KnowledgeBaseID: b, DocumentIDs: []string{d}}}}
	if s.Validate() != nil || !s.AllowsDocument(a, other) || !s.AllowsDocument(b, d) || s.AllowsDocument(b, other) || s.AllowsDocument(c, d) || !s.AllowsCreate(a) || s.AllowsCreate(b) {
		t.Fatal("mixed targets did not enforce full-library/document union")
	}
	narrow, err := s.NarrowKnowledgeBases([]string{b})
	if err != nil || !narrow.AllowsDocument(b, d) || narrow.AllowsDocument(b, other) || narrow.AllowsDocument(a, d) || !reflect.DeepEqual(narrow.DocumentProjection(), []string{d}) {
		t.Fatalf("filter widened partial-library target: %+v %v", narrow, err)
	}
	if _, err = s.NarrowKnowledgeBases([]string{a, c}); !errors.Is(err, ErrForbidden) {
		t.Fatal("mixed escape was accepted")
	}
	none, err := s.NarrowKnowledgeBases([]string{})
	if err != nil || none.All || len(none.Targets) != 0 || none.DocumentProjection() == nil {
		t.Fatal("empty selection widened")
	}
	if s.DocumentProjection() != nil {
		t.Fatal("mixed scope flattened into document-only intersection")
	}
}

func TestResourceScopeRejectsInvalidAuthority(t *testing.T) {
	a := strings.Repeat("a", 32)
	for _, scope := range []ResourceScope{
		{All: true, Targets: []SearchTarget{{KnowledgeBaseID: a}}},
		{Targets: []SearchTarget{{KnowledgeBaseID: a, DocumentIDs: []string{}}}},
		{Targets: []SearchTarget{{KnowledgeBaseID: a}, {KnowledgeBaseID: a}}},
		{Targets: []SearchTarget{{KnowledgeBaseID: a, DocumentIDs: []string{"bad"}}}},
	} {
		if scope.Validate() == nil {
			t.Errorf("accepted malformed authority: %+v", scope)
		}
	}
}
