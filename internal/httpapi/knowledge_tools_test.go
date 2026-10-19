package httpapi

import (
	"fmt"
	"testing"

	"magworkspace/internal/domain"
)

func TestKnowledgeChunkQueryPaginationBudget(t *testing.T) {
	chunks := make([]domain.IndexChunk, 40)
	for i := range chunks {
		chunks[i] = domain.IndexChunk{ID: fmt.Sprint(i), Ordinal: i, Content: "ordinary text"}
		if i%5 == 0 {
			chunks[i].Content = "住宿 Budget (500)"
		}
	}
	offset := 0
	found := map[int]bool{}
	pages := 0
	for {
		result, next, err := selectKnowledgeChunks(chunks, offset, 10, 3, "住宿 BUDGET (500)", "")
		if err != nil || len(result) > 10 {
			t.Fatalf("budget err=%v count=%d", err, len(result))
		}
		for _, c := range result {
			if c.Ordinal%5 == 0 {
				found[c.Ordinal] = true
			}
		}
		pages++
		if next == nil {
			break
		}
		if *next <= offset || pages > 20 {
			t.Fatal("query cursor made no forward progress")
		}
		offset = *next
	}
	if len(found) != 8 || pages < 2 {
		t.Fatalf("query dropped matches: %v pages=%d", found, pages)
	}
}

func TestKnowledgeChunkNeighborsAndLiteralQuery(t *testing.T) {
	chunks := []domain.IndexChunk{{ID: "first", Content: "regex.* [literal]"}, {ID: "second", Content: "second"}, {ID: "third", Content: "third"}}
	selected, next, err := selectKnowledgeChunks(chunks, 0, 1, 3, "", "first")
	if err != nil || next != nil || len(selected) != 1 || selected[0].ID != "first" {
		t.Fatalf("edge neighbors: count=%d next=%v err=%v", len(selected), next, err)
	}
	selected, next, err = selectKnowledgeChunks(chunks, 0, 1, 0, "[literal]", "")
	if err != nil || len(selected) != 1 || selected[0].ID != "first" || next != nil {
		t.Fatal("query interpreted literal punctuation as regex")
	}
	selected, _, err = selectKnowledgeChunks(chunks, 0, 1, 0, ".*missing", "")
	if err != nil || len(selected) != 0 {
		t.Fatal("no-match returned passages")
	}
	selected, _, err = selectKnowledgeChunks(chunks, 3, 1, 0, "", "")
	if err != nil || len(selected) != 0 {
		t.Fatal("end cursor returned duplicated passages")
	}
}

func TestKnowledgeChunkContextRespectsTotalLimit(t *testing.T) {
	chunks := make([]domain.IndexChunk, 12)
	for i := range chunks {
		chunks[i] = domain.IndexChunk{ID: fmt.Sprint(i), Ordinal: i, Content: "match"}
	}
	for _, limit := range []int{1, 2, 5, 10} {
		for _, focus := range []int{0, 6, 11} {
			selected, _, err := selectKnowledgeChunks(chunks, 0, limit, 3, "", chunks[focus].ID)
			found := false
			for _, c := range selected {
				if c.ID == chunks[focus].ID {
					found = true
				}
			}
			if err != nil || len(selected) > limit || !found {
				t.Fatalf("focus=%d limit=%d count=%d found=%v err=%v", focus, limit, len(selected), found, err)
			}
		}
		selected, next, err := selectKnowledgeChunks(chunks, 0, limit, 3, "match", "")
		if err != nil || len(selected) > limit || next == nil {
			t.Fatalf("query limit=%d count=%d next=%v err=%v", limit, len(selected), next, err)
		}
	}
}
