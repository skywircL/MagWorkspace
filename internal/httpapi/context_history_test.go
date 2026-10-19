package httpapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"magworkspace/internal/domain"
)

func historyPair(id string, question, answer string) []domain.Message {
	return []domain.Message{{RunID: id, Role: "user", Content: question}, {RunID: id, Role: "assistant", Content: answer}}
}

func TestContextHistoryWindowKeepsOlderCompleteTurnsAndExactBytes(t *testing.T) {
	var history []domain.Message
	for i := range 100 {
		history = append(history, historyPair(fmt.Sprint(i), strings.Repeat("界", 300), strings.Repeat("<answer>", 100))...)
	}
	selected := boundedHistory(history)
	if len(selected) != 200 || selected[0] != history[0] || selected[199] != history[199] {
		t.Fatal("the authorized 100-turn window was unexpectedly reduced")
	}
	if got := historyWindow(history); len(got) != 200 || got[0].Content != history[0].Content {
		t.Fatal("wire history lost older turns or changed Unicode content")
	}
}

func TestContextHistoryWindowSkipsWholeOversizedPairs(t *testing.T) {
	history := historyPair("old", "keep old question exactly", "keep old answer exactly")
	for i := range 4 {
		history = append(history, historyPair(fmt.Sprint(i), strings.Repeat("a", maxHydratedMessageBytes), strings.Repeat("b", maxHydratedMessageBytes))...)
	}
	// This pair is invalid at the bridge boundary and must never be shortened.
	history = append(history, historyPair("invalid", strings.Repeat("c", maxHydratedMessageBytes+1), "valid counterpart")...)
	selected := boundedHistory(history)
	if len(selected) != 8 {
		t.Fatalf("expected three newest complete turns and a smaller old pair, got %d messages", len(selected))
	}
	size := 0
	for i := 0; i < len(selected); i += 2 {
		question, answer := selected[i], selected[i+1]
		if question.RunID == "invalid" || question.RunID != answer.RunID || question.Role != "user" || answer.Role != "assistant" {
			t.Fatal("history contains half a pair or a turn outside the byte budget")
		}
		size += len(question.Content) + len(answer.Content)
	}
	encoded, _ := json.Marshal(historyWindow(selected))
	if size > maxHydratedBytes || len(encoded) > maxHydratedBytes || selected[0].RunID != "old" || selected[len(selected)-1].RunID != "3" {
		t.Fatal("encoded history budget changed, recent turns were lost, or messages were truncated")
	}
}

func TestContextHistoryWindowPreservesSelectedRunProvenanceAcrossGaps(t *testing.T) {
	history := historyPair("small", "small question", "small answer")
	history = append(history, historyPair("large", strings.Repeat("a", maxHydratedMessageBytes), strings.Repeat("b", maxHydratedMessageBytes))...)
	for i := range 4 {
		history = append(history, historyPair(fmt.Sprint(i), strings.Repeat("a", 60000), strings.Repeat("b", 60000))...)
	}
	selected := boundedHistory(history)
	if len(selected) != 10 || selected[0].RunID != "small" {
		t.Fatal("a skipped large pair prevented a smaller older authorized pair from fitting")
	}
	for _, message := range selected {
		if message.RunID == "large" {
			t.Fatal("oversized pair was partially retained")
		}
	}
}

func TestContextHistoryWindowAccountsForGoJSONEscaping(t *testing.T) {
	content := strings.Repeat("<>&", 10000)
	var history []domain.Message
	for i := range 3 {
		history = append(history, historyPair(fmt.Sprint(i), content, content)...)
	}
	selected := boundedHistory(history)
	encoded, _ := json.Marshal(historyWindow(selected))
	if len(selected) != 2 || selected[0].RunID != "2" || selected[0].Content != content || selected[1].Content != content || len(encoded) > maxHydratedBytes {
		t.Fatal("HTML escaping exceeded the IPC history budget or truncated a message")
	}
	request, _ := json.Marshal(struct {
		History any    `json:"history"`
		Query   string `json:"query"`
	}{historyWindow(selected), strings.Repeat("<", 64000)})
	if len(request) > 2<<20 {
		t.Fatal("bounded history plus the maximum escaped query exceeds the bridge input limit")
	}
}
