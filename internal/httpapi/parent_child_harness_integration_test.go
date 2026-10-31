package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// This exercises the real Go HTTP -> FastAPI -> Harness -> Go callback path.
// Only the model provider, parser, embedding, retrieval and blob boundaries are
// controlled local fixtures. No external or paid model endpoint is contacted.
func TestHTTPParentChildRealHarnessPreservesFiveTailMatches(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[streaming], func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "parent-child-harness")
			var source strings.Builder
			for i := 1; i <= 5; i++ {
				heading, tail := fmt.Sprintf("# Section %d\n", i), fmt.Sprintf("\nTAIL_EVIDENCE_%d\n\n", i)
				source.WriteString(heading)
				source.WriteString(strings.Repeat("中", 4086-utf8.RuneCountInString(heading+tail)))
				source.WriteString(tail)
			}
			write := x.upload(t, owner, source.String())
			x.process(t)
			job := x.job(t, owner, write)
			parents := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 200))
			if len(parents) != 5 {
				t.Fatalf("fixture must produce five parents, got %d", len(parents))
			}
			expected := make(map[string]domain.IndexChunk, len(parents))
			markers := make(map[string]string, len(parents))
			for i, parent := range parents {
				marker := fmt.Sprintf("TAIL_EVIDENCE_%d", i+1)
				if utf8.RuneCountInString(parent.Content) != 4086 || !strings.Contains(parent.Content, marker) || utf8.RuneCountInString(parent.Content[:strings.Index(parent.Content, marker)]) < 2043 {
					t.Fatal("fixture does not place a match beyond the old prefix excerpt")
				}
				expected[parent.ID], markers[parent.ID] = parent, marker
			}
			writes, _, _ := x.search.snapshot()
			var matches []domain.IndexCandidate
			seenParent := map[string]bool{}
			for _, batch := range writes {
				for _, child := range batch.chunks {
					if strings.Contains(child.Content, "TAIL_EVIDENCE_") && !seenParent[child.ParentID] {
						matches = append(matches, domain.IndexCandidate{ChunkID: child.ID, IndexID: child.IndexID, Generation: batch.generation, Score: 1})
						seenParent[child.ParentID] = true
					}
				}
			}
			if len(matches) != 5 {
				t.Fatal("fixture must retrieve the tail child in each parent")
			}
			x.search.setSearch(func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
				return append([]domain.IndexCandidate(nil), matches...), nil
			})
			var mu sync.Mutex
			modelCalls := 0
			evidenceIDs := map[string]string{}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				request := harnessHTTPModelInput(t, r)
				fail := func(message string) {
					t.Error(message)
					http.Error(w, "controlled provider validation failed", http.StatusBadRequest)
				}
				if request.Stream != streaming {
					fail("real Harness used the wrong provider streaming mode")
					return
				}
				step := modelCalls
				modelCalls++
				if step == 0 {
					harnessHTTPModelReply(w, request, "", "search_knowledge", `{"query":"TAIL_EVIDENCE","mode":"keyword","limit":5}`)
					return
				}
				name := "search_knowledge"
				if step == 2 {
					name = "read_document"
				} else if step != 1 {
					fail("real Harness unexpectedly repeated model rounds")
					return
				}
				var content string
				for _, message := range request.Messages {
					if message.Role == "tool" && message.ToolCallID == "http-"+name {
						content = message.Content
					}
				}
				var result struct {
					Evidence []struct {
						ID           string `json:"evidence_id"`
						ChunkID      string `json:"chunk_id"`
						Content      string `json:"content"`
						IsExcerpt    bool   `json:"content_is_excerpt"`
						ExcerptStart int    `json:"excerpt_start_byte"`
						ExcerptEnd   int    `json:"excerpt_end_byte"`
					} `json:"evidence"`
				}
				if json.Unmarshal([]byte(content), &result) != nil || len(result.Evidence) != 5 || strings.Contains(content, `"evidence_focus"`) {
					fail(name + " did not produce five safe model-visible evidence items")
					return
				}
				seen := map[string]bool{}
				for _, evidence := range result.Evidence {
					parent, exists := expected[evidence.ChunkID]
					if !exists || seen[evidence.ChunkID] || len(evidence.ID) != 32 || !strings.Contains(evidence.Content, markers[evidence.ChunkID]) {
						fail(name + " lost a retrieved tail match or changed its parent identity")
						return
					}
					seen[evidence.ChunkID] = true
					start, end := evidence.ExcerptStart-parent.StartByte, evidence.ExcerptEnd-parent.StartByte
					if !evidence.IsExcerpt || start < 0 || end <= start || end > len(parent.Content) || parent.Content[start:end] != evidence.Content || !utf8.ValidString(evidence.Content) {
						fail(name + " excerpt offsets do not preserve exact UTF-8 parent bytes")
						return
					}
					if step == 1 {
						evidenceIDs[evidence.ChunkID] = evidence.ID
					} else if evidenceIDs[evidence.ChunkID] != evidence.ID {
						fail("read_document changed an already acquired parent evidence ID")
						return
					}
				}
				if step == 1 {
					arguments, _ := json.Marshal(map[string]any{"document_id": write.Document.ID, "query": "TAIL_EVIDENCE", "limit": 5})
					harnessHTTPModelReply(w, request, "", "read_document", string(arguments))
					return
				}
				var answer strings.Builder
				for _, parent := range parents {
					fmt.Fprintf(&answer, "%s [E:%s]\n", markers[parent.ID], evidenceIDs[parent.ID])
				}
				harnessHTTPModelReply(w, request, answer.String(), "", "")
			}))
			defer provider.Close()
			callback := "http://" + harnessHTTPAddress(t)
			service := newHarnessHTTPService(t, provider.URL, callback)
			client := harnessHTTPClient(t, service.url, callback)
			installHarnessHTTPClient(x, client)
			session := x.session(t, owner, write.Document.WorkspaceID, "five parent tails")
			input := map[string]any{"content": "列出五个模块的 TAIL_EVIDENCE，附每个来源。", "document_ids": []string{write.Document.ID}}
			var completed citedAnswer
			if streaming {
				result := harnessHTTPCompletedSSE(t, x, sessionPath(session)+"/messages", owner.Token, input)
				completed.Run, completed.Answer = result.Run, result.Answer
				citations := integrationData[struct {
					Citations []evidenceResponse `json:"citations"`
				}](t, x.request(t, "GET", sessionPath(session)+"/runs/"+result.Run.ID+"/citations", owner.Token, nil, 200))
				completed.Citations = citations.Citations
			} else {
				completed = integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, input, 201))
			}
			mu.Lock()
			calls := modelCalls
			mu.Unlock()
			if calls != 3 || len(completed.Citations) != 5 || completed.Run.Status != "completed" {
				t.Fatalf("real five-parent flow incomplete: calls=%d citations=%d status=%s", calls, len(completed.Citations), completed.Run.Status)
			}
			for _, citation := range completed.Citations {
				parent, exists := expected[citation.Evidence.ChunkID]
				if !exists || citation.Evidence.Content != parent.Content || citation.Evidence.StartByte != parent.StartByte || citation.Evidence.EndByte != parent.EndByte || citation.Evidence.VersionID != write.Version.ID || !strings.Contains(completed.Answer, markers[parent.ID]) {
					t.Fatal("display excerpt replaced the immutable full-parent citation")
				}
				loaded := integrationData[evidenceResponse](t, x.request(t, "GET", citation.PassageURL, owner.Token, nil, 200))
				if loaded.Evidence.Content != parent.Content || loaded.Evidence.ID != citation.Evidence.ID {
					t.Fatal("persisted parent citation resolved to a display excerpt")
				}
			}
		})
	}
}
