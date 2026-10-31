package httpapi_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"magworkspace/internal/agent"
	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/reranking"
)

func parentChildFixtureText() string {
	return "# 第一模块\n" + strings.Repeat("甲", 3000) + "。第一模块末尾证据必须完整保留。\n\n" +
		"# 第二模块\n" + strings.Repeat("乙", 3000) + "。第二模块同样必须提供完整上下文。\n"
}

func TestHTTPParentChildRetrievalScoresChildrenAndReturnsDistinctParents(t *testing.T) {
	for _, mode := range []string{"disabled", "rerank", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "parent-child-"+mode)
			var calls atomic.Int32
			if mode != "disabled" {
				attachIntegrationReranker(t, x, integrationReranker(func(_ context.Context, _ string, passages []string) ([]reranking.Score, error) {
					calls.Add(1)
					if len(passages) != 4 {
						t.Errorf("model received %d candidates, want four children", len(passages))
					}
					for _, passage := range passages {
						// Scoring includes the child plus its title and heading path.
						// The 384-rune cap applies to canonical child content, not metadata.
						if utf8.RuneCountInString(passage) > domain.ChunkRunes+100 || !strings.Contains(passage, "知识库") {
							t.Error("reranker received a parent or lost title enrichment")
						}
					}
					if mode == "fallback" {
						return nil, reranking.ErrModelUnavailable
					}
					return rerankScores(passages, .9), nil
				}))
			}
			source := parentChildFixtureText()
			write := x.upload(t, owner, source)
			x.process(t)
			job := x.job(t, owner, write)
			parents := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 200))
			children := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job)+"?kind=child&page_size=100", owner.Token, nil, 200))
			if len(parents) != 2 || len(children) < 4 || job.ParentCount != 2 || job.ChunkCount != len(children) {
				t.Fatalf("missing parent-child storage: parents=%d children=%d job=%+v", len(parents), len(children), job)
			}
			parentByID := map[string]domain.IndexChunk{}
			for _, parent := range parents {
				if parent.Kind != "parent" || parent.ParentID != "" || parent.Content != source[parent.StartByte:parent.EndByte] || utf8.RuneCountInString(parent.Content) <= domain.ChunkRunes {
					t.Fatal("reading view is not the complete parent source")
				}
				parentByID[parent.ID] = parent
			}
			var siblings, other []domain.IndexChunk
			for _, child := range children {
				if child.Kind != "child" || child.ParentID == "" || utf8.RuneCountInString(child.Content) > domain.ChunkRunes || child.Content != source[child.StartByte:child.EndByte] {
					t.Fatal("child storage lost canonical source identity")
				}
				if child.ParentID == parents[0].ID {
					siblings = append(siblings, child)
				} else {
					other = append(other, child)
				}
			}
			if len(siblings) < 3 || len(other) == 0 {
				t.Fatal("fixture lacks sibling competition and a distinct parent")
			}
			selected := []domain.IndexChunk{siblings[len(siblings)-1], siblings[0], siblings[1], other[0]}
			candidates := make([]domain.IndexCandidate, len(selected))
			for i, child := range selected {
				candidates[i] = domain.IndexCandidate{ChunkID: child.ID, IndexID: job.ID, Generation: job.Generation, Score: 1 - float64(i)*.1}
			}
			x.search.setSearch(func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
				return append([]domain.IndexCandidate(nil), candidates...), nil
			})
			writes, _, _ := x.search.snapshot()
			indexed := 0
			for _, batch := range writes {
				for _, chunk := range batch.chunks {
					indexed++
					if chunk.Kind != "child" || chunk.ParentID == "" || utf8.RuneCountInString(chunk.Content) > domain.ChunkRunes {
						t.Fatal("parent content was embedded or written to the retrieval engine")
					}
				}
			}
			if indexed != len(children) {
				t.Fatalf("retrieval engine contains %d chunks, want %d children", indexed, len(children))
			}
			got := integrationData[rerankSearchResponse](t, x.request(t, "POST", workspacePath(write.Document.WorkspaceID)+"/search", owner.Token,
				map[string]any{"query": "模块末尾证据", "mode": "lexical", "limit": 2}, 200))
			if len(got.Hits) != 2 || got.Hits[0].Chunk.ID == got.Hits[1].Chunk.ID {
				t.Fatalf("siblings consumed distinct-parent Top K: %+v", got)
			}
			seenTail := false
			for _, hit := range got.Hits {
				parent, exists := parentByID[hit.Chunk.ID]
				if !exists || hit.Chunk.Kind != "parent" || hit.Chunk.Content != parent.Content || hit.Chunk.SHA256 != parent.SHA256 || len(hit.MatchedChildIDs) == 0 {
					t.Fatal("retrieval response did not preserve complete parent evidence")
				}
				seenTail = seenTail || strings.Contains(hit.Chunk.Content, "第一模块末尾证据必须完整保留")
			}
			if !seenTail || mode != "disabled" && calls.Load() != 1 || mode == "rerank" && !got.Rerank.Applied || mode == "fallback" && !got.Rerank.FallbackUsed {
				t.Fatalf("child scoring or tail context missing: calls=%d diagnostic=%+v", calls.Load(), got.Rerank)
			}
		})
	}
}

func TestHTTPParentChildSQLReadsSurviveObjectOutageAndPinHistory(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "parent-child-outage")
	source := parentChildFixtureText()
	write := x.upload(t, owner, source)
	x.process(t)
	job := x.job(t, owner, write)
	parents := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 200))
	if len(parents) != 2 {
		t.Fatal("fixture needs two complete parents")
	}
	var reads atomic.Int32
	x.objects.readHook(func(context.Context, string) error {
		reads.Add(1)
		return blob.ErrUnavailable
	})
	list := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 200))
	if len(list) != len(parents) || list[0].Content != parents[0].Content {
		t.Fatal("SQL parent read depends on object storage")
	}
	x.request(t, "GET", indexChunksPath(write, job)+"?kind=child", owner.Token, nil, 200)
	if hits := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(hits) != 2 {
		t.Fatal("search still depends on MinIO manifests")
	}
	session := x.session(t, owner, write.Document.WorkspaceID, "SQL parent citation")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		var result struct {
			Evidence []domain.Evidence `json:"evidence"`
		}
		if err := invokeKnowledgeTool(ctx, request, "search_knowledge", map[string]any{"query": "模块证据", "mode": "keyword", "limit": 2}, &result); err != nil {
			return agent.Result{}, err
		}
		if len(result.Evidence) != 2 {
			return agent.Result{}, errors.New("tool failed SQL parent expansion")
		}
		var target domain.Evidence
		for _, evidence := range result.Evidence {
			if evidence.ChunkID == parents[0].ID {
				target = evidence
			}
		}
		if target.ID == "" || target.Content != parents[0].Content || !strings.Contains(target.Content, "第一模块末尾证据必须完整保留") {
			return agent.Result{}, errors.New("tool truncated the complete parent or lost its tail")
		}
		var read knowledgeReadResponse
		if err := invokeKnowledgeTool(ctx, request, "read_document", map[string]any{"evidence_id": target.ID, "context": 1}, &read); err != nil {
			return agent.Result{}, err
		}
		if len(read.Evidence) != 2 || read.Evidence[0].Content != parents[0].Content || read.Evidence[1].Content != parents[1].Content {
			return agent.Result{}, errors.New("parent neighbors are not canonical complete evidence")
		}
		return agent.Result{RunID: request.RunID, Answer: "第一模块末尾证据必须完整保留。[E:" + target.ID + "]", Mode: "demo"}, nil
	})
	answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "模块证据"}, 201))
	if len(answer.Citations) != 1 || answer.Citations[0].Evidence.ChunkID != parents[0].ID || answer.Citations[0].Evidence.Content != parents[0].Content {
		t.Fatalf("parent citation not committed: %+v", answer)
	}
	citation := answer.Citations[0]
	x.request(t, "GET", citation.PassageURL, owner.Token, nil, 200)
	if reads.Load() != 0 {
		t.Fatalf("search, read_document or fixed citation attempted %d MinIO reads", reads.Load())
	}
	x.objects.readHook(nil)
	updated := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentPath(write.Document)+"/versions", owner.Token, 201,
		documentField("base_version_id", write.Version.ID), documentFile("新版.txt", []byte("新版父块证据。"))))
	x.process(t)
	if updated.Version.ID == write.Version.ID {
		t.Fatal("new version did not advance")
	}
	x.objects.readHook(func(context.Context, string) error { return blob.ErrUnavailable })
	fixed := integrationData[evidenceResponse](t, x.request(t, "GET", citation.PassageURL, owner.Token, nil, 200))
	if fixed.Evidence.Content != parents[0].Content || fixed.Evidence.VersionID != write.Version.ID {
		t.Fatal("historical parent evidence followed the current version")
	}
	foreign := x.register(t, "parent-child-foreign")
	x.request(t, "GET", citation.PassageURL, foreign.Token, nil, 404)
	x.request(t, "DELETE", documentPath(write.Document), owner.Token, nil, 204)
	x.request(t, "GET", citation.PassageURL, owner.Token, nil, 404)
	if hits := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(hits) != 0 {
		t.Fatal("deleted document remains retrievable")
	}
}

func TestHTTPParentChildRejectsInjectedParentAsSearchCandidate(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "parent-child-injected")
	write := x.upload(t, owner, "父块只能作为已验证子块的上下文返回。")
	x.process(t)
	job := x.job(t, owner, write)
	parents := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 200))
	x.search.setSearch(func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return []domain.IndexCandidate{{ChunkID: parents[0].ID, IndexID: job.ID, Generation: job.Generation, Score: 100}}, nil
	})
	got := x.find(t, owner.Token, write.Document.WorkspaceID, "lexical")
	if len(got.Hits) != 0 {
		t.Fatalf("retrieval engine promoted an injected parent: %+v", got.Hits)
	}
}
