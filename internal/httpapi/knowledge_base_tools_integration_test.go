package httpapi_test

import (
	"context"
	"errors"
	"fmt"
	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/store"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestHTTPKnowledgeBaseToolsNarrowRunScopeBeforeSearchAndPagination(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "kb-tool-owner")
	foreign := x.register(t, "kb-tool-foreign")
	wid := owner.Workspaces[0].ID
	first := createHTTPKnowledgeBase(t, x, owner, "Library one")
	second := createHTTPKnowledgeBase(t, x, owner, "Library two")
	outside := createHTTPKnowledgeBase(t, x, foreign, "Foreign")
	one := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, first.ID, "知识库唯一资料")
	x.process(t)
	two := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, second.ID, "知识库唯一资料")
	x.process(t)

	indexed, _, _ := x.search.snapshot()
	generationByDocument := map[string]string{}
	for _, write := range indexed {
		if len(write.chunks) > 0 {
			generationByDocument[write.chunks[0].DocumentID] = write.generation
		}
	}
	if generationByDocument[one.Document.ID] == "" || generationByDocument[two.Document.ID] == "" {
		t.Fatal("fixture did not index both knowledge bases")
	}
	for _, scenario := range []string{"unrestricted", "restricted", "none"} {
		t.Run(scenario, func(t *testing.T) {
			ss := x.session(t, owner, wid, scenario)
			x.agent.set(func(ctx context.Context, req agent.Request) (_ agent.Result, runErr error) {
				defer func() {
					if runErr != nil {
						t.Errorf("agent callback: %v", runErr)
					}
				}()
				var libs store.ToolKnowledgeBasePage
				if err := invokeKnowledgeTool(ctx, req, "list_knowledge_bases", map[string]any{"keyword": "Library", "limit": 1}, &libs); err != nil {
					return agent.Result{}, err
				}
				wantCount := 1
				if scenario == "none" {
					wantCount = 0
				}
				if len(libs.KnowledgeBases) != wantCount || libs.HasMore != (scenario == "unrestricted") {
					return agent.Result{}, fmt.Errorf("library scope/paging incorrect: %+v", libs)
				}
				for _, selection := range []struct {
					ids      []string
					expected int
				}{{[]string{first.ID, second.ID}, 2}, {[]string{first.ID}, 1}, {[]string{second.ID}, 1}, {[]string{}, 0}} {
					// A model filter may narrow the Run targets, never silently
					// discard forbidden libraries from a mixed selection.
					forbidden := scenario == "none" && len(selection.ids) > 0 || scenario == "restricted" && slices.Contains(selection.ids, second.ID)
					if forbidden {
						_, queriesBefore, _ := x.search.snapshot()
						embeddingsBefore := x.embedder.calls.Load()
						var result any
						if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{"knowledge_base_ids": selection.ids, "limit": 20}, &result); !errors.Is(err, domain.ErrForbidden) {
							return agent.Result{}, fmt.Errorf("directory accepted out-of-scope libraries: %v", err)
						}
						for _, mode := range []string{"semantic", "keyword", "hybrid"} {
							if err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]any{"query": "知识库", "knowledge_base_ids": selection.ids, "mode": mode, "limit": 10}, &result); !errors.Is(err, domain.ErrForbidden) {
								return agent.Result{}, fmt.Errorf("%s accepted out-of-scope libraries: %v", mode, err)
							}
						}
						_, queriesAfter, _ := x.search.snapshot()
						if len(queriesAfter) != len(queriesBefore) || x.embedder.calls.Load() != embeddingsBefore {
							return agent.Result{}, errors.New("out-of-scope library selection reached embedding or external recall")
						}
						continue
					}
					expected := selection.expected
					var docs store.ToolDocumentPage
					if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{"knowledge_base_ids": selection.ids, "limit": 20}, &docs); err != nil {
						return agent.Result{}, err
					}
					if len(docs.Documents) != expected {
						return agent.Result{}, fmt.Errorf("document scope escaped: %d != %d", len(docs.Documents), expected)
					}

					expectedGenerations := []string{}
					if scenario != "none" {
						if slices.Contains(selection.ids, first.ID) {
							expectedGenerations = append(expectedGenerations, generationByDocument[one.Document.ID])
						}
						if scenario == "unrestricted" && slices.Contains(selection.ids, second.ID) {
							expectedGenerations = append(expectedGenerations, generationByDocument[two.Document.ID])
						}
					}
					slices.Sort(expectedGenerations)
					for _, mode := range []string{"semantic", "keyword", "hybrid"} {
						_, queriesBefore, _ := x.search.snapshot()
						var result struct {
							Evidence []domain.Evidence `json:"evidence"`
						}
						if err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]any{"query": "知识库", "knowledge_base_ids": selection.ids, "mode": mode, "limit": 10}, &result); err != nil {
							return agent.Result{}, err
						}

						_, queriesAfter, _ := x.search.snapshot()
						actualQueries := queriesAfter[len(queriesBefore):]
						if (len(actualQueries) == 0) != (len(expectedGenerations) == 0) {
							return agent.Result{}, fmt.Errorf("external recall calls did not match empty/nonempty scope: %d queries for %d generations", len(actualQueries), len(expectedGenerations))
						}
						for _, query := range actualQueries {
							actualGenerations := append([]string{}, query.generations...)
							slices.Sort(actualGenerations)
							if query.workspace != wid || !slices.Equal(actualGenerations, expectedGenerations) {
								return agent.Result{}, fmt.Errorf("%s external recall escaped the narrowed Run targets: got %v want %v", mode, actualGenerations, expectedGenerations)
							}
						}
						if len(result.Evidence) != expected {
							return agent.Result{}, fmt.Errorf("%s sources %d != %d", mode, len(result.Evidence), expected)
						}
						for _, e := range result.Evidence {
							if scenario == "restricted" && e.DocumentID != one.Document.ID {
								return agent.Result{}, errors.New("document selection was broadened")
							}
						}
					}
				}
				for _, bad := range []any{nil, []string{"bad"}, []string{first.ID, first.ID}, []string{outside.ID}, []string{strings.Repeat("f", 32)}} {
					for _, name := range []string{"list_documents", "search_knowledge"} {
						args := map[string]any{"knowledge_base_ids": bad}
						if name == "search_knowledge" {
							args["query"] = "知识库"
						}
						var result any
						if err := invokeKnowledgeTool(ctx, req, name, args, &result); err == nil {
							return agent.Result{}, fmt.Errorf("%s accepted invalid library filter", name)
						}
					}
				}
				if scenario != "none" {
					var read knowledgeReadResponse
					if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]string{"document_id": one.Document.ID}, &read); err != nil {
						return agent.Result{}, err
					}
					if read.Document.KnowledgeBaseID != first.ID {
						return agent.Result{}, errors.New("read lost library provenance")
					}
				}
				return agent.Result{RunID: req.RunID, Status: "completed", Mode: "demo", Answer: "checked"}, nil
			})
			args := map[string]any{"content": "inspect"}
			if scenario == "restricted" {
				args["document_ids"] = []string{one.Document.ID}
			} else if scenario == "none" {
				args["document_ids"] = []string{}
			}
			x.request(t, "POST", sessionPath(ss)+"/messages", owner.Token, args, 201)
		})
	}
}

func TestHTTPKnowledgeBaseToolsDeleteDuringEmptyExternalSearch(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "kb-tool-delete")
	wid := owner.Workspaces[0].ID
	kb := createHTTPKnowledgeBase(t, x, owner, "Transient")
	uploadHTTPKnowledgeBase(t, x, owner.Token, wid, kb.ID, "知识库待删除正文")
	x.process(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	x.search.setSearch(func(ctx context.Context, _ indexTestQuery, _ []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return nil, indexGate(ctx, entered, release)
	})
	ss := x.session(t, owner, wid, "delete")
	x.agent.set(func(ctx context.Context, req agent.Request) (_ agent.Result, runErr error) {
		defer func() {
			if runErr != nil {
				t.Errorf("agent callback: %v", runErr)
			}
		}()
		var result any
		err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]any{"query": "知识库", "knowledge_base_ids": []string{kb.ID}}, &result)
		if !errors.Is(err, domain.ErrNotFound) {
			return agent.Result{}, fmt.Errorf("deleted selection not rejected: %v", err)
		}
		return agent.Result{RunID: req.RunID, Status: "completed", Mode: "demo", Answer: "source unavailable"}, nil
	})
	done := make(chan integrationResponse, 1)
	go func() {
		done <- x.call("POST", sessionPath(ss)+"/messages", owner.Token, map[string]string{"content": "inspect"})
	}()
	indexAwait(t, entered)
	x.request(t, "DELETE", knowledgeBasePath(wid, kb.ID), owner.Token, nil, 204)
	unblock()
	checkIntegrationResponse(t, documentUpdateResult(t, done), 201)
}
