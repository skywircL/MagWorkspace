package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/store"
)

type knowledgeReadResponse struct {
	store.ToolDocument
	Evidence   []domain.Evidence `json:"evidence"`
	NextOffset *int              `json:"next_offset"`
	HasMore    bool              `json:"has_more"`
}

func invokeKnowledgeTool(ctx context.Context, req agent.Request, name string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	value, err := req.ToolHandler(ctx, name, raw)
	if err != nil {
		return err
	}
	raw, err = json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, output)
}

func TestHTTPKnowledgeToolsReadSearchNeighborsAndPinnedPagination(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "knowledgeflow")
	write := x.upload(t, owner, strings.Repeat("国内住宿报销每晚500元。\n", 300))
	x.process(t)
	hidden := x.upload(t, owner, "未选择的内部制度")
	x.process(t)
	session := x.session(t, owner, write.Document.WorkspaceID, "tools")
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		var list store.ToolDocumentPage
		if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{"limit": 1}, &list); err != nil {
			return agent.Result{}, err
		}
		if len(list.Documents) != 1 || list.Documents[0].Document.ID != write.Document.ID || list.HasMore {
			return agent.Result{}, errors.New("list scope escaped")
		}
		var read knowledgeReadResponse
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": write.Document.ID, "limit": 1}, &read); err != nil {
			return agent.Result{}, err
		}
		if len(read.Evidence) != 1 || read.NextOffset == nil || *read.NextOffset != 1 || read.Index == nil {
			return agent.Result{}, errors.New("first read lacks evidence/cursor")
		}
		first := read.Evidence[0]
		for _, mode := range []string{"semantic", "keyword", "hybrid"} {
			var result struct {
				Evidence []domain.Evidence `json:"evidence"`
			}
			if err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]any{"query": "住宿报销", "mode": mode, "limit": 2}, &result); err != nil {
				return agent.Result{}, err
			}
			if len(result.Evidence) != 2 {
				return agent.Result{}, fmt.Errorf("%s returned %d passages", mode, len(result.Evidence))
			}
			for _, e := range result.Evidence {
				if e.DocumentID != write.Document.ID || e.Content == "" {
					return agent.Result{}, errors.New("search source escaped")
				}
			}
		}
		// The head changes after the first read. A fixed cursor must keep old bytes.
		if _, err := x.store.AppendDocumentVersion(ctx, owner.User.ID, write.Document.WorkspaceID, write.Document.ID, write.Version.ID, "new.txt", []byte("新版600元")); err != nil {
			return agent.Result{}, err
		}
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": write.Document.ID, "version_id": read.Version.ID, "index_id": read.Index.ID, "offset": 1, "limit": 1}, &read); err != nil {
			return agent.Result{}, err
		}
		if len(read.Evidence) != 1 || read.Evidence[0].VersionID != write.Version.ID || read.Evidence[0].ID == first.ID {
			return agent.Result{}, errors.New("cursor switched head or repeated first chunk")
		}
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"evidence_id": first.ID, "context": 1}, &read); err != nil {
			return agent.Result{}, err
		}
		if len(read.Evidence) != 2 || read.Evidence[0].ID != first.ID {
			return agent.Result{}, errors.New("neighbors are not original evidence")
		}
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": write.Document.ID, "version_id": read.Version.ID, "index_id": read.Index.ID, "query": "住宿 500", "context": 1, "limit": 1}, &read); err != nil {
			return agent.Result{}, err
		}
		if len(read.Evidence) == 0 || !read.HasMore {
			return agent.Result{}, errors.New("in-document literal query failed")
		}
		for _, args := range []map[string]any{
			{"document_id": write.Document.ID, "offset": 1},
			{"document_id": write.Document.ID, "version_id": write.Version.ID},
			{"evidence_id": first.ID, "document_id": write.Document.ID},
			{"document_id": write.Document.ID, "limit": 0},
			{"document_id": write.Document.ID, "context": 4},
			{"document_id": write.Document.ID, "workspace_id": "forged"},
		} {
			if err := invokeKnowledgeTool(ctx, req, "read_document", args, &read); !errors.Is(err, domain.ErrInvalid) {
				return agent.Result{}, fmt.Errorf("invalid args accepted: %v", err)
			}
		}
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": hidden.Document.ID}, &read); !errors.Is(err, domain.ErrForbidden) {
			return agent.Result{}, errors.New("unselected metadata readable")
		}
		return agent.Result{RunID: req.RunID, Answer: "住宿500元。[E:" + first.ID + "]", Mode: "demo"}, nil
	})
	answer := integrationData[citedAnswer](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "读制度", "document_ids": []string{write.Document.ID}}, 201))
	if len(answer.Citations) != 1 || answer.Citations[0].Evidence.VersionID != write.Version.ID {
		t.Fatal("final answer failed immutable citation")
	}
	_, queries, _ := x.search.snapshot()
	var lexical, vector int
	for _, q := range queries {
		if q.mode == "lexical" {
			lexical++
		}
		if q.mode == "vector" {
			vector++
		}
	}
	if lexical != 2 || vector != 2 {
		t.Fatalf("mode translation lexical=%d vector=%d", lexical, vector)
	}
}

func TestHTTPKnowledgeToolsMetadataWithoutIndexingAndHistoryScope(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "metawithoutindex")
	write := integrationData[domain.DocumentWrite](t, documentUpload(t, f, documentsPath(owner.Workspaces[0].ID), owner.Token, 201, documentFile("制度.md", []byte("住宿"))))
	session := f.session(t, owner, write.Document.WorkspaceID, "metadata")
	f.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if req.Query == "followup" {
			if len(req.History) != 0 {
				return agent.Result{}, errors.New("metadata source survived empty scope")
			}
			return agent.Result{RunID: req.RunID, Answer: "无历史", Mode: "demo"}, nil
		}
		var page store.ToolDocumentPage
		if err := invokeKnowledgeTool(ctx, req, "list_documents", map[string]any{}, &page); err != nil {
			return agent.Result{}, err
		}
		if len(page.Documents) != 1 || page.Documents[0].Index != nil {
			return agent.Result{}, errors.New("metadata list needs indexing")
		}
		var read knowledgeReadResponse
		if err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": write.Document.ID}, &read); err != nil {
			return agent.Result{}, err
		}
		if read.Version.ID != write.Version.ID || read.Index != nil || len(read.Evidence) != 0 {
			return agent.Result{}, errors.New("unindexed metadata lost")
		}
		if err := invokeKnowledgeTool(ctx, req, "search_knowledge", map[string]any{"query": "住宿"}, &read); !errors.Is(err, agent.ErrUnavailable) {
			return agent.Result{}, errors.New("disabled search not unavailable")
		}
		return agent.Result{RunID: req.RunID, Answer: "制度.md", Mode: "demo"}, nil
	})
	f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "list"}, 201)
	f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "followup", "document_ids": []string{}}, 201)
}

func TestHTTPKnowledgeToolsRecheckDeletionAfterObjectRead(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "readdelete")
	write := x.upload(t, owner, "住宿报销500元")
	x.process(t)
	session := x.session(t, owner, write.Document.WorkspaceID, "read race")
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		x.objects.readHook(func(ctx context.Context, key string) error {
			x.objects.readHook(nil)
			return x.store.DeleteDocument(ctx, owner.User.ID, write.Document.WorkspaceID, write.Document.ID)
		})
		var read knowledgeReadResponse
		err := invokeKnowledgeTool(ctx, req, "read_document", map[string]any{"document_id": write.Document.ID}, &read)
		if !errors.Is(err, domain.ErrNotFound) {
			return agent.Result{}, fmt.Errorf("deleted source exposed: %v", err)
		}
		return agent.Result{RunID: req.RunID, Answer: "source deleted", Mode: "demo"}, nil
	})
	// Even a model answer without a citation cannot publish after its observed
	// metadata source was deleted while the document tool was running.
	f := x.integrationFixture
	response := f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "read"}, 502)
	if response.Error.Code != "invalid_citation" {
		t.Fatalf("deleted metadata was not rejected at finalization: %s", response.Error.Code)
	}
}
