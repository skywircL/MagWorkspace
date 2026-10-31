package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
	"magworkspace/internal/reranking"
)

type httpKnowledgeBase struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsDefault   bool   `json:"is_default"`
}

func knowledgeBasesPath(wid string) string    { return workspacePath(wid) + "/knowledge-bases" }
func knowledgeBasePath(wid, id string) string { return knowledgeBasesPath(wid) + "/" + id }

func createHTTPKnowledgeBase(t *testing.T, x *indexFixture, owner integrationAuth, name string) httpKnowledgeBase {
	t.Helper()
	return integrationData[httpKnowledgeBase](t, x.request(t, "POST", knowledgeBasesPath(owner.Workspaces[0].ID), owner.Token,
		map[string]string{"name": name, "description": "independent document collection"}, 201))
}

func uploadHTTPKnowledgeBase(t *testing.T, x *indexFixture, token, wid, kid, content string) domain.DocumentWrite {
	t.Helper()
	envelope := documentUpload(t, x.integrationFixture, knowledgeBasePath(wid, kid)+"/documents", token, 201,
		documentFile("notes.txt", []byte(content)))
	var binding struct {
		Document struct {
			KnowledgeBaseID string `json:"knowledge_base_id"`
		} `json:"document"`
	}
	if err := json.Unmarshal(envelope.Data, &binding); err != nil || binding.Document.KnowledgeBaseID != kid {
		t.Fatalf("uploaded document lost its knowledge base binding: %s, err=%v", envelope.Data, err)
	}
	return integrationData[domain.DocumentWrite](t, envelope)
}

func findHTTPKnowledgeBase(t *testing.T, x *indexFixture, token, wid, kid, mode string) indexSearchResponse {
	t.Helper()
	return integrationData[indexSearchResponse](t, x.request(t, "POST", knowledgeBasePath(wid, kid)+"/search", token,
		map[string]any{"query": "知识库", "mode": mode, "limit": 10, "rerank": false}, 200))
}

func TestHTTPKnowledgeBasesDefaultsAndPermissions(t *testing.T) {
	x := newIndexFixture(t)
	owner, reader, contributor, admin, outsider := x.register(t, "kb-owner"), x.register(t, "kb-reader"), x.register(t, "kb-contributor"), x.register(t, "kb-admin"), x.register(t, "kb-outsider")
	wid := owner.Workspaces[0].ID
	defaults := integrationData[[]httpKnowledgeBase](t, x.request(t, "GET", knowledgeBasesPath(wid), owner.Token, nil, 200))
	if len(defaults) != 1 || !defaults[0].IsDefault || defaults[0].WorkspaceID != wid || defaults[0].ID == "" {
		t.Fatalf("registration must create one default knowledge base: %+v", defaults)
	}
	createdWorkspace := integrationData[domain.Workspace](t, x.request(t, "POST", "/api/v1/workspaces", owner.Token, map[string]string{"name": "second workspace"}, 201))
	newDefaults := integrationData[[]httpKnowledgeBase](t, x.request(t, "GET", knowledgeBasesPath(createdWorkspace.ID), owner.Token, nil, 200))
	if len(newDefaults) != 1 || !newDefaults[0].IsDefault || newDefaults[0].WorkspaceID != createdWorkspace.ID {
		t.Fatalf("new workspace default missing: %+v", newDefaults)
	}
	for _, member := range []struct {
		auth integrationAuth
		role string
	}{{reader, "viewer"}, {contributor, "contributor"}, {admin, "admin"}} {
		x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": member.auth.User.Email, "role": member.role}, 201)
	}
	kb := createHTTPKnowledgeBase(t, x, owner, "research")
	for _, member := range []integrationAuth{reader, contributor, admin} {
		x.request(t, "GET", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), member.Token, nil, 200)
		x.request(t, "GET", knowledgeBasesPath(wid), member.Token, nil, 200)
	}
	for _, member := range []integrationAuth{reader, contributor} {
		x.request(t, "POST", knowledgeBasesPath(wid), member.Token, map[string]string{"name": "denied"}, 403)
		x.request(t, "PATCH", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), member.Token, map[string]string{"name": "denied", "description": "denied"}, 403)
		x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), member.Token, nil, 403)
	}
	updated := integrationData[httpKnowledgeBase](t, x.request(t, "PATCH", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), admin.Token,
		map[string]string{"name": "renamed research", "description": "updated"}, 200))
	if updated.ID != kb.ID || updated.Name != "renamed research" || updated.Description != "updated" {
		t.Fatalf("admin metadata update failed: %+v", updated)
	}
	for _, method := range []string{"GET", "PATCH", "DELETE"} {
		x.request(t, method, knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), outsider.Token, map[string]string{"name": "foreign", "description": "foreign"}, 404)
	}
	x.request(t, "GET", knowledgeBasesPath(wid), outsider.Token, nil, 404)
	x.request(t, "POST", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID)+"/search", outsider.Token, map[string]any{"query": "知识库"}, 404)
	documentUpload(t, x.integrationFixture, knowledgeBasePath(owner.Workspaces[0].ID, kb.ID)+"/documents", reader.Token, 403, documentFile("denied.txt", []byte("denied")))
	documentUpload(t, x.integrationFixture, knowledgeBasePath(owner.Workspaces[0].ID, kb.ID)+"/documents", outsider.Token, 404, documentFile("foreign.txt", []byte("foreign")))
	write := uploadHTTPKnowledgeBase(t, x, contributor.Token, owner.Workspaces[0].ID, kb.ID, "知识库贡献者上传")
	x.process(t)
	if len(findHTTPKnowledgeBase(t, x, reader.Token, owner.Workspaces[0].ID, kb.ID, "hybrid").Hits) == 0 {
		t.Fatal("workspace reader could not search knowledge base")
	}
	x.request(t, "DELETE", documentPath(write.Document), reader.Token, nil, 403)
	x.request(t, "DELETE", documentPath(write.Document), contributor.Token, nil, 204)
}

func TestHTTPKnowledgeBasesSearchIsolationDeletionAndCleanup(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "kb-isolation")
	wid := owner.Workspaces[0].ID
	firstKB, keptKB := createHTTPKnowledgeBase(t, x, owner, "removed collection"), createHTTPKnowledgeBase(t, x, owner, "retained collection")
	first := uploadHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, firstKB.ID, "知识库完全相同的内容")
	x.process(t)
	firstJob := x.job(t, owner, first)
	firstChunks := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(first, firstJob)+"?kind=child", owner.Token, nil, 200))
	if len(firstChunks) == 0 || firstChunks[0].Kind != "child" {
		t.Fatal("cross-knowledge-base injection requires a valid searchable child chunk")
	}
	kept := uploadHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, keptKB.ID, "知识库完全相同的内容")
	x.process(t)
	keptJob := x.job(t, owner, kept)
	for _, kb := range []struct{ id, document string }{{firstKB.ID, first.Document.ID}, {keptKB.ID, kept.Document.ID}} {
		listed := x.request(t, "GET", knowledgeBasePath(owner.Workspaces[0].ID, kb.id)+"/documents?page=1&page_size=1", owner.Token, nil, 200)
		docs := integrationData[[]domain.Document](t, listed)
		if len(docs) != 1 || docs[0].ID != kb.document || listed.Total != 1 {
			t.Fatalf("knowledge base document listing crossed collection: %+v total=%d", docs, listed.Total)
		}
	}
	// The external search adapter deliberately injects a valid chunk from the
	// other collection. SQL hydration must independently enforce the KB scope.
	x.search.setSearch(func(_ context.Context, _ indexTestQuery, candidates []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return append(candidates, domain.IndexCandidate{ChunkID: firstChunks[0].ID, IndexID: firstJob.ID, Generation: firstJob.Generation, Score: 100}), nil
	})
	for _, mode := range []string{"lexical", "vector", "hybrid"} {
		hits := findHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, keptKB.ID, mode).Hits
		if len(hits) == 0 {
			t.Fatalf("scoped search empty: %s", mode)
		}
		for _, hit := range hits {
			if hit.Chunk.DocumentID != kept.Document.ID {
				t.Fatalf("forged cross-KB candidate escaped scope: %+v", hit)
			}
		}
	}
	x.search.setSearch(nil)
	if len(x.find(t, owner.Token, wid, "hybrid").Hits) != 2 {
		t.Fatal("legacy workspace search no longer covers all collections")
	}
	session := x.session(t, owner, wid, "session survives collection deletion")
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "keep this conversation"}, 201)
	memory := integrationData[domain.Memory](t, x.request(t, "POST", memoryPath(wid), owner.Token,
		map[string]string{"kind": "fact", "content": "Keep my independent workspace memory"}, 201))
	x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, firstKB.ID), owner.Token, nil, 204)
	for _, path := range []string{knowledgeBasePath(owner.Workspaces[0].ID, firstKB.ID), knowledgeBasePath(owner.Workspaces[0].ID, firstKB.ID) + "/documents", documentPath(first.Document),
		documentPath(first.Document) + "/versions", documentVersionPath(first.Document, first.Version) + "/content", indexJobPath(first),
		indexChunksPath(first, firstJob), indexChunksPath(first, firstJob) + "/" + firstChunks[0].ID} {
		x.request(t, "GET", path, owner.Token, nil, 404)
	}
	x.request(t, "POST", knowledgeBasePath(owner.Workspaces[0].ID, firstKB.ID)+"/search", owner.Token, map[string]any{"query": "知识库"}, 404)
	documentUpload(t, x.integrationFixture, knowledgeBasePath(owner.Workspaces[0].ID, firstKB.ID)+"/documents", owner.Token, 404, documentFile("late.txt", []byte("late")))
	x.request(t, "PUT", documentPath(first.Document)+"/content", owner.Token, map[string]string{"base_version_id": first.Version.ID, "content": "late"}, 404)
	x.request(t, "POST", indexJobPath(first), owner.Token, map[string]any{}, 404)
	for _, path := range []string{workspacePath(wid), workspacePath(wid) + "/members", sessionPath(session), sessionPath(session) + "/messages", knowledgeBasePath(owner.Workspaces[0].ID, keptKB.ID)} {
		x.request(t, "GET", path, owner.Token, nil, 200)
	}
	memories := integrationData[[]domain.Memory](t, x.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(memories) != 1 || memories[0].ID != memory.ID {
		t.Fatalf("KB deletion changed unrelated private memory: %+v", memories)
	}
	if hits := x.find(t, owner.Token, wid, "hybrid").Hits; len(hits) != 1 || hits[0].Chunk.DocumentID != kept.Document.ID {
		t.Fatalf("deleted KB remained searchable: %+v", hits)
	}
	x.expireCleanup(t, firstJob.Generation)
	if err := x.service.CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := x.store.CleanupObjects(context.Background()); err != nil {
		t.Fatal(err)
	}
	writes, _, deleted := x.search.snapshot()
	if len(deleted) != 1 || deleted[0] != firstJob.Generation || len(writes) != 1 || writes[0].generation != keptJob.Generation {
		t.Fatalf("collection cleanup touched wrong generation: writes=%+v deleted=%v", writes, deleted)
	}
	x.objects.mu.Lock()
	remaining, removed := len(x.objects.data), len(x.objects.deleted)
	x.objects.mu.Unlock()
	if remaining != 1 || removed != 1 {
		t.Fatalf("collection source cleanup incomplete: remaining=%d deleted=%d", remaining, removed)
	}
	assertDocumentDownload(t, x.integrationFixture, owner.Token, kept.Document, kept.Version, []byte("知识库完全相同的内容"))
}

func TestHTTPKnowledgeBasesDeleteDuringExternalWork(t *testing.T) {
	for _, stage := range []string{"search", "search_empty", "rerank", "rerank_empty", "worker"} {
		t.Run(stage, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "kb-concurrent-"+stage)
			kb := createHTTPKnowledgeBase(t, x, owner, "removed while running")
			write := uploadHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, kb.ID, "知识库待删除内容")
			if stage != "worker" {
				x.process(t)
			}
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			if stage == "worker" {
				x.search.setUpsert(func(ctx context.Context) error { return indexGate(ctx, entered, release) })
				done := indexStartWorker(x)
				indexAwait(t, entered)
				x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), owner.Token, nil, 204)
				unblock()
				_ = indexWorkerResult(t, done)
				x.request(t, "GET", indexJobPath(write), owner.Token, nil, 404)
				if len(x.find(t, owner.Token, owner.Workspaces[0].ID, "hybrid").Hits) != 0 {
					t.Fatal("worker resurrected deleted knowledge base")
				}
				return
			}
			if strings.HasPrefix(stage, "search") {
				x.search.setSearch(func(ctx context.Context, _ indexTestQuery, candidates []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
					if stage == "search_empty" {
						candidates = nil
					}
					return candidates, indexGate(ctx, entered, release)
				})
			} else {
				attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, passages []string) ([]reranking.Score, error) {
					score := 0.95
					if stage == "rerank_empty" {
						score = 0.001
					}
					return rerankScores(passages, score), indexGate(ctx, entered, release)
				}))
			}
			done := make(chan integrationResponse, 1)
			go func() {
				done <- x.call("POST", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID)+"/search", owner.Token, map[string]any{"query": "知识库", "mode": "hybrid", "limit": 10})
			}()
			indexAwait(t, entered)
			x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, kb.ID), owner.Token, nil, 204)
			unblock()
			response := documentUpdateResult(t, done)
			if strings.HasSuffix(stage, "_empty") {
				// Empty model/adapter output must still observe the deleted
				// scope, rather than returning before the final DB check.
				checkIntegrationResponse(t, response, http.StatusNotFound)
				return
			}
			envelope := checkIntegrationResponse(t, response, http.StatusOK, http.StatusNotFound)
			if response.status == http.StatusOK && len(integrationData[indexSearchResponse](t, envelope).Hits) != 0 {
				t.Fatal("search returned content deleted during external I/O")
			}
		})
	}
}

func TestHTTPKnowledgeBasesUpdatedHeadHidesOldSearchAndKeepsHistory(t *testing.T) {
	for _, method := range []string{"edit", "upload"} {
		t.Run(method, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "kb-version-"+method)
			kb := createHTTPKnowledgeBase(t, x, owner, "versioned collection")
			first := uploadHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, kb.ID, "知识库旧版正文")
			x.process(t)
			firstJob := x.job(t, owner, first)
			chunks := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(first, firstJob), owner.Token, nil, 200))
			var next domain.DocumentWrite
			if method == "edit" {
				next = integrationData[domain.DocumentWrite](t, x.request(t, "PUT", documentPath(first.Document)+"/content", owner.Token,
					map[string]string{"base_version_id": first.Version.ID, "content": "知识库新版正文"}, 200))
			} else {
				next = integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentPath(first.Document)+"/versions", owner.Token, 201,
					documentFile("new.txt", []byte("知识库新版正文")), documentField("base_version_id", first.Version.ID)))
			}
			assertNoOldHead := func() {
				t.Helper()
				for _, mode := range []string{"lexical", "vector", "hybrid"} {
					if len(findHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, kb.ID, mode).Hits) != 0 {
						t.Fatalf("old head searchable before new publication: %s", mode)
					}
				}
				old := integrationData[domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(first, firstJob)+"/"+chunks[0].ID, owner.Token, nil, 200))
				if !reflect.DeepEqual(old, chunks[0]) {
					t.Fatal("replacing current head changed an immutable historical chunk")
				}
			}
			assertNoOldHead()
			x.search.setUpsert(func(context.Context) error { return &indexing.Failure{Code: "index_unavailable"} })
			if worked, err := x.service.ProcessOne(context.Background()); !worked || err == nil {
				t.Fatalf("failed indexing was not recorded: worked=%v err=%v", worked, err)
			}
			if x.job(t, owner, next).Status != "failed" {
				t.Fatal("new head was not marked failed")
			}
			assertNoOldHead()
			x.search.setUpsert(nil)
			x.request(t, "POST", indexJobPath(next), owner.Token, map[string]any{}, 202)
			x.process(t)
			hits := findHTTPKnowledgeBase(t, x, owner.Token, owner.Workspaces[0].ID, kb.ID, "hybrid").Hits
			if len(hits) == 0 || hits[0].Chunk.VersionID != next.Version.ID {
				t.Fatalf("retry did not publish new head: %+v", hits)
			}
		})
	}
}

func TestHTTPKnowledgeBasesDefaultPromotionAndExplicitEmptyState(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "kb-default-promotion")
	wid := owner.Workspaces[0].ID
	defaults := integrationData[[]httpKnowledgeBase](t, x.request(t, "GET", knowledgeBasesPath(wid), owner.Token, nil, 200))
	first, second := createHTTPKnowledgeBase(t, x, owner, "first survivor"), createHTTPKnowledgeBase(t, x, owner, "second survivor")
	x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, defaults[0].ID), owner.Token, nil, 204)
	promoted := integrationData[httpKnowledgeBase](t, x.request(t, "GET", knowledgeBasePath(owner.Workspaces[0].ID, first.ID), owner.Token, nil, 200))
	if !promoted.IsDefault {
		t.Fatal("deleting default did not promote earliest surviving collection")
	}
	legacy := x.upload(t, owner, "知识库兼容上传到新默认")
	listed := integrationData[[]domain.Document](t, x.request(t, "GET", knowledgeBasePath(owner.Workspaces[0].ID, first.ID)+"/documents", owner.Token, nil, 200))
	if len(listed) != 1 || listed[0].ID != legacy.Document.ID {
		t.Fatal("workspace upload did not use promoted default")
	}
	x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, first.ID), owner.Token, nil, 204)
	x.request(t, "DELETE", knowledgeBasePath(owner.Workspaces[0].ID, second.ID), owner.Token, nil, 204)
	if empty := integrationData[[]httpKnowledgeBase](t, x.request(t, "GET", knowledgeBasesPath(wid), owner.Token, nil, 200)); len(empty) != 0 {
		t.Fatalf("deleting final KB silently recreated it: %+v", empty)
	}
	documentUpload(t, x.integrationFixture, documentsPath(wid), owner.Token, 409, documentFile("no-default.txt", []byte("must create explicitly")))
	replacement := createHTTPKnowledgeBase(t, x, owner, "explicit replacement")
	if !replacement.IsDefault {
		t.Fatal("first explicitly recreated KB is not default")
	}
	x.upload(t, owner, "知识库恢复上传")
}
