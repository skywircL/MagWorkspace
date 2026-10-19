package httpapi_test

import (
	"net/http"
	"reflect"
	"testing"

	"magworkspace/internal/domain"
)

func TestHTTPDocumentUpdatesRenameAndReindexText(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "document-update-owner")
	original := []byte("# 知识库\r\n第一版正文🙂\n")
	first := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(owner.Workspaces[0].ID), owner.Token, 201,
		documentFile("notes.Markdown", original), documentField("title", "初始标题")))
	x.process(t)
	job := x.job(t, owner, first)
	chunks := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(first, job), owner.Token, nil, 200))
	citation := indexChunksPath(first, job) + "/" + chunks[0].ID
	beforeEmbedding := x.embedder.calls.Load()
	x.objects.mu.Lock()
	beforeObjects := len(x.objects.data)
	x.objects.mu.Unlock()

	renamed := integrationData[domain.Document](t, x.request(t, "PATCH", documentPath(first.Document), owner.Token, map[string]string{"title": "  更新后的标题  "}, 200))
	if renamed.Title != "更新后的标题" || renamed.CurrentVersionID != first.Version.ID || renamed.CreatedBy != first.Document.CreatedBy || renamed.CreatedAt != first.Document.CreatedAt {
		t.Fatalf("rename changed immutable metadata: %+v", renamed)
	}
	if x.embedder.calls.Load() != beforeEmbedding || !reflect.DeepEqual(x.job(t, owner, first), job) {
		t.Fatal("rename started a new index")
	}
	x.objects.mu.Lock()
	afterObjects := len(x.objects.data)
	x.objects.mu.Unlock()
	if afterObjects != beforeObjects {
		t.Fatal("rename wrote document objects")
	}
	listed := x.request(t, "GET", documentPath(first.Document)+"/versions", owner.Token, nil, 200)
	if listed.Total != 1 {
		t.Fatal("rename created a content version")
	}
	result := x.find(t, owner.Token, first.Document.WorkspaceID, "hybrid")
	if len(result.Hits) == 0 || result.Hits[0].Title != renamed.Title {
		t.Fatal("search did not read the current document title")
	}

	content := "# 知识库第二版\r\n在线修改的原文🙂\t内容\n"
	second := integrationData[domain.DocumentWrite](t, x.request(t, "PUT", documentPath(first.Document)+"/content", owner.Token,
		map[string]string{"base_version_id": first.Version.ID, "content": content}, 200))
	assertDocumentVersion(t, second.Version, first.Document.ID, owner.User.ID, first.Version.Filename, []byte(content), 2)
	if second.Document.Title != renamed.Title || second.Document.ID != first.Document.ID || second.Document.CurrentVersionID != second.Version.ID {
		t.Fatalf("inline edit did not preserve document identity: %+v", second)
	}
	if queued := x.job(t, owner, second); queued.Status != "queued" || queued.CreatedBy != owner.User.ID {
		t.Fatalf("inline edit was not automatically queued: %+v", queued)
	}
	if hits := x.find(t, owner.Token, first.Document.WorkspaceID, "hybrid").Hits; len(hits) != 0 {
		t.Fatal("old version appeared as the new head before publication")
	}
	x.process(t)
	for _, mode := range []string{"lexical", "vector", "hybrid"} {
		hits := x.find(t, owner.Token, first.Document.WorkspaceID, mode).Hits
		if len(hits) == 0 {
			t.Fatalf("updated text not searchable: %s", mode)
		}
		for _, hit := range hits {
			if hit.Chunk.VersionID != second.Version.ID || hit.Chunk.SourceSHA256 != second.Version.SHA256 || hit.Title != renamed.Title {
				t.Fatalf("search returned stale update metadata: %+v", hit)
			}
		}
	}
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, original)
	assertDocumentDownload(t, x.integrationFixture, owner.Token, second.Document, second.Version, []byte(content))
	oldChunk := integrationData[domain.IndexChunk](t, x.request(t, "GET", citation, owner.Token, nil, 200))
	if !reflect.DeepEqual(oldChunk, chunks[0]) {
		t.Fatal("editing changed a fixed historical reference")
	}
	versions := x.request(t, "GET", documentPath(first.Document)+"/versions", owner.Token, nil, http.StatusOK)
	if versions.Total != 2 {
		t.Fatal("inline editing did not append exactly one immutable version")
	}
}
