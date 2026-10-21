package httpapi_test

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/reranking"
	"magworkspace/internal/store"
)

type indexTestObjects struct {
	mu         sync.Mutex
	data       map[string][]byte
	beforePut  func(context.Context, string) error
	beforeRead func(context.Context, string) error
	deleted    []string
}

func (b *indexTestObjects) Put(ctx context.Context, key string, data []byte) error {
	b.mu.Lock()
	hook := b.beforePut
	b.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.data[key]; exists {
		return blob.ErrIntegrity
	}
	b.data[key] = append([]byte(nil), data...)
	return nil
}
func (b *indexTestObjects) Read(ctx context.Context, key string, maximum int) ([]byte, error) {
	b.mu.Lock()
	hook := b.beforeRead
	b.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.data[key]
	if !ok {
		return nil, blob.ErrUnavailable
	}
	if len(data) > maximum {
		return nil, blob.ErrIntegrity
	}
	return append([]byte(nil), data...), nil
}
func (b *indexTestObjects) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, key)
	b.deleted = append(b.deleted, key)
	return nil
}
func (b *indexTestObjects) putHook(fn func(context.Context, string) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beforePut = fn
}
func (b *indexTestObjects) readHook(fn func(context.Context, string) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beforeRead = fn
}

type indexTestParser struct{ calls atomic.Int32 }

func (p *indexTestParser) Parse(ctx context.Context, _ domain.DocumentVersion, data []byte) (string, error) {
	p.calls.Add(1)
	return string(data), ctx.Err()
}

type indexTestEmbedder struct{ calls atomic.Int32 }

func (e *indexTestEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	e.calls.Add(1)
	result := make([][]float32, len(texts))
	for i, text := range texts {
		if strings.TrimSpace(text) == "" {
			return nil, &indexing.Failure{Code: "embedding_invalid"}
		}
		result[i] = []float32{1, 2, 3}
	}
	return result, ctx.Err()
}

type indexTestWrite struct {
	profile, generation, workspace string
	chunks                         []domain.IndexChunk
}
type indexTestQuery struct {
	profile, workspace, text, mode string
	generations                    []string
	vector                         []float32
	limit                          int
}
type indexTestSearch struct {
	mu          sync.Mutex
	writes      []indexTestWrite
	queries     []indexTestQuery
	deleted     []string
	upsertHook  func(context.Context) error
	searchHook  func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error)
	deleteError bool
}

func (s *indexTestSearch) Ensure(ctx context.Context, _ string, _ int) error { return ctx.Err() }
func (s *indexTestSearch) Upsert(ctx context.Context, profile, generation, workspace string, chunks []domain.IndexChunk, vectors [][]float32) error {
	if len(chunks) != len(vectors) {
		return errors.New("test mismatched vector count")
	}
	s.mu.Lock()
	s.writes = append(s.writes, indexTestWrite{profile, generation, workspace, append([]domain.IndexChunk(nil), chunks...)})
	hook := s.upsertHook
	s.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return ctx.Err()
}
func (s *indexTestSearch) Search(ctx context.Context, profile, workspace string, generations []string, text string, vector []float32, mode string, limit int) ([]domain.IndexCandidate, error) {
	q := indexTestQuery{profile, workspace, text, mode, append([]string(nil), generations...), append([]float32(nil), vector...), limit}
	s.mu.Lock()
	s.queries = append(s.queries, q)
	hook := s.searchHook
	allowed := map[string]bool{}
	for _, g := range generations {
		allowed[g] = true
	}
	result := []domain.IndexCandidate{}
	for _, w := range s.writes {
		if w.workspace == workspace && allowed[w.generation] {
			for _, c := range w.chunks {
				result = append(result, domain.IndexCandidate{ChunkID: c.ID, IndexID: c.IndexID, Generation: w.generation, Score: 1})
			}
		}
	}
	s.mu.Unlock()
	if hook != nil {
		return hook(ctx, q, result)
	}
	return result, ctx.Err()
}
func (s *indexTestSearch) DeleteGeneration(ctx context.Context, _ string, generation string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteError {
		return &indexing.Failure{Code: "index_unavailable"}
	}
	s.deleted = append(s.deleted, generation)
	kept := s.writes[:0]
	for _, w := range s.writes {
		if w.generation != generation {
			kept = append(kept, w)
		}
	}
	s.writes = kept
	return nil
}
func (s *indexTestSearch) setUpsert(fn func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertHook = fn
}
func (s *indexTestSearch) setSearch(fn func(context.Context, indexTestQuery, []domain.IndexCandidate) ([]domain.IndexCandidate, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.searchHook = fn
}
func (s *indexTestSearch) snapshot() ([]indexTestWrite, []indexTestQuery, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]indexTestWrite(nil), s.writes...), append([]indexTestQuery(nil), s.queries...), append([]string(nil), s.deleted...)
}

type indexFixture struct {
	*integrationFixture
	objects  *indexTestObjects
	search   *indexTestSearch
	parser   *indexTestParser
	embedder *indexTestEmbedder
	service  *indexing.Service
	profile  domain.IndexProfile
}

func newIndexFixture(t *testing.T) *indexFixture {
	t.Helper()
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	f.server.Close()
	f.server = nil
	f.store.Close()
	f.store = nil
	x := &indexFixture{integrationFixture: f, objects: &indexTestObjects{data: map[string][]byte{}}, search: &indexTestSearch{}, parser: &indexTestParser{}, embedder: &indexTestEmbedder{}, profile: indexing.Profile("offline-embedding", 3, "test-parser", "test-embedding", "test-vector", 256)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := store.Open(ctx, f.dsn, x.profile)
	if err != nil {
		t.Fatal(err)
	}
	f.store = db
	db.SetObjectStorage(x.objects)
	x.service = indexing.NewService(db, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{JobTimeout: 10 * time.Second})
	f.config.Indexing = x.service
	f.server = httptest.NewServer(httpapi.New(db, f.agent, f.config))
	return x
}
func (x *indexFixture) upload(t *testing.T, owner integrationAuth, text string) domain.DocumentWrite {
	t.Helper()
	return integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(owner.Workspaces[0].ID), owner.Token, 201, documentFile("知识库.txt", []byte(text))))
}
func indexJobPath(write domain.DocumentWrite) string {
	return documentVersionPath(write.Document, write.Version) + "/index"
}
func indexChunksPath(write domain.DocumentWrite, job domain.IndexJob) string {
	return documentVersionPath(write.Document, write.Version) + "/indexes/" + job.ID + "/chunks"
}
func (x *indexFixture) job(t *testing.T, owner integrationAuth, write domain.DocumentWrite) domain.IndexJob {
	t.Helper()
	return integrationData[domain.IndexJob](t, x.request(t, "GET", indexJobPath(write), owner.Token, nil, 200))
}
func (x *indexFixture) process(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	worked, err := x.service.ProcessOne(ctx)
	if err != nil || !worked {
		t.Fatalf("worker worked=%v err=%v", worked, err)
	}
}

func (x *indexFixture) expireCleanup(t *testing.T, generation string) {
	t.Helper()
	// Advance only this generation in the fixture's isolated database. Production
	// cleanup must first allow uncertain remote writes to finish their grace period.
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, `UPDATE index_cleanup SET not_before=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE profile_id=? AND generation=?`, x.profile.ID, generation)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("cleanup generation was not durably queued: count=%d err=%v", count, err)
	}
}

func (x *indexFixture) assertCleanupGrace(t *testing.T) {
	t.Helper()
	if err := x.service.CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _, deleted := x.search.snapshot()
	if len(deleted) != 0 {
		t.Fatalf("generation deleted before late-write grace elapsed: %v", deleted)
	}
}

type indexSearchResponse struct {
	Mode      string             `json:"mode"`
	ProfileID string             `json:"profile_id"`
	Hits      []domain.SearchHit `json:"hits"`
}

func (x *indexFixture) find(t *testing.T, token, workspace, mode string) indexSearchResponse {
	t.Helper()
	return integrationData[indexSearchResponse](t, x.request(t, "POST", workspacePath(workspace)+"/search", token, map[string]any{"query": "知识库", "mode": mode, "limit": 10}, 200))
}
func indexAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("external boundary was not reached")
	}
}
func indexWorkerResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not finish")
		return nil
	}
}
func indexStartWorker(x *indexFixture) <-chan error {
	ch := make(chan error, 1)
	go func() {
		worked, err := x.service.ProcessOne(context.Background())
		if err == nil && !worked {
			err = errors.New("worker did not claim uploaded job")
		}
		ch <- err
	}()
	return ch
}
func indexGate(ctx context.Context, entered chan<- struct{}, release <-chan struct{}) error {
	entered <- struct{}{}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHTTPIndexesPublishAfterSearchAndSQLChunks(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "index-publish")
	text := strings.Repeat("知识库保存可信原文。", 100)
	write := x.upload(t, owner, text)
	queued := x.job(t, owner, write)
	if queued.Status != "queued" || queued.SourceSHA256 != write.Version.SHA256 || queued.CreatedBy != owner.User.ID || queued.Attempts != 0 {
		t.Fatalf("upload was not atomically queued: %+v", queued)
	}
	upEntered, upRelease := make(chan struct{}, 1), make(chan struct{})
	var upOnce sync.Once
	defer upOnce.Do(func() { close(upRelease) })
	x.search.setUpsert(func(ctx context.Context) error { return indexGate(ctx, upEntered, upRelease) })
	done := indexStartWorker(x)
	indexAwait(t, upEntered)
	job := x.job(t, owner, write)
	if job.Status != "running" || job.ChunkCount != 0 || job.ParentCount != 0 {
		t.Fatal("published before external persistence", job)
	}
	x.request(t, "GET", indexChunksPath(write, job), owner.Token, nil, 409)
	if hits := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(hits) != 0 {
		t.Fatal("unpublished generation searchable")
	}
	upOnce.Do(func() { close(upRelease) })
	if err := indexWorkerResult(t, done); err != nil {
		t.Fatal(err)
	}
	ready := x.job(t, owner, write)
	if ready.Status != "ready" || ready.ChunkCount < 2 || ready.Attempts != 1 || ready.Generation == "" {
		t.Fatalf("missing durable publication: %+v", ready)
	}
	page := x.request(t, "GET", indexChunksPath(write, ready)+"?page=1&page_size=1", owner.Token, nil, 200)
	chunks := integrationData[[]domain.IndexChunk](t, page)
	if page.Total != ready.ParentCount || len(chunks) != 1 || chunks[0].IndexID != ready.ID || chunks[0].VersionID != write.Version.ID || chunks[0].SourceSHA256 != write.Version.SHA256 {
		t.Fatalf("invalid manifest chunk: %+v", chunks)
	}
	chunk := integrationData[domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, ready)+"/"+chunks[0].ID, owner.Token, nil, 200))
	if !reflect.DeepEqual(chunk, chunks[0]) {
		t.Fatal("single chunk does not match immutable manifest")
	}
	result := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid")
	if result.Mode != "hybrid" || result.ProfileID != x.profile.ID || len(result.Hits) != ready.ParentCount {
		t.Fatalf("hybrid result=%+v", result)
	}
	for _, hit := range result.Hits {
		if hit.Chunk.Content != text[hit.Chunk.StartByte:hit.Chunk.EndByte] || hit.Chunk.IndexID != ready.ID || hit.Score <= 0 {
			t.Fatalf("result is not canonical: %+v", hit)
		}
	}
	_, queries, _ := x.search.snapshot()
	if len(queries) != 2 || queries[0].mode != "lexical" || queries[1].mode != "vector" || len(queries[1].vector) != 3 {
		t.Fatalf("hybrid did not use both routes: %+v", queries)
	}
	for _, q := range queries {
		if q.workspace != write.Document.WorkspaceID || !reflect.DeepEqual(q.generations, []string{ready.Generation}) {
			t.Fatalf("search scope=%+v", q)
		}
	}
	if len(x.agent.calls()) != 0 {
		t.Fatal("indexing invoked conversation agent")
	}
}

func TestHTTPIndexesIgnoreWhitespaceOnlySearchChunks(t *testing.T) {
	for _, test := range []struct{ name, text string }{
		{"between", "知识库正文" + strings.Repeat("\n", 1600) + "知识库结尾"},
		{"leading", strings.Repeat(" \t　", 600) + "知识库正文"},
		{"trailing", "知识库正文" + strings.Repeat(" \t　", 600)},
	} {
		t.Run(test.name, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "index-whitespace-"+test.name)
			write := x.upload(t, owner, test.text)
			x.process(t)
			job := x.job(t, owner, write)
			if job.Status != "ready" {
				t.Fatalf("valid document did not become ready: %+v", job)
			}
			response := x.request(t, "GET", indexChunksPath(write, job)+"?kind=child&page_size=100", owner.Token, nil, 200)
			chunks := integrationData[[]domain.IndexChunk](t, response)
			blank := 0
			for _, chunk := range chunks {
				if chunk.Content != test.text[chunk.StartByte:chunk.EndByte] {
					t.Fatal("canonical span changed")
				}
				if strings.TrimSpace(chunk.Content) == "" {
					blank++
				}
			}
			if blank == 0 || len(chunks) != job.ChunkCount {
				t.Fatal("fixture did not preserve whitespace spans in its immutable manifest")
			}
			writes, _, _ := x.search.snapshot()
			if len(writes) != 1 || len(writes[0].chunks) != len(chunks)-blank {
				t.Fatal("search index did not exclude only whitespace spans")
			}
			for _, chunk := range writes[0].chunks {
				if strings.TrimSpace(chunk.Content) == "" {
					t.Fatal("whitespace-only chunk reached the search index")
				}
			}
			result := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid")
			if len(result.Hits) != job.ParentCount {
				t.Fatal("nonempty document chunks were lost from search")
			}
			for _, hit := range result.Hits {
				if strings.TrimSpace(hit.Chunk.Content) == "" || hit.Chunk.Content != test.text[hit.Chunk.StartByte:hit.Chunk.EndByte] {
					t.Fatal("search result lost canonical nonempty text")
				}
			}
		})
	}
}

func TestHTTPIndexesViewerScopeAndCanonicalCandidates(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "index-scope-owner")
	viewer := x.register(t, "index-scope-viewer")
	outsider := x.register(t, "index-outsider")
	wid := owner.Workspaces[0].ID
	x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
	write := x.upload(t, owner, "知识库当前版本权威正文")
	x.process(t)
	job := x.job(t, owner, write)
	foreign := x.upload(t, outsider, "foreign secret must never appear")
	x.process(t)
	foreignJob := x.job(t, outsider, foreign)
	writes, _, _ := x.search.snapshot()
	var local, other domain.IndexCandidate
	for _, w := range writes {
		c := w.chunks[0]
		candidate := domain.IndexCandidate{ChunkID: c.ID, IndexID: c.IndexID, Generation: w.generation, Score: 1000}
		if w.workspace == wid {
			local = candidate
		} else {
			other = candidate
		}
	}
	x.request(t, "POST", indexJobPath(write), viewer.Token, map[string]any{}, 403)
	x.request(t, "GET", indexJobPath(write), viewer.Token, nil, 200)
	x.request(t, "GET", indexChunksPath(write, job), viewer.Token, nil, 200)
	x.request(t, "GET", strings.Replace(indexChunksPath(write, job), wid, outsider.Workspaces[0].ID, 1), outsider.Token, nil, 404)
	x.request(t, "GET", indexChunksPath(write, foreignJob), owner.Token, nil, 404)
	x.request(t, "GET", indexChunksPath(write, job)+"/"+other.ChunkID, owner.Token, nil, 404)
	x.search.setSearch(func(_ context.Context, _ indexTestQuery, _ []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		return []domain.IndexCandidate{
			other, {ChunkID: strings.Repeat("f", 32), IndexID: job.ID, Generation: job.Generation, Score: 999},
			{ChunkID: strings.Repeat("e", 32), IndexID: "forged", Generation: job.Generation, Score: 999}, local,
		}, nil
	})
	got := x.find(t, viewer.Token, wid, "hybrid").Hits
	if len(got) != 1 || got[0].Chunk.Content != "知识库当前版本权威正文" || (len(got[0].MatchedChildIDs) != 1 || got[0].MatchedChildIDs[0] != local.ChunkID) {
		t.Fatalf("untrusted hints escaped manifest scope: %+v", got)
	}
	x.search.setSearch(func(_ context.Context, _ indexTestQuery, _ []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
		forged := local
		forged.Generation = foreignJob.Generation
		return []domain.IndexCandidate{forged}, nil
	})
	if got := x.find(t, viewer.Token, wid, "lexical").Hits; len(got) != 0 {
		t.Fatalf("stale generation accepted: %+v", got)
	}
	_, before, _ := x.search.snapshot()
	x.request(t, "POST", workspacePath(wid)+"/search", outsider.Token, map[string]any{"query": "知识库"}, 404)
	_, after, _ := x.search.snapshot()
	if len(before) != len(after) {
		t.Fatal("foreign member reached external search")
	}
	for _, body := range []map[string]any{{"query": "知识库", "user_id": owner.User.ID}, {"query": "知识库", "mode": "unknown"}, {"query": "x"}, {"query": "知识库", "limit": 51}} {
		x.request(t, "POST", workspacePath(wid)+"/search", viewer.Token, body, 400)
	}
}

func TestHTTPIndexesRevalidateAfterRemoteReads(t *testing.T) {
	for _, boundary := range []string{"search", "rerank"} {
		for _, change := range []string{"revoke", "new head", "delete"} {
			t.Run(boundary+"/"+change, func(t *testing.T) {
				x := newIndexFixture(t)
				owner := x.register(t, "index-race-owner")
				viewer := x.register(t, "index-race-viewer")
				wid := owner.Workspaces[0].ID
				x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": viewer.User.Email, "role": "viewer"}, 201)
				write := x.upload(t, owner, "知识库发生并发变更前的内容")
				x.process(t)
				entered, release := make(chan struct{}, 1), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				if boundary == "search" {
					x.search.setSearch(func(ctx context.Context, _ indexTestQuery, c []domain.IndexCandidate) ([]domain.IndexCandidate, error) {
						if err := indexGate(ctx, entered, release); err != nil {
							return nil, err
						}
						return c, nil
					})
				} else {
					attachIntegrationReranker(t, x, integrationReranker(func(ctx context.Context, _ string, passages []string) ([]reranking.Score, error) {
						if err := indexGate(ctx, entered, release); err != nil {
							return nil, err
						}
						return rerankScores(passages, .9), nil
					}))
				}
				result := make(chan integrationResponse, 1)
				go func() {
					result <- x.call("POST", workspacePath(wid)+"/search", viewer.Token, map[string]any{"query": "知识库", "mode": "lexical"})
				}()
				indexAwait(t, entered)
				switch change {
				case "revoke":
					x.request(t, "DELETE", workspacePath(wid)+"/members/"+viewer.User.ID, owner.Token, nil, 204)
				case "new head":
					documentUpload(t, x.integrationFixture, documentPath(write.Document)+"/versions", owner.Token, 201, documentFile("new.txt", []byte("新的当前版本")), documentField("base_version_id", write.Version.ID))
				case "delete":
					x.request(t, "DELETE", documentPath(write.Document), owner.Token, nil, 204)
				}
				once.Do(func() { close(release) })
				select {
				case response := <-result:
					if change == "revoke" {
						checkIntegrationResponse(t, response, 404)
					} else {
						hits := integrationData[indexSearchResponse](t, checkIntegrationResponse(t, response, 200)).Hits
						if len(hits) != 0 {
							t.Fatalf("stale result escaped final filtering: %+v", hits)
						}
					}
				case <-time.After(10 * time.Second):
					t.Fatal("search did not finish")
				}
			})
		}
	}
}

func TestHTTPIndexesFailureRetryAndCleanup(t *testing.T) {
	for _, stage := range []string{"search", "publication"} {
		t.Run(stage, func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "index-retry")
			write := x.upload(t, owner, "知识库失败后重试内容")
			if stage == "search" {
				x.search.setUpsert(func(context.Context) error { return &indexing.Failure{Code: "index_unavailable"} })
			} else {
				outboxSQL(t, x, `CREATE TRIGGER fail_chunk_publish BEFORE INSERT ON chunks FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='chunk write outage'`)
			}
			worked, err := x.service.ProcessOne(context.Background())
			if !worked || err == nil {
				t.Fatalf("failure was hidden: worked=%v err=%v", worked, err)
			}
			failed := x.job(t, owner, write)
			wantCode := "index_unavailable"
			if stage == "publication" {
				wantCode = "publication_unavailable"
			}
			if failed.Status != "failed" || failed.ErrorCode != wantCode || failed.Generation == "" {
				t.Fatalf("wrong failed state: %+v", failed)
			}
			if got := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(got) != 0 {
				t.Fatal("failed generation searchable")
			}
			x.search.setUpsert(nil)
			if stage == "publication" {
				outboxSQL(t, x, "DROP TRIGGER fail_chunk_publish")
			}
			queued := integrationData[domain.IndexJob](t, x.request(t, "POST", indexJobPath(write), owner.Token, map[string]any{}, 202))
			if queued.ID != failed.ID || queued.Status != "queued" {
				t.Fatalf("retry changed job identity: %+v", queued)
			}
			x.process(t)
			ready := x.job(t, owner, write)
			if ready.Status != "ready" || ready.Attempts != 2 || ready.Generation == failed.Generation {
				t.Fatalf("retry did not fence old generation: %+v", ready)
			}
			x.assertCleanupGrace(t)
			x.expireCleanup(t, failed.Generation)
			x.search.mu.Lock()
			x.search.deleteError = true
			x.search.mu.Unlock()
			if err := x.service.CleanupOnce(context.Background()); err == nil {
				t.Fatal("cleanup failure was ignored")
			}
			x.search.mu.Lock()
			x.search.deleteError = false
			x.search.mu.Unlock()
			// Failed cleanups now have durable backoff; advance this fixture only.
			x.expireCleanup(t, failed.Generation)
			if err := x.service.CleanupOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, _, deleted := x.search.snapshot()
			if !reflect.DeepEqual(deleted, []string{failed.Generation}) {
				t.Fatalf("cleanup touched wrong generation: %v", deleted)
			}
			if got := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(got) != 1 || got[0].Chunk.IndexID != ready.ID {
				t.Fatalf("cleanup removed current generation: %+v", got)
			}
		})
	}
}

func TestHTTPIndexesFixedVersionChunksAndSQLIntegrity(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "index-history")
	write := x.upload(t, owner, "知识库不可变历史内容")
	x.process(t)
	first := x.job(t, owner, write)
	oldChunks := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, first), owner.Token, nil, 200))
	second := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentPath(write.Document)+"/versions", owner.Token, 201, documentFile("new.txt", []byte("知识库新的当前内容")), documentField("base_version_id", write.Version.ID)))
	x.process(t)
	current := x.job(t, owner, second)
	if x.job(t, owner, write).Status != "ready" {
		t.Fatal("new version changed immutable old publication")
	}
	again := integrationData[[]domain.IndexChunk](t, x.request(t, "GET", indexChunksPath(write, first), owner.Token, nil, 200))
	if !reflect.DeepEqual(oldChunks, again) {
		t.Fatal("historical chunks changed after append")
	}
	x.request(t, "GET", indexChunksPath(second, first), owner.Token, nil, 404)
	if got := x.find(t, owner.Token, write.Document.WorkspaceID, "hybrid").Hits; len(got) != 1 || got[0].Chunk.VersionID != second.Version.ID {
		t.Fatalf("search included historical head: %+v", got)
	}
	// MinIO outage cannot break published SQL chunk reads.
	x.objects.readHook(func(context.Context, string) error { return blob.ErrUnavailable })
	x.request(t, "GET", indexChunksPath(second, current), owner.Token, nil, 200)
	if got := x.find(t, owner.Token, write.Document.WorkspaceID, "lexical").Hits; len(got) != 1 {
		t.Fatal("SQL read depends on MinIO")
	}
	outboxSQL(t, x, "UPDATE chunks SET content=REPEAT('x',OCTET_LENGTH(content)) WHERE index_id=? AND kind='parent'", current.ID)
	x.request(t, "GET", indexChunksPath(second, current), owner.Token, nil, 409)
	x.request(t, "POST", workspacePath(write.Document.WorkspaceID)+"/search", owner.Token, map[string]any{"query": "知识库", "mode": "lexical"}, 409)
	x.request(t, "DELETE", documentPath(write.Document), owner.Token, nil, 204)
	x.request(t, "GET", indexChunksPath(write, first), owner.Token, nil, 404)
	if got := x.find(t, owner.Token, write.Document.WorkspaceID, "lexical").Hits; len(got) != 0 {
		t.Fatal("deleted document remained searchable")
	}
}

func TestHTTPIndexesWriterRevokedBeforePublication(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "index-admission-owner")
	writer := x.register(t, "index-admission-writer")
	wid := owner.Workspaces[0].ID
	x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": writer.User.Email, "role": "contributor"}, 201)
	write := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(wid), writer.Token, 201, documentFile("draft.txt", []byte("知识库撤权时尚未发布"))))
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.search.setUpsert(func(ctx context.Context) error { return indexGate(ctx, entered, release) })
	done := indexStartWorker(x)
	indexAwait(t, entered)
	x.request(t, "DELETE", workspacePath(wid)+"/members/"+writer.User.ID, owner.Token, nil, 204)
	once.Do(func() { close(release) })
	if err := indexWorkerResult(t, done); err == nil {
		t.Fatal("revoked writer published an index")
	}
	failed := x.job(t, owner, write)
	if failed.Status != "failed" || failed.ErrorCode != "authorization_changed" {
		t.Fatalf("wrong revoked-worker terminal state: %+v", failed)
	}
	if got := x.find(t, owner.Token, wid, "hybrid").Hits; len(got) != 0 {
		t.Fatal("revoked worker output searchable")
	}
	x.assertCleanupGrace(t)
	x.expireCleanup(t, failed.Generation)
	if err := x.service.CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _, deleted := x.search.snapshot()
	if !reflect.DeepEqual(deleted, []string{failed.Generation}) {
		t.Fatalf("revoked generation not cleaned: %v", deleted)
	}
}
