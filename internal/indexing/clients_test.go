package indexing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

const (
	testProfile       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testGeneration    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testWorkspace     = "cccccccccccccccccccccccccccccccc"
	testIndex         = "dddddddddddddddddddddddddddddddd"
	testChunk         = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	testKnowledgeBase = "11111111111111111111111111111111"
	testDocument      = "22222222222222222222222222222222"
	testVersion       = "33333333333333333333333333333333"
)

func requireClientError(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || ErrorCode(err) != code {
		t.Fatalf("error=%v, want safe code %s", err, code)
	}
}

func TestEmbeddingBatchesAndResponseOrder(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Errorf("wrong embedding request: %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Model          string   `json:"model"`
			Input          []string `json:"input"`
			EncodingFormat string   `json:"encoding_format"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request.Model != "test-model" || request.EncodingFormat != "float" {
			t.Errorf("invalid request schema: %+v %v", request, err)
		}
		wantCount := min(embeddingBatchSize, 19-calls*embeddingBatchSize)
		if len(request.Input) != wantCount {
			t.Errorf("batch length=%d want=%d", len(request.Input), wantCount)
		}
		var data []any
		for i := len(request.Input) - 1; i >= 0; i-- {
			ordinal := calls*embeddingBatchSize + i
			if request.Input[i] != fmt.Sprintf("chunk %d", ordinal) {
				t.Errorf("request order changed: %s", request.Input[i])
			}
			data = append(data, map[string]any{"index": i, "embedding": []float32{float32(ordinal + 1), 0.5}})
		}
		calls++
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer server.Close()
	embedder, err := NewEmbedder(server.URL+"/v1/", "test-secret", "test-model", 2)
	if err != nil {
		t.Fatal(err)
	}
	texts := make([]string, 19)
	for i := range texts {
		texts[i] = fmt.Sprintf("chunk %d", i)
	}
	got, err := embedder.Embed(context.Background(), texts)
	if err != nil || len(got) != len(texts) || calls != 3 {
		t.Fatalf("result count=%d calls=%d err=%v", len(got), calls, err)
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], []float32{float32(i + 1), 0.5}) {
			t.Fatalf("response index was not preserved at %d: %v", i, got[i])
		}
	}
}

func TestEmbeddingRejectsIncompleteOrInvalidResponses(t *testing.T) {
	for _, test := range []struct{ name, body string }{
		{"wrong dimension", `{"data":[{"index":0,"embedding":[1]}]}`},
		{"missing index", `{"data":[{"embedding":[1,2]}]}`},
		{"out of range", `{"data":[{"index":1,"embedding":[1,2]}]}`},
		{"negative index", `{"data":[{"index":-1,"embedding":[1,2]}]}`},
		{"partial", `{"data":[]}`},
		{"null vector", `{"data":[{"index":0,"embedding":null}]}`},
		{"null value", `{"data":[{"index":0,"embedding":[null,1]}]}`},
		{"zero", `{"data":[{"index":0,"embedding":[0,0]}]}`},
		{"NaN", `{"data":[{"index":0,"embedding":[NaN,1]}]}`},
		{"infinity", `{"data":[{"index":0,"embedding":[1e999,1]}]}`},
		{"float32 overflow", `{"data":[{"index":0,"embedding":[1e100,1]}]}`},
		{"float32 underflow", `{"data":[{"index":0,"embedding":[1e-100,0]}]}`},
		{"provider error", `{"data":[{"index":0,"embedding":[1,2]}],"error":{"message":"secret-provider-body"}}`},
		{"malformed", `{`},
		{"trailing JSON", `{"data":[{"index":0,"embedding":[1,2]}]} {}`},
		{"oversized", strings.Repeat(" ", maxProviderResponse+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, test.body) }))
			defer server.Close()
			embedder, err := NewEmbedder(server.URL, "", "model", 2)
			if err != nil {
				t.Fatal(err)
			}
			result, err := embedder.Embed(context.Background(), []string{"synthetic"})
			requireClientError(t, err, "embedding_invalid")
			if result != nil {
				t.Fatalf("invalid response returned partial vectors: %v", result)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"data":[{"index":0,"embedding":[1,2]},{"index":0,"embedding":[3,4]}]}`)
	}))
	defer server.Close()
	embedder, _ := NewEmbedder(server.URL, "", "model", 2)
	_, err := embedder.Embed(context.Background(), []string{"one", "two"})
	requireClientError(t, err, "embedding_invalid")
}

func TestEmbeddingFailureDoesNotReturnEarlierBatch(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "private provider details")
			return
		}
		data := make([]any, embeddingBatchSize)
		for i := range data {
			data[i] = map[string]any{"index": i, "embedding": []int{1, 2}}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer server.Close()
	embedder, _ := NewEmbedder(server.URL, "secret", "model", 2)
	texts := make([]string, embeddingBatchSize+1)
	for i := range texts {
		texts[i] = "synthetic"
	}
	result, err := embedder.Embed(context.Background(), texts)
	requireClientError(t, err, "embedding_unavailable")
	if result != nil || calls != 2 || err.Error() != "embedding_unavailable" {
		t.Fatalf("partial result or unsafe error: calls=%d result=%v err=%v", calls, result, err)
	}
}

func TestProviderCancellationRedirectAndConfiguration(t *testing.T) {
	for _, base := range []string{"", "ftp://localhost", "http://", "http://user:secret@localhost", "http://localhost/?key=secret", "http://localhost/?", "http://localhost/#", "http://localhost/#fragment", " http://localhost"} {
		if _, err := NewEmbedder(base, "", "model", 2); err == nil {
			t.Fatalf("unsafe embedding URL accepted: %q", base)
		}
		if _, err := NewSearchStore(base, ""); err == nil {
			t.Fatalf("unsafe search URL accepted: %q", base)
		}
	}
	for _, dimensions := range []int{0, -1, maxVectorDimensions + 1} {
		if _, err := NewEmbedder("http://localhost", "", "model", dimensions); err == nil {
			t.Fatalf("invalid dimension accepted: %d", dimensions)
		}
	}
	var redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	embedder, _ := NewEmbedder(origin.URL, "private-key", "model", 2)
	_, err := embedder.Embed(context.Background(), []string{"synthetic"})
	requireClientError(t, err, "embedding_unavailable")
	if redirected.Load() != 0 || embedder.(*httpEmbedder).http.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("redirect or environment proxy was enabled")
	}
	for _, service := range []string{"embedding", "index"} {
		t.Run(service, func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if service == "embedding" {
					client, _ := NewEmbedder(server.URL, "", "model", 2)
					_, err := client.Embed(ctx, []string{"synthetic"})
					done <- err
				} else {
					client, _ := NewSearchStore(server.URL, "")
					done <- client.DeleteGeneration(ctx, testProfile, testGeneration)
				}
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("provider request did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation was lost: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("provider request ignored cancellation")
			}
		})
	}
}

func testMapping() map[string]any {
	mapping := searchMapping(2)
	return map[string]any{searchIndex(testProfile): map[string]any{"mappings": mapping}}
}

func testChunks(count int) ([]domain.IndexChunk, [][]float32) {
	chunks := make([]domain.IndexChunk, count)
	vectors := make([][]float32, count)
	for i := range chunks {
		chunks[i] = domain.IndexChunk{ID: fmt.Sprintf("%032x", i+1), IndexID: testIndex, Generation: testGeneration,
			WorkspaceID: testWorkspace, KnowledgeBaseID: testKnowledgeBase, DocumentID: testDocument, VersionID: testVersion,
			Content: fmt.Sprintf("知识库合成片段 %d", i)}
		vectors[i] = []float32{1, float32(i + 1)}
	}
	return chunks, vectors
}

func testBulkResult(chunks []domain.IndexChunk) map[string]any {
	items := make([]any, len(chunks))
	for i, chunk := range chunks {
		items[i] = map[string]any{"index": map[string]any{"_index": searchIndex(testProfile), "_id": chunk.ID, "status": 201,
			"_shards": map[string]int{"total": 1, "successful": 1, "failed": 0}}}
	}
	return map[string]any{"errors": false, "items": items}
}

func testSearchHit(chunk string, score float64) map[string]any {
	return map[string]any{"_index": searchIndex(testProfile), "_id": chunk, "_score": score,
		"_source": map[string]any{"chunk_id": chunk, "index_id": testIndex, "generation_id": testGeneration}}
}

func testSearchResult(hits ...any) map[string]any {
	if hits == nil {
		hits = []any{}
	}
	return map[string]any{"timed_out": false, "_shards": map[string]int{"total": 1, "successful": 1, "failed": 0}, "hits": map[string]any{"hits": hits}}
}

func assertSameJSON(t *testing.T, actual, expected []byte) {
	t.Helper()
	var a, b any
	if json.Unmarshal(actual, &a) != nil || json.Unmarshal(expected, &b) != nil || !reflect.DeepEqual(a, b) {
		t.Errorf("JSON differs: actual=%s expected=%s", actual, expected)
	}
}

func TestSearchEnsureCreatesAndVerifiesSchema(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		if r.Header.Get("Authorization") != "ApiKey test-search-secret" {
			t.Error("wrong Elasticsearch authorization")
		}
		switch len(requests) {
		case 1:
			w.WriteHeader(404)
		case 2:
			var request struct {
				Settings map[string]int             `json:"settings"`
				Mappings map[string]json.RawMessage `json:"mappings"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.Settings["number_of_shards"] != 1 || request.Settings["number_of_replicas"] != 0 || string(request.Mappings["dynamic"]) != `"strict"` {
				t.Errorf("wrong index settings: %+v", request)
			}
			var properties map[string]map[string]any
			json.Unmarshal(request.Mappings["properties"], &properties)
			if len(properties) != 5 || properties["content"]["analyzer"] != "cjk" || properties["content"]["similarity"] != "BM25" || properties["embedding"] != nil {
				t.Errorf("wrong search mapping: %v", properties)
			}
			for _, field := range []string{"chunk_id", "index_id", "workspace_id", "generation_id"} {
				if properties[field]["type"] != "keyword" {
					t.Errorf("scope field %s is not keyword", field)
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"acknowledged": true, "shards_acknowledged": true, "index": searchIndex(testProfile)})
		case 3:
			json.NewEncoder(w).Encode(testMapping())
		default:
			t.Errorf("unexpected request: %s", r.URL)
		}
	}))
	defer server.Close()
	client, _ := NewSearchStore(server.URL, "test-search-secret")
	if err := client.Ensure(context.Background(), testProfile, 2); err != nil {
		t.Fatal(err)
	}
	path := "/" + searchIndex(testProfile)
	if !reflect.DeepEqual(requests, []string{"GET " + path + "/_mapping", "PUT " + path, "GET " + path + "/_mapping"}) {
		t.Fatalf("wrong ensure sequence: %v", requests)
	}
}

func TestSearchEnsureRejectsMismatchedMapping(t *testing.T) {
	for _, kind := range []string{"analyzer", "similarity", "unexpected vector", "scope type", "scope index", "text index", "dynamic", "missing metadata", "wrong index", "empty", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			mapping := searchMapping(2)
			fields := mapping["properties"].(map[string]any)
			text := fields["content"].(map[string]string)
			switch kind {
			case "analyzer":
				text["analyzer"] = "standard"
			case "similarity":
				text["similarity"] = "boolean"
			case "unexpected vector":
				fields["embedding"] = map[string]any{"type": "dense_vector"}
			case "scope type":
				fields["workspace_id"] = map[string]string{"type": "text"}
			case "scope index":
				fields["workspace_id"] = map[string]any{"type": "keyword", "index": false}
			case "text index":
				fields["content"] = map[string]any{"type": "text", "analyzer": "cjk", "similarity": "BM25", "index": false}
			case "dynamic":
				mapping["dynamic"] = "true"
			case "missing metadata":
				delete(fields, "index_id")
			}
			name := searchIndex(testProfile)
			if kind == "wrong index" {
				name = "other"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("mismatched mapping was mutated")
				}
				if kind == "malformed" {
					io.WriteString(w, "{")
					return
				}
				if kind == "empty" {
					io.WriteString(w, "{}")
					return
				}
				json.NewEncoder(w).Encode(map[string]any{name: map[string]any{"mappings": mapping}})
			}))
			defer server.Close()
			client, _ := NewSearchStore(server.URL, "")
			requireClientError(t, client.Ensure(context.Background(), testProfile, 2), "index_invalid")
		})
	}
}

func TestSearchEnsureConcurrentCreationAndIncompleteAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		body    string
		code    string
		lookups int
	}{
		{"race", 400, `{"error":{"type":"resource_already_exists_exception"}}`, "", 2},
		{"unrelated bad request", 400, `{"error":{"type":"mapper_parsing_exception"}}`, "index_unavailable", 1},
		{"missing acknowledgement", 200, `{}`, "index_invalid", 1},
		{"pending shards", 200, `{"acknowledged":true,"shards_acknowledged":false,"index":"mw_` + testProfile + `"}`, "index_invalid", 1},
		{"wrong index", 200, `{"acknowledged":true,"shards_acknowledged":true,"index":"other"}`, "index_invalid", 1},
		{"unavailable", 503, "provider-private-details", "index_unavailable", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookups := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					lookups++
					if lookups == 1 {
						w.WriteHeader(404)
						return
					}
					json.NewEncoder(w).Encode(testMapping())
				} else {
					w.WriteHeader(test.status)
					io.WriteString(w, test.body)
				}
			}))
			defer server.Close()
			client, _ := NewSearchStore(server.URL, "")
			err := client.Ensure(context.Background(), testProfile, 2)
			if test.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				requireClientError(t, err, test.code)
			}
			if lookups != test.lookups {
				t.Errorf("mapping lookups=%d want=%d", lookups, test.lookups)
			}
		})
	}
}

func TestSearchBulkBatchesAndScopedQueries(t *testing.T) {
	chunks, vectors := testChunks(35)
	upserted, calls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost {
			t.Error("expected POST")
		}
		if strings.HasSuffix(r.URL.Path, "/_bulk") {
			if r.Header.Get("Content-Type") != "application/x-ndjson" || r.URL.Query().Get("refresh") != "wait_for" || r.URL.Query().Get("include_source_on_error") != "false" {
				t.Errorf("wrong bulk headers/refresh: %s", r.URL)
			}
			body, _ := io.ReadAll(r.Body)
			if len(body) == 0 || body[len(body)-1] != '\n' {
				t.Error("missing trailing newline")
			}
			lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
			count := min(searchBatchSize, len(chunks)-upserted)
			if len(lines) != count*2 {
				t.Fatalf("bulk lines=%d want=%d", len(lines), count*2)
			}
			for i := 0; i < count; i++ {
				var metadata map[string]map[string]string
				json.Unmarshal([]byte(lines[2*i]), &metadata)
				var document searchDocument
				dec := json.NewDecoder(strings.NewReader(lines[2*i+1]))
				dec.DisallowUnknownFields()
				if err := dec.Decode(&document); err != nil {
					t.Error(err)
				}
				chunk := chunks[upserted+i]
				if metadata["index"]["_id"] != chunk.ID || document.ChunkID != chunk.ID || document.IndexID != testIndex || document.GenerationID != testGeneration || document.WorkspaceID != testWorkspace || document.Content != chunk.Content {
					t.Errorf("bulk identity/content differs: %+v", document)
				}
			}
			json.NewEncoder(w).Encode(testBulkResult(chunks[upserted : upserted+count]))
			upserted += count
			return
		}
		if r.URL.Path != "/"+searchIndex(testProfile)+"/_search" || r.URL.Query().Get("allow_partial_search_results") != "false" {
			t.Errorf("wrong search endpoint: %s", r.URL)
		}
		var request map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&request)
		assertSameJSON(t, request["_source"], []byte(`["chunk_id","index_id","generation_id"]`))
		if string(request["size"]) != "3" || string(request["track_total_hits"]) != "false" {
			t.Errorf("wrong search size: %v", request)
		}
		filter := `[{"term":{"workspace_id":"` + testWorkspace + `"}},{"terms":{"generation_id":["` + testGeneration + `"]}}]`
		if calls == 3 {
			if _, ok := request["knn"]; ok {
				t.Error("lexical query included vector OR branch")
			}
			assertSameJSON(t, request["query"], []byte(`{"bool":{"filter":`+filter+`,"must":{"match":{"content":{"query":"知识库 * : 查询"}}}}}`))
		} else {
			if _, ok := request["query"]; ok {
				t.Error("vector query included unfiltered lexical OR branch")
			}
			assertSameJSON(t, request["knn"], []byte(`{"field":"embedding","query_vector":[1,2],"k":3,"num_candidates":100,"filter":{"bool":{"filter":`+filter+`}}}`))
		}
		json.NewEncoder(w).Encode(testSearchResult(testSearchHit(chunks[0].ID, 0.5), testSearchHit(chunks[1].ID, 0.8)))
	}))
	defer server.Close()
	client, _ := NewSearchStore(server.URL, "")
	if err := client.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, chunks, vectors); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"lexical"} {
		got, err := client.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "知识库 * : 查询", []float32{1, 2}, mode, 3)
		want := []domain.IndexCandidate{{ChunkID: chunks[1].ID, IndexID: testIndex, Generation: testGeneration, Score: 0.8}, {ChunkID: chunks[0].ID, IndexID: testIndex, Generation: testGeneration, Score: 0.5}}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s search=%v err=%v", mode, got, err)
		}
		got, err = client.Search(context.Background(), testProfile, testWorkspace, nil, "synthetic", []float32{1, 2}, mode, 3)
		if err != nil || len(got) != 0 {
			t.Fatalf("empty generation result=%v err=%v", got, err)
		}
	}
	if calls != 3 || upserted != len(chunks) {
		t.Fatalf("calls=%d indexed=%d", calls, upserted)
	}
}

func TestSearchBulkPartialFailures(t *testing.T) {
	for _, kind := range []string{"errors", "missing errors", "partial", "wrong ID", "wrong index", "failed item", "error body", "missing shards", "failed shards", "no successful shard", "wrong operation", "malformed", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			chunks, vectors := testChunks(2)
			response := testBulkResult(chunks)
			items := response["items"].([]any)
			item := items[0].(map[string]any)["index"].(map[string]any)
			switch kind {
			case "errors":
				response["errors"] = true
			case "missing errors":
				delete(response, "errors")
			case "partial":
				response["items"] = items[:1]
			case "wrong ID":
				item["_id"] = testChunk
			case "wrong index":
				item["_index"] = "other"
			case "failed item":
				item["status"] = 400
			case "error body":
				item["error"] = map[string]string{"reason": "private-provider-detail"}
			case "missing shards":
				delete(item, "_shards")
			case "failed shards":
				item["_shards"].(map[string]int)["failed"] = 1
			case "no successful shard":
				item["_shards"].(map[string]int)["successful"] = 0
			case "wrong operation":
				items[0] = map[string]any{"create": item}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if kind == "malformed" {
					io.WriteString(w, "{")
					return
				}
				if kind == "oversized" {
					io.WriteString(w, strings.Repeat(" ", maxProviderResponse+1))
					return
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client, _ := NewSearchStore(server.URL, "")
			err := client.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, chunks, vectors)
			requireClientError(t, err, "index_invalid")
			if err.Error() != "index_invalid" {
				t.Fatal("unsafe provider error")
			}
		})
	}
}

func TestSearchLateBulkFailureStopsLaterBatches(t *testing.T) {
	chunks, vectors := testChunks(searchBatchSize*2 + 1)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			json.NewEncoder(w).Encode(testBulkResult(chunks[:searchBatchSize]))
		} else {
			io.WriteString(w, `{"errors":true,"items":[]}`)
		}
	}))
	defer server.Close()
	client, _ := NewSearchStore(server.URL, "")
	requireClientError(t, client.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, chunks, vectors), "index_invalid")
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestSearchRejectsWrongScopePartialAndInvalidResponses(t *testing.T) {
	for _, kind := range []string{"generation", "chunk", "index ID", "document ID", "index name", "missing score", "negative score", "duplicate", "too many", "missing hits", "null hits", "missing timeout", "timed out", "missing shards", "failed shards", "malformed", "NaN", "infinity", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			hit := testSearchHit(testChunk, 0.8)
			source := hit["_source"].(map[string]any)
			response := testSearchResult(hit)
			switch kind {
			case "generation":
				source["generation_id"] = testIndex
			case "chunk":
				source["chunk_id"] = "forged"
			case "index ID":
				source["index_id"] = ""
			case "document ID":
				hit["_id"] = testIndex
			case "index name":
				hit["_index"] = "other"
			case "missing score":
				delete(hit, "_score")
			case "negative score":
				hit["_score"] = -1
			case "duplicate", "too many":
				response = testSearchResult(hit, hit)
			case "missing hits":
				response["hits"] = map[string]any{}
			case "null hits":
				response["hits"] = map[string]any{"hits": nil}
			case "missing timeout":
				delete(response, "timed_out")
			case "timed out":
				response["timed_out"] = true
			case "missing shards":
				delete(response, "_shards")
			case "failed shards":
				response["_shards"].(map[string]int)["failed"] = 1
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if kind == "malformed" {
					io.WriteString(w, "{")
					return
				}
				if kind == "oversized" {
					io.WriteString(w, strings.Repeat(" ", maxProviderResponse+1))
					return
				}
				body, _ := json.Marshal(response)
				if kind == "NaN" {
					body = []byte(strings.Replace(string(body), `"_score":0.8`, `"_score":NaN`, 1))
				}
				if kind == "infinity" {
					body = []byte(strings.Replace(string(body), `"_score":0.8`, `"_score":1e999`, 1))
				}
				w.Write(body)
			}))
			defer server.Close()
			client, _ := NewSearchStore(server.URL, "")
			limit := 2
			if kind == "too many" {
				limit = 1
			}
			got, err := client.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "synthetic", nil, "lexical", limit)
			requireClientError(t, err, "index_invalid")
			if got != nil {
				t.Fatal("invalid response returned partial candidates")
			}
		})
	}
}

func TestSearchGenerationDeleteChecksCompletion(t *testing.T) {
	for _, test := range []struct {
		name, body, code string
		status           int
	}{
		{"complete", `{"timed_out":false,"failures":[],"version_conflicts":0,"total":2,"deleted":2}`, "", 200},
		{"empty", `{"timed_out":false,"failures":[],"version_conflicts":0,"total":0,"deleted":0}`, "", 200},
		{"missing", ``, "", 404},
		{"unavailable", `private-details`, "index_unavailable", 503},
		{"timeout", `{"timed_out":true,"failures":[],"version_conflicts":0,"total":2,"deleted":2}`, "index_invalid", 200},
		{"conflict", `{"timed_out":false,"failures":[],"version_conflicts":1,"total":2,"deleted":1}`, "index_invalid", 200},
		{"failures", `{"timed_out":false,"failures":[{"reason":"private"}],"version_conflicts":0,"total":2,"deleted":1}`, "index_invalid", 200},
		{"partial", `{"timed_out":false,"failures":[],"version_conflicts":0,"total":2,"deleted":1}`, "index_invalid", 200},
		{"missing fields", `{}`, "index_invalid", 200},
		{"null failures", `{"timed_out":false,"failures":null,"version_conflicts":0,"total":0,"deleted":0}`, "index_invalid", 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/"+searchIndex(testProfile)+"/_delete_by_query" || r.URL.Query().Get("refresh") != "true" || r.URL.Query().Get("wait_for_completion") != "true" {
					t.Errorf("wrong delete endpoint: %s", r.URL)
				}
				body, _ := io.ReadAll(r.Body)
				assertSameJSON(t, body, []byte(`{"query":{"term":{"generation_id":"`+testGeneration+`"}}}`))
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, _ := NewSearchStore(server.URL, "")
			err := client.DeleteGeneration(context.Background(), testProfile, testGeneration)
			if test.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				requireClientError(t, err, test.code)
			}
		})
	}
}

func TestClientInputsFailBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	embedder, _ := NewEmbedder(server.URL, "", "model", 2)
	for _, texts := range [][]string{{""}, {" "}, {string([]byte{0xff})}, {strings.Repeat("a", (64<<10)+1)}} {
		_, err := embedder.Embed(context.Background(), texts)
		requireClientError(t, err, "embedding_invalid")
	}
	client, _ := NewSearchStore(server.URL, "")
	chunks, _ := testChunks(1)
	for _, vector := range [][]float32{nil, {0, 0}, {float32(math.NaN()), 1}, {float32(math.Inf(1)), 1}, make([]float32, maxSearchDimensions+1)} {
		requireClientError(t, client.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, chunks, [][]float32{vector}), "index_invalid")
	}
	for _, kind := range []string{"invalid ID", "missing index", "empty text", "duplicate"} {
		bad, vectors := testChunks(1)
		switch kind {
		case "invalid ID":
			bad[0].ID = "../doc"
		case "missing index":
			bad[0].IndexID = ""
		case "empty text":
			bad[0].Content = ""
		case "duplicate":
			bad = append(bad, bad[0])
			vectors = append(vectors, vectors[0])
		}
		requireClientError(t, client.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, bad, vectors), "index_invalid")
	}
	_, vectorErr := client.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "text", []float32{1, 2}, "vector", 3)
	requireClientError(t, vectorErr, "index_invalid")

	for _, mode := range []string{"wrong", "lexical", "vector"} {
		_, err := client.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "", nil, mode, 3)
		requireClientError(t, err, "index_invalid")
	}
	_, err := client.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "text", nil, "lexical", 101)
	requireClientError(t, err, "index_invalid")
	requireClientError(t, client.Ensure(context.Background(), testProfile, maxSearchDimensions+1), "index_invalid")
	requireClientError(t, client.DeleteGeneration(context.Background(), "../index", testGeneration), "index_invalid")
	if calls.Load() != 0 {
		t.Fatal("invalid input reached provider")
	}
}

func TestProviderDeadlineBoundsStalledResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer server.Close()
	client, err := newProviderHTTP(server.URL, "secret", "embedding", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := client.call(context.Background(), http.MethodPost, "/embeddings", map[string]string{"input": "synthetic"}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		requireClientError(t, err, "embedding_timeout")
		if err.Error() != "embedding_timeout" {
			t.Fatalf("unsafe transport error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP timeout did not stop provider")
	}
}

func TestEmbeddingConfiguredTimeoutAndCallerDeadline(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			return
		case <-time.After(1100 * time.Millisecond):
			io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0]}]}`)
		}
	}))
	defer server.Close()
	short, err := NewEmbedderWithTimeout(server.URL, "private-key", "model", 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err := short.Embed(context.Background(), []string{"synthetic"})
	requireClientError(t, err, "embedding_timeout")
	if got != nil || calls.Load() != 1 {
		t.Fatal("timeout returned partial data or retried")
	}
	longer, err := NewEmbedderWithTimeout(server.URL, "private-key", "model", 2, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err = longer.Embed(context.Background(), []string{"synthetic"})
	if err != nil || len(got) != 1 {
		t.Fatalf("configured timeout not applied: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	got, err = longer.Embed(ctx, []string{"synthetic"})
	if got != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline lost: %v", err)
	}
	for _, timeout := range []time.Duration{0, -1, time.Millisecond, 10*time.Minute + time.Second} {
		if _, err := NewEmbedderWithTimeout(server.URL, "", "model", 2, timeout); err == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
}

func TestEmbeddingResponseBodyTimeoutIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := newProviderHTTP(server.URL, "secret", "embedding", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, body, err := client.call(context.Background(), http.MethodPost, "/embeddings", map[string]string{"input": "synthetic"}, nil)
	requireClientError(t, err, "embedding_timeout")
	if body != nil || err.Error() != "embedding_timeout" {
		t.Fatal("partial body or private detail escaped")
	}
}
