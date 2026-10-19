package indexing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func qdrantSchema() map[string]any {
	return map[string]any{"status": "ok", "result": map[string]any{
		"config":         map[string]any{"params": map[string]any{"vectors": map[string]any{"dense": map[string]any{"size": 2, "distance": "Cosine", "datatype": "float32"}}, "sparse_vectors": map[string]any{"bm25": map[string]any{"modifier": "idf"}}}},
		"payload_schema": map[string]any{"workspace_id": map[string]any{"data_type": "keyword"}, "generation_id": map[string]any{"data_type": "keyword"}},
	}}
}
func TestQdrantNativeRequests(t *testing.T) {
	chunks, vectors := testChunks(35)
	calls, upserts := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("api-key") != "private-qdrant-key" || r.Header.Get("Authorization") != "" {
			t.Error("wrong Qdrant authentication")
		}
		var body map[string]json.RawMessage
		if r.Method != "GET" {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
		}
		switch {
		case r.Method == "GET":
			json.NewEncoder(w).Encode(qdrantSchema())
		case strings.HasSuffix(r.URL.Path, "/points"):
			if r.Method != "PUT" || r.URL.Query().Get("wait") != "true" {
				t.Error("write must wait")
			}
			var points []struct {
				ID      string                     `json:"id"`
				Vector  map[string]json.RawMessage `json:"vector"`
				Payload pointPayload               `json:"payload"`
			}
			if json.Unmarshal(body["points"], &points) != nil || len(points) != min(searchBatchSize, len(chunks)-upserts) {
				t.Fatal("wrong batch")
			}
			for i, p := range points {
				c := chunks[upserts+i]
				if p.ID != pointID(c.ID) || p.Payload.ChunkID != c.ID || p.Payload.WorkspaceID != testWorkspace || p.Payload.GenerationID != testGeneration {
					t.Error("incorrect scope")
				}
				var bm map[string]any
				json.Unmarshal(p.Vector["bm25"], &bm)
				if len(p.Vector) != 2 || bm["text"] != c.Content || bm["model"] != "qdrant/bm25" {
					t.Error("both vectors required on one point")
				}
				assertSameJSON(t, mustJSON(bm["options"]), []byte(`{"tokenizer":"multilingual","language":"none","avg_len":128,"k":1.2,"b":0.75}`))
			}
			upserts += len(points)
			fmt.Fprint(w, `{"status":"ok","result":{"status":"completed"}}`)
		case strings.HasSuffix(r.URL.Path, "/points/query"):
			if r.Method != "POST" {
				t.Error("query method")
			}
			if string(body["with_payload"]) != "true" || string(body["with_vector"]) != "false" {
				t.Error("payload/vector projection")
			}
			if len(body["prefetch"]) > 0 {
				assertSameJSON(t, body["query"], []byte(`{"rrf":{"k":61}}`))
				var prefetch []map[string]json.RawMessage
				json.Unmarshal(body["prefetch"], &prefetch)
				if len(prefetch) != 2 {
					t.Fatal("two branches required")
				}
				for _, branch := range prefetch {
					assertSameJSON(t, branch["filter"], body["filter"])
					if string(branch["limit"]) != "100" {
						t.Error("candidate depth")
					}
				}
			}
			fmt.Fprintf(w, `{"status":"ok","result":{"points":[{"id":%q,"score":0.03,"payload":{"chunk_id":%q,"index_id":%q,"workspace_id":%q,"generation_id":%q}}]}}`, pointID(testChunk), testChunk, testIndex, testWorkspace, testGeneration)
		case strings.HasSuffix(r.URL.Path, "/points/delete"):
			if r.URL.Query().Get("wait") != "true" {
				t.Error("delete must wait")
			}
			assertSameJSON(t, body["filter"], []byte(`{"must":[{"key":"generation_id","match":{"value":"`+testGeneration+`"}}]}`))
			fmt.Fprint(w, `{"status":"ok","result":{"status":"completed"}}`)
		default:
			t.Error("unexpected path")
		}
	}))
	defer server.Close()
	q, err := NewQdrant(server.URL, "private-qdrant-key", 128)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Ensure(context.Background(), testProfile, 2); err != nil {
		t.Fatal(err)
	}
	if err = q.Upsert(context.Background(), testProfile, testGeneration, testWorkspace, chunks, vectors); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"lexical", "vector", "hybrid"} {
		hits, err := q.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "内存不足", []float32{1, 2}, mode, 5)
		if err != nil || len(hits) != 1 {
			t.Fatalf("%s: %v %v", mode, hits, err)
		}
		if mode == "hybrid" && hits[0].Score != 0.03 {
			t.Error("RRF score was changed")
		}
	}
	if err = q.DeleteGeneration(context.Background(), testProfile, testGeneration); err != nil {
		t.Fatal(err)
	}
	if upserts != 35 || calls != 8 {
		t.Fatalf("batch/requests %d/%d", upserts, calls)
	}
}
func mustJSON(value any) []byte { b, _ := json.Marshal(value); return b }
func TestQdrantRejectsInvalidAndForeignResults(t *testing.T) {
	for _, kind := range []string{"workspace", "generation", "chunk", "point", "score", "duplicate", "missing", "malformed", "error", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			p := pointPayload{testChunk, testIndex, testWorkspace, testGeneration}
			hit := map[string]any{"id": pointID(testChunk), "score": 0.2, "payload": p}
			switch kind {
			case "workspace":
				p.WorkspaceID = strings.Repeat("f", 32)
			case "generation":
				p.GenerationID = strings.Repeat("f", 32)
			case "chunk":
				p.ChunkID = "invalid"
			case "point":
				hit["id"] = testChunk
			case "score":
				hit["score"] = -2
			}
			hit["payload"] = p
			points := []any{hit}
			if kind == "duplicate" {
				points = append(points, hit)
			}
			response := map[string]any{"status": "ok", "result": map[string]any{"points": points}}
			if kind == "missing" {
				response["result"] = map[string]any{}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "malformed":
					fmt.Fprint(w, "{")
				case "error":
					w.WriteHeader(503)
					fmt.Fprint(w, "private provider detail")
				case "oversized":
					fmt.Fprint(w, strings.Repeat("x", maxProviderResponse+1))
				default:
					json.NewEncoder(w).Encode(response)
				}
			}))
			defer server.Close()
			q, _ := NewQdrant(server.URL, "", 256)
			hits, err := q.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "synthetic", []float32{1, 0}, "hybrid", 5)
			if err == nil || hits != nil || strings.Contains(err.Error(), "private") {
				t.Fatalf("unsafe response %v %v", hits, err)
			}
		})
	}
}
func TestQdrantSchemaFailureAndRepair(t *testing.T) {
	for _, kind := range []string{"wrong dimension", "no idf", "wrong payload", "repair", "create race", "missing after create", "pending write"} {
		t.Run(kind, func(t *testing.T) {
			schema := qdrantSchema()
			result := schema["result"].(map[string]any)
			params := result["config"].(map[string]any)["params"].(map[string]any)
			switch kind {
			case "wrong dimension":
				params["vectors"].(map[string]any)["dense"].(map[string]any)["size"] = 3
			case "no idf":
				params["sparse_vectors"].(map[string]any)["bm25"].(map[string]any)["modifier"] = "none"
			case "wrong payload":
				result["payload_schema"].(map[string]any)["workspace_id"] = map[string]string{"data_type": "text"}
			case "repair", "pending write":
				delete(result["payload_schema"].(map[string]any), "generation_id")
			}
			gets, writes := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					gets++
					if (kind == "create race" && gets == 1) || kind == "missing after create" {
						w.WriteHeader(404)
						return
					}
					json.NewEncoder(w).Encode(schema)
					return
				}
				writes++
				if kind == "create race" {
					w.WriteHeader(409)
					return
				}
				if kind == "missing after create" {
					fmt.Fprint(w, `{"status":"ok","result":true}`)
					return
				}
				if kind == "repair" {
					result["payload_schema"].(map[string]any)["generation_id"] = map[string]string{"data_type": "keyword"}
					fmt.Fprint(w, `{"status":"ok","result":{"status":"completed"}}`)
					return
				}
				fmt.Fprint(w, `{"status":"ok","result":{"status":"acknowledged"}}`)
			}))
			defer server.Close()
			q, _ := NewQdrant(server.URL, "", 256)
			err := q.Ensure(context.Background(), testProfile, 2)
			if kind == "repair" || kind == "create race" {
				if err != nil || writes != 1 {
					t.Fatalf("repair/race: %v %d", err, writes)
				}
			} else if err == nil {
				t.Fatal("invalid schema accepted")
			}
		})
	}
}
func TestQdrantInputsAndProfile(t *testing.T) {
	for _, avg := range []float64{0, -1, math.NaN(), math.Inf(1), 65537} {
		if _, err := NewQdrant("http://localhost", "", avg); err == nil {
			t.Fatal("invalid avg_len")
		}
	}
	for _, url := range []string{"", "http://user:secret@localhost", "http://localhost/?key=secret"} {
		if _, err := NewQdrant(url, "", 256); err == nil {
			t.Fatal("invalid URL")
		}
	}
	q, _ := NewQdrant("http://127.0.0.1:1", "", 256)
	for _, mode := range []string{"lexical", "vector", "hybrid"} {
		if _, err := q.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "", nil, mode, 10); ErrorCode(err) != "qdrant_invalid" {
			t.Fatal(err)
		}
	}
	a := Profile("model", 2, "parser", "embed", "qdrant", 256)
	if a.ID == Profile("model", 2, "parser", "embed", "qdrant", 128).ID || a.ID == Profile("model", 2, "parser", "embed", "other", 256).ID {
		t.Fatal("profile did not isolate BM25 or endpoint")
	}
}

func TestQdrantLiveNativeChineseBM25RRFAndMutations(t *testing.T) {
	base := os.Getenv("MAGWORKSPACE_TEST_QDRANT_URL")
	if base == "" {
		t.Skip("MAGWORKSPACE_TEST_QDRANT_URL not configured")
	}
	qstore, err := NewQdrant(base, os.Getenv("MAGWORKSPACE_TEST_QDRANT_API_KEY"), 256)
	if err != nil {
		t.Fatal(err)
	}
	q := qstore.(*qdrantClient)
	random := make([]byte, 32)
	rand.Read(random)
	profile := hex.EncodeToString(random)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Cleanup(func() {
		_, _, err := q.http.call(context.Background(), http.MethodDelete, collectionPath(profile), nil, nil)
		if err != nil {
			t.Error(err)
		}
	})
	if err = q.Ensure(ctx, profile, 2); err != nil {
		t.Fatal(err)
	}
	chunks, vecs := testChunks(3)
	chunks[0].Content = "虚拟机内存不足导致进程终止"
	chunks[1].Content = "住宿报销标准和员工制度"
	chunks[2].Content = "内存不足需要检查虚拟机配置"
	vecs[0] = []float32{1, 0}
	vecs[1] = []float32{0, 1}
	vecs[2] = []float32{0.5, 0.5}
	if err = q.Upsert(ctx, profile, testGeneration, testWorkspace, chunks, vecs); err != nil {
		t.Fatal(err)
	}
	search := func(mode, workspace, generation, query string) []domain.IndexCandidate {
		t.Helper()
		v, err := q.Search(ctx, profile, workspace, []string{generation}, query, []float32{1, 0}, mode, 10)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	lex := search("lexical", testWorkspace, testGeneration, "内存不足")
	dense := search("vector", testWorkspace, testGeneration, "内存不足")
	hybrid := search("hybrid", testWorkspace, testGeneration, "内存不足")
	if len(lex) != 2 || len(dense) != 3 || len(hybrid) != 3 {
		t.Fatalf("native Chinese recall %d/%d/%d", len(lex), len(dense), len(hybrid))
	}
	want, _ := mergeCandidates(lex, dense)
	for _, hit := range hybrid {
		if math.Abs(hit.Score-want[hit.ChunkID]) > 1e-7 {
			t.Fatalf("native RRF %v != %v", hit.Score, want[hit.ChunkID])
		}
	}
	if len(search("hybrid", strings.Repeat("f", 32), testGeneration, "内存不足")) != 0 || len(search("hybrid", testWorkspace, strings.Repeat("f", 32), "内存不足")) != 0 {
		t.Fatal("scope leak")
	}
	// Resubmission is idempotent; a real update replaces both named vectors.
	if err = q.Upsert(ctx, profile, testGeneration, testWorkspace, chunks, vecs); err != nil {
		t.Fatal(err)
	}
	before := search("lexical", testWorkspace, testGeneration, "住宿报销")[0].Score
	chunks[0].Content = "住宿报销标准新增说明"
	if err = q.Upsert(ctx, profile, testGeneration, testWorkspace, chunks[:1], vecs[:1]); err != nil {
		t.Fatal(err)
	}
	if len(search("lexical", testWorkspace, testGeneration, "内存不足")) != 1 {
		t.Fatal("old terms survived update")
	}
	after := search("lexical", testWorkspace, testGeneration, "住宿报销")
	if len(after) != 2 {
		t.Fatal("updated terms missing")
	}
	for _, hit := range after {
		if hit.ChunkID == chunks[1].ID && hit.Score >= before {
			t.Fatal("IDF did not update after term frequency grew")
		}
	}
	if err = q.DeleteGeneration(ctx, profile, testGeneration); err != nil {
		t.Fatal(err)
	}
	if len(search("hybrid", testWorkspace, testGeneration, "住宿报销")) != 0 {
		t.Fatal("deleted points returned")
	}
	if err = q.DeleteGeneration(ctx, profile, testGeneration); err != nil {
		t.Fatal(err)
	}
	t.Log("real Qdrant: multilingual BM25, native RRF, scopes, idempotent upsert, TF/IDF update and deletion passed")
}
