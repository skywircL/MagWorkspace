package indexing

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

type qdrantClient struct {
	http   *providerHTTP
	avgLen float64
}

func NewQdrant(baseURL, apiKey string, avgLen float64) (SearchStore, error) {
	if math.IsNaN(avgLen) || math.IsInf(avgLen, 0) || avgLen < 1 || avgLen > 65536 {
		return nil, qdrantInvalid()
	}
	client, err := newProviderHTTP(baseURL, apiKey, "qdrant", 60*time.Second)
	if err != nil {
		return nil, err
	}
	return &qdrantClient{http: client, avgLen: avgLen}, nil
}

// NativeFusion keeps Qdrant RRF scores intact during trusted hydration.
func (*qdrantClient) NativeFusion() bool { return true }

func bm25Options(avgLen float64) map[string]any {
	return map[string]any{"tokenizer": "multilingual", "language": "none", "avg_len": avgLen, "k": 1.2, "b": 0.75}
}
func (q *qdrantClient) bm25(text string) map[string]any {
	return map[string]any{"text": text, "model": "qdrant/bm25", "options": bm25Options(q.avgLen)}
}
func collectionPath(profile string) string { return "/collections/" + searchIndex(profile) }
func pointID(chunk string) string {
	return chunk[:8] + "-" + chunk[8:12] + "-" + chunk[12:16] + "-" + chunk[16:20] + "-" + chunk[20:]
}
func qdrantInvalid() error     { return &Failure{Code: "qdrant_invalid"} }
func qdrantUnavailable() error { return &Failure{Code: "qdrant_unavailable"} }

func (q *qdrantClient) collection(ctx context.Context, profile string, dimensions int) (bool, map[string]string, error) {
	status, body, err := q.http.call(ctx, http.MethodGet, collectionPath(profile), nil, nil)
	if err != nil {
		return false, nil, err
	}
	if status == http.StatusNotFound {
		return false, nil, nil
	}
	if status != http.StatusOK {
		return false, nil, qdrantUnavailable()
	}
	var response struct {
		Status string `json:"status"`
		Result struct {
			Config struct {
				Params struct {
					Vectors map[string]struct {
						Size     int    `json:"size"`
						Distance string `json:"distance"`
						Datatype string `json:"datatype"`
					} `json:"vectors"`
					Sparse map[string]struct {
						Modifier string `json:"modifier"`
					} `json:"sparse_vectors"`
				} `json:"params"`
				Quantization json.RawMessage `json:"quantization_config"`
			} `json:"config"`
			Payload map[string]struct {
				Type string `json:"data_type"`
			} `json:"payload_schema"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &response) != nil || response.Status != "ok" {
		return false, nil, qdrantInvalid()
	}
	v := response.Result.Config.Params.Vectors["dense"]
	sparse := response.Result.Config.Params.Sparse
	quant := response.Result.Config.Quantization
	if len(response.Result.Config.Params.Vectors) != 1 || len(sparse) != 1 || sparse["bm25"].Modifier != "idf" || v.Size != dimensions || v.Distance != "Cosine" || (v.Datatype != "" && v.Datatype != "float32") || (len(quant) > 0 && string(quant) != "null") {
		return false, nil, qdrantInvalid()
	}
	fields := map[string]string{}
	for k, v := range response.Result.Payload {
		fields[k] = v.Type
	}
	for _, name := range []string{"workspace_id", "generation_id"} {
		if fields[name] != "" && fields[name] != "keyword" {
			return false, nil, qdrantInvalid()
		}
	}
	return true, fields, nil
}
func (q *qdrantClient) Ensure(ctx context.Context, profile string, dimensions int) error {
	if !hexID(profile, 64) || dimensions < 1 || dimensions > maxSearchDimensions {
		return qdrantInvalid()
	}
	exists, fields, err := q.collection(ctx, profile, dimensions)
	if err != nil {
		return err
	}
	if !exists {
		status, body, err := q.http.call(ctx, http.MethodPut, collectionPath(profile), map[string]any{"shard_number": 1, "vectors": map[string]any{"dense": map[string]any{"size": dimensions, "distance": "Cosine", "datatype": "float32"}}, "sparse_vectors": map[string]any{"bm25": map[string]string{"modifier": "idf"}}}, nil)
		if err != nil {
			return err
		}
		if status == http.StatusOK {
			var result struct {
				Status string `json:"status"`
				Result bool   `json:"result"`
			}
			if json.Unmarshal(body, &result) != nil || result.Status != "ok" || !result.Result {
				return qdrantInvalid()
			}
		} else if status != http.StatusConflict {
			return qdrantUnavailable()
		}
		exists, fields, err = q.collection(ctx, profile, dimensions)
		if err != nil {
			return err
		}
		if !exists {
			return qdrantUnavailable()
		}
	}
	// Repair interrupted payload-index creation even when the collection exists.
	for _, name := range []string{"workspace_id", "generation_id"} {
		if fields[name] == "keyword" {
			continue
		}
		status, body, err := q.http.call(ctx, http.MethodPut, collectionPath(profile)+"/index", map[string]string{"field_name": name, "field_schema": "keyword"}, url.Values{"wait": {"true"}})
		if err != nil {
			return err
		}
		if err = qdrantCompleted(status, body); err != nil {
			return err
		}
	}
	exists, fields, err = q.collection(ctx, profile, dimensions)
	if err != nil {
		return err
	}
	if !exists || fields["workspace_id"] != "keyword" || fields["generation_id"] != "keyword" {
		return qdrantInvalid()
	}
	return nil
}
func qdrantCompleted(status int, body []byte) error {
	if status != http.StatusOK {
		return qdrantUnavailable()
	}
	var result struct {
		Status string `json:"status"`
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &result) != nil || result.Status != "ok" || result.Result.Status != "completed" {
		return qdrantInvalid()
	}
	return nil
}

type pointPayload struct {
	ChunkID      string `json:"chunk_id"`
	IndexID      string `json:"index_id"`
	WorkspaceID  string `json:"workspace_id"`
	GenerationID string `json:"generation_id"`
}

func (q *qdrantClient) Upsert(ctx context.Context, profile, generation, workspace string, chunks []domain.IndexChunk, vectors [][]float32) error {
	if !hexID(profile, 64) || !hexID(generation, 32) || !hexID(workspace, 32) || len(chunks) != len(vectors) || len(chunks) > domain.MaxIndexChunks {
		return qdrantInvalid()
	}
	seen := map[string]bool{}
	for i, c := range chunks {
		if strings.TrimSpace(c.Content) == "" || !utf8.ValidString(c.Content) || len(c.Content) > 64<<10 || !hexID(c.ID, 32) || !hexID(c.IndexID, 32) || c.IndexID != chunks[0].IndexID || seen[c.ID] || !validVector(vectors[i]) || len(vectors[i]) > maxSearchDimensions || len(vectors[i]) != len(vectors[0]) {
			return qdrantInvalid()
		}
		seen[c.ID] = true
	}
	for start := 0; start < len(chunks); start += searchBatchSize {
		end := min(start+searchBatchSize, len(chunks))
		points := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			c := chunks[i]
			points = append(points, map[string]any{"id": pointID(c.ID), "vector": map[string]any{"dense": vectors[i], "bm25": q.bm25(c.Content)}, "payload": pointPayload{c.ID, c.IndexID, workspace, generation}})
		}
		status, body, err := q.http.call(ctx, http.MethodPut, collectionPath(profile)+"/points", map[string]any{"points": points}, url.Values{"wait": {"true"}})
		if err != nil {
			return err
		}
		if err = qdrantCompleted(status, body); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (q *qdrantClient) Search(ctx context.Context, profile, workspace string, generations []string, query string, vector []float32, mode string, limit int) ([]domain.IndexCandidate, error) {
	if !hexID(profile, 64) || !hexID(workspace, 32) || (mode != "vector" && mode != "lexical" && mode != "hybrid") || limit < 1 || limit > 200 || (mode != "lexical" && (!validVector(vector) || len(vector) > maxSearchDimensions)) || (mode != "vector" && (strings.TrimSpace(query) == "" || !utf8.ValidString(query) || len(query) > 64<<10)) {
		return nil, qdrantInvalid()
	}
	allowed := map[string]bool{}
	for _, g := range generations {
		if !hexID(g, 32) {
			return nil, qdrantInvalid()
		}
		allowed[g] = true
	}
	results := []domain.IndexCandidate{}
	if len(generations) == 0 {
		return results, ctx.Err()
	}
	filter := map[string]any{"must": []any{
		map[string]any{"key": "workspace_id", "match": map[string]string{"value": workspace}},
		map[string]any{"key": "generation_id", "match": map[string]any{"any": generations}},
	}}
	request := map[string]any{"filter": filter, "limit": limit, "with_payload": true, "with_vector": false}
	params := map[string]any{"hnsw_ef": max(100, limit*10), "exact": false}
	switch mode {
	case "lexical":
		request["query"] = q.bm25(query)
		request["using"] = "bm25"
	case "vector":
		request["query"] = vector
		request["using"] = "dense"
		request["params"] = params
	case "hybrid":
		request["prefetch"] = []any{
			map[string]any{"query": q.bm25(query), "using": "bm25", "filter": filter, "limit": max(100, limit)},
			map[string]any{"query": vector, "using": "dense", "filter": filter, "limit": max(100, limit), "params": params},
		}
		// Qdrant uses zero-based ranks: k=61 matches our former 1/(60+rank)
		// with one-based ranks. Never fuse these already-fused scores again.
		request["query"] = map[string]any{"rrf": map[string]int{"k": 61}}
	}
	status, body, err := q.http.call(ctx, http.MethodPost, collectionPath(profile)+"/points/query", request, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, qdrantUnavailable()
	}
	var response struct {
		Status string `json:"status"`
		Result struct {
			Points *[]struct {
				ID      string       `json:"id"`
				Score   *float64     `json:"score"`
				Payload pointPayload `json:"payload"`
			} `json:"points"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &response) != nil || response.Status != "ok" || response.Result.Points == nil || len(*response.Result.Points) > limit {
		return nil, qdrantInvalid()
	}
	seen := map[string]bool{}
	for _, hit := range *response.Result.Points {
		p := hit.Payload
		if !hexID(p.ChunkID, 32) || !hexID(p.IndexID, 32) || hit.ID != pointID(p.ChunkID) || p.WorkspaceID != workspace || !allowed[p.GenerationID] || seen[p.ChunkID] || hit.Score == nil || math.IsNaN(*hit.Score) || math.IsInf(*hit.Score, 0) || (mode == "vector" && (*hit.Score < -1.00001 || *hit.Score > 1.00001)) || (mode != "vector" && *hit.Score < 0) {
			return nil, qdrantInvalid()
		}
		seen[p.ChunkID] = true
		// Cosine [-1,1] becomes the API's nonnegative similarity [0,1]. RRF
		// depends only on rank; tiny float accumulation overshoots are clamped.
		score := *hit.Score
		if mode == "vector" {
			score = (max(-1, min(1, score)) + 1) / 2
		}
		results = append(results, domain.IndexCandidate{ChunkID: p.ChunkID, IndexID: p.IndexID, Generation: p.GenerationID, Score: score})
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	return results, nil
}
func (q *qdrantClient) DeleteGeneration(ctx context.Context, profile, generation string) error {
	if !hexID(profile, 64) || !hexID(generation, 32) {
		return qdrantInvalid()
	}
	status, body, err := q.http.call(ctx, http.MethodPost, collectionPath(profile)+"/points/delete", map[string]any{"filter": map[string]any{"must": []any{map[string]any{"key": "generation_id", "match": map[string]string{"value": generation}}}}}, url.Values{"wait": {"true"}})
	if err != nil {
		return err
	}
	// Old Elasticsearch-only profiles never had a Qdrant collection.
	if status == http.StatusNotFound {
		return nil
	}
	return qdrantCompleted(status, body)
}
