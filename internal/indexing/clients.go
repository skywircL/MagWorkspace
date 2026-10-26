package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const (
	maxProviderResponse = 4 << 20
	maxVectorDimensions = 8192
	maxSearchDimensions = 4096
	embeddingBatchSize  = 8
	searchBatchSize     = 32
)

type Embedder interface {
	Embed(context.Context, []string) ([][]float32, error)
}

type SearchStore interface {
	StorageID() string
	Ensure(context.Context, string, int) error
	Upsert(context.Context, string, string, string, []domain.IndexChunk, [][]float32) error
	Search(context.Context, string, string, []string, string, []float32, string, int) ([]domain.IndexCandidate, error)
	DeleteGeneration(context.Context, string, string) error
}

// providerHTTP never follows redirects or uses environment proxies. Credentials
// belong only to the explicitly configured service; errors never include its URL
// or response text.
type providerHTTP struct {
	base   *url.URL
	client *http.Client
	key    string
	prefix string
}

func newProviderHTTP(baseURL, key, prefix string, timeout time.Duration) (*providerHTTP, error) {
	u, err := url.Parse(baseURL)
	if err != nil || strings.TrimSpace(baseURL) != baseURL || u == nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(baseURL, "#") ||
		strings.ContainsAny(key, "\r\n") {
		return nil, &Failure{Code: prefix + "_invalid"}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	transport := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: timeout,
		IdleConnTimeout: 30 * time.Second, MaxIdleConns: 8, MaxIdleConnsPerHost: 4,
		DisableCompression: true,
	}
	return &providerHTTP{base: u, key: key, prefix: prefix, client: &http.Client{
		Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (p *providerHTTP) call(ctx context.Context, method, path string, input any, query url.Values) (int, []byte, error) {
	var body []byte
	if input != nil {
		var err error
		body, err = json.Marshal(input)
		if err != nil {
			return 0, nil, &Failure{Code: p.prefix + "_invalid"}
		}
	}
	return p.send(ctx, method, path, body, "application/json", query)
}

func (p *providerHTTP) send(ctx context.Context, method, path string, body []byte, contentType string, query url.Values) (int, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	u := *p.base
	u.Path += path
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return 0, nil, &Failure{Code: p.prefix + "_invalid"}
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if p.key != "" {
		if p.prefix == "embedding" {
			req.Header.Set("Authorization", "Bearer "+p.key)
		} else if p.prefix == "qdrant" {
			req.Header.Set("api-key", p.key)
		} else {
			req.Header.Set("Authorization", "ApiKey "+p.key)
		}
	}
	response, err := p.client.Do(req)
	if err != nil {
		return 0, nil, p.transportError(ctx, err)
	}
	defer response.Body.Close()
	body, err = io.ReadAll(io.LimitReader(response.Body, maxProviderResponse+1))
	if err != nil {
		return 0, nil, p.transportError(ctx, err)
	}
	if len(body) > maxProviderResponse {
		return 0, nil, &Failure{Code: p.prefix + "_invalid"}
	}
	return response.StatusCode, body, nil
}

type httpEmbedder struct {
	http       *providerHTTP
	model      string
	dimensions int
}

func (p *providerHTTP) transportError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var networkError net.Error
	if p.prefix == "embedding" && errors.As(err, &networkError) && networkError.Timeout() {
		return &Failure{Code: "embedding_timeout"}
	}
	return &Failure{Code: p.prefix + "_unavailable"}
}

func NewEmbedder(baseURL, apiKey, model string, dimensions int) (Embedder, error) {
	return NewEmbedderWithTimeout(baseURL, apiKey, model, dimensions, 120*time.Second)
}

// Timeout covers each batch, including provider queueing and response reads.
// The caller's context still bounds the whole indexing job or search request.
func NewEmbedderWithTimeout(baseURL, apiKey, model string, dimensions int, timeout time.Duration) (Embedder, error) {
	if timeout < time.Second || timeout > 10*time.Minute {
		return nil, &Failure{Code: "embedding_invalid"}
	}
	if strings.TrimSpace(model) == "" || len(model) > 255 || !utf8.ValidString(model) || dimensions < 1 || dimensions > maxVectorDimensions {
		return nil, &Failure{Code: "embedding_invalid"}
	}
	client, err := newProviderHTTP(baseURL, apiKey, "embedding", timeout)
	if err != nil {
		return nil, err
	}
	return &httpEmbedder{http: client, model: model, dimensions: dimensions}, nil
}

func (e *httpEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) > domain.MaxIndexChunks {
		return nil, &Failure{Code: "embedding_invalid"}
	}
	for _, text := range texts {
		if strings.TrimSpace(text) == "" || len(text) > 64<<10 || !utf8.ValidString(text) {
			return nil, &Failure{Code: "embedding_invalid"}
		}
	}
	result := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += embeddingBatchSize {
		end := min(start+embeddingBatchSize, len(texts))
		status, body, err := e.http.call(ctx, http.MethodPost, "/embeddings", map[string]any{
			"model": e.model, "input": texts[start:end], "encoding_format": "float",
		}, nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, &Failure{Code: "embedding_unavailable"}
		}
		var response struct {
			Data []struct {
				Index     *int       `json:"index"`
				Embedding []*float64 `json:"embedding"`
			} `json:"data"`
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal(body, &response) != nil || len(response.Data) != end-start ||
			(len(response.Error) > 0 && string(response.Error) != "null") {
			return nil, &Failure{Code: "embedding_invalid"}
		}
		batch := make([][]float32, end-start)
		for _, item := range response.Data {
			if item.Index == nil || *item.Index < 0 || *item.Index >= len(batch) || batch[*item.Index] != nil || len(item.Embedding) != e.dimensions {
				return nil, &Failure{Code: "embedding_invalid"}
			}
			vector := make([]float32, e.dimensions)
			for i, value := range item.Embedding {
				if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) {
					return nil, &Failure{Code: "embedding_invalid"}
				}
				vector[i] = float32(*value)
			}
			if !validVector(vector) {
				return nil, &Failure{Code: "embedding_invalid"}
			}
			batch[*item.Index] = vector
		}
		result = append(result, batch...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validVector(vector []float32) bool {
	if len(vector) == 0 || len(vector) > maxVectorDimensions {
		return false
	}
	nonzero := false
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return false
		}
		nonzero = nonzero || value != 0
	}
	return nonzero
}

func hexID(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type elasticClient struct{ http *providerHTTP }

func (e *elasticClient) StorageID() string {
	return domain.StorageBindingID("elasticsearch", e.http.base.String(), "mw_")
}

func NewSearchStore(baseURL, apiKey string) (SearchStore, error) {
	client, err := newProviderHTTP(baseURL, apiKey, "index", 60*time.Second)
	if err != nil {
		return nil, err
	}
	return &elasticClient{http: client}, nil
}

func searchIndex(profileID string) string { return "mw_" + profileID }

func searchMapping(dimensions int) map[string]any {
	properties := map[string]any{}
	for _, field := range []string{"chunk_id", "index_id", "workspace_id", "generation_id"} {
		properties[field] = map[string]string{"type": "keyword"}
	}
	properties["content"] = map[string]string{"type": "text", "analyzer": "cjk", "similarity": "BM25"}
	return map[string]any{"dynamic": "strict", "properties": properties}
}

func (e *elasticClient) mapping(ctx context.Context, profileID string, dimensions int) (bool, error) {
	status, body, err := e.http.call(ctx, http.MethodGet, "/"+searchIndex(profileID)+"/_mapping", nil, nil)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return false, nil
	}
	if status != http.StatusOK {
		return false, &Failure{Code: "index_unavailable"}
	}
	type field struct {
		Index      *bool  `json:"index"`
		Type       string `json:"type"`
		Analyzer   string `json:"analyzer"`
		Similarity string `json:"similarity"`
	}
	var response map[string]struct {
		Mappings struct {
			Dynamic    string           `json:"dynamic"`
			Properties map[string]field `json:"properties"`
		} `json:"mappings"`
	}
	if json.Unmarshal(body, &response) != nil || len(response) != 1 {
		return false, &Failure{Code: "index_invalid"}
	}
	mapping := response[searchIndex(profileID)].Mappings
	if mapping.Dynamic != "strict" || len(mapping.Properties) != 5 {
		return false, &Failure{Code: "index_invalid"}
	}
	for _, name := range []string{"chunk_id", "index_id", "workspace_id", "generation_id"} {
		f := mapping.Properties[name]
		if f.Type != "keyword" || (f.Index != nil && !*f.Index) {
			return false, &Failure{Code: "index_invalid"}
		}
	}
	text := mapping.Properties["content"]
	if text.Type != "text" || text.Analyzer != "cjk" || text.Similarity != "BM25" || (text.Index != nil && !*text.Index) {
		return false, &Failure{Code: "index_invalid"}
	}
	return true, nil
}

func (e *elasticClient) Ensure(ctx context.Context, profileID string, dimensions int) error {
	if !hexID(profileID, 64) || dimensions < 1 || dimensions > maxSearchDimensions {
		return &Failure{Code: "index_invalid"}
	}
	exists, err := e.mapping(ctx, profileID, dimensions)
	if err != nil || exists {
		return err
	}
	status, body, err := e.http.call(ctx, http.MethodPut, "/"+searchIndex(profileID), map[string]any{
		"settings": map[string]int{"number_of_shards": 1, "number_of_replicas": 0}, "mappings": searchMapping(dimensions),
	}, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		var result struct {
			Acknowledged       bool   `json:"acknowledged"`
			ShardsAcknowledged bool   `json:"shards_acknowledged"`
			Index              string `json:"index"`
		}
		if json.Unmarshal(body, &result) != nil || !result.Acknowledged || !result.ShardsAcknowledged || result.Index != searchIndex(profileID) {
			return &Failure{Code: "index_invalid"}
		}
	} else {
		var result struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		// Another worker may have created this profile. A verified existing
		// mapping is required; unrelated bad requests cannot be accepted.
		if status != http.StatusBadRequest || json.Unmarshal(body, &result) != nil || result.Error.Type != "resource_already_exists_exception" {
			return &Failure{Code: "index_unavailable"}
		}
	}
	if exists, err = e.mapping(ctx, profileID, dimensions); err != nil {
		return err
	} else if !exists {
		return &Failure{Code: "index_unavailable"}
	}
	return nil
}

type searchDocument struct {
	ChunkID      string `json:"chunk_id"`
	IndexID      string `json:"index_id"`
	WorkspaceID  string `json:"workspace_id"`
	GenerationID string `json:"generation_id"`
	Content      string `json:"content"`
}

func (e *elasticClient) Upsert(ctx context.Context, profileID, generation, workspaceID string, chunks []domain.IndexChunk, vectors [][]float32) error {
	if !hexID(profileID, 64) || !hexID(generation, 32) || !hexID(workspaceID, 32) || len(chunks) != len(vectors) || len(chunks) > domain.MaxIndexChunks {
		return &Failure{Code: "index_invalid"}
	}
	seen := make(map[string]bool, len(chunks))
	for i, chunk := range chunks {
		if !hexID(chunk.ID, 32) || !hexID(chunk.IndexID, 32) || chunk.IndexID != chunks[0].IndexID || seen[chunk.ID] ||
			strings.TrimSpace(chunk.Content) == "" || !utf8.ValidString(chunk.Content) || len(chunk.Content) > 64<<10 ||
			!validVector(vectors[i]) || len(vectors[i]) > maxSearchDimensions || len(vectors[i]) != len(vectors[0]) {
			return &Failure{Code: "index_invalid"}
		}
		seen[chunk.ID] = true
	}
	for start := 0; start < len(chunks); start += searchBatchSize {
		end := min(start+searchBatchSize, len(chunks))
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		for i := start; i < end; i++ {
			// Encode appends the newline required by the Bulk NDJSON API.
			if err := encoder.Encode(map[string]any{"index": map[string]string{"_id": chunks[i].ID}}); err != nil {
				return &Failure{Code: "index_invalid"}
			}
			if err := encoder.Encode(searchDocument{chunks[i].ID, chunks[i].IndexID, workspaceID, generation, chunks[i].Content}); err != nil {
				return &Failure{Code: "index_invalid"}
			}
		}
		status, body, err := e.http.send(ctx, http.MethodPost, "/"+searchIndex(profileID)+"/_bulk", buffer.Bytes(), "application/x-ndjson",
			url.Values{"refresh": {"wait_for"}, "include_source_on_error": {"false"}})
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return &Failure{Code: "index_unavailable"}
		}
		var result struct {
			Errors *bool `json:"errors"`
			Items  []struct {
				Index struct {
					ID     string          `json:"_id"`
					Index  string          `json:"_index"`
					Status int             `json:"status"`
					Error  json.RawMessage `json:"error"`
					Shards struct {
						Failed     *int `json:"failed"`
						Successful int  `json:"successful"`
					} `json:"_shards"`
				} `json:"index"`
			} `json:"items"`
		}
		if json.Unmarshal(body, &result) != nil || result.Errors == nil || *result.Errors || len(result.Items) != end-start {
			return &Failure{Code: "index_invalid"}
		}
		for i, item := range result.Items {
			r := item.Index
			if r.ID != chunks[start+i].ID || r.Index != searchIndex(profileID) || (r.Status != 200 && r.Status != 201) ||
				(len(r.Error) != 0 && string(r.Error) != "null") || r.Shards.Failed == nil || *r.Shards.Failed != 0 || r.Shards.Successful < 1 {
				return &Failure{Code: "index_invalid"}
			}
		}
	}
	return ctx.Err()
}

func (e *elasticClient) Search(ctx context.Context, profileID, workspaceID string, generations []string, queryText string, queryVector []float32, mode string, limit int) ([]domain.IndexCandidate, error) {
	if !hexID(profileID, 64) || !hexID(workspaceID, 32) || limit < 1 || limit > 100 || mode != "lexical" ||
		(mode == "lexical" && (strings.TrimSpace(queryText) == "" || len(queryText) > 64<<10 || !utf8.ValidString(queryText))) ||
		(mode == "vector" && (!validVector(queryVector) || len(queryVector) > maxSearchDimensions)) {
		return nil, &Failure{Code: "index_invalid"}
	}
	allowed := make(map[string]bool, len(generations))
	for _, generation := range generations {
		if !hexID(generation, 32) {
			return nil, &Failure{Code: "index_invalid"}
		}
		allowed[generation] = true
	}
	result := []domain.IndexCandidate{}
	if len(generations) == 0 {
		return result, ctx.Err()
	}
	filter := []any{map[string]any{"term": map[string]string{"workspace_id": workspaceID}}, map[string]any{"terms": map[string]any{"generation_id": generations}}}
	request := map[string]any{"size": limit, "_source": []string{"chunk_id", "index_id", "generation_id"}, "track_total_hits": false}
	request["query"] = map[string]any{"bool": map[string]any{"filter": filter, "must": map[string]any{"match": map[string]any{"content": map[string]string{"query": queryText}}}}}
	status, body, err := e.http.call(ctx, http.MethodPost, "/"+searchIndex(profileID)+"/_search", request, url.Values{"allow_partial_search_results": {"false"}})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &Failure{Code: "index_unavailable"}
	}
	var response struct {
		TimedOut *bool `json:"timed_out"`
		Shards   struct {
			Total  int  `json:"total"`
			Failed *int `json:"failed"`
		} `json:"_shards"`
		Hits struct {
			Hits *[]struct {
				ID     string   `json:"_id"`
				Index  string   `json:"_index"`
				Score  *float64 `json:"_score"`
				Source struct {
					ChunkID      string `json:"chunk_id"`
					IndexID      string `json:"index_id"`
					GenerationID string `json:"generation_id"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if json.Unmarshal(body, &response) != nil || response.TimedOut == nil || *response.TimedOut || response.Shards.Total < 1 || response.Shards.Failed == nil || *response.Shards.Failed != 0 || response.Hits.Hits == nil || len(*response.Hits.Hits) > limit {
		return nil, &Failure{Code: "index_invalid"}
	}
	seen := make(map[string]bool)
	for _, hit := range *response.Hits.Hits {
		p := hit.Source
		if !hexID(p.ChunkID, 32) || !hexID(p.IndexID, 32) || hit.ID != p.ChunkID || hit.Index != searchIndex(profileID) || seen[p.ChunkID] || !allowed[p.GenerationID] ||
			hit.Score == nil || math.IsNaN(*hit.Score) || math.IsInf(*hit.Score, 0) || *hit.Score < 0 {
			return nil, &Failure{Code: "index_invalid"}
		}
		seen[p.ChunkID] = true
		result = append(result, domain.IndexCandidate{ChunkID: p.ChunkID, IndexID: p.IndexID, Generation: p.GenerationID, Score: *hit.Score})
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Score > result[j].Score })
	return result, nil
}

func (e *elasticClient) DeleteGeneration(ctx context.Context, profileID, generation string) error {
	if !hexID(profileID, 64) || !hexID(generation, 32) {
		return &Failure{Code: "index_invalid"}
	}
	status, body, err := e.http.call(ctx, http.MethodPost, "/"+searchIndex(profileID)+"/_delete_by_query", map[string]any{
		"query": map[string]any{"term": map[string]string{"generation_id": generation}},
	}, url.Values{"refresh": {"true"}, "wait_for_completion": {"true"}})
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return &Failure{Code: "index_unavailable"}
	}
	var result struct {
		TimedOut         *bool              `json:"timed_out"`
		Failures         *[]json.RawMessage `json:"failures"`
		VersionConflicts *int               `json:"version_conflicts"`
		Total            *int               `json:"total"`
		Deleted          *int               `json:"deleted"`
	}
	if json.Unmarshal(body, &result) != nil || result.TimedOut == nil || *result.TimedOut || result.Failures == nil || len(*result.Failures) != 0 ||
		result.VersionConflicts == nil || *result.VersionConflicts != 0 || result.Total == nil || result.Deleted == nil || *result.Total < 0 || *result.Total != *result.Deleted {
		return &Failure{Code: "index_invalid"}
	}
	return nil
}
