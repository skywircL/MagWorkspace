package reranking

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxResponseBytes = 128 << 10
	// Matches the pinned service payload limit, including JSON framing.
	maxRequestBytes = 2_000_000
)

// HTTPClient uses the configured TEI endpoint. It never follows redirects or
// environment proxies, and never includes URLs, credentials or provider bodies
// in returned errors.
type HTTPClient struct {
	endpoint string
	key      string
	client   *http.Client
}

// NewHTTPClient takes the full endpoint including /rerank. TEI owns the model
// configuration; a request cannot select or download another model.
func NewHTTPClient(endpoint, key string, timeout time.Duration) (*HTTPClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") ||
		u.Hostname() == "" || strings.TrimSpace(endpoint) != endpoint || u.User != nil ||
		u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(endpoint, "#") ||
		len(key) > 4096 || strings.ContainsAny(key, "\r\n") ||
		timeout < time.Second || timeout > 10*time.Minute {
		return nil, ErrInvalidInput
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: min(timeout, 10*time.Second), KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: min(timeout, 10*time.Second), ResponseHeaderTimeout: timeout,
		IdleConnTimeout: 30 * time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 4,
		DisableCompression: true,
	}
	return &HTTPClient{endpoint: u.String(), key: key, client: &http.Client{
		Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// Score requests every candidate's normalized model score. Partial top-n
// responses cannot implement thresholding correctly and are rejected.
func (c *HTTPClient) Score(ctx context.Context, query string, passages []string) ([]Score, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(query) == "" || !utf8.ValidString(query) || len(query) > maxQueryBytes ||
		len(passages) == 0 || len(passages) > DefaultMaxCandidates {
		return nil, ErrInvalidInput
	}
	for _, passage := range passages {
		if strings.TrimSpace(passage) == "" || !utf8.ValidString(passage) ||
			utf8.RuneCountInString(passage) > maxPassageRunes {
			return nil, ErrInvalidInput
		}
	}
	body, err := json.Marshal(struct {
		Query      string   `json:"query"`
		Texts      []string `json:"texts"`
		Truncate   bool     `json:"truncate"`
		RawScores  bool     `json:"raw_scores"`
		ReturnText bool     `json:"return_text"`
	}{Query: query, Texts: passages, Truncate: true})
	if err != nil || len(body) > maxRequestBytes {
		return nil, ErrInvalidInput
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, ErrInvalidInput
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Provider error bodies may contain submitted text and are not read.
		return nil, ErrModelUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, transportError(ctx, err)
	}
	if len(data) > maxResponseBytes {
		return nil, ErrInvalidResponse
	}
	var result []struct {
		Index *int     `json:"index"`
		Score *float64 `json:"score"`
	}
	if json.Unmarshal(data, &result) != nil {
		return nil, ErrInvalidResponse
	}
	scores := make([]Score, 0, len(result))
	for _, item := range result {
		if item.Index == nil || item.Score == nil {
			return nil, ErrInvalidResponse
		}
		scores = append(scores, Score{Index: *item.Index, RelevanceScore: *item.Score})
	}
	if !validScores(scores, len(passages)) {
		return nil, ErrInvalidResponse
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return scores, nil
}

func transportError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return ErrTimeout
	}
	return ErrModelUnavailable
}
