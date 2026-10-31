package main

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"magworkspace/internal/indexing"
	"magworkspace/internal/reranking"
)

func configureReranking(opts *indexing.Options) error {
	enabled, err := strconv.ParseBool(env("RERANK_ENABLED", "false"))
	if err != nil {
		return errors.New("invalid RERANK_ENABLED")
	}
	if !enabled {
		return nil
	}
	maxCandidates, err := strconv.Atoi(env("RERANK_MAX_CANDIDATES", "10"))
	if err != nil || maxCandidates < 1 || maxCandidates > 200 {
		return errors.New("RERANK_MAX_CANDIDATES must be 1..200")
	}
	maxRunes, err := strconv.Atoi(env("RERANK_MAX_PASSAGE_RUNES", "2048"))
	if err != nil || maxRunes < 128 || maxRunes > 8192 {
		return errors.New("RERANK_MAX_PASSAGE_RUNES must be 128..8192")
	}
	threshold, err := strconv.ParseFloat(env("RERANK_THRESHOLD", "0.2"), 64)
	if err != nil || math.IsNaN(threshold) || math.IsInf(threshold, 0) || threshold < 0 || threshold > 1 {
		return errors.New("RERANK_THRESHOLD must be 0..1")
	}
	timeout, err := time.ParseDuration(env("RERANK_REQUEST_TIMEOUT", "30s"))
	if err != nil || timeout < time.Second || timeout > time.Minute {
		return errors.New("RERANK_REQUEST_TIMEOUT must be 1s..1m")
	}
	endpoint, key := env("RERANK_URL", "http://127.0.0.1:18091/rerank"), env("RERANK_API_KEY", "")
	if len(key) < 32 {
		return errors.New("RERANK_API_KEY must contain at least 32 characters")
	}
	model := env("RERANK_MODEL", "BAAI/bge-reranker-base")
	revision := env("RERANK_MODEL_REVISION", "2cfc18c9415c912f9d8155881c133215df768a70")
	if strings.TrimSpace(model) == "" || strings.TrimSpace(revision) == "" || len(model) > 256 || len(revision) > 128 {
		return errors.New("invalid RERANK_MODEL or RERANK_MODEL_REVISION")
	}
	maxTokens, err := strconv.Atoi(env("RERANK_MAX_INPUT_TOKENS", "1024"))
	if err != nil || maxTokens < 128 || maxTokens > 8192 {
		return errors.New("RERANK_MAX_INPUT_TOKENS must be 128..8192")
	}
	client, err := reranking.NewHTTPClient(endpoint, key, timeout)
	if err != nil {
		return errors.New("invalid RERANK_URL")
	}
	opts.Reranker = client
	opts.RerankOptions = reranking.DefaultOptions()
	opts.RerankOptions.MaxCandidates, opts.RerankOptions.MaxPassageRunes = maxCandidates, maxRunes
	opts.RerankOptions.Threshold = threshold
	identity, _ := json.Marshal([]any{model, revision, endpoint, timeout.String(), maxTokens})
	opts.RerankModelID = string(identity)
	return nil
}
