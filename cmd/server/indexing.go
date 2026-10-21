package main

import (
	"errors"
	"strconv"
	"time"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
)

type indexSetup struct {
	profile  domain.IndexProfile
	parser   indexing.Parser
	embedder indexing.Embedder
	search   indexing.SearchStore
	opts     indexing.Options
}

func configureIndexing() (*indexSetup, error) {
	enabled, err := strconv.ParseBool(env("DOCUMENT_INDEXING_ENABLED", "true"))
	if err != nil {
		return nil, errors.New("invalid DOCUMENT_INDEXING_ENABLED")
	}
	if !enabled {
		return nil, nil
	}
	dimensions, err := strconv.Atoi(env("EMBEDDING_DIMENSIONS", "1024"))
	if err != nil || dimensions < 1 || dimensions > 4096 {
		return nil, errors.New("EMBEDDING_DIMENSIONS must be 1..4096")
	}
	workers, err := strconv.Atoi(env("INDEX_WORKERS", "1"))
	if err != nil || workers < 1 || workers > 4 {
		return nil, errors.New("INDEX_WORKERS must be 1..4")
	}
	maxRetries, err := strconv.Atoi(env("INDEX_MAX_RETRIES", "3"))
	if err != nil || maxRetries < 0 || maxRetries > 10 {
		return nil, errors.New("INDEX_MAX_RETRIES must be 0..10")
	}
	timeout, err := time.ParseDuration(env("INDEX_JOB_TIMEOUT", "5m"))
	if err != nil || timeout < time.Second || timeout > 30*time.Minute {
		return nil, errors.New("INDEX_JOB_TIMEOUT must be 1s..30m")
	}
	execution := env("RETRIEVAL_EXECUTION", "qdrant_native")
	if execution != "qdrant_native" && execution != "baseline" && execution != "optimized" {
		return nil, errors.New("RETRIEVAL_EXECUTION must be qdrant_native (legacy baseline/optimized accepted)")
	}
	parserURL := env("TIKA_URL", "http://127.0.0.1:19998")
	embeddingURL := env("EMBEDDING_BASE_URL", "http://127.0.0.1:11435/v1")
	vectorURL := env("QDRANT_URL", "http://127.0.0.1:16333")
	model := env("EMBEDDING_MODEL", "bge-m3:567m")
	parser, err := indexing.NewParser(parserURL)
	if err != nil {
		return nil, errors.New("invalid TIKA_URL")
	}
	embeddingTimeout, err := time.ParseDuration(env("EMBEDDING_REQUEST_TIMEOUT", "120s"))
	if err != nil || embeddingTimeout < time.Second || embeddingTimeout > 10*time.Minute {
		return nil, errors.New("EMBEDDING_REQUEST_TIMEOUT must be 1s..10m")
	}
	embedder, err := indexing.NewEmbedderWithTimeout(embeddingURL, env("EMBEDDING_API_KEY", ""), model, dimensions, embeddingTimeout)
	if err != nil {
		return nil, errors.New("invalid embedding configuration")
	}
	avgLen, err := strconv.ParseFloat(env("BM25_AVG_LEN", "256"), 64)
	if err != nil {
		return nil, errors.New("invalid BM25_AVG_LEN")
	}
	search, err := indexing.NewQdrant(vectorURL, env("QDRANT_API_KEY", ""), avgLen)
	if err != nil {
		return nil, errors.New("invalid QDRANT_URL or BM25_AVG_LEN")
	}

	if env("CHUNKING_STRATEGY", indexing.ChunkingParentChild) != indexing.ChunkingParentChild {
		return nil, errors.New("CHUNKING_STRATEGY must be parent_child_v1; legacy indexing is retired")
	}
	profile, err := indexing.ProfileWithChunking(model, dimensions, parserURL, embeddingURL, vectorURL, avgLen, env("CHUNKING_STRATEGY", indexing.ChunkingParentChild))
	if err != nil {
		return nil, errors.New("CHUNKING_STRATEGY must be parent_child_v1")
	}
	opts := indexing.Options{Workers: workers, MaxRetries: maxRetries, JobTimeout: timeout, Execution: execution}
	if err := configureReranking(&opts); err != nil {
		return nil, err
	}
	return &indexSetup{profile: profile, parser: parser, embedder: embedder, search: search, opts: opts}, nil
}
