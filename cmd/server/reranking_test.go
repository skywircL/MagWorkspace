package main

import (
	"strings"
	"testing"

	"magworkspace/internal/indexing"
)

func TestConfigureReranking(t *testing.T) {
	t.Setenv("RERANK_ENABLED", "true")
	t.Setenv("RERANK_API_KEY", strings.Repeat("k", 32))
	var opts indexing.Options
	if err := configureReranking(&opts); err != nil || opts.Reranker == nil || opts.RerankOptions.MaxCandidates != 10 || opts.RerankOptions.Threshold != .2 {
		t.Fatal("missing configured reranker", err)
	}
	for name, values := range map[string][]string{
		"RERANK_ENABLED":           {"bad"},
		"RERANK_API_KEY":           {"short"},
		"RERANK_MAX_CANDIDATES":    {"0", "201"},
		"RERANK_MAX_PASSAGE_RUNES": {"127", "8193"},
		"RERANK_THRESHOLD":         {"NaN", "Inf", "-0.1", "1.1"},
		"RERANK_REQUEST_TIMEOUT":   {"999ms", "61s"},
		"RERANK_MAX_INPUT_TOKENS":  {"127", "8193"},
		"RERANK_URL":               {"file:///tmp/model", "http://user:pass@localhost/rerank"},
	} {
		for _, value := range values {
			t.Run(name+"/"+value, func(t *testing.T) {
				t.Setenv(name, value)
				var candidate indexing.Options
				if err := configureReranking(&candidate); err == nil {
					t.Fatal("accepted invalid configuration")
				}
			})
		}
	}
	t.Setenv("RERANK_ENABLED", "false")
	var disabled indexing.Options
	if err := configureReranking(&disabled); err != nil || disabled.Reranker != nil {
		t.Fatal("disabled reranker instantiated", err)
	}
}
