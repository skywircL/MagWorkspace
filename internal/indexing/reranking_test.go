package indexing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
	"magworkspace/internal/reranking"
)

func TestContextHeadersPreserveCanonicalChunks(t *testing.T) {
	text := "# 产品\n\n## C#\n\n" + strings.Repeat("编程说明保留原文内容。\n", 90) +
		"\n```markdown\n# 伪造章节\n```\n\n## 配额 ###\n\n" + strings.Repeat("配额不能超出范围。\n", 90) +
		"\n第一章 环境\n\n第一节 部署\n\n" + strings.Repeat("部署环境具体要求。\n", 90)
	for _, strategy := range []string{ChunkingParentChild} {
		chunks, err := SplitWithStrategy(text, strategy)
		if err != nil {
			t.Fatal(err)
		}
		for i := range chunks {
			chunks[i].ID = chunks[i].SHA256
		}
		before, _ := json.Marshal(chunks)
		headers := contextHeaders(chunks)
		after, _ := json.Marshal(chunks)
		if string(before) != string(after) {
			t.Fatal("context enrichment changed evidence")
		}
		seen := map[string]bool{}
		for _, c := range chunks {
			sum := sha256.Sum256([]byte(c.Content))
			if c.Content != text[c.StartByte:c.EndByte] || c.SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("canonical evidence changed")
			}
			seen[headers[c.ID]] = true
			if strings.Contains(headers[c.ID], "伪造") {
				t.Fatal("code interpreted as heading")
			}
		}
		for _, want := range []string{"产品 → C#", "产品 → 配额", "第一章 环境 → 第一节 部署"} {
			if !seen[want] {
				t.Fatalf("%s missing heading path %s: %v", strategy, want, seen)
			}
		}
	}
}

func TestContextHeadingSyntax(t *testing.T) {
	for _, tc := range []struct {
		line, title string
		level       int
	}{
		{"## C#", "C#", 2}, {"## title ###", "title", 2}, {"#### Child", "Child", 4},
		{"第一节 部署环境", "第一节 部署环境", 2},
		{"第二章 总结", "第二章 总结", 1},
		{"普通文本", "", 0}, {"####### 非标题", "", 0},
	} {
		level, title := contextHeading(tc.line)
		if level != tc.level || title != tc.title {
			t.Fatalf("%q got %d/%q", tc.line, level, title)
		}
	}
}

func TestRerankBudgetAndPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	child, stop := rerankContext(ctx)
	defer stop()
	if child.Err() != context.DeadlineExceeded || ctx.Err() != nil {
		t.Fatal("final authorization budget not reserved")
	}
	a := &Service{opts: Options{RerankOptions: reranking.DefaultOptions(), RerankModelID: "model@revision"}}
	b := &Service{opts: a.opts}
	if a.retrievalPolicyID(true) != b.retrievalPolicyID(true) || a.retrievalPolicyID(true) == a.retrievalPolicyID(false) {
		t.Fatal("unstable policy")
	}
	b.opts.RerankOptions.Threshold = .4
	if a.retrievalPolicyID(true) == b.retrievalPolicyID(true) || a.retrievalPolicyID(false) != b.retrievalPolicyID(false) {
		t.Fatal("policy failed to isolate request-time configuration")
	}
}

type deadlineReranker struct{}

func (deadlineReranker) Score(ctx context.Context, _ string, _ []string) ([]reranking.Score, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRerankModelBudgetFallbackPreservesCaller(t *testing.T) {
	service := &Service{opts: Options{Reranker: deadlineReranker{}, RerankOptions: reranking.DefaultOptions()}}
	hits := []domain.SearchHit{{Title: "源文档", Score: .025, Chunk: domain.IndexChunk{Content: "原文保持不变"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2050*time.Millisecond)
	defer cancel()
	var diag reranking.Diagnostics
	got, err := service.rerankHits(ctx, "查询原文", hits, nil, 1, false, &diag)
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(got, hits) || diag.Outcome != "model_error" || diag.ErrorCode != "rerank_timeout" || !diag.FallbackUsed || diag.Applied {
		t.Fatalf("model budget did not safely degrade: %+v %v", diag, err)
	}
	cancel()
	if _, err := service.rerankHits(ctx, "查询原文", hits, nil, 1, false, &diag); err != context.Canceled {
		t.Fatal("caller cancellation became fallback", err)
	}
}

func TestQdrantRerankCandidateDepth(t *testing.T) {
	var captured struct {
		Limit    int `json:"limit"`
		Prefetch []struct {
			Limit int `json:"limit"`
		} `json:"prefetch"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Error(err)
		}
		w.Write([]byte(`{"status":"ok","result":{"points":[]}}`))
	}))
	defer server.Close()
	q, _ := NewQdrant(server.URL, "", 256)
	hits, err := q.Search(context.Background(), testProfile, testWorkspace, []string{testGeneration}, "候选深度", []float32{1, 2}, "hybrid", 200)
	if err != nil || !reflect.DeepEqual(hits, []domain.IndexCandidate{}) || captured.Limit != 200 || len(captured.Prefetch) != 2 || captured.Prefetch[0].Limit != 200 || captured.Prefetch[1].Limit != 200 {
		t.Fatalf("candidate depth not carried through fusion: %+v %v", captured, err)
	}
}
