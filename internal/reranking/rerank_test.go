package reranking

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

type modelFunc func(context.Context, string, []string) ([]Score, error)

func (f modelFunc) Score(ctx context.Context, query string, passages []string) ([]Score, error) {
	return f(ctx, query, passages)
}

func scoresModel(values ...float64) Model {
	return modelFunc(func(_ context.Context, _ string, passages []string) ([]Score, error) {
		scores := make([]Score, len(values))
		for i, value := range values {
			scores[i] = Score{Index: i, RelevanceScore: value}
		}
		return scores, nil
	})
}

func TestRunBuildsBoundedEnrichedPassagesWithoutChangingEvidence(t *testing.T) {
	candidates := []Candidate{
		{Title: "售后服务说明", ContextHeader: "退款政策", Content: "**7 天内**可申请。 [说明](https://example.test)\n", RetrievalScore: 1},
		{Title: "配送说明", ContextHeader: "送达时间", Content: "通常 3 天内发货。", RetrievalScore: 0.5},
	}
	original := append([]Candidate(nil), candidates...)
	model := modelFunc(func(_ context.Context, query string, passages []string) ([]Score, error) {
		if query != "退款期限是多少？" || len(passages) != 2 {
			t.Fatalf("wrong model input: query length %d, candidates %d", len(query), len(passages))
		}
		if passages[0] != "售后服务说明\n\n退款政策\n\n7 天内可申请。 说明" {
			t.Errorf("missing semantic metadata or cleaning: %q", passages[0])
		}
		return []Score{{Index: 1, RelevanceScore: 0.05}, {Index: 0, RelevanceScore: 0.92}}, nil
	})
	result, err := Run(context.Background(), model, "退款期限是多少？", candidates, DefaultOptions())
	if err != nil || len(result.Selections) != 1 || result.Selections[0].Index != 0 ||
		math.Abs(result.Selections[0].Score-0.952) > 1e-12 || result.Diagnostics.Outcome != "ok" {
		t.Fatalf("unexpected selection: %+v, %v", result, err)
	}
	if !reflect.DeepEqual(candidates, original) {
		t.Fatal("canonical candidate mutated")
	}
	long := Candidate{Title: strings.Repeat("题", 1000), ContextHeader: strings.Repeat("节", 1000), Content: strings.Repeat("文", 5000)}
	body := CleanPassage(long.Content)
	passage := buildPassage(long, body, 128)
	if !utf8.ValidString(passage) || utf8.RuneCountInString(passage) != 128 || !strings.Contains(passage, strings.Repeat("文", 60)) {
		t.Fatal("metadata consumed body budget or UTF-8 was truncated")
	}
}

func TestRunThresholdFilteringAndScopedFallback(t *testing.T) {
	tests := []struct {
		name      string
		threshold float64
		scores    []float64
		scope     bool
		outcome   string
		count     int
		effective float64
	}{
		{"ordinary", 0.2, []float64{0.8, 0.1}, false, "ok", 1, 0.2},
		{"threshold boundary included", 0.2, []float64{0.2, 0.1}, false, "ok", 1, 0.2},
		{"relax once", 0.8, []float64{0.6, 0.4}, false, "threshold_degraded", 1, 0.56},
		{"relax floor", 0.4, []float64{0.31, 0.1}, false, "threshold_degraded", 1, 0.3},
		{"fallback minimum", 0.2, []float64{0.15, 0.1}, false, "fallback_top1", 1, 0.2},
		{"no evidence", 0.2, []float64{0.149, 0.01}, false, "all_below_threshold", 0, 0.2},
		{"no repeated relax", 1, []float64{0.14, 0.1}, false, "all_below_threshold", 0, 0.7},
		{"explicit scope", 0.2, []float64{0.0, 0.01}, true, "fallback_top1", 1, 0.2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := DefaultOptions()
			options.Threshold, options.KeepBestInScope = test.threshold, test.scope
			calls := 0
			model := modelFunc(func(ctx context.Context, query string, passages []string) ([]Score, error) {
				calls++
				return scoresModel(test.scores...).Score(ctx, query, passages)
			})
			result, err := Run(context.Background(), model, "query", []Candidate{
				{Content: "甲", RetrievalScore: 0.03}, {Content: "乙", RetrievalScore: 0.02},
			}, options)
			if err != nil || calls != 1 || len(result.Selections) != test.count ||
				result.Diagnostics.Outcome != test.outcome || !result.Diagnostics.Applied ||
				math.Abs(result.Diagnostics.EffectiveThreshold-test.effective) > 1e-12 {
				t.Fatalf("unexpected filtering: %+v, calls %d, err %v", result, calls, err)
			}
		})
	}
}

func TestRunUsesRetrievalScoreWithoutCandidateRescaling(t *testing.T) {
	base := []Candidate{{Content: "甲", RetrievalScore: 0.1}, {Content: "乙", RetrievalScore: 0.9}, {Content: "丙", RetrievalScore: 0.5}}
	opts := DefaultOptions()
	opts.MaxCandidates = 2
	first, err := Run(context.Background(), scoresModel(0.8, 0.8), "query", base, opts)
	if err != nil || len(first.Selections) != 2 || first.Selections[0].Index != 1 || math.Abs(first.Selections[0].Score-0.85) > 1e-12 || first.Selections[0].RetrievalScore != 0.9 {
		t.Fatalf("wrong direct composite: %+v %v", first, err)
	}
	alone, err := Run(context.Background(), scoresModel(0.8), "query", base[2:], opts)
	if err != nil || math.Abs(alone.Selections[0].Score-first.Selections[1].Score) > 1e-12 || alone.Selections[0].RetrievalScore != 0.5 {
		t.Fatalf("removing best candidate inflated score: %+v %v", alone, err)
	}
	opts.MaxCandidates = 1
	capped, err := Run(context.Background(), scoresModel(0.8), "query", base, opts)
	if err != nil || capped.Selections[0].Score != first.Selections[0].Score {
		t.Fatalf("candidate cap changed score: %+v %v", capped, err)
	}
	for _, score := range []float64{-0.1, 1.01, math.NaN(), math.Inf(1)} {
		_, err := Run(context.Background(), scoresModel(0.8), "query", []Candidate{{Content: "甲", RetrievalScore: score}}, opts)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted unbounded retrieval score %v: %v", score, err)
		}
	}
	zero, err := Run(context.Background(), scoresModel(0.5), "query", []Candidate{{Content: "甲"}}, opts)
	if err != nil || zero.Selections[0].Score != 0.4 {
		t.Fatalf("zero retrieval: %+v %v", zero, err)
	}
}

func TestRunMMRReducesChineseRedundancyAndExcludesMetadata(t *testing.T) {
	candidates := []Candidate{
		{Title: "共享共享共享标题", ContextHeader: "共享章节", Content: "退款期限为七天。", RetrievalScore: 1},
		{Title: "不同标题", Content: "**退款期限为七天。**", RetrievalScore: 1},
		{Title: "共享共享共享标题", ContextHeader: "共享章节", Content: "申请材料包括订单号。", RetrievalScore: 1},
	}
	opts := DefaultOptions()
	opts.TopK = 2
	result, err := Run(context.Background(), scoresModel(0.9, 0.89, 0.8), "退款", candidates, opts)
	if err != nil || len(result.Selections) != 2 || result.Selections[0].Index != 0 || result.Selections[1].Index != 2 {
		t.Fatalf("near duplicate displaced complementary evidence: %+v, %v", result, err)
	}
	if got := jaccard(tokenize("退款期限为七天。"), tokenize("退款期限为七天！")); got != 1 {
		t.Fatalf("punctuation changed Chinese token set: %v", got)
	}
	if got := jaccard(tokenize("ＡＢＣ１２３"), tokenize("abc123")); got != 1 {
		t.Fatalf("NFKC not applied: %v", got)
	}
}

func TestRunRejectsMalformedProviderScoresWithoutRawErrors(t *testing.T) {
	tests := []struct {
		name   string
		scores []Score
	}{
		{"missing", []Score{{Index: 0, RelevanceScore: 0.5}}},
		{"duplicate", []Score{{Index: 0, RelevanceScore: 0.5}, {Index: 0, RelevanceScore: 0.5}}},
		{"negative index", []Score{{Index: -1, RelevanceScore: 0.5}, {Index: 1, RelevanceScore: 0.5}}},
		{"index too large", []Score{{Index: 0, RelevanceScore: 0.5}, {Index: 2, RelevanceScore: 0.5}}},
		{"NaN", []Score{{Index: 0, RelevanceScore: math.NaN()}, {Index: 1, RelevanceScore: 0.5}}},
		{"infinite", []Score{{Index: 0, RelevanceScore: math.Inf(1)}, {Index: 1, RelevanceScore: 0.5}}},
		{"raw logit", []Score{{Index: 0, RelevanceScore: -0.1}, {Index: 1, RelevanceScore: 0.5}}},
		{"above one", []Score{{Index: 0, RelevanceScore: 1.1}, {Index: 1, RelevanceScore: 0.5}}},
	}
	candidates := []Candidate{{Content: "甲"}, {Content: "乙"}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := modelFunc(func(context.Context, string, []string) ([]Score, error) { return test.scores, nil })
			result, err := Run(context.Background(), model, "query", candidates, DefaultOptions())
			if !errors.Is(err, ErrInvalidResponse) || result.Diagnostics.Applied ||
				result.Diagnostics.Outcome != "model_error" || len(result.Selections) != 0 {
				t.Fatalf("invalid provider accepted: %+v, %v", result, err)
			}
		})
	}
	for _, providerErr := range []error{errors.New("secret query and credentials"), ErrTimeout, ErrInvalidResponse} {
		model := modelFunc(func(context.Context, string, []string) ([]Score, error) { return nil, providerErr })
		result, err := Run(context.Background(), model, "query", candidates, DefaultOptions())
		if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(result.Diagnostics.ErrorCode, "secret") {
			t.Fatal("raw model error leaked")
		}
	}
}

func TestRunStableTiesCancellationAndEmptyBodies(t *testing.T) {
	candidates := []Candidate{{Content: "甲", RetrievalScore: 1}, {Content: "乙", RetrievalScore: 1}}
	reversed := modelFunc(func(context.Context, string, []string) ([]Score, error) {
		return []Score{{Index: 1, RelevanceScore: 0.7}, {Index: 0, RelevanceScore: 0.7}}, nil
	})
	result, err := Run(context.Background(), reversed, "query", candidates, DefaultOptions())
	if err != nil || result.Selections[0].Index != 0 {
		t.Fatalf("provider order controlled stable ties: %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelling := modelFunc(func(context.Context, string, []string) ([]Score, error) {
		cancel()
		return []Score{{Index: 0, RelevanceScore: 0.7}, {Index: 1, RelevanceScore: 0.7}}, nil
	})
	_, err = Run(ctx, cancelling, "query", candidates, DefaultOptions())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request did not propagate: %v", err)
	}
	empty, err := Run(context.Background(), nil, "query", []Candidate{{Title: "看似相关的标题", Content: "  \n| --- | --- |\n"}}, DefaultOptions())
	if err != nil || empty.Diagnostics.Outcome != "no_candidates" || len(empty.Selections) != 0 {
		t.Fatalf("title-only candidate counted as evidence: %+v %v", empty, err)
	}
}

func TestCleanPassageRetainsCodeFormulaTablesAndChineseText(t *testing.T) {
	content := "# 配置\n\n```sh\ncurl https://example.test --flag\nx < y && a*b\n```\n" +
		"$$\nx*y + z\n$$\n" +
		"| 项目 | 数值 |\n| --- | ---: |\n| 内存 | 4G |\n" +
		"- **退款** [说明](https://example.test) https://example.test，保留后文。\n" +
		"行内 `a < b` 与 `https://example.test`。"
	cleaned := CleanPassage(content)
	for _, required := range []string{
		"curl https://example.test --flag", "x < y && a*b", "x*y + z",
		"项目, 数值", "内存, 4G", "退款 说明 ，保留后文", "行内 a < b 与 https://example.test。",
	} {
		if !strings.Contains(cleaned, required) {
			t.Errorf("missing meaningful text %q in %q", required, cleaned)
		}
	}
	for _, noise := range []string{"```", "$$", "| ---", "**", "](https://"} {
		if strings.Contains(cleaned, noise) {
			t.Errorf("format noise retained: %q", noise)
		}
	}
	for input, expected := range map[string]string{
		"$x*y*z$":                        "$x*y*z$",
		`\(x*y*z\)`:                      "x*y*z",
		`\[a < b\]`:                      "a < b",
		`\(|x|\)`:                        "|x|",
		`\[|x|\]`:                        "|x|",
		"$$|x|$$":                        "|x|",
		"`|flags|`":                      "|flags|",
		"[说明](https://example.test/$id)": "说明",
		"cost $20 now":                   "cost $20 now",
		"$20 and $30":                    "$20 and $30",
		"$$x*y*z$$":                      "x*y*z",
		"**计算** $$x*y*z$$ 和 `a*b*c`":     "计算 x*y*z 和 a*b*c",
		"$$a < b$$":                      "a < b",
		"``print(`x`)`` 后面的 **正文**":      "print(`x`) 后面的 正文",
	} {
		if got := CleanPassage(input); got != expected {
			t.Errorf("inline literal content changed: got %q, want %q", got, expected)
		}
	}
}
