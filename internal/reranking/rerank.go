// Package reranking implements the shared, bounded second stage of retrieval.
// It operates on copies and returns positions, never changing source evidence.
package reranking

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	DefaultThreshold     = 0.2
	DefaultMaxCandidates = 200
	DefaultPassageRunes  = 2048
	DefaultMMRLambda     = 0.7
	maxQueryBytes        = 16 << 10
	maxContentBytes      = 64 << 10
	maxPassageRunes      = 8192
)

var (
	ErrInvalidInput     = errors.New("rerank_invalid")
	ErrInvalidResponse  = errors.New("rerank_invalid_response")
	ErrModelUnavailable = errors.New("rerank_unavailable")
	ErrTimeout          = errors.New("rerank_timeout")
)

// Candidate contains only material the caller has already authorized and
// hydrated. Content remains the exact source chunk; cleaning makes a copy.
type Candidate struct {
	// Non-empty GroupID selects at most one scored candidate per parent.
	GroupID        string
	Title          string
	ContextHeader  string
	Content        string
	RetrievalScore float64
}

// Score uses the position in the submitted passages, not a resource identifier.
// RelevanceScore must be normalized by the provider to [0, 1].
type Score struct {
	Index          int
	RelevanceScore float64
}

type Model interface {
	Score(context.Context, string, []string) ([]Score, error)
}

type Options struct {
	TopK            int
	MaxCandidates   int
	MaxPassageRunes int
	Threshold       float64
	KeepBestInScope bool
}

func DefaultOptions() Options {
	return Options{
		TopK: 10, MaxCandidates: DefaultMaxCandidates,
		MaxPassageRunes: DefaultPassageRunes, Threshold: DefaultThreshold,
	}
}

// Selection indexes the original candidates. RetrievalScore is the bounded
// score supplied by retrieval; it is never rescaled here. ModelScore is the
// original provider score; Score is the composite score before MMR selection.
type Selection struct {
	Index          int     `json:"index"`
	ModelScore     float64 `json:"model_score"`
	RetrievalScore float64 `json:"retrieval_score"`
	Score          float64 `json:"score"`
}

// Diagnostics contains only bounded counts, numeric scores and fixed codes.
// Neither model passages nor queries belong in logs or traces.
type Diagnostics struct {
	Outcome            string  `json:"outcome"`
	Applied            bool    `json:"applied"`
	CandidateCount     int     `json:"candidate_count"`
	ResultCount        int     `json:"result_count"`
	Threshold          float64 `json:"threshold"`
	EffectiveThreshold float64 `json:"effective_threshold"`
	TopScore           float64 `json:"top_score"`
	FallbackUsed       bool    `json:"fallback_used"`
	ErrorCode          string  `json:"error_code,omitempty"`
}

type Result struct {
	Selections  []Selection
	Diagnostics Diagnostics
}

// Run distinguishes a successful rejection (empty result, nil error) from an
// unavailable or malformed model (safe error). The caller owns retrieval-order
// fallback and must recheck authorization after this external operation.
func Run(ctx context.Context, model Model, query string, candidates []Candidate, opts Options) (Result, error) {
	result := Result{Selections: []Selection{}, Diagnostics: Diagnostics{
		Threshold: opts.Threshold, EffectiveThreshold: opts.Threshold,
	}}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !validOptions(opts) || !utf8.ValidString(query) || len(query) > maxQueryBytes ||
		strings.TrimSpace(query) == "" || len(candidates) > 1000 {
		return result, ErrInvalidInput
	}
	positions := make([]int, len(candidates))
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !finite(candidate.RetrievalScore) || candidate.RetrievalScore < 0 || candidate.RetrievalScore > 1 ||
			!utf8.ValidString(candidate.Content) || len(candidate.Content) > maxContentBytes ||
			!utf8.ValidString(candidate.Title) || len(candidate.Title) > maxContentBytes ||
			!utf8.ValidString(candidate.ContextHeader) || len(candidate.ContextHeader) > maxContentBytes {
			return result, ErrInvalidInput
		}
		positions[i] = i
	}
	sort.SliceStable(positions, func(i, j int) bool {
		return candidates[positions[i]].RetrievalScore > candidates[positions[j]].RetrievalScore
	})
	positions = positions[:min(len(positions), opts.MaxCandidates)]
	submitted := make([]int, 0, len(positions))
	passages := make([]string, 0, len(positions))
	bodies := make([]string, 0, len(positions))
	for _, position := range positions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		candidate := candidates[position]
		body := CleanPassage(candidate.Content)
		if strings.TrimSpace(body) == "" {
			continue
		}
		submitted = append(submitted, position)
		passages = append(passages, buildPassage(candidate, body, opts.MaxPassageRunes))
		bodies = append(bodies, truncateRunes(body, opts.MaxPassageRunes))
	}
	result.Diagnostics.CandidateCount = len(submitted)
	if len(submitted) == 0 {
		result.Diagnostics.Outcome = "no_candidates"
		return result, nil
	}
	if model == nil {
		return failed(result, ErrModelUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	scores, err := model.Score(ctx, query, passages)
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidResponse):
			return failed(result, ErrInvalidResponse)
		case errors.Is(err, ErrTimeout):
			return failed(result, ErrTimeout)
		default:
			return failed(result, ErrModelUnavailable)
		}
	}
	if !validScores(scores, len(submitted)) {
		return failed(result, ErrInvalidResponse)
	}
	result.Diagnostics.Applied = true
	// Canonicalize provider ordering so equal-score ties do not depend on its
	// sorting implementation. Submitted rows already have stable retrieval order.
	modelScores := make([]float64, len(scores))
	for _, score := range scores {
		modelScores[score.Index] = score.RelevanceScore
	}
	top := 0
	for i, score := range modelScores {
		if score > modelScores[top] {
			top = i
		}
	}
	result.Diagnostics.TopScore = modelScores[top]
	threshold := opts.Threshold
	passing := filter(modelScores, threshold)
	result.Diagnostics.Outcome = "ok"
	if len(passing) == 0 && threshold > 0.3 {
		threshold = math.Max(threshold*0.7, 0.3)
		passing = filter(modelScores, threshold)
		result.Diagnostics.Outcome = "threshold_degraded"
	}
	result.Diagnostics.EffectiveThreshold = threshold
	if len(passing) == 0 {
		if modelScores[top] >= 0.15 || opts.KeepBestInScope {
			passing = []int{top}
			result.Diagnostics.Outcome = "fallback_top1"
			result.Diagnostics.FallbackUsed = true
		} else {
			result.Diagnostics.Outcome = "all_below_threshold"
			return result, nil
		}
	}
	scored := make([]Selection, 0, len(passing))
	mmrBodies := make([]string, 0, len(passing))
	for _, index := range passing {
		retrieval := candidates[submitted[index]].RetrievalScore
		score := modelScores[index]
		scored = append(scored, Selection{
			Index: submitted[index], ModelScore: score, RetrievalScore: retrieval,
			Score: math.Min(1, math.Max(0, 0.6*score+0.3*retrieval+0.1)),
		})
		mmrBodies = append(mmrBodies, bodies[index])
	}
	// Keep the strongest scored sibling before MMR, so duplicate child hits
	// cannot consume the parent Top K budget. All siblings were scored first.
	groups := make(map[string]int)
	unique := make([]Selection, 0, len(scored))
	uniqueBodies := make([]string, 0, len(scored))
	for i, selection := range scored {
		group := candidates[selection.Index].GroupID
		if position, ok := groups[group]; group != "" && ok {
			if selection.Score > unique[position].Score {
				unique[position], uniqueBodies[position] = selection, mmrBodies[i]
			}
			continue
		}
		if group != "" {
			groups[group] = len(unique)
		}
		unique = append(unique, selection)
		uniqueBodies = append(uniqueBodies, mmrBodies[i])
	}
	selected, err := selectMMR(ctx, unique, uniqueBodies, opts.TopK)
	if err != nil {
		return result, err
	}
	result.Selections = selected
	result.Diagnostics.ResultCount = len(selected)
	return result, nil
}

func failed(result Result, err error) (Result, error) {
	result.Diagnostics.Outcome = "model_error"
	result.Diagnostics.ErrorCode = err.Error()
	return result, err
}

func validOptions(opts Options) bool {
	return opts.TopK > 0 && opts.TopK <= DefaultMaxCandidates &&
		opts.MaxCandidates > 0 && opts.MaxCandidates <= DefaultMaxCandidates &&
		opts.MaxPassageRunes >= 128 && opts.MaxPassageRunes <= maxPassageRunes &&
		finite(opts.Threshold) && opts.Threshold >= 0 && opts.Threshold <= 1
}

func validScores(scores []Score, count int) bool {
	if len(scores) != count {
		return false
	}
	seen := make([]bool, count)
	for _, score := range scores {
		if score.Index < 0 || score.Index >= count || seen[score.Index] ||
			!finite(score.RelevanceScore) || score.RelevanceScore < 0 || score.RelevanceScore > 1 {
			return false
		}
		seen[score.Index] = true
	}
	return true
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func filter(scores []float64, threshold float64) []int {
	passing := make([]int, 0, len(scores))
	for i, score := range scores {
		if score >= threshold {
			passing = append(passing, i)
		}
	}
	return passing
}
