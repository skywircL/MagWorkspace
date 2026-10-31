package indexing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"magworkspace/internal/domain"
	"magworkspace/internal/observability"
	"magworkspace/internal/reranking"
)

type SearchResult struct {
	Hits              []domain.SearchHit    `json:"hits"`
	Rerank            reranking.Diagnostics `json:"rerank"`
	RetrievalPolicyID string                `json:"retrieval_policy_id"`
}

// SearchScopedDetailed is shared by the API and Agent tools. An explicit false
// supports comparisons on the same immutable index without re-embedding.
func (s *Service) SearchScopedDetailed(ctx context.Context, userID, workspaceID, query, mode string, limit int, documentIDs []string, enabled *bool) (SearchResult, error) {
	return s.searchScopedDetailed(ctx, userID, workspaceID, nil, query, mode, limit, documentIDs, enabled, nil)
}

// Knowledge-base searches use the same retrieval and reranking policy. The
// database scope is retained through every revalidation, including empty output.
func (s *Service) SearchKnowledgeBase(ctx context.Context, userID, workspaceID, knowledgeBaseID, query, mode string, limit int, enabled *bool) (SearchResult, error) {
	if knowledgeBaseID == "" {
		return SearchResult{}, domain.ErrInvalid
	}
	return s.searchScopedDetailed(ctx, userID, workspaceID, []string{knowledgeBaseID}, query, mode, limit, nil, enabled, nil)
}

func (s *Service) searchScopedDetailed(ctx context.Context, userID, workspaceID string, knowledgeBaseIDs []string, query, mode string, limit int, documentIDs []string, enabled *bool, resources *domain.ResourceScope) (SearchResult, error) {
	use := s.opts.Reranker != nil && (enabled == nil || *enabled)
	result := SearchResult{RetrievalPolicyID: s.retrievalPolicyID(use)}
	result.Rerank.Outcome = "disabled"
	if use {
		result.Rerank.Outcome = "no_candidates"
		result.Rerank.Threshold = s.opts.RerankOptions.Threshold
		result.Rerank.EffectiveThreshold = s.opts.RerankOptions.Threshold
	}
	hits, err := s.searchScoped(ctx, userID, workspaceID, knowledgeBaseIDs, query, mode, limit, documentIDs, use, &result.Rerank, resources)
	result.Hits = hits
	result.Rerank.ResultCount = len(hits)
	return result, err
}

// The index profile describes persistent artifacts; this separate policy
// describes request-time ranking. Changing it never invalidates stored chunks.
func (s *Service) retrievalPolicyID(enabled bool) string {
	policy := []any{"retrieval-policy-v3/score-children-select-distinct-parents", s.Execution(), "candidate-cap-200", "fusion-theoretical-rrf61/single-bounded-v1", enabled}
	if enabled {
		policy = append(policy, "passage-context-v1/retrieval-direct/.6-.3-.1/mmr-body-jaccard-.7",
			s.opts.RerankModelID, s.opts.RerankOptions.MaxCandidates,
			s.opts.RerankOptions.MaxPassageRunes, s.opts.RerankOptions.Threshold)
	}
	data, _ := json.Marshal(policy)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Service) rerankHits(ctx context.Context, query string, hits []domain.SearchHit, headers map[string]string, limit int, scoped bool, diagnostics *reranking.Diagnostics) ([]domain.SearchHit, error) {
	candidates := make([]reranking.Candidate, len(hits))
	for i, hit := range hits {
		candidates[i] = reranking.Candidate{
			GroupID: hit.Chunk.ParentID,
			Title:   hit.Title, ContextHeader: headers[hit.Chunk.ID],
			Content: hit.Chunk.Content, RetrievalScore: hit.Score,
		}
	}
	opts := s.opts.RerankOptions
	opts.TopK, opts.KeepBestInScope = limit, scoped
	stageCtx, finish := s.opts.Telemetry.Start(ctx, "retrieval.rerank")
	started := time.Now()
	// Reserve time for the final authorization check and response. Expiry of
	// this model-only budget degrades; cancellation of the caller still fails.
	modelCtx, cancel := rerankContext(stageCtx)
	defer cancel()
	result, err := reranking.Run(modelCtx, s.opts.Reranker, query, candidates, opts)
	if err != nil && modelCtx.Err() != nil && ctx.Err() == nil {
		err = reranking.ErrTimeout
		result.Diagnostics.Outcome = "model_error"
		result.Diagnostics.ErrorCode = reranking.ErrTimeout.Error()
	}
	*diagnostics = result.Diagnostics
	if err != nil && ctx.Err() == nil {
		diagnostics.Outcome = "model_error"
		diagnostics.FallbackUsed = true
		diagnostics.ResultCount = min(len(hits), limit)
	}
	modelScores, compositeScores := make([]float64, 0, len(result.Selections)), make([]float64, 0, len(result.Selections))
	for _, selected := range result.Selections {
		modelScores = append(modelScores, selected.ModelScore)
		compositeScores = append(compositeScores, selected.Score)
	}
	trace.SpanFromContext(stageCtx).SetAttributes(
		attribute.String("rerank.outcome", diagnostics.Outcome),
		attribute.Bool("rerank.applied", diagnostics.Applied),
		attribute.Int("rerank.candidates", diagnostics.CandidateCount),
		attribute.Int("rerank.results", diagnostics.ResultCount),
		attribute.Float64("rerank.threshold", diagnostics.Threshold),
		attribute.Float64("rerank.effective_threshold", diagnostics.EffectiveThreshold),
		attribute.Float64("rerank.top_score", diagnostics.TopScore),
		attribute.Bool("rerank.fallback_used", diagnostics.FallbackUsed),
		attribute.Float64Slice("rerank.selected_model_scores", modelScores),
		attribute.Float64Slice("rerank.selected_composite_scores", compositeScores),
	)
	finish(err)
	observability.Logger(ctx).InfoContext(ctx, "rerank finished",
		"outcome", diagnostics.Outcome, "error_code", diagnostics.ErrorCode,
		"applied", diagnostics.Applied, "candidates", diagnostics.CandidateCount,
		"results", diagnostics.ResultCount, "threshold", diagnostics.Threshold,
		"effective_threshold", diagnostics.EffectiveThreshold, "top_score", diagnostics.TopScore,
		"duration_ms", time.Since(started).Milliseconds())
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		// Only dependency errors degrade. Cancellation of the parent request is
		// never converted into a successful search. No raw provider errors leak.
		return hits, nil
	}
	selected := make([]domain.SearchHit, 0, len(result.Selections))
	for _, selection := range result.Selections {
		hit := hits[selection.Index]
		raw, model, normalized := hit.Score, selection.ModelScore, selection.RetrievalScore
		hit.RetrievalScore, hit.RerankScore, hit.NormalizedRetrievalScore = &raw, &model, &normalized
		hit.Score = selection.Score
		selected = append(selected, hit)
	}
	return selected, nil
}

func rerankContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(ctx, deadline.Add(-2*time.Second))
	}
	return context.WithCancel(ctx)
}

func sortRetrievalHits(hits []domain.SearchHit) {
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].Chunk.ID < hits[j].Chunk.ID
		}
		return hits[i].Score > hits[j].Score
	})
}

func (s *Service) currentHits(ctx context.Context, userID, workspaceID string, knowledgeBaseIDs, ids, documentIDs []string, published map[string]domain.IndexJob, hits []domain.SearchHit, resources *domain.ResourceScope) ([]domain.SearchHit, error) {
	stageCtx, finish := s.opts.Telemetry.Start(ctx, "retrieval.filter")
	var refs []domain.IndexReference
	var err error
	if resources != nil {
		refs, err = s.db.ResolveIndexesForResources(stageCtx, userID, workspaceID, s.profile.ID, ids, *resources)
	} else {
		refs, err = s.db.ResolveIndexesInKnowledgeBases(stageCtx, userID, workspaceID, s.profile.ID, knowledgeBaseIDs, ids, documentIDs)
	}
	finish(err)
	if err != nil {
		return nil, err
	}
	current := make(map[string]domain.IndexReference)
	for _, ref := range refs {
		prior := published[ref.Job.ID]
		if prior.Generation == ref.Job.Generation && ref.Job.StorageID == s.profile.StorageID && prior.StorageID == ref.Job.StorageID {
			current[ref.Job.ID] = ref
		}
	}
	filtered := hits[:0]
	for _, hit := range hits {
		if ref, ok := current[hit.Chunk.IndexID]; ok {
			hit.Title = ref.Title
			filtered = append(filtered, hit)
		}
	}
	return filtered, nil
}

// SearchKnowledgeBasesScoped intersects optional library selection with the
// backend-owned run document scope before recall and every trusted reread.
func (s *Service) SearchKnowledgeBasesScoped(ctx context.Context, userID, workspaceID, query, mode string, limit int, documentIDs, knowledgeBaseIDs []string) ([]domain.SearchHit, error) {
	result, err := s.searchScopedDetailed(ctx, userID, workspaceID, knowledgeBaseIDs, query, mode, limit, documentIDs, nil, nil)
	return result.Hits, err
}

// SearchResourcesScoped searches the union of backend-authorized per-library
// targets in one candidate pool and retains that boundary through reranking.
func (s *Service) SearchResourcesScoped(ctx context.Context, userID, workspaceID, query, mode string, limit int, scope domain.ResourceScope) ([]domain.SearchHit, error) {
	result, err := s.searchScopedDetailed(ctx, userID, workspaceID, nil, query, mode, limit, nil, nil, &scope)
	return result.Hits, err
}
