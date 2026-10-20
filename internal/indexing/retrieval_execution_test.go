package indexing

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

type executionEmbedder func(context.Context, []string) ([][]float32, error)

func (e executionEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return e(ctx, texts)
}

type executionSearch func(context.Context, string) ([]domain.IndexCandidate, error)

func (e executionSearch) Ensure(context.Context, string, int) error { panic("unexpected Ensure") }
func (e executionSearch) Upsert(context.Context, string, string, string, []domain.IndexChunk, [][]float32) error {
	panic("unexpected Upsert")
}
func (e executionSearch) DeleteGeneration(context.Context, string, string) error {
	panic("unexpected DeleteGeneration")
}
func (e executionSearch) Search(ctx context.Context, _, _ string, _ []string, _ string, _ []float32, mode string, _ int) ([]domain.IndexCandidate, error) {
	return e(ctx, mode)
}

func executionService(strategy string, embed executionEmbedder, search executionSearch) *Service {
	return NewService(nil, nil, embed, search, nil, domain.IndexProfile{}, Options{Execution: strategy})
}

type executionResult struct {
	lexical, vector []domain.IndexCandidate
	err             error
}

func awaitExecution(t *testing.T, results <-chan executionResult) executionResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("retrieval did not stop")
		return executionResult{}
	}
}

func awaitStarted(t *testing.T, started <-chan string, expected int) {
	t.Helper()
	for range expected {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("independent retrieval branches did not both start")
		}
	}
}

func TestRetrievalExecutionOverlapsHybrid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	s := executionService("optimized", func(ctx context.Context, _ []string) ([][]float32, error) {
		started <- "embed"
		select {
		case <-release:
			return [][]float32{{1}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, func(ctx context.Context, mode string) ([]domain.IndexCandidate, error) {
		if mode == "lexical" {
			started <- "lexical"
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []domain.IndexCandidate{{ChunkID: mode}}, nil
	})
	results := make(chan executionResult, 1)
	go func() {
		lexical, vector, err := s.retrieveCandidates(ctx, "workspace", []string{"generation"}, "claim", "hybrid", 100)
		results <- executionResult{lexical, vector, err}
	}()
	// Each branch waits for release. Reaching both starts proves overlap
	// without comparing noisy wall-clock durations.
	awaitStarted(t, started, 2)
	close(release)
	result := awaitExecution(t, results)
	if result.err != nil || len(result.lexical) != 1 || len(result.vector) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRetrievalExecutionBaselineIsSequential(t *testing.T) {
	var lexicalFinished atomic.Bool
	s := executionService("baseline", func(context.Context, []string) ([][]float32, error) {
		if !lexicalFinished.Load() {
			t.Error("embedding began before lexical retrieval finished")
		}
		return [][]float32{{1}}, nil
	}, func(_ context.Context, mode string) ([]domain.IndexCandidate, error) {
		if mode == "lexical" {
			lexicalFinished.Store(true)
		}
		return nil, nil
	})
	if _, _, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", "hybrid", 100); err != nil {
		t.Fatal(err)
	}
}

func TestRetrievalExecutionFusionParity(t *testing.T) {
	lexical := []domain.IndexCandidate{{ChunkID: "a"}, {ChunkID: "a"}, {ChunkID: "b"}}
	vector := []domain.IndexCandidate{{ChunkID: "b"}, {ChunkID: "a"}, {ChunkID: "c"}}
	var baselineScores map[string]float64
	var baselineCandidates map[string]domain.IndexCandidate
	for _, strategy := range []string{"baseline", "optimized"} {
		t.Run(strategy, func(t *testing.T) {
			s := executionService(strategy, func(context.Context, []string) ([][]float32, error) {
				return [][]float32{{1}}, nil
			}, func(_ context.Context, mode string) ([]domain.IndexCandidate, error) {
				if mode == "lexical" {
					return lexical, nil
				}
				return vector, nil
			})
			lex, vec, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", "hybrid", 100)
			if err != nil {
				t.Fatal(err)
			}
			scores, candidates := mergeCandidates(lex, vec)
			if len(scores) != 3 || math.Abs(scores["a"]-(1.0/61+1.0/62)) > 1e-15 || math.Abs(scores["b"]-(1.0/63+1.0/61)) > 1e-15 || math.Abs(scores["c"]-1.0/63) > 1e-15 {
				t.Fatalf("fusion changed RRF or duplicate handling: %v", scores)
			}
			ranked := []string{"c", "b", "a"}
			sort.Slice(ranked, func(i, j int) bool {
				if scores[ranked[i]] == scores[ranked[j]] {
					return ranked[i] < ranked[j]
				}
				return scores[ranked[i]] > scores[ranked[j]]
			})
			if !reflect.DeepEqual(ranked, []string{"a", "b", "c"}) {
				t.Fatalf("unexpected ranking: %v", ranked)
			}
			if strategy == "baseline" {
				baselineScores, baselineCandidates = scores, candidates
			} else if !reflect.DeepEqual(scores, baselineScores) || !reflect.DeepEqual(candidates, baselineCandidates) {
				t.Fatalf("optimized changed fusion: %v / %v", scores, candidates)
			}
		})
	}
}

func TestRetrievalExecutionLexicalFailureCancelsOtherBranch(t *testing.T) {
	lexicalErr := &Failure{Code: "index_unavailable"}
	embedStarted := make(chan struct{})
	embedStopped := make(chan struct{})
	s := executionService("optimized", func(ctx context.Context, _ []string) ([][]float32, error) {
		close(embedStarted)
		<-ctx.Done()
		close(embedStopped)
		return nil, ctx.Err()
	}, func(_ context.Context, mode string) ([]domain.IndexCandidate, error) {
		if mode != "lexical" {
			t.Error("vector search called after canceled embedding")
		}
		<-embedStarted
		return nil, lexicalErr
	})
	results := make(chan executionResult, 1)
	go func() {
		lexical, vector, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", "hybrid", 100)
		results <- executionResult{lexical, vector, err}
	}()
	if result := awaitExecution(t, results); result.err != lexicalErr {
		t.Fatalf("lexical error priority changed: %v", result.err)
	}
	select {
	case <-embedStopped:
	default:
		t.Fatal("retrieval returned before other branch stopped")
	}
}

func TestRetrievalExecutionFailurePriority(t *testing.T) {
	lexicalErr := &Failure{Code: "index_unavailable"}
	vectorErr := &Failure{Code: "embedding_unavailable"}
	for _, strategy := range []string{"baseline", "optimized"} {
		t.Run(strategy, func(t *testing.T) {
			s := executionService(strategy, func(context.Context, []string) ([][]float32, error) {
				return nil, vectorErr
			}, func(context.Context, string) ([]domain.IndexCandidate, error) {
				return nil, lexicalErr
			})
			if _, _, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", "hybrid", 100); err != lexicalErr {
				t.Fatalf("lexical failure did not take priority: %v", err)
			}
		})
	}
}

func TestRetrievalExecutionVectorFailureWaitsForLexical(t *testing.T) {
	vectorErr := &Failure{Code: "embedding_unavailable"}
	vectorFailed := make(chan struct{})
	s := executionService("optimized", func(context.Context, []string) ([][]float32, error) {
		close(vectorFailed)
		return nil, vectorErr
	}, func(ctx context.Context, mode string) ([]domain.IndexCandidate, error) {
		if mode != "lexical" {
			t.Error("vector search called with failed embedding")
		}
		<-vectorFailed
		if ctx.Err() != nil {
			t.Error("vector failure canceled higher-priority lexical branch")
		}
		return []domain.IndexCandidate{{ChunkID: "a"}}, nil
	})
	if _, _, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", "hybrid", 100); err != vectorErr {
		t.Fatalf("unexpected failure: %v", err)
	}
}

func TestRetrievalExecutionParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	var stopped atomic.Int32
	s := executionService("optimized", func(ctx context.Context, _ []string) ([][]float32, error) {
		started <- "embed"
		<-ctx.Done()
		stopped.Add(1)
		return nil, ctx.Err()
	}, func(ctx context.Context, mode string) ([]domain.IndexCandidate, error) {
		started <- mode
		<-ctx.Done()
		stopped.Add(1)
		return nil, ctx.Err()
	})
	results := make(chan executionResult, 1)
	go func() {
		lexical, vector, err := s.retrieveCandidates(ctx, "workspace", nil, "claim", "hybrid", 100)
		results <- executionResult{lexical, vector, err}
	}()
	awaitStarted(t, started, 2)
	cancel()
	if result := awaitExecution(t, results); !errors.Is(result.err, context.Canceled) || stopped.Load() != 2 {
		t.Fatalf("cancellation did not stop both branches: %+v, stopped=%d", result, stopped.Load())
	}
}

func TestRetrievalExecutionSingleModeAndInvalidEmbedding(t *testing.T) {
	for _, strategy := range []string{"baseline", "optimized"} {
		for _, mode := range []string{"lexical", "vector", "hybrid"} {
			t.Run(strategy+"/"+mode, func(t *testing.T) {
				var embeds, lexical, vector atomic.Int32
				s := executionService(strategy, func(context.Context, []string) ([][]float32, error) {
					embeds.Add(1)
					return nil, nil
				}, func(_ context.Context, branch string) ([]domain.IndexCandidate, error) {
					if branch == "lexical" {
						lexical.Add(1)
					} else {
						vector.Add(1)
					}
					return nil, nil
				})
				_, _, err := s.retrieveCandidates(context.Background(), "workspace", nil, "claim", mode, 100)
				if mode == "lexical" {
					if err != nil || embeds.Load() != 0 || lexical.Load() != 1 {
						t.Fatalf("lexical mode called embedding: %v", err)
					}
				} else if ErrorCode(err) != "embedding_invalid" || embeds.Load() != 1 || vector.Load() != 0 {
					t.Fatalf("invalid embedding reached vector search: %v", err)
				}
				if mode == "vector" && lexical.Load() != 0 {
					t.Fatal("vector-only mode called lexical search")
				}
			})
		}
	}
}

func TestRetrievalExecutionDefault(t *testing.T) {
	for _, value := range []string{"", "baseline", "unknown"} {
		if executionService(value, nil, nil).opts.Execution != "baseline" {
			t.Fatalf("unsafe default execution for %q", value)
		}
	}
}
