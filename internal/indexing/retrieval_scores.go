package indexing

import "math"

// normalizeRetrievalScores runs before hydration, candidate truncation and
// reranking. Missing branches still count in the hybrid theoretical maximum:
// a candidate ranked first by only one of the two retrievers scores 0.5.
// Qdrant uses zero-based k=61, equivalent to one-based k=60.
func normalizeRetrievalScores(scores map[string]float64, mode string, native bool) {
	divisor := 1.0
	switch {
	case mode == "hybrid":
		divisor = 2.0 / 61
	case !native:
		// The compatibility merger represents a single branch by reciprocal rank.
		divisor = 1.0 / 61
	case mode == "lexical":
		// BM25 is unbounded. Only rescale if the full recalled set exceeds one;
		// already bounded values retain their magnitude.
		for _, score := range scores {
			divisor = math.Max(divisor, score)
		}
	}
	for id, score := range scores {
		scores[id] = math.Max(0, math.Min(1, score/divisor))
	}
}
