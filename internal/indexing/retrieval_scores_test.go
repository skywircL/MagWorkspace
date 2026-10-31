package indexing

import (
	"math"
	"testing"
)

func TestRetrievalScoresUseTheoreticalHybridMaximum(t *testing.T) {
	for _, native := range []bool{false, true} {
		scores := map[string]float64{
			"both_first": 2.0 / 61,
			"one_first":  1.0 / 61,
			"both_later": 1.0/65 + 1.0/70,
		}
		normalizeRetrievalScores(scores, "hybrid", native)
		if scores["both_first"] != 1 || scores["one_first"] != 0.5 ||
			math.Abs(scores["both_later"]-(1.0/65+1.0/70)/(2.0/61)) > 1e-12 {
			t.Fatalf("wrong theoretical normalization: %+v", scores)
		}
		// Neither an empty second branch nor removing the strongest candidate
		// may promote the remaining first-ranked single-branch hit to one.
		alone := map[string]float64{"one_first": 1.0 / 61}
		normalizeRetrievalScores(alone, "hybrid", native)
		if alone["one_first"] != scores["one_first"] {
			t.Fatal("normalization depends on the observed candidate maximum")
		}
	}
}

func TestRetrievalScoresSingleBranchBounds(t *testing.T) {
	for _, test := range []struct {
		mode        string
		native      bool
		input, want float64
	}{
		{"vector", true, 0.7, 0.7},
		{"lexical", true, 0.6, 0.6},
		{"lexical", true, 8, 1},
		{"vector", false, 1.0 / 61, 1},
	} {
		scores := map[string]float64{"hit": test.input, "zero": 0}
		normalizeRetrievalScores(scores, test.mode, test.native)
		if scores["hit"] != test.want || scores["zero"] != 0 {
			t.Fatalf("%+v: %+v", test, scores)
		}
	}
}
