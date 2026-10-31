package reranking

import (
	"context"
	"math"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// selectMMR compares only cleaned body tokens. A common title or heading is
// useful to the cross encoder but must not make unrelated sections duplicates.
func selectMMR(ctx context.Context, scored []Selection, bodies []string, count int) ([]Selection, error) {
	tokens := make([]map[string]struct{}, len(bodies))
	for i, body := range bodies {
		tokens[i] = tokenize(body)
	}
	selected := make([]Selection, 0, min(count, len(scored)))
	taken := make([]bool, len(scored))
	redundancy := make([]float64, len(scored))
	for len(selected) < count && len(selected) < len(scored) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		best, bestValue := -1, math.Inf(-1)
		for i, candidate := range scored {
			value := DefaultMMRLambda*candidate.Score - (1-DefaultMMRLambda)*redundancy[i]
			if !taken[i] && value > bestValue {
				best, bestValue = i, value
			}
		}
		selected = append(selected, scored[best])
		taken[best] = true
		for i := range scored {
			if !taken[i] {
				redundancy[i] = math.Max(redundancy[i], jaccard(tokens[best], tokens[i]))
			}
		}
	}
	return selected, nil
}

func tokenize(text string) map[string]struct{} {
	result := make(map[string]struct{})
	var word strings.Builder
	previousHan := rune(0)
	flush := func() {
		if word.Len() > 0 {
			result[word.String()] = struct{}{}
			word.Reset()
		}
	}
	for _, r := range norm.NFKC.String(strings.ToLower(text)) {
		if unicode.Is(unicode.Han, r) {
			flush()
			result[string(r)] = struct{}{}
			if previousHan != 0 {
				result[string([]rune{previousHan, r})] = struct{}{}
			}
			previousHan = r
			continue
		}
		previousHan = 0
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			word.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return result
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	intersection := 0
	for token := range a {
		if _, ok := b[token]; ok {
			intersection++
		}
	}
	return float64(intersection) / float64(len(a)+len(b)-intersection)
}
