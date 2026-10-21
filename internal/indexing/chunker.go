package indexing

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const (
	boundaryWord uint8 = iota + 1
	boundaryLine
	boundarySentence
	boundaryParagraph
	boundaryBlock
	boundaryHeading
	boundaryRankMask  uint8 = 7
	boundaryProtected uint8 = 1 << 3
	boundaryNoCut     uint8 = 1 << 4
	boundarySection   uint8 = 1 << 5
)

// Split returns the retrievable children of the current parent-child strategy.
func Split(text string) ([]domain.IndexChunk, error) {
	_, children, err := SplitParentChild(text)
	return children, err
}

// SplitParentChild preserves canonical parser bytes at both levels. Parents
// provide context; only their smaller children are embedded and retrieved.
func SplitParentChild(text string) (parents, children []domain.IndexChunk, err error) {
	if len(text) > domain.MaxExtractedBytes {
		return nil, nil, &Failure{Code: "extraction_limit"}
	}
	if !validParserText(text) {
		return nil, nil, &Failure{Code: "invalid_text"}
	}
	if strings.TrimSpace(text) == "" {
		return nil, nil, &Failure{Code: "empty_document"}
	}
	runes := []rune(text)
	offsets := make([]int, 0, len(runes)+1)
	for offset := range text {
		offsets = append(offsets, offset)
	}
	offsets = append(offsets, len(text))
	parentBoundaries := chunkBoundaries(text, runes, offsets, domain.ParentChunkRunes)
	childBoundaries := chunkBoundaries(text, runes, offsets, domain.ChunkRunes)
	parents = splitStructured(text, offsets, parentBoundaries, domain.ParentChunkRunes, 0, false)
	if parents == nil {
		parents = splitStructured(text, offsets, parentBoundaries, domain.ParentChunkRunes, 0, true)
	}
	parentStart := 0
	for i := range parents {
		parent := &parents[i]
		parent.Kind = "parent"
		parent.ParentOrdinal = -1
		parentEnd := parentStart + utf8.RuneCountInString(parent.Content)
		// Keep document-wide code/table state even if a parent begins inside
		// an oversized block. Byte offsets remain relative to the source.
		localOffsets := offsets[parentStart : parentEnd+1]
		localBoundaries := childBoundaries[parentStart : parentEnd+1]
		subs := splitStructured(text, localOffsets, localBoundaries, domain.ChunkRunes, domain.ChunkOverlapRunes, false)
		if subs == nil {
			subs = splitStructured(text, localOffsets, localBoundaries, domain.ChunkRunes, domain.ChunkOverlapRunes, true)
		}
		if len(subs) == 0 || len(parents)+len(children)+len(subs) > domain.MaxIndexChunks {
			return nil, nil, &Failure{Code: "extraction_limit"}
		}
		for _, child := range subs {
			child.Ordinal = len(children)
			child.Kind = "child"
			child.ParentOrdinal = i
			children = append(children, child)
		}
		parentStart = parentEnd
	}
	if len(parents) == 0 {
		return nil, nil, &Failure{Code: "extraction_limit"}
	}
	assignContextHeaders(text, parents, children)
	return parents, children, nil
}

func splitStructured(text string, offsets []int, boundaries []uint8, size, overlap int, dense bool) []domain.IndexChunk {
	runes := len(offsets) - 1
	chunks := make([]domain.IndexChunk, 0, runes/size+1)
	previousEnd := 0
	for start := 0; start < runes; {
		end := min(start+size, runes)
		if end < runes {
			if !dense {
				// Retreat to the start of a fitting protected block. This can
				// make a short chunk, but never an empty or non-progressing one.
				for end > previousEnd && boundaries[end]&boundaryNoCut != 0 {
					end--
				}
				if end <= previousEnd {
					return nil // Overlapping protections exceed this chunk's budget.
				}
			}
			minimum := start + size/2
			if dense {
				minimum = start + size - overlap
			}
			minimum = max(minimum, previousEnd+1)
			best, rank := end, uint8(0)
			for pos := minimum; pos <= end; pos++ {
				flags := boundaries[pos]
				if !dense && flags&boundaryNoCut != 0 {
					continue
				}
				if candidate := flags & boundaryRankMask; candidate >= rank && candidate != 0 {
					best, rank = pos, candidate
				}
			}
			end = best
		}
		if len(chunks) == domain.MaxIndexChunks {
			return nil
		}
		content := text[offsets[start]:offsets[end]]
		digest := sha256.Sum256([]byte(content))
		chunks = append(chunks, domain.IndexChunk{
			Ordinal: len(chunks), StartByte: offsets[start], EndByte: offsets[end],
			Content: content, SHA256: hex.EncodeToString(digest[:]),
		})
		if end == runes {
			break
		}
		previousEnd = end
		start = semanticOverlap(boundaries, start, end, overlap)
	}
	return chunks
}

// A repeated suffix must start at a complete sentence or paragraph boundary.
// Long sentences/rows without such a suffix simply have zero overlap; a fixed
// window would otherwise introduce truncated sentences and partial table rows.
func semanticOverlap(boundaries []uint8, start, end, overlap int) int {
	if boundaries[end]&(boundaryProtected|boundarySection) != 0 {
		return end
	}
	minimum := max(start+1, end-overlap)
	for pos := end - 1; pos >= minimum; pos-- {
		if boundaries[pos]&(boundaryProtected|boundarySection) != 0 {
			minimum = pos + 1
			break
		}
	}
	for pos := minimum; pos < end; pos++ {
		if boundaries[pos]&boundaryRankMask >= boundarySentence {
			return pos
		}
	}
	return end
}

func setChunkBoundary(boundaries []uint8, pos int, rank uint8) {
	if rank > boundaries[pos]&boundaryRankMask {
		boundaries[pos] = boundaries[pos]&^boundaryRankMask | rank
	}
}

func chunkBoundaries(text string, runes []rune, offsets []int, size int) []uint8 {
	boundaries := make([]uint8, len(runes)+1)
	for i := 0; i < len(runes); i++ {
		if unicode.IsSpace(runes[i]) {
			// Mark only the end of a whitespace run, retaining every byte.
			end := i + 1
			newlines := 0
			for end <= len(runes) && unicode.IsSpace(runes[end-1]) {
				if runes[end-1] == '\n' {
					newlines++
				}
				if end == len(runes) || !unicode.IsSpace(runes[end]) {
					break
				}
				end++
			}
			rank := boundaryWord
			if newlines > 0 {
				rank = boundaryLine
			}
			if newlines > 1 {
				rank = boundaryParagraph
			}
			setChunkBoundary(boundaries, end, rank)
			i = end - 1
			continue
		}
		if !isSentenceEnd(runes, i) {
			continue
		}
		end := i + 1
		for end < len(runes) && strings.ContainsRune("\"'”’」』）》】)]}", runes[end]) {
			end++
		}
		for end < len(runes) && unicode.IsSpace(runes[end]) {
			end++
		}
		setChunkBoundary(boundaries, end, boundarySentence)
	}
	markChunkStructure(text, runes, offsets, boundaries, size)
	return boundaries
}

func isSentenceEnd(runes []rune, i int) bool {
	switch runes[i] {
	case '。', '！', '？', '；':
		return true
	case '!', '?', ';':
		end := i + 1
		for end < len(runes) && strings.ContainsRune("\"'”’)]}", runes[end]) {
			end++
		}
		return end == len(runes) || unicode.IsSpace(runes[end])
	case '.':
		end := i + 1
		for end < len(runes) && strings.ContainsRune("\"'”’)]}", runes[end]) {
			end++
		}
		if end < len(runes) && !unicode.IsSpace(runes[end]) {
			return false // Decimal numbers, URLs, and dotted identifiers.
		}
		start := i - 1
		for start >= 0 && unicode.IsLetter(runes[start]) && i-start <= 5 {
			start--
		}
		word := strings.ToLower(string(runes[start+1 : i]))
		switch word {
		case "mr", "mrs", "ms", "dr", "prof", "vs", "etc", "fig", "no":
			return false
		}
		return len(word) != 1 // Initials such as A. Smith are not sentences.
	}
	return false
}

type chunkLine struct{ start, end, next int }

func nextChunkLine(runes []rune, start int) chunkLine {
	end := start
	for end < len(runes) && runes[end] != '\n' {
		end++
	}
	next := end
	if next < len(runes) {
		next++
	}
	return chunkLine{start, end, next}
}

func markChunkStructure(text string, runes []rune, offsets []int, boundaries []uint8, size int) {
	var fence rune
	fenceCount, fenceStart := 0, 0
	tableStart, tableEnd := -1, 0
	var previous chunkLine
	previousTableHeader := false
	for pos := 0; pos < len(runes); {
		line := nextChunkLine(runes, pos)
		raw := text[offsets[line.start]:offsets[line.end]]
		trimmed := strings.TrimSpace(raw)
		marker, count, closing := chunkFence(trimmed)
		if fence != 0 {
			keepCodeLineBoundary(boundaries, line.next)
			if marker == fence && count >= fenceCount && closing {
				markProtectedChunk(boundaries, fenceStart, line.next, true, size)
				fence = 0
			}
			pos = line.next
			continue
		}
		if tableStart >= 0 && (!strings.ContainsRune(trimmed, '|') || trimmed == "" || marker != 0) {
			markProtectedChunk(boundaries, tableStart, tableEnd, true, size)
			tableStart = -1
		}
		if marker != 0 {
			keepCodeLineBoundary(boundaries, line.next)
			fence, fenceCount, fenceStart = marker, count, line.start
			previousTableHeader = false
		} else if tableStart >= 0 {
			tableEnd = line.next
		} else if previousTableHeader && isTableDelimiter(trimmed) {
			tableStart, tableEnd = previous.start, line.next
		} else if isChunkHeading(trimmed) {
			setChunkBoundary(boundaries, line.start, boundaryHeading)
			boundaries[line.start] |= boundarySection
			markProtectedChunk(boundaries, line.start, headingBodyEnd(text, runes, offsets, line, size), false, size)
			// A heading's newline is not a useful chunk end. Its following
			// body should travel with it whenever the size budget allows.
			boundaries[line.next] &= ^boundaryRankMask
		}
		previous, previousTableHeader = line, strings.ContainsRune(trimmed, '|') && trimmed != ""
		pos = line.next
	}
	if fence != 0 {
		markProtectedChunk(boundaries, fenceStart, len(runes), true, size)
	}
	if tableStart >= 0 {
		markProtectedChunk(boundaries, tableStart, tableEnd, true, size)
	}
}

func keepCodeLineBoundary(boundaries []uint8, pos int) {
	if boundaries[pos]&boundaryRankMask == boundarySentence {
		boundaries[pos] = boundaries[pos]&^boundaryRankMask | boundaryLine
	}
}

// Bind a heading to its first body line when it fits. This includes intervening
// blank lines, which otherwise create a high-priority cut just after the title.
// Separate code/table protection owns those blocks so overlapping protections
// cannot accidentally require a span larger than the hard limit.
func headingBodyEnd(text string, runes []rune, offsets []int, heading chunkLine, size int) int {
	start := heading.next
	for start < len(runes) && unicode.IsSpace(runes[start]) {
		start++
	}
	if start == len(runes) {
		return heading.next
	}
	body := nextChunkLine(runes, start)
	line := strings.TrimSpace(text[offsets[body.start]:offsets[body.end]])
	marker, _, _ := chunkFence(line)
	if marker != 0 || strings.ContainsRune(line, '|') || isChunkHeading(line) {
		return heading.next
	}
	end := body.next
	if end-heading.start > size {
		end = min(body.end, start+64)
	}
	if end-heading.start > size {
		return heading.next
	}
	return end
}

func markProtectedChunk(boundaries []uint8, start, end int, block bool, size int) {
	for pos := start; pos <= end; pos++ {
		boundaries[pos] |= boundaryProtected
		if pos > start && pos < end && end-start <= size {
			boundaries[pos] |= boundaryNoCut
		}
		if block && pos > start && pos < end && boundaries[pos]&boundaryRankMask == boundarySentence {
			boundaries[pos] = boundaries[pos]&^boundaryRankMask | boundaryWord
		}
	}
	if block {
		setChunkBoundary(boundaries, start, boundaryBlock)
		setChunkBoundary(boundaries, end, boundaryBlock)
	}
}

func chunkFence(line string) (rune, int, bool) {
	if len(line) < 3 || (line[0] != '`' && line[0] != '~') {
		return 0, 0, false
	}
	count := 0
	for count < len(line) && line[count] == line[0] {
		count++
	}
	if count < 3 || (line[0] == '`' && strings.ContainsRune(line[count:], '`')) {
		return 0, 0, false
	}
	return rune(line[0]), count, strings.TrimSpace(line[count:]) == ""
}

func isTableDelimiter(line string) bool {
	if !strings.ContainsRune(line, '|') {
		return false
	}
	line = strings.Trim(strings.TrimSpace(line), "|")
	cells := 0
	for len(line) > 0 {
		cell, tail, found := strings.Cut(line, "|")
		cell = strings.Trim(strings.TrimSpace(cell), ":")
		if len(cell) < 3 || strings.Trim(cell, "-") != "" {
			return false
		}
		cells++
		if !found {
			break
		}
		line = tail
	}
	return cells >= 2
}

func isChunkHeading(line string) bool {
	if len(line) == 0 || utf8.RuneCountInString(line) > 120 {
		return false
	}
	if line[0] == '#' {
		i := 0
		for i < len(line) && line[i] == '#' {
			i++
		}
		return i <= 6 && i < len(line) && (line[i] == ' ' || line[i] == '\t') && strings.TrimSpace(line[i:]) != ""
	}
	if !strings.HasPrefix(line, "第") {
		return false
	}
	for _, r := range strings.TrimPrefix(line, "第") {
		if strings.ContainsRune("章节篇部", r) {
			return !strings.ContainsAny(line, "。！？；")
		}
		if !strings.ContainsRune("零〇一二三四五六七八九十百千万两0123456789 ", r) {
			return false
		}
	}
	return false
}
