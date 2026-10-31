package indexing

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// contextHeaders reads the persisted enrichment without reconstructing a
// document. Callers may pass arbitrary children, parents, or mixed subsets.
func contextHeaders(chunks []domain.IndexChunk) map[string]string {
	headers := make(map[string]string, len(chunks))
	for _, chunk := range chunks {
		headers[chunk.ID] = chunk.ContextHeader
	}
	return headers
}

// assignContextHeaders derives context once from canonical parser text. Each
// level is in source order, including children with semantic overlap.
func assignContextHeaders(text string, levels ...[]domain.IndexChunk) {
	type heading struct {
		offset int
		path   string
	}
	var headings []heading
	var path [6]string
	fenceChar, fenceLength := rune(0), 0
	offset := 0
	for _, line := range strings.SplitAfter(text, "\n") {
		trimmed := strings.TrimSpace(line)
		char, count, closes := chunkFence(trimmed)
		if fenceChar != 0 {
			if char == fenceChar && count >= fenceLength && closes {
				fenceChar, fenceLength = 0, 0
			}
		} else if char != 0 {
			fenceChar, fenceLength = char, count
		} else if level, title := contextHeading(trimmed); level > 0 {
			path[level-1] = title
			for i := level; i < len(path); i++ {
				path[i] = ""
			}
			parts := make([]string, 0, 6)
			for _, title := range path {
				if title != "" {
					parts = append(parts, title)
				}
			}
			headings = append(headings, heading{offset, strings.Join(parts, " → ")})
		}
		offset += len(line)
	}
	for _, chunks := range levels {
		cursor, header := 0, ""
		for i := range chunks {
			chunk := &chunks[i]
			// A heading preceded only by whitespace at the start belongs to
			// this passage; a later section does not rename the whole chunk.
			start := chunk.StartByte + len(chunk.Content) - len(strings.TrimLeftFunc(chunk.Content, unicode.IsSpace))
			for cursor < len(headings) && headings[cursor].offset <= start {
				header = headings[cursor].path
				cursor++
			}
			chunk.ContextHeader = header
		}
	}
}

func contextHeading(line string) (int, string) {
	if utf8.RuneCountInString(line) > 120 || !isChunkHeading(line) {
		return 0, ""
	}
	level := len(line) - len(strings.TrimLeft(line, "#"))
	if level > 0 {
		title := strings.TrimSpace(line[level:])
		trimmed := strings.TrimRight(title, "#")
		// Closing hashes require preceding whitespace: "## C#" keeps C#.
		if len(trimmed) < len(title) && strings.TrimRightFunc(trimmed, unicode.IsSpace) != trimmed {
			title = strings.TrimSpace(trimmed)
		}
		return level, title
	}
	// Explicit Chinese chapters reset the hierarchy; sections remain below.
	for _, r := range strings.TrimPrefix(line, "第") {
		if strings.ContainsRune("章篇部", r) {
			return 1, line
		}
		if r == '节' {
			return 2, line
		}
	}
	return 0, ""
}
