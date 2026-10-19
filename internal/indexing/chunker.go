package indexing

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

// Split preserves its input bytes. Callers pass Parser's normalized text so
// every returned span can be resolved directly against that stored text.
func Split(text string) ([]domain.IndexChunk, error) {
	if len(text) > domain.MaxExtractedBytes {
		return nil, &Failure{Code: "extraction_limit"}
	}
	if !validParserText(text) {
		return nil, &Failure{Code: "invalid_text"}
	}
	if strings.TrimSpace(text) == "" {
		return nil, &Failure{Code: "empty_document"}
	}
	offsets := make([]int, 0, utf8.RuneCountInString(text)+1)
	for offset := range text {
		offsets = append(offsets, offset)
	}
	offsets = append(offsets, len(text))
	runes := len(offsets) - 1
	chunks := make([]domain.IndexChunk, 0, (runes/domain.ChunkRunes)+1)
	for start := 0; start < runes; {
		end := min(start+domain.ChunkRunes, runes)
		if end < runes {
			// Prefer a paragraph, then a newline in the final overlap-sized
			// window. Keep enough new text to avoid tiny, repeating chunks.
			lineEnd := 0
			for boundary := end; boundary >= end-domain.ChunkOverlapRunes; boundary-- {
				if text[offsets[boundary-1]] != '\n' {
					continue
				}
				if lineEnd == 0 {
					lineEnd = boundary
				}
				if boundary >= 2 && text[offsets[boundary-2]] == '\n' {
					lineEnd = boundary
					break
				}
			}
			if lineEnd != 0 {
				end = lineEnd
			}
		}
		if len(chunks) >= domain.MaxIndexChunks {
			return nil, &Failure{Code: "extraction_limit"}
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
		start = end - domain.ChunkOverlapRunes
	}
	return chunks, nil
}
