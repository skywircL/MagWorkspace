package indexing_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
)

func assertChunkCoverage(t *testing.T, text string, chunks []domain.IndexChunk) {
	t.Helper()
	if len(chunks) == 0 || len(chunks) > domain.MaxIndexChunks || chunks[0].StartByte != 0 || chunks[len(chunks)-1].EndByte != len(text) {
		t.Fatalf("chunks do not cover the full source: count=%d bytes=%d", len(chunks), len(text))
	}
	var reconstructed strings.Builder
	for i, chunk := range chunks {
		if chunk.Ordinal != i || chunk.StartByte < 0 || chunk.EndByte <= chunk.StartByte || chunk.EndByte > len(text) || chunk.Content != text[chunk.StartByte:chunk.EndByte] || !utf8.ValidString(chunk.Content) || utf8.RuneCountInString(chunk.Content) > domain.ChunkRunes {
			t.Fatalf("invalid byte span or rune boundary: %+v", chunk)
		}
		if chunk.ID != "" || chunk.IndexID != "" || chunk.DocumentID != "" || chunk.VersionID != "" || chunk.SourceSHA256 != "" {
			t.Fatal("chunker invented storage or source identity")
		}
		digest := sha256.Sum256([]byte(chunk.Content))
		if chunk.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("chunk hash is not calculated from exact span bytes")
		}
		if i == 0 {
			reconstructed.WriteString(chunk.Content)
			continue
		}
		previous := chunks[i-1]
		if chunk.StartByte <= previous.StartByte || chunk.StartByte >= previous.EndByte || chunk.EndByte <= previous.EndByte || utf8.RuneCountInString(text[chunk.StartByte:previous.EndByte]) != domain.ChunkOverlapRunes {
			t.Fatalf("chunk coverage has a gap, duplicate tail, or incorrect overlap: prior=%+v next=%+v", previous, chunk)
		}
		reconstructed.WriteString(text[previous.EndByte:chunk.EndByte])
	}
	if reconstructed.String() != text {
		t.Fatal("chunks cannot reconstruct normalized source text without loss")
	}
}

func TestChunkerUTF8OffsetsOverlapAndDeterminism(t *testing.T) {
	for _, text := range []string{
		"短文🙂",
		strings.Repeat("a", domain.ChunkRunes),
		strings.Repeat("a", domain.ChunkRunes+1),
		strings.Repeat("中文🙂é", 800),
		strings.Repeat("段落里的中文🙂\n\n", 500),
		strings.Repeat("x", domain.MaxExtractedBytes),
	} {
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		again, err := indexing.Split(text)
		if err != nil || len(again) != len(chunks) {
			t.Fatal("repeated chunking changed result")
		}
		for i := range chunks {
			if again[i] != chunks[i] {
				t.Fatal("chunk spans or hashes are not deterministic")
			}
		}
	}
}

func TestChunkerPrefersNearbyParagraphAndLineBoundaries(t *testing.T) {
	for _, test := range []struct {
		text     string
		firstEnd int
	}{
		{strings.Repeat("a", 730) + "\n\n" + strings.Repeat("b", 38) + "\n" + strings.Repeat("c", 1000), 732},
		{strings.Repeat("中", 750) + "\n" + strings.Repeat("🙂", 1000), 750*3 + 1},
		{strings.Repeat("a", 300) + "\n\n" + strings.Repeat("b", 1000), domain.ChunkRunes},
	} {
		chunks, err := indexing.Split(test.text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, test.text, chunks)
		if chunks[0].EndByte != test.firstEnd {
			t.Fatalf("unexpected preferred boundary=%d want=%d", chunks[0].EndByte, test.firstEnd)
		}
	}
}

func TestChunkerRejectsEmptyBinaryAndOversizedInput(t *testing.T) {
	for _, test := range []struct{ text, code string }{
		{"", "empty_document"},
		{" \t\n　", "empty_document"},
		{"text\x00binary", "invalid_text"},
		{"text\xff", "invalid_text"},
		{strings.Repeat("x", domain.MaxExtractedBytes+1), "extraction_limit"},
	} {
		chunks, err := indexing.Split(test.text)
		if err == nil || chunks != nil || indexing.ErrorCode(err) != test.code {
			t.Fatalf("invalid text produced chunks: error=%v count=%d expected=%s", err, len(chunks), test.code)
		}
	}
}
