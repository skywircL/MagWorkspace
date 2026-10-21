package indexing_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
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
		if chunk.ID != "" || chunk.IndexID != "" || chunk.DocumentID != "" || chunk.VersionID != "" || chunk.SourceSHA256 != "" || chunk.ParentID != "" || chunk.Generation != "" {
			t.Fatal("chunker invented storage or source identity")
		}
		if chunk.Kind != "child" || chunk.ParentOrdinal < 0 {
			t.Fatal("retrievable chunk lacks its parent association")
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
		if chunk.StartByte <= previous.StartByte || chunk.StartByte > previous.EndByte || chunk.EndByte <= previous.EndByte || utf8.RuneCountInString(text[chunk.StartByte:previous.EndByte]) > domain.ChunkOverlapRunes {
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
		{strings.Repeat("a", 330) + "\n\n" + strings.Repeat("b", 18) + "\n" + strings.Repeat("c", 1000), 332},
		{strings.Repeat("中", 350) + "\n" + strings.Repeat("🙂", 1000), 350*3 + 1},
		{strings.Repeat("a", 150) + "\n\n" + strings.Repeat("b", 1000), domain.ChunkRunes},
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

func TestChunkerSentenceBoundariesAndSemanticOverlap(t *testing.T) {
	for _, sentence := range []string{
		strings.Repeat("甲", 149) + "。",
		"A data node requires enough memory before a distributed job can start. ",
		"内存不足时，DataNode 或 Worker 相关进程可能被终止。",
		"The installation succeeds! The cluster is ready? Check its status; ",
	} {
		text := strings.Repeat(sentence, 40)
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		for i, chunk := range chunks {
			if chunk.EndByte != len(text) && !strings.ContainsRune("。.!?;", []rune(strings.TrimSpace(chunk.Content))[utf8.RuneCountInString(strings.TrimSpace(chunk.Content))-1]) {
				t.Fatalf("chunk %d splits a sentence despite an available boundary", i)
			}
			if i > 0 && chunk.StartByte < chunks[i-1].EndByte {
				overlap := strings.TrimSpace(text[chunk.StartByte:chunks[i-1].EndByte])
				if !strings.HasSuffix(overlap, ".") && !strings.HasSuffix(overlap, "。") && !strings.HasSuffix(overlap, "!") && !strings.HasSuffix(overlap, "?") && !strings.HasSuffix(overlap, ";") {
					t.Fatalf("overlap is not a complete sentence: %q", overlap)
				}
			}
		}
		if utf8.RuneCountInString(sentence) > domain.ChunkOverlapRunes && chunks[1].StartByte != chunks[0].EndByte {
			t.Fatal("a long sentence was truncated merely to fill the overlap budget")
		}
	}
}

func TestChunkerPrefersSentenceOverPDFSoftWrap(t *testing.T) {
	prefix := strings.Repeat("甲", 250) + "。\n"
	text := prefix + strings.Repeat("乙", 100) + "\n" + strings.Repeat("丙", 100) + "\n" + strings.Repeat("丁", 500) + "。"
	chunks, err := indexing.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	assertChunkCoverage(t, text, chunks)
	if chunks[0].Content != prefix {
		t.Fatalf("soft line wrap displaced the complete sentence: first end=%d want=%d", chunks[0].EndByte, len(prefix))
	}
}

func TestChunkerKeepsHeadingWithBodyAndResetsOverlap(t *testing.T) {
	for _, heading := range []string{"# 内存与配置\n", "## Deployment\n", "第一章 安装部署\n", "第2节 推荐配置\n"} {
		prefix := strings.Repeat("前文说明。", 50) + "\n\n"
		text := prefix + heading + strings.Repeat("配置说明需要与标题一起保留。", 100)
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		if chunks[0].EndByte != len(prefix) || chunks[1].StartByte != len(prefix) || !strings.HasPrefix(chunks[1].Content, heading+"配置说明") {
			t.Fatalf("heading detached from body or overlap crossed section: %+v", chunks[:2])
		}
	}
}

func TestChunkerProtectsFencesAndTablesWithoutSyntheticContent(t *testing.T) {
	blocks := []string{
		"````go\n# This is code, not a heading\n" + strings.Repeat("fmt.Println(\"hello\")\n", 12) + "```\ninside the longer fence\n````\n",
		"~~~text\n" + strings.Repeat("第一章 此行属于代码\n", 20) + "~~~\n",
		"| 方式 | 内存 |\n| :--- | ---: |\n" + strings.Repeat("| 完全分布式 | 4 GB |\n", 18),
	}
	for _, block := range blocks {
		prefix := strings.Repeat("a", 250) + "\n\n"
		text := prefix + block + "\n" + strings.Repeat("尾部说明。", 220)
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		found := 0
		for _, chunk := range chunks {
			if strings.Contains(chunk.Content, block) {
				found++
			}
			if chunk.StartByte > len(prefix) && chunk.StartByte < len(prefix)+len(block) || chunk.EndByte > len(prefix) && chunk.EndByte < len(prefix)+len(block) {
				t.Fatalf("fitting protected block was split: start=%d end=%d", chunk.StartByte, chunk.EndByte)
			}
		}
		if found != 1 {
			t.Fatalf("protected block was dropped or duplicated: count=%d", found)
		}
	}
}

func TestChunkerOversizedProtectedBlocksRespectHardLimit(t *testing.T) {
	for _, text := range []string{
		"```text\n" + strings.Repeat(strings.Repeat("x", 90)+"\n", 40) + "```\n",
		"```code\n" + strings.Repeat("run task "+strings.Repeat("x", 75)+";\n", 40) + "```\n",
		"~~~text\n" + strings.Repeat("x", 2000), // Unclosed fence stays protected.
		"| Key | Value |\n| --- | --- |\n" + strings.Repeat("| value | "+strings.Repeat("x", 70)+" |\n", 40),
	} {
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		for i, chunk := range chunks {
			if i > 0 && chunk.StartByte != chunks[i-1].EndByte {
				t.Fatal("oversized code/table repeated a partial row through overlap")
			}
			if strings.Count(text, "\n") > 10 && chunk.EndByte != len(text) && !strings.HasSuffix(chunk.Content, "\n") {
				t.Fatal("oversized protected block ignored a fitting line boundary")
			}
		}
	}
}

func TestChunkerDoesNotTreatURLPunctuationAsSentences(t *testing.T) {
	for _, punctuation := range []string{"?", ";", "!"} {
		text := strings.Repeat("x", 250) + "https://example.test/path" + punctuation + strings.Repeat("tenant=value", 50)
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
		if chunks[0].EndByte != domain.ChunkRunes {
			t.Fatal("URL punctuation was mistaken for a complete sentence")
		}
	}
}

func TestChunkerPacksTinySectionsAndBoundsAdversarialInput(t *testing.T) {
	// Fitting protected blocks alternate with short prose. Both levels must
	// keep complete byte coverage within the global parent-plus-child cap.
	fullFence := "```\n" + strings.Repeat("x", domain.ChunkRunes-9) + "\n```\n"
	for _, sample := range []string{"# x\nx\n", "a\n\n", fullFence + strings.Repeat("p", 19) + "\n", "\n"} {
		text := "x" + strings.Repeat(sample, domain.MaxExtractedBytes/len(sample)+1)
		text = text[:domain.MaxExtractedBytes]
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
	}
}

func TestChunkerHeadingBlankLinesDoNotOrphanTitle(t *testing.T) {
	prefix := strings.Repeat("x", 350) + "\n"
	heading := "# " + strings.Repeat("标题", 34) + "\n\n"
	body := "这段说明属于上面的标题，应保留在同一块中。\n"
	text := prefix + heading + body + "\n" + strings.Repeat("更多说明。", 200)
	chunks, err := indexing.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	assertChunkCoverage(t, text, chunks)
	found := false
	for _, chunk := range chunks {
		if strings.Contains(chunk.Content, heading+body) {
			found = true
		}
		if strings.HasSuffix(strings.TrimSpace(chunk.Content), strings.TrimSpace(heading)) {
			t.Fatal("a blank line left a heading at the end of a chunk without its body")
		}
	}
	if !found {
		t.Fatal("heading and fitting first body line were separated")
	}
}

func TestChunkerAdjacentHeadingAndFullSizeFenceMakeProgress(t *testing.T) {
	text := "# Heading\n" + "```\n" + strings.Repeat("x", domain.ChunkRunes-9) + "\n```\n" + strings.Repeat(" tail", 400)
	chunks, err := indexing.Split(text)
	if err != nil {
		t.Fatal(err)
	}
	assertChunkCoverage(t, text, chunks)
}

func TestChunkerRandomizedCoverage(t *testing.T) {
	rng := rand.New(rand.NewSource(31))
	fragments := []string{"中🙂é", "text ", "first sentence. ", "最后一句。\n", "\n\n", "# 标题\n", "第二章 方法\n", "```\nx\n```\n", "| x | y |\n| --- | --- |\n| 1 | 2 |\n", strings.Repeat("x", 400)}
	for example := 0; example < 120; example++ {
		var source strings.Builder
		for fragment := 0; fragment < 100; fragment++ {
			source.WriteString(fragments[rng.Intn(len(fragments))])
		}
		text := source.String()
		chunks, err := indexing.Split(text)
		if err != nil {
			t.Fatal(err)
		}
		assertChunkCoverage(t, text, chunks)
	}
}

func BenchmarkChunkerScale(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20} {
		for _, fixture := range []struct{ name, text string }{
			{"sentences", "A worker needs memory. Configuration affects the cluster.\n"},
			{"newlines", "\n"},
			{"sections", "# Section\nA short paragraph.\n\n"},
		} {
			text := "x" + strings.Repeat(fixture.text, size/len(fixture.text)+1)
			text = text[:size]
			b.Run(fmt.Sprintf("%s/%d", fixture.name, size), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					chunks, err := indexing.Split(text)
					if err != nil || len(chunks) == 0 {
						b.Fatal("valid source failed", err)
					}
				}
			})
		}
	}
}
