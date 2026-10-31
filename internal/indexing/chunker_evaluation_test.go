package indexing_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
)

// This opt-in, offline measurement does not load queries or relevance judgments.
// Boundary heuristics describe segmentation, not retrieval quality: they miss
// headings and table/code structure and can mistake abbreviations for sentences.
// No raw document content or user paths are included in its JSON artifact.
func TestChunkerOfflineCorpusMeasurement(t *testing.T) {
	corpusPath := os.Getenv("MAGWORKSPACE_CHUNK_EVAL_CORPUS")
	textPath := os.Getenv("MAGWORKSPACE_CHUNK_EVAL_TEXT")
	if corpusPath == "" && textPath == "" {
		t.Skip("set MAGWORKSPACE_CHUNK_EVAL_CORPUS or MAGWORKSPACE_CHUNK_EVAL_TEXT for offline chunk measurements")
	}
	outputPath := os.Getenv("MAGWORKSPACE_CHUNK_EVAL_OUTPUT")
	if outputPath == "" {
		t.Fatal("MAGWORKSPACE_CHUNK_EVAL_OUTPUT is required for the metrics artifact")
	}
	var inputs []chunkEvaluationInput
	if corpusPath != "" {
		inputs = append(inputs, readChunkEvaluationCorpus(t, corpusPath))
	}
	if textPath != "" {
		raw, err := os.ReadFile(textPath)
		if err != nil {
			t.Fatal("cannot read normalized text input")
		}
		text := string(raw)
		if normalized := normalizeChunkEvaluationText(t, text); normalized != text {
			t.Fatal("MAGWORKSPACE_CHUNK_EVAL_TEXT must already contain canonical parser text")
		}
		inputs = append(inputs, chunkEvaluationInput{
			Label: "normalized_pdf_text", ContentPolicy: "canonical-parser-text",
			SourceSHA256: chunkEvaluationDigest(raw), Documents: []string{text},
		})
	}
	fixture := chunkEvaluationChineseFixture()
	inputs = append(inputs, chunkEvaluationInput{
		Label: "synthetic_zh_v1", ContentPolicy: "fixed-synthetic-technical-text-v1",
		SourceSHA256: chunkEvaluationDigest([]byte(fixture)), Documents: []string{fixture},
	})
	report := chunkEvaluationReport{
		Schema: 1,
		Heuristics: map[string]string{
			"internal_edges":              "Every noninitial chunk start and nonfinal chunk end; an adjacent zero-overlap boundary is counted twice.",
			"mid_ascii_word_edges":        "Both immediately adjacent source runes are ASCII letters or digits; Chinese word boundaries are not inferred.",
			"sentence_or_paragraph_edges": "Whitespace around the edge contains a blank line or form feed, or the preceding nonspace rune is 。！？；.!?; (ASCII punctuation requires following whitespace).",
			"other_edges":                 "Internal edges not classified as sentence/paragraph; includes legitimate heading, table and code boundaries and is not an error count.",
			"duplication_ratio":           "(Sum of chunk rune counts - source rune count) / source rune count; exact source coverage is checked separately.",
		},
		Limitations: []string{
			"No query, qrels, embeddings, vector database or language model is used; these are not Recall, Hit or answer-quality results.",
			"Boundary heuristics are language/layout approximations; abbreviations and decimal notation can be misclassified. No semantic correctness is inferred.",
			"Elapsed nanoseconds sum individual split calls in one sequential pass, parent-child strategy; they are descriptive and cannot establish a speedup.",
			"The normalized text sample does not recover PDF layout discarded during extraction; the Chinese fixture is synthetic, not a business evaluation set.",
		},
	}
	for _, input := range inputs {
		result := chunkEvaluationDataset{
			Label: input.Label, ContentPolicy: input.ContentPolicy,
			SourceSHA256:             input.SourceSHA256,
			CanonicalDocumentsSHA256: chunkEvaluationDocumentDigest(input.Documents),
			CanonicalDigestPolicy:    "sha256-of-ordered-u64-big-endian-byte-length-then-canonical-utf8-document-v1",
		}
		for _, strategy := range []string{indexing.ChunkingParentChild} {
			result.Strategies = append(result.Strategies, measureChunkEvaluation(t, input.Documents, strategy))
		}
		report.Datasets = append(report.Datasets, result)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal("cannot encode chunk measurements")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0700); err != nil {
		t.Fatal("cannot create metrics artifact directory")
	}
	if err := os.WriteFile(outputPath, append(data, '\n'), 0600); err != nil {
		t.Fatal("cannot write chunk measurements")
	}
}

type chunkEvaluationInput struct {
	Label, ContentPolicy, SourceSHA256 string
	Documents                          []string
}

type chunkEvaluationReport struct {
	Schema      int                      `json:"schema"`
	Heuristics  map[string]string        `json:"heuristics"`
	Limitations []string                 `json:"limitations"`
	Datasets    []chunkEvaluationDataset `json:"datasets"`
}

type chunkEvaluationDataset struct {
	Label                    string                   `json:"label"`
	ContentPolicy            string                   `json:"content_policy"`
	SourceSHA256             string                   `json:"source_sha256"`
	CanonicalDocumentsSHA256 string                   `json:"canonical_documents_sha256"`
	CanonicalDigestPolicy    string                   `json:"canonical_digest_policy"`
	Strategies               []chunkEvaluationMetrics `json:"strategies"`
}

type chunkEvaluationMetrics struct {
	Strategy                 string  `json:"strategy"`
	Documents                int     `json:"documents"`
	ExactCoverageDocuments   int     `json:"exact_coverage_documents"`
	Chunks                   int     `json:"chunks"`
	SourceRunes              int64   `json:"source_runes"`
	ChunkRunes               int64   `json:"chunk_runes"`
	DuplicatedRunes          int64   `json:"duplicated_runes"`
	DuplicationRatio         float64 `json:"duplication_ratio"`
	InternalEdges            int64   `json:"internal_edges"`
	MidASCIIWordEdges        int64   `json:"mid_ascii_word_edges"`
	SentenceOrParagraphEdges int64   `json:"sentence_or_paragraph_edges"`
	OtherEdges               int64   `json:"other_edges"`
	MinChunkRunes            int     `json:"min_chunk_runes"`
	MaxChunkRunes            int     `json:"max_chunk_runes"`
	SplitElapsedNS           int64   `json:"split_elapsed_ns"`
}

func readChunkEvaluationCorpus(t *testing.T, path string) chunkEvaluationInput {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("cannot read corpus JSONL")
	}
	if !utf8.Valid(raw) {
		t.Fatal("corpus JSONL is not UTF-8")
	}
	input := chunkEvaluationInput{
		Label: "scifact_corpus", ContentPolicy: "beir-title-text-cc-space-v1 + canonical-parser-text",
		SourceSHA256: chunkEvaluationDigest(raw),
	}
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	seen := make(map[string]bool)
	for line := 1; scanner.Scan(); line++ {
		var row map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("invalid corpus JSON at line %d", line)
		}
		var id, title, body string
		if err := json.Unmarshal(row["_id"], &id); err != nil || id == "" || seen[id] {
			t.Fatalf("invalid or duplicate corpus ID at line %d", line)
		}
		seen[id] = true
		if rawTitle, exists := row["title"]; exists {
			if string(rawTitle) == "null" || json.Unmarshal(rawTitle, &title) != nil {
				t.Fatalf("invalid corpus title at line %d", line)
			}
		}
		if string(row["text"]) == "null" || json.Unmarshal(row["text"], &body) != nil {
			t.Fatalf("invalid corpus body at line %d", line)
		}
		// Match scripts/retrieval_eval/data.py before applying the same
		// production parser normalization as imported corpus.txt documents.
		content := strings.Map(func(r rune) rune {
			if unicode.IsControl(r) && !strings.ContainsRune("\n\r\t\f", r) {
				return ' '
			}
			return r
		}, title+"\n\n"+body)
		content = strings.TrimSpace(content)
		input.Documents = append(input.Documents, normalizeChunkEvaluationText(t, content))
	}
	if scanner.Err() != nil || len(input.Documents) == 0 {
		t.Fatal("corpus JSONL is empty or could not be fully read")
	}
	return input
}

func normalizeChunkEvaluationText(t *testing.T, text string) string {
	t.Helper()
	parser, err := indexing.NewParser("")
	if err != nil {
		t.Fatal("cannot initialize local text parser")
	}
	normalized, err := parser.Parse(context.Background(), domain.DocumentVersion{Filename: "corpus.txt"}, []byte(text))
	if err != nil {
		t.Fatalf("input is not accepted by canonical text parser: %s", indexing.ErrorCode(err))
	}
	return normalized
}

func measureChunkEvaluation(t *testing.T, documents []string, strategy string) chunkEvaluationMetrics {
	t.Helper()
	m := chunkEvaluationMetrics{Strategy: strategy, Documents: len(documents)}
	for documentNumber, text := range documents {
		started := time.Now()
		chunks, err := indexing.SplitWithStrategy(text, strategy)
		m.SplitElapsedNS += time.Since(started).Nanoseconds()
		if err != nil {
			t.Fatalf("%s document %d splitting failed: %s", strategy, documentNumber, indexing.ErrorCode(err))
		}
		if len(chunks) == 0 || chunks[0].StartByte != 0 || chunks[len(chunks)-1].EndByte != len(text) {
			t.Fatalf("%s document %d does not cover source endpoints", strategy, documentNumber)
		}
		if len(chunks) > domain.MaxIndexChunks {
			t.Fatalf("%s document %d exceeds the chunk count limit", strategy, documentNumber)
		}
		m.SourceRunes += int64(utf8.RuneCountInString(text))
		m.Chunks += len(chunks)
		for i, chunk := range chunks {
			if chunk.StartByte < 0 || chunk.EndByte <= chunk.StartByte || chunk.EndByte > len(text) ||
				chunk.Content != text[chunk.StartByte:chunk.EndByte] || !utf8.ValidString(chunk.Content) ||
				chunk.SHA256 != chunkEvaluationDigest([]byte(chunk.Content)) || chunk.Ordinal != i {
				t.Fatalf("%s document %d chunk %d is not an exact source span", strategy, documentNumber, i)
			}
			if i > 0 && (chunk.StartByte <= chunks[i-1].StartByte || chunk.StartByte > chunks[i-1].EndByte || chunk.EndByte <= chunks[i-1].EndByte) {
				t.Fatalf("%s document %d chunk %d has a gap or fails to advance", strategy, documentNumber, i)
			}
			size := utf8.RuneCountInString(chunk.Content)
			if size > domain.ChunkRunes {
				t.Fatalf("%s document %d chunk %d exceeds the rune limit", strategy, documentNumber, i)
			}
			m.ChunkRunes += int64(size)
			if m.MinChunkRunes == 0 || size < m.MinChunkRunes {
				m.MinChunkRunes = size
			}
			m.MaxChunkRunes = max(m.MaxChunkRunes, size)
			for _, edge := range []int{chunk.StartByte, chunk.EndByte} {
				if edge == 0 || edge == len(text) {
					continue
				}
				m.InternalEdges++
				left, _ := utf8.DecodeLastRuneInString(text[:edge])
				right, _ := utf8.DecodeRuneInString(text[edge:])
				if chunkEvaluationASCIIWord(left) && chunkEvaluationASCIIWord(right) {
					m.MidASCIIWordEdges++
				}
				if chunkEvaluationSentenceOrParagraph(text, edge) {
					m.SentenceOrParagraphEdges++
				} else {
					m.OtherEdges++
				}
			}
		}
		m.ExactCoverageDocuments++
	}
	m.DuplicatedRunes = m.ChunkRunes - m.SourceRunes
	if m.SourceRunes > 0 {
		m.DuplicationRatio = float64(m.DuplicatedRunes) / float64(m.SourceRunes)
	}
	return m
}

func chunkEvaluationSentenceOrParagraph(text string, edge int) bool {
	left, right := edge, edge
	for left > 0 {
		r, size := utf8.DecodeLastRuneInString(text[:left])
		if !unicode.IsSpace(r) {
			break
		}
		left -= size
	}
	for right < len(text) {
		r, size := utf8.DecodeRuneInString(text[right:])
		if !unicode.IsSpace(r) {
			break
		}
		right += size
	}
	whitespace := text[left:right]
	if strings.Count(whitespace, "\n") >= 2 || strings.ContainsRune(whitespace, '\f') {
		return true
	}
	previous, _ := utf8.DecodeLastRuneInString(text[:left])
	return strings.ContainsRune("。！？；", previous) || (strings.ContainsRune(".!?;", previous) && right > left)
}

func chunkEvaluationASCIIWord(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func chunkEvaluationDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func chunkEvaluationDocumentDigest(documents []string) string {
	digest := sha256.New()
	for _, text := range documents {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(text)))
		digest.Write(size[:])
		digest.Write([]byte(text))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func chunkEvaluationChineseFixture() string {
	var text strings.Builder
	text.WriteString("# 个人知识库实验说明\n\n本样例为固定的合成技术文本，用于测量分块边界，不代表任何真实业务资料。\n\n")
	for section := 1; section <= 4; section++ {
		fmt.Fprintf(&text, "## 第%d章 服务部署与验证\n\n", section)
		for paragraph := 0; paragraph < 7; paragraph++ {
			text.WriteString("实验先启动容器，再检查服务的健康状态。客户端通过认证后才能访问当前工作空间中的文档；文档更新会产生新版本，旧版本仍可追溯。检索结果必须附带固定版本和来源位置，不能仅依赖模型自行判断权限。\n")
			text.WriteString("如果后台任务暂时失败，应保留原始文件并记录可重试的状态。恢复时需要重新核验任务租约和当前版本，避免过期的工作进程覆盖新的结果。\n\n")
		}
		text.WriteString("| 配置项 | 建议值 |\n| --- | --- |\n| 最大分块长度 | 384 字符 |\n| 重叠长度上限 | 76 字符 |\n\n```text\n# 这行位于代码块中，不是章节标题\nservice.status = healthy\n```\n\n")
	}
	return text.String()
}
