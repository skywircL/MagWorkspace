package indexing_test

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"magworkspace/internal/domain"
	"magworkspace/internal/indexing"
)

func assertParentChildCoverage(t *testing.T, text string, parents, children []domain.IndexChunk) {
	t.Helper()
	if len(parents) == 0 || len(children) == 0 || len(parents)+len(children) > domain.MaxIndexChunks {
		t.Fatalf("invalid parent/child count: %d/%d", len(parents), len(children))
	}
	assertChunkCoverage(t, text, children)
	end, nextChild := 0, 0
	for i, parent := range parents {
		if parent.Ordinal != i || parent.Kind != "parent" || parent.ParentOrdinal != -1 || parent.ParentID != "" || parent.ID != "" || parent.Generation != "" || parent.StartByte != end || parent.EndByte <= end || parent.EndByte > len(text) || parent.Content != text[parent.StartByte:parent.EndByte] || !utf8.ValidString(parent.Content) || utf8.RuneCountInString(parent.Content) > domain.ParentChunkRunes {
			t.Fatalf("parent %d is not a contiguous source span", i)
		}
		digest := sha256.Sum256([]byte(parent.Content))
		if parent.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("parent hash differs from its exact bytes")
		}
		if nextChild == len(children) || children[nextChild].ParentOrdinal != i || children[nextChild].StartByte != parent.StartByte {
			t.Fatalf("parent %d has no child starting at its beginning", i)
		}
		for nextChild < len(children) && children[nextChild].ParentOrdinal == i {
			child := children[nextChild]
			if child.StartByte < parent.StartByte || child.EndByte > parent.EndByte || child.Content != parent.Content[child.StartByte-parent.StartByte:child.EndByte-parent.StartByte] {
				t.Fatalf("child %d escapes parent %d", nextChild, i)
			}
			nextChild++
		}
		if children[nextChild-1].EndByte != parent.EndByte {
			t.Fatalf("children do not reach parent %d end", i)
		}
		end = parent.EndByte
	}
	if end != len(text) || nextChild != len(children) {
		t.Fatal("parent-child spans lost document bytes or left orphan children")
	}
}

func TestParentChildExactByteCoverageAndDeterminism(t *testing.T) {
	for _, text := range []string{
		"短文🙂é\r\n",
		strings.Repeat("重复内容🙂\r\n\r\n", 2400),
		strings.Repeat("text", domain.MaxExtractedBytes/4),
		"x" + strings.Repeat("\n", domain.MaxExtractedBytes-1),
		"# 文档\n" + strings.Repeat("```text\n重复块🙂\n```\n\n", 3000),
	} {
		parents, children, err := indexing.SplitParentChild(text)
		if err != nil {
			t.Fatal(err)
		}
		assertParentChildCoverage(t, text, parents, children)
		againParents, againChildren, err := indexing.SplitParentChild(text)
		if err != nil || !reflect.DeepEqual(parents, againParents) || !reflect.DeepEqual(children, againChildren) {
			t.Fatal("repeated parent-child splitting changed offsets or metadata")
		}
		flat, err := indexing.Split(text)
		if err != nil || !reflect.DeepEqual(flat, children) {
			t.Fatal("Split did not return exactly the retrievable child set")
		}
	}
}

func TestParentChildShortDocumentHasExplicitParent(t *testing.T) {
	text := "# 配置\n\n每台机器至少 4 GB 内存。"
	parents, children, err := indexing.SplitParentChild(text)
	if err != nil || len(parents) != 1 || len(children) != 1 || children[0].ParentOrdinal != 0 || parents[0].Content != text || children[0].Content != text {
		t.Fatalf("short document hierarchy is missing: parents=%d children=%d err=%v", len(parents), len(children), err)
	}
	assertParentChildCoverage(t, text, parents, children)
}

func TestParentChildCarriesDocumentHeadingPathAcrossParents(t *testing.T) {
	text := "# 实验指导书\n\n## Hadoop\n\n" + strings.Repeat("系统部署需要足够的内存。\n", 750) +
		"\n## Spark\n\n" + strings.Repeat("计算节点的内存应满足运行要求。\n", 400)
	parents, children, err := indexing.SplitParentChild(text)
	if err != nil {
		t.Fatal(err)
	}
	assertParentChildCoverage(t, text, parents, children)
	if len(parents) < 3 {
		t.Fatal("fixture did not span multiple parents")
	}
	seenInherited, seenSpark := false, false
	for _, child := range children {
		if child.ParentOrdinal > 0 && child.ContextHeader == "实验指导书 → Hadoop" {
			seenInherited = true
		}
		if child.ContextHeader == "实验指导书 → Spark" {
			seenSpark = true
		}
	}
	if !seenInherited || !seenSpark {
		t.Fatal("heading path was lost at a parent boundary or child section")
	}
}

func TestParentChildLongFenceDoesNotInventHeadingsAfterParentBoundary(t *testing.T) {
	text := "# 部署\n\n```markdown\n" + strings.Repeat("# 此行属于代码\n代码不能变成标题。\n", 600) + "```\n\n## 完成\n完成后检查服务。"
	parents, children, err := indexing.SplitParentChild(text)
	if err != nil {
		t.Fatal(err)
	}
	assertParentChildCoverage(t, text, parents, children)
	if len(parents) < 2 {
		t.Fatal("fixture did not cut an oversized fenced block")
	}
	for _, chunk := range append(parents, children...) {
		if strings.Contains(chunk.ContextHeader, "此行属于代码") {
			t.Fatal("a continuation parent reset the document fence state")
		}
	}
}
