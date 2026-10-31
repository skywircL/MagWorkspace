package reranking

import (
	"html"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	imagePattern  = regexp.MustCompile(`!\[([^\]]*)\]\([^()\s]*(?:\([^)]*\)[^()\s]*)*\)`)
	linkPattern   = regexp.MustCompile(`\[([^\]]+)\]\([^()\s]*(?:\([^)]*\)[^()\s]*)*\)`)
	urlPattern    = regexp.MustCompile(`https?://[a-zA-Z0-9._~:/?#\[\]@!$&'()*+,;=%-]+`)
	tagPattern    = regexp.MustCompile(`</?[a-zA-Z][^>]*>`)
	headingPrefix = regexp.MustCompile(`^#{1,6}[ \t]+`)
	listPrefix    = regexp.MustCompile(`^[ \t]*(?:[-*+]|\d+[.)])[ \t]+`)
	tableRule     = regexp.MustCompile(`^[ \t]*\|?[ \t:|-]*-[- \t:|]*\|?[ \t]*$`)
	emphasis      = regexp.MustCompile(`\*{1,3}([^*\n]+)\*{1,3}`)
)

// CleanPassage removes formatting in a derived string, preserving code and
// formula contents. It does not rewrite the canonical source or its offsets.
func CleanPassage(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	fence := ""
	fenceLength := 0
	formula := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if fence != "" {
			if strings.HasPrefix(trimmed, strings.Repeat(fence, fenceLength)) &&
				strings.Trim(trimmed, fence+" \t") == "" {
				fence = ""
				continue
			}
			out = append(out, line)
			continue
		}
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			fence = trimmed[:1]
			fenceLength = len(trimmed) - len(strings.TrimLeft(trimmed, fence))
			continue
		}
		if trimmed == "$$" || trimmed == `\[` || trimmed == `\]` {
			formula = !formula
			continue
		}
		if formula {
			out = append(out, line)
			continue
		}
		if tableRule.MatchString(line) {
			continue
		}
		line = headingPrefix.ReplaceAllString(line, "")
		line = listPrefix.ReplaceAllString(line, "")
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), ">"))
		if strings.HasPrefix(line, "|") && strings.HasSuffix(line, "|") {
			cells := strings.Split(strings.Trim(line, "|"), "|")
			for i := range cells {
				cells[i] = strings.TrimSpace(cells[i])
			}
			line = strings.Join(cells, ", ")
		}
		line = cleanInline(line)
		if line == "" && (len(out) == 0 || out[len(out)-1] == "") {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// Inline code and display formula contents are not passed through
// URL/HTML/emphasis cleaning. Otherwise a formula such as $$x*y*z$$ loses
// multiplication operators as if they were Markdown emphasis.
func cleanInline(line string) string {
	var output strings.Builder
	for line != "" {
		start, delimiter, closeDelimiter := -1, "", ""
		for _, pair := range [][2]string{{"`", "`"}, {"$$", "$$"}, {"$", "$"}, {`\(`, `\)`}, {`\[`, `\]`}} {
			position := literalStart(line, pair[0])
			if position >= 0 && (start < 0 || position < start) {
				start, delimiter, closeDelimiter = position, pair[0], pair[1]
			}
		}
		if start < 0 {
			output.WriteString(cleanPlain(line))
			break
		}
		output.WriteString(cleanPlain(line[:start]))
		if delimiter == "`" {
			length := len(line[start:]) - len(strings.TrimLeft(line[start:], "`"))
			delimiter = strings.Repeat("`", length)
			closeDelimiter = delimiter
		}
		line = line[start+len(delimiter):]
		end := strings.Index(line, closeDelimiter)
		if end < 0 {
			if delimiter != "`" && !strings.HasPrefix(delimiter, "`") {
				output.WriteString(delimiter)
			}
			output.WriteString(line)
			break
		}
		// Keep single dollar signs: they can also be currency punctuation.
		// Protect their contents without changing monetary meaning.
		if delimiter == "$" {
			output.WriteString("$")
		}
		output.WriteString(line[:end])
		if delimiter == "$" {
			output.WriteString("$")
		}
		line = line[end+len(closeDelimiter):]
	}
	return output.String()
}

// Dollar signs inside URLs are ordinary URL characters, not math delimiters.
func literalStart(line, delimiter string) int {
	position := strings.Index(line, delimiter)
	if position < 0 || !strings.HasPrefix(delimiter, "$") {
		return position
	}
	urls := urlPattern.FindAllStringIndex(line, -1)
	for position >= 0 {
		inside := false
		for _, span := range urls {
			if position >= span[0] && position < span[1] {
				inside = true
				break
			}
		}
		if !inside {
			return position
		}
		next := strings.Index(line[position+len(delimiter):], delimiter)
		if next < 0 {
			return -1
		}
		position += len(delimiter) + next
	}
	return -1
}

func cleanPlain(text string) string {
	text = imagePattern.ReplaceAllString(text, "$1")
	text = linkPattern.ReplaceAllString(text, "$1")
	text = urlPattern.ReplaceAllString(text, "")
	text = tagPattern.ReplaceAllString(text, "")
	text = emphasis.ReplaceAllString(text, "$1")
	return html.UnescapeString(text)
}

func buildPassage(candidate Candidate, body string, limit int) string {
	// Metadata gets a bounded minority of the passage budget so a very long
	// title or breadcrumb cannot replace the evidence body.
	title := truncateRunes(CleanPassage(candidate.Title), min(256, limit/8))
	header := truncateRunes(CleanPassage(candidate.ContextHeader), min(512, limit/4))
	var parts []string
	if title != "" {
		parts = append(parts, title)
	}
	if header != "" && header != title {
		parts = append(parts, header)
	}
	if len(parts) == 0 {
		return truncateRunes(body, limit)
	}
	prefix := strings.Join(parts, "\n\n") + "\n\n"
	return prefix + truncateRunes(body, limit-utf8.RuneCountInString(prefix))
}

func truncateRunes(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	count := 0
	for position := range text {
		if count == limit {
			return text[:position]
		}
		count++
	}
	return text
}
