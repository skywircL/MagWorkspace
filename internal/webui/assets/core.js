// SSE framing is independent of network chunk boundaries, including UTF-8 splits.
export async function consumeEvents(body, onEvent) {
  const reader = body.getReader(),
    decoder = new TextDecoder();
  let pending = "",
    terminal = false;
  try {
    while (true) {
      const { value, done } = await reader.read();
      pending += decoder.decode(value, { stream: !done });
      let match;
      while ((match = /\r?\n\r?\n/.exec(pending))) {
        const frame = pending.slice(0, match.index);
        pending = pending.slice(match.index + match[0].length);
        let name = "",
          data = [];
        for (const line of frame.split(/\r?\n/)) {
          if (line.startsWith("event:")) name = line.slice(6).trim();
          if (line.startsWith("data:"))
            data.push(line.slice(5).replace(/^ /, ""));
        }
        if (name && data.length) {
          await onEvent(name, JSON.parse(data.join("\n")));
          if (["run.completed", "run.failed", "run.suspended"].includes(name))
            terminal = true;
        }
      }
      if (pending.length > 2 * 1024 * 1024) throw new Error("事件超出大小限制");
      if (done) return terminal;
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
// A bounded line comparison: unchanged prefix/suffix surround an exact changed
// block. No quadratic diff allocation for large documents; no text is omitted.
export function changedLines(before, after) {
  const a = before.split("\n"),
    b = after.split("\n");
  let start = 0,
    end = 0;
  while (start < a.length && start < b.length && a[start] === b[start]) start++;
  while (
    end < a.length - start &&
    end < b.length - start &&
    a[a.length - 1 - end] === b[b.length - 1 - end]
  )
    end++;
  return {
    before: a.map((text, i) => ({
      text,
      changed: i >= start && i < a.length - end,
    })),
    after: b.map((text, i) => ({
      text,
      changed: i >= start && i < b.length - end,
    })),
  };
}

// Match the server's deployment policy, including exact-boundary uploads.
export function validateUpload(file, limits) {
  const mb = limits?.max_file_size_mb;
  if (
    !Number.isInteger(mb) ||
    mb < 1 ||
    mb > 512 ||
    limits.max_file_size_bytes !== mb * 1024 * 1024
  )
    throw Error("无法读取上传限制，请刷新后重试");
  if (
    !file?.name ||
    !Number.isSafeInteger(file.size) ||
    file.size < 0 ||
    file.size > limits.max_file_size_bytes
  )
    throw Error(`请选择不超过 ${mb} MB 的文件`);
}

// Only known server states become CSS classes; unknown states stay neutral.
export function indexPresentation(job) {
  const states = {
    queued: { label: "等待处理", className: "status-pending" },
    running: { label: "正在处理", className: "status-running" },
    ready: { label: "可检索", className: "status-ready" },
    failed: { label: "处理失败", className: "status-failed" },
  };
  const stages = {loading: "读取文件", parsing: "解析文档", chunking: "分块", embedding: "生成向量", writing: "写入索引", publishing: "发布索引"};
  if (job?.status === "running" && Object.hasOwn(stages, job.progress?.stage))
    return {label: stages[job.progress.stage], className: "status-running"};
  return Object.hasOwn(states, job?.status)
    ? states[job.status]
    : { label: "状态未知", className: "status-unavailable" };
}

// Deliberately bounded Markdown subset. Build DOM nodes only; model text never
// enters innerHTML, and remote images / arbitrary HTML are not interpreted.
export function renderAnswer(target, text, { onCitation } = {}) {
  const doc = target.ownerDocument;
  const make = (tag, value, cls) => {
    const el = doc.createElement(tag);
    if (value !== undefined) el.textContent = value;
    if (cls) el.className = cls;
    return el;
  };
  const citations = new Map();
  function inline(parent, value) {
    const pattern = /`([^`\n]+)`|\*\*([^*\n]+)\*\*|\[E:([0-9a-f]{32})\]|\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/g;
    let offset = 0;
    for (const match of value.matchAll(pattern)) {
      parent.append(doc.createTextNode(value.slice(offset, match.index)));
      if (match[1]) parent.append(make("code", match[1]));
      else if (match[2]) { const strong = make("strong"); inline(strong, match[2]); parent.append(strong); }
      else if (match[3]) {
        const id = match[3];
        if (!citations.has(id)) citations.set(id, citations.size + 1);
        const ref = make(onCitation ? "button" : "span", String(citations.get(id)), "citation-mark");
        ref.dataset.evidence = id;
        ref.title = onCitation ? "查看来源 " + citations.get(id) : "回答完成后可核对来源";
        ref.setAttribute("aria-label", ref.title);
        if (onCitation) { ref.type = "button"; ref.addEventListener("click", () => onCitation(id)); }
        parent.append(ref);
      } else {
        const link = make("a", match[4]);
        link.href = match[5]; link.target = "_blank"; link.rel = "noopener noreferrer";
        parent.append(link);
      }
      offset = match.index + match[0].length;
    }
    parent.append(doc.createTextNode(value.slice(offset)));
  }
  const lines = String(text).replace(/\r\n?/g, "\n").split("\n");
  const fragment = doc.createDocumentFragment();
  const cells = (line) => line.trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map((v) => v.trim());
  const tableRule = (line) => line.includes("|") && cells(line).every((v) => /^:?-{3,}:?$/.test(v));
  const special = (i) => /^(?:\s*```|#{1,6}\s|\s*[-*+]\s|\s*\d+\.\s|>\s?)/.test(lines[i]) || (i + 1 < lines.length && tableRule(lines[i + 1]));
  for (let i = 0; i < lines.length;) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    if (/^\s*```/.test(line)) {
      const language = line.trim().slice(3).trim();
      const block = make("section", undefined, "answer-code");
      if (language) block.append(make("small", language));
      const content = [];
      for (i++; i < lines.length && !/^\s*```/.test(lines[i]); i++) content.push(lines[i]);
      i++;
      const pre = make("pre"); pre.append(make("code", content.join("\n"))); block.append(pre); fragment.append(block);
    } else if (i + 1 < lines.length && line.includes("|") && tableRule(lines[i + 1])) {
      const wrapper = make("div", undefined, "answer-table");
      wrapper.tabIndex = 0; wrapper.setAttribute("role", "region"); wrapper.setAttribute("aria-label", "回答表格，可横向滚动");
      const table = make("table"), head = make("thead"), row = make("tr");
      for (const value of cells(line)) { const cell = make("th"); cell.scope = "col"; inline(cell, value); row.append(cell); }
      head.append(row); table.append(head); i += 2;
      const body = make("tbody");
      while (i < lines.length && lines[i].trim() && lines[i].includes("|")) {
        const row = make("tr");
        for (const value of cells(lines[i++])) { const cell = make("td"); inline(cell, value); row.append(cell); }
        body.append(row);
      }
      table.append(body); wrapper.append(table); fragment.append(wrapper);
    } else if (/^#{1,6}\s/.test(line)) {
      const match = /^(#{1,6})\s+(.*)$/.exec(line);
      const heading = make("h" + Math.min(match[1].length + 1, 6)); inline(heading, match[2]); fragment.append(heading); i++;
    } else if (/^\s*(?:[-*+]|\d+\.)\s/.test(line)) {
      const ordered = /^\s*\d+\./.test(line), list = make(ordered ? "ol" : "ul");
      if (ordered) list.start = Number(line.trim().match(/^\d+/)[0]);
      const pattern = ordered ? /^\s*\d+\.\s+(.*)$/ : /^\s*[-*+]\s+(.*)$/;
      while (i < lines.length && pattern.test(lines[i])) {
        const item = make("li"); inline(item, lines[i++].match(pattern)[1]); list.append(item);
      }
      fragment.append(list);
    } else if (/^>\s?/.test(line)) {
      const quote = make("blockquote"); inline(quote, line.replace(/^>\s?/, "")); fragment.append(quote); i++;
    } else {
      const content = [lines[i++]];
      while (i < lines.length && lines[i].trim() && !special(i)) content.push(lines[i++]);
      const paragraph = make("p"); inline(paragraph, content.join("\n")); fragment.append(paragraph);
    }
  }
  target.replaceChildren(fragment);
  return citations.size;
}
