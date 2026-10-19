import test from "node:test";
import assert from "node:assert/strict";
import { consumeEvents, changedLines } from "../internal/webui/assets/core.js";
const encode = new TextEncoder();
function body(bytes) {
  return new ReadableStream({
    start(c) {
      for (const byte of bytes) c.enqueue(new Uint8Array([byte]));
      c.close();
    },
  });
}
test("SSE survives byte boundaries, heartbeat, Chinese and discard, then persisted terminal", async () => {
  const frames =
    ': heartbeat\r\n\r\nevent: message.start\r\ndata: {}\r\n\r\nevent: message.delta\ndata: {"delta":"你好"}\n\nevent: message.discard\ndata: {}\n\nevent: run.completed\ndata: {"answer":"已保存"}\n\n';
  let draft = "";
  const names = [];
  const terminal = await consumeEvents(
    body(encode.encode(frames)),
    (name, p) => {
      names.push(name);
      if (name === "message.start" || name === "message.discard") draft = "";
      if (name === "message.delta") draft += p.delta;
      if (name === "run.completed") draft = p.answer;
    },
  );
  assert.equal(terminal, true);
  assert.equal(draft, "已保存");
  assert.equal(names.length, 4);
});
test("EOF without terminal is unknown, never success", async () =>
  assert.equal(
    await consumeEvents(
      body(
        encode.encode('event: message.delta\ndata: {"delta":"partial"}\n\n'),
      ),
      () => {},
    ),
    false,
  ));
test("suspension and failure both end connection", async () => {
  for (const name of ["run.suspended", "run.failed"])
    assert.equal(
      await consumeEvents(
        body(encode.encode(`event: ${name}\ndata: {}\n\n`)),
        () => {},
      ),
      true,
    );
});
test("diff preserves all lines and marks changed block", () => {
  const result = changedLines("a\n旧文\n尾行\n", "a\n新文\n尾行\n");
  assert.equal(result.before.map((l) => l.text).join("\n"), "a\n旧文\n尾行\n");
  assert.deepEqual(
    result.after.map((l) => l.changed),
    [false, true, false, false],
  );
  assert.deepEqual(changedLines("", "").after, [{ text: "", changed: false }]);
});

test("upload policy follows deployment config and accepts exact limit", async () => {
  const { validateUpload } = await import("../internal/webui/assets/core.js");
  for (const mb of [1, 50, 80, 512]) {
    const limits = {
      max_file_size_mb: mb,
      max_file_size_bytes: mb * 1024 * 1024,
    };
    validateUpload(
      { name: "file.pdf", size: limits.max_file_size_bytes },
      limits,
    );
    assert.throws(() =>
      validateUpload(
        { name: "file.pdf", size: limits.max_file_size_bytes + 1 },
        limits,
      ),
    );
  }
  for (const limits of [
    null,
    {},
    { max_file_size_mb: 50, max_file_size_bytes: 8 * 1024 * 1024 },
  ]) {
    assert.throws(() => validateUpload({ name: "file.pdf", size: 1 }, limits));
  }
});

test("unrecognized index states cannot become ready or inject a CSS class", async () => {
  const { indexPresentation } = await import("../internal/webui/assets/core.js");
  for (const status of [undefined, "__proto__", "constructor", "ready injected", "unknown"]) {
    assert.deepEqual(indexPresentation({ status }), {
      label: "状态未知", className: "status-unavailable",
    });
  }
  assert.equal(indexPresentation({ status: "ready" }).className, "status-ready");
  assert.equal(indexPresentation({ status: "failed" }).label, "处理失败");
});
