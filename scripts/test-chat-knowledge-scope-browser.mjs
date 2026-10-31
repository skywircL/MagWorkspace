// Real Chrome acceptance for per-session chat knowledge scopes against synthetic HTTP fixtures.
// This fixture never calls a production backend, database, Harness or model provider.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = path.join(root, "artifacts/acceptance/048/chat-scope-browser");
const assets = {
  "/": ["index.html", "text/html; charset=utf-8"], "/app.js": ["app.js", "text/javascript; charset=utf-8"],
  "/core.js": ["core.js", "text/javascript; charset=utf-8"], "/style.css": ["style.css", "text/css; charset=utf-8"],
};
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
const user = { id: "scope-fixture-user", name: "范围验收", email: "scope@example.invalid" };
const workspaces = [{ id: "owner", name: "研究工作空间", role: "owner" }, { id: "second", name: "另一个工作空间", role: "owner" }];
const id = (n) => n.toString(16).padStart(32, "0");
const literalName = '<img src=x onerror="window.__scopeXSS=1"> 技术研究与长期项目资料';
const bases = Array.from({ length: 24 }, (_, index) => ({ id: id(index + 1), workspace_id: "owner",
  name: index === 0 ? "财务资料" : index === 1 ? "技术研究" : index === 2 ? literalName : "项目资料 " + (index + 1),
  description: "仅供浏览器验收的合成知识库 " + (index + 1), is_default: index === 0 }));
bases.push({ id: id(100), workspace_id: "second", name: "第二空间资料", description: "合成资料", is_default: true });
const documents = Array.from({ length: 26 }, (_, index) => ({ id: id(200 + index), workspace_id: "owner", knowledge_base_id: id(index % 2 + 1), title: index === 0 ? "财务年度报告" : index === 1 ? "服务器采购规范" : index === 2 ? literalName : "采购规范附录 " + (index + 1) }));
let documentGate = null, rejectDocuments = false;
const sessions = [{ id: "session-one", workspace_id: "owner", title: "财务会话" },
  { id: "session-two", workspace_id: "owner", title: "独立会话" },
  { id: "session-second", workspace_id: "second", title: "另一空间会话" }];
const messages = new Map(sessions.map((s) => [s.id, []])), runs = new Map();
const requests = [], unexpected = [], errors = [];
const results = { fixture: true, scope: "Real Chrome with synthetic HTTP fixtures; no production data, model calls or backend permission claims",
  started_at: new Date().toISOString(), checks: [], screenshots: [], source_sha256: {} };
let origin, server, browser, page, authenticated = true, runningGate, rejectBaseList = false, nextSession = 0;

function json(response, data, status = 200, extra = {}) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify({ success: true, data, ...extra }));
}
function failure(response, status, code) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify({ success: false, error: { code, message: "synthetic fixture" } }));
}
function list(response, items, url) {
  const page = Number(url.searchParams.get("page") || 1), pageSize = Number(url.searchParams.get("page_size") || 10);
  json(response, items.slice((page - 1) * pageSize, page * pageSize), 200, { total: items.length, page, page_size: pageSize });
}
async function readJSON(request) {
  let body = ""; for await (const chunk of request) body += chunk;
  return body ? JSON.parse(body) : {};
}
function prompt(sessionID, runID) {
  return { id: "scope-prompt", kind: "ask_user", run_id: runID, session_id: sessionID, question: "请确认报告时间。",
    status: "pending", expires_at: "2099-01-01T00:00:00Z", options: ["本季度"] };
}
function complete(run, sessionID) {
  run.status = "completed"; delete run.error_code;
  const history = messages.get(sessionID);
  for (const message of history) if (message.run_id === run.id) message.status = "completed";
  history.push({ id: run.id + "-assistant", role: "assistant", content: "合成验收回答。", run_id: run.id, status: "completed" });
}
function startSSE(response, runID) {
  response.writeHead(200, { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-store", "X-Run-ID": runID });
  return (name, payload) => response.write(`event: ${name}\ndata: ${JSON.stringify(payload)}\n\n`);
}
async function streamMessage(response, sessionID, body) {
  const run = { id: "run-" + (runs.size + 1), status: "running" };
  runs.set(run.id, run);
  messages.get(sessionID).push({ id: run.id + "-user", role: "user", content: body.content, run_id: run.id, status: "running" });
  const send = startSSE(response, run.id);
  send("run.started", { run_id: run.id, run, session_id: sessionID });
  send("message.delta", { delta: "正在核对指定资料…" });
  if (body.content === "运行中检查") {
    assert.ok(runningGate, "test must install running gate");
    runningGate.started(); await runningGate.wait;
  }
  if (body.content === "等待答复") {
    run.status = "suspended"; messages.get(sessionID).at(-1).status = "suspended";
    send("run.suspended", { run_id: run.id, run, prompt: prompt(sessionID, run.id) });
  } else if (body.content === "恢复测试") {
    run.status = "failed"; run.error_code = "interrupted"; messages.get(sessionID).at(-1).status = "failed";
    send("run.failed", { run_id: run.id, run, error_code: run.error_code });
  } else {
    complete(run, sessionID); send("run.completed", { run_id: run.id, run, answer: "合成验收回答。", citations: [] });
  }
  response.end();
}
async function handle(request, response) {
  const url = new URL(request.url, origin), route = url.pathname, method = request.method;
  if (assets[route] && method === "GET") {
    const [name, type] = assets[route];
    response.writeHead(200, { "Content-Type": type, "Cache-Control": "no-store", "Content-Security-Policy": csp });
    response.end(await readFile(path.join(root, "internal/webui/assets", name))); return;
  }
  if (route === "/favicon.ico") { response.writeHead(204); response.end(); return; }
  const entry = { method, route, query: url.search }; requests.push(entry);
  if (route.startsWith("/api/v1/auth/browser/") && method === "POST") {
    assert.equal(request.headers["x-magworkspace-browser"], "1");
    if (route.endsWith("/logout")) { authenticated = false; response.writeHead(204); response.end(); return; }
    if (route.endsWith("/login")) {
      const body = await readJSON(request); assert.equal(body.email, user.email); assert.equal(body.password, "fixture-password-123"); authenticated = true;
    }
    if (!authenticated) { failure(response, 401, "unauthorized"); return; }
    json(response, { user, workspaces, token: "scope-fixture-only", expires_at: "2099-01-01T00:00:00Z" }); return;
  }
  assert.equal(request.headers.authorization, "Bearer scope-fixture-only");
  if (route === "/api/v1/auth/me") { json(response, { user, workspaces }); return; }
  if (route === "/api/v1/config") { json(response, { max_file_size_mb: 50, max_file_size_bytes: 50 * 1024 * 1024 }); return; }
  const match = /^\/api\/v1\/workspaces\/([^/]+)(.*)$/.exec(route);
  if (match) {
    const [, workspaceID, suffix] = match;
    assert.ok(workspaces.some((w) => w.id === workspaceID));
    if (suffix === "/knowledge-bases" && method === "GET") {
      if (rejectBaseList) failure(response, 503, "temporarily_unavailable"); else json(response, bases.filter((kb) => kb.workspace_id === workspaceID));
      return;
    }
    if (/^\/knowledge-bases\/[^/]+\/documents$/.test(suffix) && method === "GET") { list(response, [], url); return; }
    if (suffix === "/documents" && method === "GET") {
      const query = url.searchParams.get("keyword") || "";
      const found = documents.filter((doc) => doc.workspace_id === workspaceID && doc.title.includes(query));
      if (documentGate?.query === query) { documentGate.started(); await documentGate.wait; }
      if (rejectDocuments) failure(response, 503, "temporarily_unavailable"); else list(response, found, url);
      return;
    }
    if (/^\/documents\/[^/]+$/.test(suffix) && method === "GET") {
      const doc = documents.find((doc) => doc.id === suffix.split("/")[2] && doc.workspace_id === workspaceID);
      if (doc) json(response, doc); else failure(response, 404, "not_found"); return;
    }
    if (suffix === "/sessions") {
      if (method === "GET") { list(response, sessions.filter((s) => s.workspace_id === workspaceID), url); return; }
      if (method === "POST") {
        entry.body = await readJSON(request);
        const session = { id: "session-created-" + ++nextSession, workspace_id: workspaceID, title: entry.body.title };
        sessions.unshift(session); messages.set(session.id, []); json(response, session, 201); return;
      }
    }
    const sessionMatch = /^\/sessions\/([^/]+)(.*)$/.exec(suffix);
    if (sessionMatch) {
      const [, sessionID, resource] = sessionMatch, session = sessions.find((s) => s.id === sessionID && s.workspace_id === workspaceID);
      assert.ok(session, "unknown session");
      if (!resource && method === "GET") { json(response, session); return; }
      if (resource === "/messages" && method === "GET") { list(response, messages.get(sessionID), url); return; }
      if (resource === "/messages" && method === "POST") {
        entry.body = await readJSON(request);
        assert.equal(request.headers.accept, "text/event-stream");
        assert.ok(Object.keys(entry.body).every((key) => ["content", "knowledge_base_ids", "document_ids"].includes(key)));
        if (Object.hasOwn(entry.body, "knowledge_base_ids")) {
          assert.ok(Array.isArray(entry.body.knowledge_base_ids));
          assert.ok(entry.body.knowledge_base_ids.length <= 20);
          assert.equal(new Set(entry.body.knowledge_base_ids).size, entry.body.knowledge_base_ids.length);
          for (const selected of entry.body.knowledge_base_ids) assert.ok(bases.some((kb) => kb.id === selected && kb.workspace_id === workspaceID));
        }
        if (Object.hasOwn(entry.body, "document_ids")) {
          assert.ok(Array.isArray(entry.body.document_ids)); assert.ok(entry.body.document_ids.length <= 100);
          for (const selected of entry.body.document_ids) assert.ok(documents.some((doc) => doc.id === selected && doc.workspace_id === workspaceID));
        }
        if (entry.body.content === "拒绝请求保留草稿") { failure(response, 503, "temporarily_unavailable"); return; }
        await streamMessage(response, sessionID, entry.body); return;
      }
      const runMatch = /^\/runs\/([^/]+)(.*)$/.exec(resource);
      if (runMatch) {
        const [, runID, child] = runMatch, run = runs.get(runID); assert.ok(run, "unknown run");
        if (!child && method === "GET") { json(response, run); return; }
        if (child === "/memory" && method === "GET") { json(response, { status: "not_scheduled", saved_count: 0 }); return; }
        if (child === "/prompt" && method === "GET") {
          if (run.status === "suspended") json(response, prompt(sessionID, runID)); else failure(response, 404, "not_found");
          return;
        }
        if ((child === "/prompts/scope-prompt/answer" || child === "/recover") && method === "POST") {
          entry.body = await readJSON(request);
          assert.deepEqual(entry.body, child.endsWith("answer") ? { answer: "本季度" } : {});
          assert.equal(run.status, child.endsWith("answer") ? "suspended" : "failed");
          complete(run, sessionID); const send = startSSE(response, runID);
          send("run.started", { run_id: runID }); send("run.completed", { run_id: runID, answer: "合成验收回答。" }); response.end(); return;
        }
      }
    }
  }
  unexpected.push(method + " " + route); failure(response, 404, "not_found");
}
async function idle(page) { await page.waitForFunction(() => document.getElementById("shell").getAttribute("aria-busy") === "false"); }
async function check(name, action) {
  try { await action(); results.checks.push({ name, status: "passed" }); process.stdout.write("PASS " + name + "\n"); }
  catch (error) { results.checks.push({ name, status: "failed", error: error.message }); throw error; }
}
const messagePosts = () => requests.filter((entry) => entry.method === "POST" && entry.route.endsWith("/messages"));
async function send(page, content) {
  const before = messagePosts().length;
  await page.locator("#query").fill(content); await page.locator("#send").click(); await idle(page);
  assert.equal(messagePosts().length, before + 1, "each click must send exactly one request");
  return messagePosts().at(-1).body;
}
async function openScope(page) {
  const before = requests.filter((r) => r.method === "GET" && r.route.endsWith("/knowledge-bases")).length;
  await page.locator("#chat-knowledge-scope").click(); await idle(page);
  await page.locator("#chat-scope-dialog").waitFor({ state: "visible" });
  assert.equal(requests.filter((r) => r.method === "GET" && r.route.endsWith("/knowledge-bases")).length, before + 1, "opening scope refreshes available bases");
}
async function setScope(page, mode, selected = []) {
  await openScope(page); await page.locator(`input[name="chat-scope-mode"][value="${mode}"]`).check();
  if (mode === "selected") {
    const checked = await page.locator("#chat-scope-options input[type=checkbox]:checked").evaluateAll((inputs) => inputs.map((input) => input.value));
    for (const selectedID of checked) await page.locator(`#chat-scope-options input[value="${selectedID}"]`).uncheck();
    for (const selectedID of selected) await page.locator(`#chat-scope-options input[value="${selectedID}"]`).check();
  }
  await page.locator("#chat-scope-apply").click(); await idle(page);
  await page.locator("#chat-scope-dialog").waitFor({ state: "hidden" });
}
async function selectSession(page, title) { await page.locator("#sessions button").filter({ hasText: new RegExp("^" + title + "$") }).click(); await idle(page); }
async function screenshot(page, name) {
  // A viewport resize can finish before Chrome composites the new mobile layout.
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  const dimensions = await page.evaluate(() => ({ viewport: innerWidth, height: innerHeight, body: document.body.scrollWidth, root: document.documentElement.scrollWidth }));
  assert.ok(dimensions.body <= dimensions.viewport + 1 && dimensions.root <= dimensions.viewport + 1, "horizontal overflow: " + JSON.stringify(dimensions));
  if (name === "mention-mobile.png") {
    const bounds = await page.evaluate(() => {
      const elements = Object.fromEntries([".sidebar", "#chat-form", "#query", "#send", "#chat-mention-picker"].map((selector) => {
        const rect = document.querySelector(selector).getBoundingClientRect();
        return [selector, { x: rect.x, y: rect.y, width: rect.width, height: rect.height, bottom: rect.bottom }];
      }));
      const button = document.getElementById("send"), rect = button.getBoundingClientRect();
      return { ...elements, height: innerHeight, scrollHeight: document.documentElement.scrollHeight,
        sendUnobscured: button.contains(document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2)) };
    });
    assert.ok(bounds["#chat-form"].bottom <= bounds.height, "composer fits the mobile viewport");
    assert.ok(bounds["#send"].bottom <= bounds.height && bounds.sendUnobscured, "send remains visible and unobscured");
    assert.ok(bounds["#chat-mention-picker"].y >= 0, "mention picker fits above the composer");
    assert.equal(bounds.scrollHeight, bounds.height, "mobile chat does not add page overflow");
    results.mobile_layout = bounds;
  }
  // Mobile chat intentionally fits the viewport; avoid full-page capture resizing it.
  await page.screenshot({ path: path.join(output, name), fullPage: dimensions.viewport > 600, animations: "disabled" }); results.screenshots.push(name);
}

try {
  await mkdir(output, { recursive: true });
  for (const [name] of Object.values(assets)) results.source_sha256[name] = createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex");
  const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || "playwright");
  server = createServer((request, response) => { handle(request, response).catch((error) => {
    unexpected.push(error.stack || error.message); if (!response.headersSent) failure(response, 500, "fixture_error"); else response.destroy(error);
  }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve)); origin = "http://127.0.0.1:" + server.address().port;
  browser = await chromium.launch({ headless: true, ...(process.env.MAGWORKSPACE_BROWSER_CHANNEL ? { channel: process.env.MAGWORKSPACE_BROWSER_CHANNEL } : {}) });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1050 }, locale: "zh-CN" });
  await context.route("**/*", (route) => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  page = await context.newPage(); page.setDefaultTimeout(10000); page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(origin); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
  await page.locator("#nav-chat").click(); await idle(page); await selectSession(page, "财务会话");

  await check("all / selected / none serialize distinct request scopes, independently of document-library selection", async () => {
    assert.deepEqual(await send(page, "默认范围"), { content: "默认范围" });
    await setScope(page, "selected", [id(1), id(2)]);
    assert.match(await page.locator("#chat-scope-chips").textContent(), /财务资料/);
    assert.match(await page.locator("#chat-scope-chips").textContent(), /技术研究/);
    await page.locator("#nav-docs").click(); await idle(page); await page.locator("#knowledge-base").selectOption(id(4)); await idle(page);
    await page.locator("#nav-chat").click(); await idle(page);
    assert.deepEqual(await send(page, "指定两库"), { content: "指定两库", knowledge_base_ids: [id(1), id(2)], document_ids: [] });
    await setScope(page, "none"); assert.deepEqual(await send(page, "不用知识库"), { content: "不用知识库", knowledge_base_ids: [], document_ids: [] });
    await setScope(page, "all"); assert.deepEqual(await send(page, "恢复全部库"), { content: "恢复全部库" });
  });
  await check("search retains hidden selections, cancel is non-mutating, and names render as inert text", async () => {
    await setScope(page, "selected", [id(1)]); await openScope(page);
    await page.locator("#chat-scope-search").fill("技术研究");
    await page.locator(`#chat-scope-options input[value="${id(2)}"]`).check();
    await page.locator("#chat-scope-search").fill("");
    assert.equal(await page.locator(`#chat-scope-options input[value="${id(1)}"]`).isChecked(), true);
    await page.locator("#chat-scope-cancel").click(); await idle(page);
    assert.deepEqual((await send(page, "取消后范围")).knowledge_base_ids, [id(1)]);
    await openScope(page); await page.locator(`#chat-scope-options input[value="${id(3)}"]`).check();
    assert.equal(await page.locator("#chat-scope-options img, #chat-scope-options script").count(), 0);
    await page.locator("#chat-scope-apply").click(); await idle(page);
    assert.ok((await page.locator("#chat-scope-chips").textContent()).includes(literalName));
    assert.equal(await page.evaluate(() => window.__scopeXSS), undefined);
    assert.deepEqual((await send(page, "隐藏过滤仍保留")).knowledge_base_ids, [id(1), id(3)]);
  });
  await check("selected mode requires one base and never permits more than 20", async () => {
    await setScope(page, "all"); await openScope(page); await page.locator('input[name="chat-scope-mode"][value="selected"]').check();
    assert.equal(await page.locator("#chat-scope-apply").isDisabled(), true);
    for (let n = 1; n <= 20; n++) await page.locator(`#chat-scope-options input[value="${id(n)}"]`).check();
    const twentyFirst = page.locator(`#chat-scope-options input[value="${id(21)}"]`);
    if (await twentyFirst.isEnabled()) await twentyFirst.click();
    assert.equal(await twentyFirst.isChecked(), false, "21st selection must be rejected");
    assert.equal(await page.locator("#chat-scope-options input:checked").count(), 20);
    await page.locator("#chat-scope-apply").click(); await idle(page);
    assert.equal((await send(page, "最多二十库")).knowledge_base_ids.length, 20);
  });
  await check("each session retains its own scope, and new sessions begin with all bases", async () => {
    await setScope(page, "selected", [id(1), id(2)]); await selectSession(page, "独立会话");
    assert.deepEqual(await send(page, "独立会话默认范围"), { content: "独立会话默认范围" });
    await setScope(page, "none"); await selectSession(page, "财务会话");
    assert.deepEqual((await send(page, "恢复财务会话范围")).knowledge_base_ids, [id(1), id(2)]);
    await selectSession(page, "独立会话"); assert.deepEqual((await send(page, "恢复独立会话范围")).knowledge_base_ids, []);
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#session-title").textContent(), "独立会话");
    await selectSession(page, "财务会话");
    assert.deepEqual((await send(page, "刷新另一会话后仍限制原会话")).knowledge_base_ids, [id(1), id(2)]);
    await selectSession(page, "独立会话"); assert.deepEqual((await send(page, "刷新后恢复不使用知识库")).knowledge_base_ids, []);
    await page.locator("#new-session").click(); await idle(page);
    await page.locator("#detail input").fill("新建独立会话");
    await page.locator('#detail button[type="submit"]').click(); await idle(page);
    assert.deepEqual(await send(page, "新会话默认范围"), { content: "新会话默认范围" });
    await selectSession(page, "财务会话");
  });
  await check("removing the last selected chip explicitly switches to no knowledge bases", async () => {
    await setScope(page, "selected", [id(1)]);
    await page.locator("#chat-scope-chips button").click(); await idle(page);
    assert.deepEqual((await send(page, "移除最后一个库")).knowledge_base_ids, []);
  });
  await check("refresh restores the current session range without persisting knowledge names or message text", async () => {
    await setScope(page, "selected", [id(1), id(3)]);
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#session-title").textContent(), "财务会话");
    assert.deepEqual((await send(page, "刷新恢复当前范围")).knowledge_base_ids, [id(1), id(3)]);
    const saved = await page.evaluate(() => Object.values(sessionStorage).join("\n"));
    assert.ok(saved.includes(id(1))); assert.ok(!saved.includes(literalName)); assert.ok(!saved.includes("刷新恢复当前范围"));
  });
  await check("deleted selected bases remain visible and block new messages instead of widening the scope", async () => {
    await setScope(page, "selected", [id(2)]);
    const removed = bases.splice(bases.findIndex((kb) => kb.id === id(2)), 1)[0];
    try {
      await openScope(page); await page.locator("#chat-scope-refresh").click(); await idle(page);
      await page.locator("#chat-scope-cancel").click(); await idle(page);
      assert.equal(await page.locator("#send").isDisabled(), true);
      assert.ok((await page.locator("#chat-scope-chips").textContent()).trim(), "deleted selection must not disappear");
      assert.match(await page.locator("#chat-scope-hint").textContent(), /不可用|无法访问|失效|删除/);
      const before = messagePosts().length;
      await page.locator("#query").fill("缺失知识库不应发送"); await page.locator("#query").press("Control+Enter"); await idle(page);
      assert.equal(messagePosts().length, before);
      await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
      assert.equal(await page.locator("#send").isDisabled(), true);
      await setScope(page, "none"); assert.deepEqual((await send(page, "明确改为不用知识库")).knowledge_base_ids, []);
    } finally { bases.push(removed); }
  });
  await check("rejected message preserves draft and scope without automatically retrying", async () => {
    await setScope(page, "selected", [id(1)]);
    assert.deepEqual((await send(page, "拒绝请求保留草稿")).knowledge_base_ids, [id(1)]);
    assert.equal(await page.locator("#query").inputValue(), "拒绝请求保留草稿");
    assert.match(await page.locator("#chat-scope-chips").textContent(), /财务资料/);
    const before = messagePosts().length;
    await page.locator("#refresh-chat").click(); await idle(page); assert.equal(messagePosts().length, before);
    assert.deepEqual((await send(page, "用户主动重新发送")).knowledge_base_ids, [id(1)]);
  });
  await check("send-time refresh catches a newly deleted selected base before posting the message", async () => {
    await setScope(page, "selected", [id(4)]);
    const removed = bases.splice(bases.findIndex((kb) => kb.id === id(4)), 1)[0];
    try {
      const before = messagePosts().length;
      await page.locator("#query").fill("发送前库已删除"); await page.locator("#send").click(); await idle(page);
      assert.equal(messagePosts().length, before);
      assert.equal(await page.locator("#query").inputValue(), "发送前库已删除");
      assert.equal(await page.locator("#send").isDisabled(), true);
      assert.match(await page.locator("#chat-scope-hint").textContent(), /不可用|无法访问|失效|删除/);
      await setScope(page, "none");
    } finally { bases.push(removed); }
  });
  await check("unavailable scope refresh preserves selection and draft and does not send or silently widen", async () => {
    await setScope(page, "selected", [id(1)]); const before = messagePosts().length;
    rejectBaseList = true;
    try {
      await page.locator("#query").fill("范围服务暂不可用"); await page.locator("#send").click(); await idle(page);
      assert.equal(messagePosts().length, before); assert.equal(await page.locator("#query").inputValue(), "范围服务暂不可用");
      assert.match(await page.locator("#chat-scope-chips").textContent(), /财务资料/);
    } finally { rejectBaseList = false; }
    assert.deepEqual((await send(page, "范围服务恢复后手动发送")).knowledge_base_ids, [id(1)]);
  });
  await check("running and suspended Runs lock selection, and prompt answers never submit a new scope", async () => {
    let release, started;
    const wait = new Promise((resolve) => { release = resolve; }); const entered = new Promise((resolve) => { started = resolve; });
    runningGate = { wait, started };
    await page.locator("#query").fill("运行中检查"); await page.locator("#send").click(); await entered;
    try { assert.equal(await page.locator("#chat-knowledge-scope").isDisabled(), true); }
    finally { release(); }
    await idle(page); runningGate = null;
    await send(page, "等待答复"); assert.equal(await page.locator("#chat-knowledge-scope").isDisabled(), true);
    assert.equal(await page.locator("#send").isDisabled(), true);
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#chat-knowledge-scope").isDisabled(), true);
    assert.match(await page.locator("#chat-scope-hint").textContent(), /原.*范围|原.*知识库|发送时.*范围|固定/);
    await page.locator("#prompt").getByRole("button", { name: "本季度", exact: true }).click(); await idle(page);
    assert.deepEqual(requests.filter((r) => r.route.endsWith("/answer")).at(-1).body, { answer: "本季度" });
    assert.equal(await page.locator("#chat-knowledge-scope").isEnabled(), true);
  });
  await check("recovery retains the original Run contract even after changing the next-message scope", async () => {
    await setScope(page, "selected", [id(1)]); await send(page, "恢复测试");
    await setScope(page, "none");
    await page.getByRole("button", { name: "尝试恢复原运行", exact: true }).click(); await idle(page);
    assert.deepEqual(requests.filter((r) => r.route.endsWith("/recover")).at(-1).body, {});
    assert.deepEqual((await send(page, "恢复后新问题用新范围")).knowledge_base_ids, []);
  });
  await check("scope dialog and composer fit desktop and mobile screens with long literal names", async () => {
    await setScope(page, "selected", [id(1), id(2), id(3)]);
    await screenshot(page, "chat-scope-desktop.png"); await openScope(page); await screenshot(page, "chat-scope-dialog-desktop.png");
    await page.locator("#chat-scope-cancel").click(); await idle(page);
    await page.setViewportSize({ width: 390, height: 844 });
    await screenshot(page, "chat-scope-mobile.png"); await openScope(page); await screenshot(page, "chat-scope-dialog-mobile.png");
    assert.equal(await page.locator("#chat-scope-apply").isVisible(), true);
    await page.locator("#chat-scope-cancel").click(); await idle(page); await page.setViewportSize({ width: 1440, height: 1050 });
  });
  await check("typing @ opens keyboard selection, removes only the trigger and keeps selected resources as chips", async () => {
    bases.sort((a, b) => a.id.localeCompare(b.id));
    await setScope(page, "all");
    await page.locator("#query").fill("比较 @");
    await page.locator("#chat-mention-picker").waitFor({ state: "visible" });
    await page.locator("#query").press("ArrowDown"); await page.locator("#query").press("Enter");
    assert.equal(await page.locator("#query").inputValue(), "比较 ");
    assert.equal(await page.locator("#chat-mention-picker").isHidden(), true);
    assert.deepEqual((await send(page, "方向键选择技术库")).knowledge_base_ids, [id(2)]);
    await page.locator("#query").fill("邮箱 user@example.invalid");
    assert.equal(await page.locator("#chat-mention-picker").isHidden(), true);
    await page.locator("#query").fill("比较@技术"); await page.locator("#query").press("Enter");
    assert.equal(await page.locator("#query").inputValue(), "比较");
    await page.locator("#query").fill("@财务"); await page.locator("#query").press("Escape");
    assert.equal(await page.locator("#query").inputValue(), "@财务");
    assert.equal(await page.locator("#chat-mention-picker").isHidden(), true);
  });
  await check("IME Enter never selects a mention or sends a message", async () => {
    const before = messagePosts().length;
    await page.locator("#query").fill("@财务");
    await page.locator("#query").dispatchEvent("compositionstart");
    await page.locator("#query").dispatchEvent("keydown", { key: "Enter", ctrlKey: true, isComposing: true });
    assert.equal(await page.locator("#chat-mention-picker").isHidden(), true);
    assert.equal(messagePosts().length, before);
    await page.locator("#query").dispatchEvent("compositionend");
    await page.locator("#chat-mention-picker").waitFor({ state: "visible" });
    await page.locator("#query").press("Escape");
  });
  await check("@ document and full knowledge-base choices submit a mixed union, and a full base covers its document", async () => {
    await setScope(page, "selected", [id(1)]);
    await page.locator("#query").fill("请比较 @服务器");
    await page.locator('#chat-mention-tabs [data-kind="document"]').click();
    await page.locator(`#chat-mention-options [data-id="${id(201)}"]`).click();
    assert.equal(await page.locator("#query").inputValue(), "请比较 ");
    assert.deepEqual(await send(page, "全财务库加采购规范"), { content: "全财务库加采购规范", knowledge_base_ids: [id(1)], document_ids: [id(201)] });
    await page.locator("#query").fill("@技术"); await page.locator("#query").press("Enter");
    assert.deepEqual(await send(page, "全库覆盖单份文档"), { content: "全库覆盖单份文档", knowledge_base_ids: [id(1), id(2)], document_ids: [] });
    await openScope(page); await page.locator('#chat-scope-tabs [data-kind="document"]').click();
    await page.locator(`#chat-scope-options input[value="${id(201)}"]`).waitFor();
    assert.equal(await page.locator(`#chat-scope-options input[value="${id(201)}"]`).isDisabled(), true);
    assert.match(await page.locator("#chat-scope-options").textContent(), /已包含在所选知识库/);
    await page.locator("#chat-scope-cancel").click(); await idle(page);
  });
  await check("documents are searchable and paginated; selected documents survive search and reload without persisted titles", async () => {
    await setScope(page, "none"); await openScope(page);
    await page.locator('input[name="chat-scope-mode"][value="selected"]').check();
    await page.locator('#chat-scope-tabs [data-kind="document"]').click();
    await page.locator("#chat-scope-pages").getByRole("button", { name: "下一页" }).click();
    await page.locator(`#chat-scope-options input[value="${id(225)}"]`).check();
    await page.locator("#chat-scope-search").fill("服务器");
    await page.locator(`#chat-scope-options input[value="${id(201)}"]`).check();
    await page.locator("#chat-scope-apply").click(); await idle(page);
    assert.deepEqual(await send(page, "分页选两份文档"), { content: "分页选两份文档", knowledge_base_ids: [], document_ids: [id(225), id(201)] });
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.match(await page.locator("#chat-scope-chips").textContent(), /服务器采购规范/);
    assert.deepEqual((await send(page, "刷新恢复文档范围")).document_ids, [id(225), id(201)]);
    const saved = await page.evaluate(() => Object.values(sessionStorage).join("\n"));
    assert.ok(saved.includes(id(201))); assert.ok(!saved.includes("服务器采购规范"));
  });
  await check("deleted selected documents block sending and removing the final document stays explicitly empty", async () => {
    await setScope(page, "none"); await page.locator("#query").fill("@服务器");
    await page.locator('#chat-mention-tabs [data-kind="document"]').click();
    await page.locator(`#chat-mention-options [data-id="${id(201)}"]`).click();
    const index = documents.findIndex((doc) => doc.id === id(201)), removed = documents.splice(index, 1)[0];
    try {
      const before = messagePosts().length;
      await page.locator("#query").fill("发送前文档已删除"); await page.locator("#send").click(); await idle(page);
      assert.equal(messagePosts().length, before); assert.equal(await page.locator("#send").isDisabled(), true);
      assert.match(await page.locator("#chat-scope-chips").textContent(), /文档已不可用/);
      await openScope(page); await page.locator('#chat-scope-tabs [data-kind="document"]').click();
      await page.locator(`#chat-scope-options input[value="${id(201)}"]`).click();
      assert.equal(await page.locator(`#chat-scope-options input[value="${id(201)}"]`).count(), 0);
      await page.locator("#chat-scope-cancel").click(); await idle(page);
      assert.equal(await page.locator("#send").isDisabled(), true, "cancel preserves the unavailable selection");
      await page.locator("#chat-scope-chips button").click(); await idle(page);
      assert.deepEqual(await send(page, "移除最后一份文档"), { content: "移除最后一份文档", knowledge_base_ids: [], document_ids: [] });
    } finally { documents.splice(index, 0, removed); }
  });
  await check("late document search cannot replace a newer query and failed lookups do not clear the scope", async () => {
    await page.locator("#query").fill("@服务器");
    let release, started;
    const wait = new Promise((resolve) => { release = resolve; }), entered = new Promise((resolve) => { started = resolve; });
    documentGate = { query: "服务器", wait, started };
    await page.locator('#chat-mention-tabs [data-kind="document"]').click(); await entered;
    await page.locator("#query").fill("@财务");
    await page.locator(`#chat-mention-options [data-id="${id(200)}"]`).waitFor();
    release(); documentGate = null;
    await page.waitForLoadState("networkidle");
    assert.equal(await page.locator(`#chat-mention-options [data-id="${id(201)}"]`).count(), 0);
    await page.locator(`#chat-mention-options [data-id="${id(200)}"]`).click();
    rejectDocuments = true;
    try {
      await page.locator("#query").fill("@失败"); await page.locator('#chat-mention-tabs [data-kind="document"]').click();
      await page.locator("#chat-mention-options").getByText(/操作未完成/).waitFor();
      assert.match(await page.locator("#chat-scope-chips").textContent(), /财务年度报告/);
    } finally { rejectDocuments = false; await page.locator("#query").press("Escape"); }
  });
  await check("mixed mention chips and document picker fit desktop and mobile", async () => {
    await setScope(page, "selected", [id(1)]); await page.locator("#query").fill("@服务器");
    await page.locator('#chat-mention-tabs [data-kind="document"]').click();
    await page.locator(`#chat-mention-options [data-id="${id(201)}"]`).click();
    await page.locator("#query").fill("对比两份资料的采购要求 @");
    await page.locator('#chat-mention-tabs [data-kind="document"]').click();
    await page.locator(`#chat-mention-options [data-id="${id(201)}"]`).waitFor();
    assert.equal(await page.locator("#chat-mention-options img, #chat-mention-options script").count(), 0);
    assert.equal(await page.evaluate(() => window.__scopeXSS), undefined);
    await page.waitForFunction(() => document.getElementById("notice").textContent === "");
    await screenshot(page, "mention-desktop.png");
    await page.setViewportSize({ width: 390, height: 844 }); await screenshot(page, "mention-mobile.png");
    assert.equal(await page.locator("#chat-mention-picker").isVisible(), true);
    await page.locator("#query").press("Escape"); await page.setViewportSize({ width: 1440, height: 1050 });
  });
  await check("Workspace changes clear old ranges; logout removes scope persistence and private labels", async () => {
    await setScope(page, "selected", [id(1)]);
    await page.locator("#workspace").selectOption("second"); await idle(page); await selectSession(page, "另一空间会话");
    assert.deepEqual(await send(page, "另一空间默认范围"), { content: "另一空间默认范围" });
    await page.locator("#workspace").selectOption("owner"); await idle(page); await selectSession(page, "财务会话");
    assert.deepEqual(await send(page, "切空间后不沿用旧选择"), { content: "切空间后不沿用旧选择" });
    await setScope(page, "selected", [id(3)]); await page.locator("#logout").click(); await idle(page);
    await page.locator("#auth").waitFor({ state: "visible" });
    assert.equal(await page.locator("#chat-scope-chips").textContent(), "");
    assert.equal(await page.locator("#chat-scope-options").textContent(), "");
    assert.ok(!(await page.evaluate(() => Object.values(sessionStorage).join("\n"))).includes(id(3)));
    await page.locator('#auth-form input[name="email"]').fill(user.email); await page.locator('#auth-form input[name="password"]').fill("fixture-password-123");
    await page.locator("#auth-submit").click(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    await page.locator("#nav-chat").click(); await idle(page); await selectSession(page, "财务会话");
    assert.deepEqual(await send(page, "重新登录不沿用旧选择"), { content: "重新登录不沿用旧选择" });
  });
  await check("logout discards pending private document results and never restores labels or selections", async () => {
    let release, started;
    const wait = new Promise((resolve) => { release = resolve; }), entered = new Promise((resolve) => { started = resolve; });
    documentGate = { query: "服务器", wait, started };
    await page.locator("#query").fill("@服务器");
    await page.locator('#chat-mention-tabs [data-kind="document"]').click(); await entered;
    await page.evaluate(() => { const channel = new BroadcastChannel("magworkspace-auth"); channel.postMessage({ kind: "logout" }); channel.close(); });
    await page.locator("#auth").waitFor({ state: "visible" });
    release(); documentGate = null; await page.waitForLoadState("networkidle");
    assert.equal(await page.locator("#chat-mention-picker").isHidden(), true);
    assert.equal(await page.locator("#chat-mention-options").textContent(), "");
    assert.equal(await page.locator("#chat-scope-chips").textContent(), "");
    assert.ok(!(await page.evaluate(() => Object.values(sessionStorage).join("\n"))).includes(id(201)));
  });
  assert.deepEqual(errors, []); assert.deepEqual(unexpected, []);
  for (const [name] of Object.values(assets)) assert.equal(createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex"), results.source_sha256[name], name + " changed during checks");
  results.status = "passed";
} catch (error) {
  results.status = "failed"; results.error = error.stack || error.message; process.stderr.write(results.error + "\n"); process.exitCode = 1;
  results.unexpected = unexpected; results.page_errors = errors; results.requests = requests;
  if (page) { results.page_text = await page.locator("body").innerText().catch(() => "unavailable"); await page.screenshot({ path: path.join(output, "failure.png"), fullPage: true }).catch(() => {}); }
} finally {
  if (browser) await browser.close();
  if (server) await new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); });
  results.finished_at = new Date().toISOString();
  await mkdir(output, { recursive: true }); await writeFile(path.join(output, "results.json"), JSON.stringify(results, null, 2) + "\n");
}
