// Isolated browser acceptance for knowledge-base navigation and management.
// All data below is synthetic. No production backend or model is contacted.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = process.env.MAGWORKSPACE_KB_TEST_OUTPUT || path.join(root, "artifacts/acceptance/042/knowledge-base-browser");
const assets = {
  "/": ["index.html", "text/html; charset=utf-8"], "/app.js": ["app.js", "text/javascript; charset=utf-8"],
  "/core.js": ["core.js", "text/javascript; charset=utf-8"], "/style.css": ["style.css", "text/css; charset=utf-8"],
};
const user = { id: "kb-fixture", name: "知识库测试", email: "kb@example.invalid" };
const workspaces = ["owner", "admin", "contributor", "viewer"].map((role) => ({ id: role, name: role + " 工作空间", role }));
const bases = workspaces.map((workspace) => ({ id: workspace.id + "-default", workspace_id: workspace.id,
  name: "默认知识库", description: "团队共享资料", is_default: true }));
bases.push({ id: "research", workspace_id: "owner", name: "研究资料", description: "论文、实验记录与调研笔记", is_default: false });
const documents = bases.map((kb) => ({ id: "doc-" + kb.id, knowledge_base_id: kb.id, workspace_id: kb.workspace_id,
  title: kb.id === "research" ? "检索评测笔记.md" : "团队知识手册.md", current_version_id: "v-" + kb.id,
  updated_at: "2026-10-23T00:00:00Z" }));
const sessions = [{ id: "kept-session", title: "保留的会话" }];
const requests = [], errors = [], unexpected = [];
const results = { scope: "Real Chrome with synthetic HTTP fixtures; no production data or model calls", checks: [], screenshots: [], source_sha256: {} };
let browser, server, origin, deleteFailure = false, nextID = 0, gate;
function json(response, data, status = 200, extra = {}) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify({ success: true, data, ...extra }));
}
function fail(response, status, code) {
  response.writeHead(status, { "Content-Type": "application/json" }); response.end(JSON.stringify({ success: false, error: { code } }));
}
function list(response, items) { json(response, items, 200, { total: items.length, page: 1, page_size: 10 }); }
async function readBody(request) { const parts = []; for await (const part of request) parts.push(part); return Buffer.concat(parts); }
function delay(route) {
  let release, started;
  const wait = new Promise((resolve) => { release = resolve; });
  const entered = new Promise((resolve) => { started = resolve; });
  gate = { route, wait, started }; return { release, entered };
}
async function handle(request, response) {
  const route = new URL(request.url, origin).pathname, method = request.method;
  if (assets[route]) {
    const [name, type] = assets[route]; response.writeHead(200, { "Content-Type": type, "Cache-Control": "no-store" });
    response.end(await readFile(path.join(root, "internal/webui/assets", name))); return;
  }
  if (route === "/favicon.ico") { response.writeHead(204); response.end(); return; }
  requests.push({ method, route });
  if (route === "/api/v1/auth/browser/refresh") { json(response, { token: "kb-fixture-only", expires_at: "2099-01-01T00:00:00Z" }); return; }
  assert.equal(request.headers.authorization, "Bearer kb-fixture-only");
  if (route === "/api/v1/config") { json(response, { max_file_size_mb: 50, max_file_size_bytes: 50 * 1024 * 1024 }); return; }
  if (route === "/api/v1/auth/me") { json(response, { user, workspaces }); return; }
  const match = /^\/api\/v1\/workspaces\/([^/]+)(.*)$/.exec(route);
  if (match) {
    const [, wid, suffix] = match, role = workspaces.find((item) => item.id === wid)?.role;
    if (!role) { fail(response, 404, "not_found"); return; }
    if (suffix === "/sessions" && method === "GET") { list(response, sessions); return; }
    if (suffix === "/knowledge-bases" && method === "GET") { json(response, bases.filter((kb) => kb.workspace_id === wid)); return; }
    if (suffix === "/knowledge-bases" && method === "POST") {
      assert.ok(["owner", "admin"].includes(role));
      const body = JSON.parse((await readBody(request)).toString());
      assert.deepEqual(Object.keys(body).sort(), ["description", "name"]);
      const kb = { ...body, id: "created-" + ++nextID, workspace_id: wid, is_default: false };
      bases.push(kb); json(response, kb, 201); return;
    }
    const kbMatch = /^\/knowledge-bases\/([^/]+)(.*)$/.exec(suffix);
    if (kbMatch) {
      const [, id, resource] = kbMatch, kb = bases.find((item) => item.id === id && item.workspace_id === wid);
      if (!kb) { fail(response, 404, "not_found"); return; }
      if (!resource && method === "PATCH") {
        assert.ok(["owner", "admin"].includes(role));
        const body = JSON.parse((await readBody(request)).toString());
        assert.deepEqual(Object.keys(body).sort(), ["description", "name"]);
        Object.assign(kb, body); json(response, kb); return;
      }
      if (!resource && method === "DELETE") {
        assert.ok(["owner", "admin"].includes(role));
        if (deleteFailure) { fail(response, 503, "storage_unavailable"); return; }
        bases.splice(bases.indexOf(kb), 1);
        for (let i = documents.length - 1; i >= 0; i--) if (documents[i].knowledge_base_id === id) documents.splice(i, 1);
        response.writeHead(204); response.end(); return;
      }
      if (resource === "/documents" && method === "POST") {
        assert.notEqual(role, "viewer");
        assert.match(request.headers["content-type"], /^multipart\/form-data/);
        const raw = (await readBody(request)).toString(); assert.ok(raw.includes("知识库独立上传正文"));
        const doc = { id: "upload-" + ++nextID, knowledge_base_id: id, workspace_id: wid, title: "新知识库资料.md", current_version_id: "upload-v1", updated_at: "2026-10-23T00:00:00Z" };
        documents.push(doc); json(response, { document: doc }, 201); return;
      }
      if (resource === "/documents" && method === "GET") {
        const docs = documents.filter((item) => item.knowledge_base_id === id);
        if (gate?.route === route) {
          const held = gate; gate = null; response.writeHead(200, { "Content-Type": "application/json" }); response.flushHeaders();
          held.started(); await held.wait;
          response.end(JSON.stringify({ success: true, data: docs, total: docs.length, page: 1, page_size: 10 })); return;
        }
        list(response, docs); return;
      }
      if (resource === "/search" && method === "POST") {
        const body = JSON.parse((await readBody(request)).toString());
        assert.equal(body.mode, "hybrid"); assert.equal(body.limit, 10);
        const hits = documents.filter((doc) => doc.knowledge_base_id === id).map((doc) => ({ title: doc.title,
          chunk: { document_id: doc.id, content: "检索来自 " + kb.name + "，不包含其他知识库正文。" } }));
        json(response, { hits }); return;
      }
    }
    const documentMatch = /^\/documents\/([^/]+)(.*)$/.exec(suffix);
    if (documentMatch) {
      const [, id, resource] = documentMatch, doc = documents.find((item) => item.id === id && item.workspace_id === wid);
      if (!doc) { fail(response, 404, "not_found"); return; }
      if (!resource && method === "GET") { json(response, doc); return; }
      if (resource === "/versions") { list(response, [{ id: doc.current_version_id, filename: doc.title, number: 1, size_bytes: 30, created_at: doc.updated_at }]); return; }
      if (resource.endsWith("/index")) { json(response, { status: "ready" }); return; }
    }
  }
  unexpected.push(method + " " + route); fail(response, 404, "not_found");
}
async function idle(page) { await page.waitForFunction(() => document.getElementById("shell").getAttribute("aria-busy") === "false"); }
async function check(name, fn) { await fn(); results.checks.push({ name, status: "passed" }); process.stdout.write("PASS " + name + "\n"); }
async function search(page) {
  await page.locator('#search-form input[name="query"]').fill("资料");
  await page.locator('#search-form button[type="submit"]').click(); await idle(page);
}
async function screenshot(page, name) {
  await page.screenshot({ path: path.join(output, name), fullPage: true }); results.screenshots.push(name);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, "horizontal page overflow");
}
async function deleteCurrent(page) {
  await page.locator("#delete-knowledge-base").click(); await idle(page);
  await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
}
try {
  await mkdir(output, { recursive: true });
  for (const [name] of Object.values(assets)) results.source_sha256[name] = createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex");
  const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || "playwright");
  server = createServer((request, response) => { handle(request, response).catch((error) => {
    unexpected.push(error.stack || error.message); if (!response.headersSent) fail(response, 500, "fixture_error"); else response.destroy(error);
  }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve)); origin = "http://127.0.0.1:" + server.address().port;
  browser = await chromium.launch({ headless: true, ...(process.env.MAGWORKSPACE_BROWSER_CHANNEL ? { channel: process.env.MAGWORKSPACE_BROWSER_CHANNEL } : {}) });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1050 }, locale: "zh-CN" });
  await context.route("**/*", (route) => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  const page = await context.newPage(); page.setDefaultTimeout(10000); page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(origin); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
  await check("default knowledge base scopes listing and search; navigation clears stale hits before loading", async () => {
    assert.equal(await page.locator("#knowledge-base").inputValue(), "owner-default");
    assert.equal(await page.locator("#documents .document-title").allTextContents().then((v) => v.join()), "团队知识手册.md");
    await search(page); assert.match(await page.locator("#search-results").textContent(), /检索来自 默认知识库/);
    const held = delay("/api/v1/workspaces/owner/knowledge-bases/research/documents");
    try {
      await page.locator("#knowledge-base").selectOption("research"); await held.entered;
      assert.equal(await page.locator("#search-results").textContent(), "");
      assert.equal(await page.locator("#documents").textContent(), "");
      held.release(); await idle(page);
    } finally { held.release(); }
    assert.deepEqual(await page.locator("#documents .document-title").allTextContents(), ["检索评测笔记.md"]);
    await search(page); assert.match(await page.locator("#search-results").textContent(), /检索来自 研究资料/);
    assert.doesNotMatch(await page.locator("#search-results").textContent(), /团队知识手册/);
    await page.locator("#clear-search").click(); await idle(page);
    await screenshot(page, "knowledge-bases-desktop.png");
    const mobile = await browser.newContext({ viewport: { width: 390, height: 844 }, locale: "zh-CN", isMobile: true });
    try {
      await mobile.route("**/*", (route) => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
      const mobilePage = await mobile.newPage();
      await mobilePage.goto(origin); await mobilePage.locator("#shell").waitFor({ state: "visible" }); await idle(mobilePage);
      await mobilePage.locator("#knowledge-base").selectOption("research"); await idle(mobilePage);
      await screenshot(mobilePage, "knowledge-bases-mobile.png");
    } finally { await mobile.close(); }
    await page.reload(); await idle(page); assert.equal(await page.locator("#knowledge-base").inputValue(), "research");
  });
  await check("owner creates and edits a knowledge base with text-only names and scoped upload", async () => {
    await page.locator("#new-knowledge-base").click(); await idle(page);
    await page.getByLabel("知识库名称", { exact: true }).fill("项目资料");
    await page.getByLabel("知识库说明", { exact: true }).fill("仅保存本项目文档");
    await page.getByRole("button", { name: "创建知识库", exact: true }).click(); await idle(page);
    const id = await page.locator("#knowledge-base").inputValue(); assert.match(id, /^created-/);
    assert.match(await page.locator("#documents").textContent(), /知识库还空着/);
    await page.locator("#edit-knowledge-base").click(); await idle(page);
    const literal = '<img src=x onerror="window.__kbXSS=1">';
    await page.getByLabel("知识库名称", { exact: true }).fill(literal);
    await page.getByLabel("知识库说明", { exact: true }).fill("修订后的说明");
    await page.getByRole("button", { name: "保存知识库", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#knowledge-base option:checked").textContent(), literal);
    assert.equal(await page.locator("#knowledge-base-description").textContent(), "修订后的说明");
    assert.equal(await page.evaluate(() => window.__kbXSS), undefined);
    await page.locator("#show-upload").click(); await idle(page);
    assert.equal(await page.locator("#upload-knowledge-base").textContent(), "上传至：" + literal);
    await page.locator('#upload-form input[type="file"]').setInputFiles({ name: "sample.md", mimeType: "text/markdown", buffer: Buffer.from("知识库独立上传正文") });
    await page.locator('#upload-form button[type="submit"]').click(); await idle(page);
    assert.deepEqual(await page.locator("#documents .document-title").allTextContents(), ["新知识库资料.md"]);
    assert.equal(documents.find((doc) => doc.title === "新知识库资料.md").knowledge_base_id, id);
  });
  await check("deletion requires named confirmation, preserves other bases and chat, and handles failures", async () => {
    await search(page);
    const id = await page.locator("#knowledge-base").inputValue();
    await page.locator("#delete-knowledge-base").click(); await idle(page);
    assert.match(await page.locator("#detail-body").textContent(), /不会删除工作空间、会话或个人记忆/);
    assert.ok((await page.locator("#detail-body").textContent()).includes(bases.find((kb) => kb.id === id).name));
    await page.locator("#close-detail").click(); await idle(page);
    assert.ok(bases.some((kb) => kb.id === id));
    deleteFailure = true; await deleteCurrent(page);
    assert.equal(await page.locator("#detail").evaluate((el) => el.open), true);
    assert.equal(await page.locator("#search-results .search-hit").count(), 1);
    deleteFailure = false;
    await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
    assert.equal(bases.some((kb) => kb.id === id), false); assert.equal(await page.locator("#search-results").textContent(), "");
    assert.ok(documents.some((doc) => doc.knowledge_base_id === "research"));
    assert.equal(await page.locator("#session-total").textContent(), "1");
    await page.locator("#nav-chat").click(); await idle(page);
    assert.match(await page.locator("#page-description").textContent(), /输入 @ 选择知识库或文档/);
    assert.match(await page.locator("#chat-knowledge-scope").textContent(), /全部知识库/);
    assert.match(await page.locator("#sessions").textContent(), /保留的会话/);
    await page.locator("#nav-docs").click(); await idle(page);
  });
  await check("deleting the final base leaves an explicit empty state without automatic recreation", async () => {
    await page.locator("#knowledge-base").selectOption("research"); await idle(page); await deleteCurrent(page);
    assert.equal(await page.locator("#knowledge-base").inputValue(), "owner-default");
    const creations = requests.filter((r) => r.method === "POST" && r.route.endsWith("/knowledge-bases")).length;
    await deleteCurrent(page);
    assert.equal(await page.locator("#knowledge-base").isDisabled(), true);
    assert.equal(await page.locator("#show-upload").isDisabled(), true);
    assert.equal(await page.locator('#search-form button[type="submit"]').isDisabled(), true);
    assert.equal(await page.locator("#new-knowledge-base").isEnabled(), true);
    assert.match(await page.locator("#documents").textContent(), /还没有知识库/);
    await screenshot(page, "knowledge-base-empty.png");
    await page.reload(); await idle(page);
    assert.equal(requests.filter((r) => r.method === "POST" && r.route.endsWith("/knowledge-bases")).length, creations);
    assert.equal(bases.some((kb) => kb.workspace_id === "owner"), false);
  });
  await check("admin manages bases; contributors upload only; viewers remain read-only", async () => {
    await page.locator("#workspace").selectOption("admin"); await idle(page);
    assert.equal(await page.locator("#new-knowledge-base").isVisible(), true);
    assert.equal(await page.locator("#delete-knowledge-base").isEnabled(), true);
    await page.locator("#workspace").selectOption("contributor"); await idle(page);
    for (const id of ["new-knowledge-base", "edit-knowledge-base", "delete-knowledge-base"]) assert.equal(await page.locator("#" + id).isVisible(), false);
    assert.equal(await page.locator("#show-upload").isEnabled(), true);
    await page.locator("#documents .document-title").click(); await idle(page);
    assert.equal(await page.getByRole("button", { name: "删除文档", exact: true }).isVisible(), true);
    await page.locator("#close-detail").click(); await idle(page);
    await page.locator("#workspace").selectOption("viewer"); await idle(page);
    assert.equal(await page.locator("#show-upload").isDisabled(), true);
    assert.equal(await page.locator('#search-form button[type="submit"]').isEnabled(), true);
    await page.locator("#documents .document-title").click(); await idle(page);
    assert.equal(await page.getByRole("button", { name: "删除文档", exact: true }).count(), 0);
    await page.locator("#close-detail").click(); await idle(page);
  });
  await check("late KB document response cannot repopulate private UI after logout", async () => {
    const held = delay("/api/v1/workspaces/viewer/knowledge-bases/viewer-default/documents");
    const second = await context.newPage();
    try {
      await second.goto(origin); await idle(second);
      await page.locator("#refresh-docs").click(); await held.entered;
      await second.evaluate(() => { const channel = new BroadcastChannel("magworkspace-auth"); channel.postMessage({ kind: "logout" }); channel.close(); });
      await page.locator("#auth").waitFor({ state: "visible" }); held.release(); await idle(page);
      for (const id of ["documents", "knowledge-base", "knowledge-base-description", "upload-knowledge-base", "search-results"]) assert.equal(await page.locator("#" + id).textContent(), "");
    } finally { held.release(); await second.close(); }
  });
  assert.deepEqual(errors, []); assert.deepEqual(unexpected, []);
  assert.equal(requests.some((r) => /^\/api\/v1\/workspaces\/[^/]+\/(documents|search)$/.test(r.route)), false, "document page used unscoped workspace endpoint");
  assert.equal(requests.some((r) => r.method === "DELETE" && !r.route.includes("/knowledge-bases/")), false, "knowledge base deletion touched workspace or chat");
  for (const [name] of Object.values(assets)) assert.equal(createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex"), results.source_sha256[name], name + " changed during checks");
  results.status = "passed";
} catch (error) {
  results.status = "failed"; results.error = error.stack || error.message; process.stderr.write(results.error + "\n"); process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  if (server) await new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); });
  await mkdir(output, { recursive: true }); await writeFile(path.join(output, "results.json"), JSON.stringify(results, null, 2) + "\n");
}
