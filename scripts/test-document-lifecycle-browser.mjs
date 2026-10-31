// Real browser interaction with synthetic HTTP data; never connects to a live workspace.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = path.join(root, "artifacts/acceptance/042/document-browser");
const assets = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/app.js": ["app.js", "text/javascript; charset=utf-8"],
  "/core.js": ["core.js", "text/javascript; charset=utf-8"],
  "/style.css": ["style.css", "text/css; charset=utf-8"],
};
const stamp = "2026-10-23T00:00:00Z";
const user = { id: "test-user", name: "文档测试", email: "test@example.invalid" };
const workspaces = [{ id: "test-space", name: "合成知识库", role: "owner" }];
const documents = ["replace", "edit", "delete"].map((id) => ({
  id, title: id + ".md", current_version_id: "v1-" + id, updated_at: stamp,
}));
const bodies = new Map(documents.map((doc) => [doc.current_version_id, "旧正文 " + doc.id]));
let refuseDelete = true, searchGate, server, browser, origin;
const unexpected = [], mutations = [], pageErrors = [];
const results = { scope: "Chromium with synthetic HTTP fixtures, no production data or model calls", checks: [], source_sha256: {} };
function json(response, data, extra = {}) {
  response.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify({ success: true, data, ...extra }));
}
function fail(response, status, code) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify({ success: false, error: { code } }));
}
async function bytes(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return Buffer.concat(chunks);
}
function list(response, items) { json(response, items, { total: items.length, page: 1, page_size: 10 }); }
async function handle(request, response) {
  const url = new URL(request.url, origin), route = url.pathname, method = request.method;
  if (assets[route]) {
    const [name, type] = assets[route];
    response.writeHead(200, { "Content-Type": type, "Cache-Control": "no-store" });
    response.end(await readFile(path.join(root, "internal/webui/assets", name))); return;
  }
  if (route === "/favicon.ico") { response.writeHead(204); response.end(); return; }
  if (route === "/api/v1/auth/browser/refresh") {
    json(response, { token: "fixture-only", expires_at: "2099-01-01T00:00:00Z", user, workspaces }); return;
  }
  assert.equal(request.headers.authorization, "Bearer fixture-only");
  if (route === "/api/v1/config") { json(response, { max_file_size_mb: 50, max_file_size_bytes: 50 * 1024 * 1024 }); return; }
  if (route === "/api/v1/auth/me") { json(response, { user, workspaces }); return; }
  const base = "/api/v1/workspaces/test-space";
  if (route === base + "/knowledge-bases") { json(response, [{ id: "default", name: "默认知识库", workspace_id: "test-space", is_default: true }]); return; }
  if (route === base + "/knowledge-bases/default/documents" && method === "GET") { list(response, documents); return; }
  if (route === base + "/sessions") { list(response, []); return; }
  if (route === base + "/knowledge-bases/default/search" && method === "POST") {
    const { query } = JSON.parse((await bytes(request)).toString());
    const doc = documents.find((d) => d.id === query);
    const payload = { hits: doc ? [{ title: doc.title, chunk: { document_id: doc.id, content: bodies.get(doc.current_version_id) } }] : [] };
    if (searchGate) {
      const gate = searchGate; searchGate = null;
      response.writeHead(200, { "Content-Type": "application/json" }); response.flushHeaders();
      gate.started(); await gate.wait;
      response.end(JSON.stringify({ success: true, data: payload })); return;
    }
    json(response, payload); return;
  }
  const match = new RegExp("^" + base + "/documents/([^/]+)(.*)$").exec(route);
  if (match) {
    const [, id, suffix] = match, doc = documents.find((d) => d.id === id);
    if (!doc) { fail(response, 404, "not_found"); return; }
    if (!suffix && method === "GET") { json(response, doc); return; }
    if (!suffix && method === "DELETE") {
      mutations.push({ method, id });
      if (refuseDelete) { fail(response, 503, "storage_unavailable"); return; }
      documents.splice(documents.indexOf(doc), 1); response.writeHead(204); response.end(); return;
    }
    if (suffix.endsWith("/index")) { json(response, { status: "ready" }); return; }
    if (suffix === "/versions" && method === "GET") {
      list(response, [{ id: doc.current_version_id, number: doc.current_version_id.startsWith("v2") ? 2 : 1,
        filename: doc.title, size_bytes: Buffer.byteLength(bodies.get(doc.current_version_id)), created_at: stamp }]); return;
    }
    if (suffix === "/versions" && method === "POST") {
      const raw = (await bytes(request)).toString();
      assert.match(request.headers["content-type"], /^multipart\/form-data/);
      assert.ok(raw.includes(doc.current_version_id)); assert.ok(raw.includes("替换后全文"));
      doc.current_version_id = "v2-" + id; bodies.set(doc.current_version_id, "替换后全文");
      mutations.push({ method, id }); json(response, { document: doc }); return;
    }
    if (suffix === "/content" && method === "PUT") {
      const body = JSON.parse((await bytes(request)).toString());
      assert.equal(body.base_version_id, doc.current_version_id); assert.equal(body.content, "编辑后全文");
      doc.current_version_id = "v2-" + id; bodies.set(doc.current_version_id, body.content);
      mutations.push({ method, id }); json(response, { document: doc }); return;
    }
    const content = /^\/versions\/([^/]+)\/content$/.exec(suffix);
    if (content && method === "GET") {
      response.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" }); response.end(bodies.get(content[1])); return;
    }
  }
  unexpected.push(method + " " + route); fail(response, 404, "not_found");
}
async function idle(page) { await page.waitForFunction(() => document.getElementById("shell").getAttribute("aria-busy") === "false"); }
async function check(name, fn) { await fn(); results.checks.push({ name, status: "passed" }); process.stdout.write("PASS " + name + "\n"); }
async function search(page, query) {
  await page.locator('#search-form input[name="query"]').fill(query);
  await page.locator('#search-form button[type="submit"]').click(); await idle(page);
  assert.equal(await page.locator("#search-results .search-hit").count(), 1);
}
async function source(page) { await page.getByRole("button", { name: "查看来源文档", exact: true }).click(); await idle(page); }
async function cleared(page) {
  assert.equal(await page.locator("#search-results").textContent(), "");
  assert.equal(await page.locator("#search-summary").textContent(), "");
  assert.equal(await page.locator("#documents").isVisible(), true);
  assert.equal(await page.locator("#clear-search").isVisible(), false);
  assert.equal(await page.locator("#doc-list-heading").textContent(), "全部文档");
}
try {
  for (const [name] of Object.values(assets)) results.source_sha256[name] = createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex");
  const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || "playwright");
  server = createServer((request, response) => { handle(request, response).catch((error) => {
    unexpected.push(error.stack || error.message); if (!response.headersSent) fail(response, 500, "fixture_error"); else response.destroy(error);
  }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  origin = "http://127.0.0.1:" + server.address().port;
  browser = await chromium.launch({ headless: true, ...(process.env.MAGWORKSPACE_BROWSER_CHANNEL ? { channel: process.env.MAGWORKSPACE_BROWSER_CHANNEL } : {}) });
  const context = await browser.newContext();
  await context.route("**/*", (route) => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  const page = await context.newPage(); page.setDefaultTimeout(10000);
  page.on("pageerror", (error) => pageErrors.push(error.message));
  await page.goto(origin); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
  await check("failed document deletion retains search and dialog; success removes stale hits", async () => {
    await search(page, "delete"); await source(page);
    await page.getByRole("button", { name: "删除文档", exact: true }).click(); await idle(page);
    assert.match(await page.locator("#detail-body").textContent(), /全部历史版本.*原文件和检索索引.*无法恢复/);
    await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#search-results .search-hit").count(), 1);
    assert.equal(await page.locator("#detail").evaluate((el) => el.open), true);
    refuseDelete = false;
    await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
    await cleared(page); assert.equal(await page.locator("#detail").evaluate((el) => el.open), false);
    assert.equal(documents.some((doc) => doc.id === "delete"), false);
  });
  await check("file replacement clears old search while retaining new version detail", async () => {
    await search(page, "replace"); await source(page);
    await page.getByLabel("替换文档文件").setInputFiles({ name: "replace.md", mimeType: "text/markdown", buffer: Buffer.from("替换后全文") });
    await page.getByRole("button", { name: "上传新版本", exact: true }).click(); await idle(page);
    await cleared(page); assert.match(await page.locator("#detail-body").textContent(), /版本 2/);
    await page.locator("#close-detail").click(); await idle(page);
  });
  await check("online text edit clears old search only after the new version saves", async () => {
    await search(page, "edit"); await source(page);
    await page.getByRole("button", { name: "查看正文", exact: true }).click(); await idle(page);
    await page.getByLabel("文档正文", { exact: true }).fill("编辑后全文");
    await page.getByRole("button", { name: "保存新版本", exact: true }).click(); await idle(page);
    await cleared(page); assert.match(await page.locator("#detail-body").textContent(), /版本 2/);
    await page.locator("#close-detail").click(); await idle(page);
  });
  await check("late search body cannot restore private results after another tab logs out", async () => {
    let release, started;
    const wait = new Promise((resolve) => { release = resolve; });
    const entered = new Promise((resolve) => { started = resolve; });
    searchGate = { wait, started };
    await page.locator('#search-form input[name="query"]').fill("edit");
    await page.locator('#search-form button[type="submit"]').click(); await entered;
    const second = await context.newPage();
    try {
      await second.goto(origin);
      await second.evaluate(() => { const channel = new BroadcastChannel("magworkspace-auth"); channel.postMessage({ kind: "logout" }); channel.close(); });
      await page.locator("#auth").waitFor({ state: "visible" });
      release(); await idle(page);
      assert.equal(await page.locator("#search-results").textContent(), "");
      assert.equal(await page.locator("#shell").isVisible(), false);
    } finally { release(); await second.close(); }
  });
  assert.deepEqual(unexpected, []); assert.deepEqual(pageErrors, []);
  assert.deepEqual(mutations, [{ method: "DELETE", id: "delete" }, { method: "DELETE", id: "delete" }, { method: "POST", id: "replace" }, { method: "PUT", id: "edit" }]);
  for (const [name] of Object.values(assets)) assert.equal(createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex"), results.source_sha256[name], name + " changed during checks");
  results.status = "passed";
} catch (error) {
  results.status = "failed"; results.error = error.stack || error.message;
  process.stderr.write(results.error + "\n"); process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  if (server) await new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); });
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, "results.json"), JSON.stringify(results, null, 2) + "\n");
}
