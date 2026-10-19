// Real browser checks against synthetic HTTP fixtures. No database, Harness or model calls.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = path.join(root, "artifacts/acceptance/022");
const assets = {
  "/": ["index.html", "text/html; charset=utf-8"],
  "/app.js": ["app.js", "text/javascript; charset=utf-8"],
  "/core.js": ["core.js", "text/javascript; charset=utf-8"],
  "/style.css": ["style.css", "text/css; charset=utf-8"],
};
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
const timestamp = "2026-10-17T08:00:00Z";
const maliciousTitle = '<img src=x onerror="window.__injected=1">.md';
const maliciousBody = '<script>window.__injected=2</script>\n<img src=x onerror="window.__injected=3">';
const durableAnswer = "这是从持久消息重新读取的答案。\n" + maliciousBody;
const evidenceID = "a".repeat(32);
const sourceText = "知识准备、服务部署、数据处理、结果验证。演示建议：单机 2 核 6 GB；三台虚拟机各 1 核 4 GB。此处为合成验收资料。";
const richAnswer = [
  "## 从资料到可执行的计划", "", "依据**团队知识手册**，可以把实践分成四个阶段：",
  "- **知识准备**：核对环境与前置条件。", "- **服务部署**：完成安装并检查服务状态。",
  "- **数据处理**：运行示例任务，记录输入与输出。", "- **结果验证**：对照预期检查结果。[E:" + evidenceID + "]",
  "", "### 推荐的演示环境", "| 部署方式 | 机器数量 | 每台建议配置 |", "| --- | --- | --- |",
  "| 单机环境 | 1 台 | 2 核 / 6 GB |", "| 分布式环境 | 3 台 | 1 核 / 4 GB |",
  "", "以上配置来自手册中的合成验收样例。[E:" + evidenceID + "]",
  "", "```bash", "free -h", "```", "", "> 先检查资源，再启动任务，可以更快定位环境问题。",
].join("\n");
const user = { id: "fixture-user", name: "演示用户", email: "browser@example.test" };
const workspaces = [
  { id: "owner", name: "我的知识空间", role: "owner" },
  { id: "viewer", name: "只读参考空间", role: "viewer" },
];
const documents = [
  { id: "guide", title: "团队知识手册.pdf", current_version_id: "v-guide", status: "ready" },
  { id: "notes", title: "产品研究笔记.md", current_version_id: "v-notes", status: "running" },
  { id: "literal", title: maliciousTitle, current_version_id: "v-literal", status: "failed" },
].map((document) => ({ ...document, updated_at: timestamp }));
const sessions = [];
const messages = new Map();
const runs = new Map();
const requests = [];
const unexpected = [];
const results = {
  fixture: true,
  scope: "Chromium DOM and HTTP fixture behavior; no real auth, database, Harness, indexing service or model provider",
  started_at: new Date().toISOString(),
  checks: [], screenshots: [], source_sha256: {},
};
let browser;
let server;
let origin;
let rejectAuthentication = false;
let delayedBody;

function delayNextBody(route, status = 200) {
  let release;
  const body = new Promise((resolve) => { release = resolve; });
  delayedBody = { route, body, status };
  return release;
}
async function sendDelayedBody(response, route, type, body) {
  if (delayedBody?.route !== route) return false;
  const gate = delayedBody;
  delayedBody = null;
  response.writeHead(gate.status, { "Content-Type": type, "Cache-Control": "no-store" });
  // Fetch receives headers while the authenticated body is withheld.
  response.flushHeaders();
  await gate.body;
  response.end(gate.status === 401 ? JSON.stringify({ success: false, error: { code: "unauthorized" } }) : body);
  return true;
}

function json(response, data, status = 200, extra = {}, headers = {}) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store", ...headers });
  response.end(JSON.stringify({ success: true, data, ...extra }));
}
function failure(response, status, code) {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify({ success: false, error: { code, message: "synthetic fixture" } }));
}
function list(response, items, url) {
  const page = Number(url.searchParams.get("page") || 1);
  const pageSize = Number(url.searchParams.get("page_size") || 10);
  json(response, items.slice((page - 1) * pageSize, page * pageSize), 200,
    { total: items.length, page, page_size: pageSize });
}
async function readJSON(request) {
  let body = "";
  for await (const chunk of request) body += chunk;
  return body ? JSON.parse(body) : {};
}
function prompt(sessionID, runID) {
  return {
    id: "approval", kind: "tool_approval", run_id: runID, session_id: sessionID,
    question: "请审阅这次文档更新。", status: "pending", expires_at: "2099-01-01T00:00:00Z",
    approval: {
      tool: "update_document", document_id: "literal", base_version_id: "missing-baseline",
      title: "待审批笔记", filename: "notes.md", content_bytes: Buffer.byteLength(maliciousBody),
      arguments: { content: maliciousBody },
    },
  };
}
async function streamMessage(response, sessionID, content) {
  const runID = `run-${runs.size + 1}`;
  const suspended = content === "测试审批基线失败";
  const stoppable = content === "测试停止生成";
  const status = stoppable ? "running" : suspended ? "suspended" : "completed";
  const run = { id: runID, status };
  runs.set(runID, run);
  const history = messages.get(sessionID);
  history.push({ id: `${runID}-user`, role: "user", content, run_id: runID, status });
  if (!suspended && !stoppable) history.push({ id: `${runID}-assistant`, role: "assistant", content: durableAnswer, run_id: runID, status });
  response.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache, no-store",
    "X-Run-ID": runID, "X-Accel-Buffering": "no",
  });
  const send = (name, payload) => response.write(`event: ${name}\ndata: ${JSON.stringify(payload)}\n\n`);
  send("run.started", { run_id: runID, session_id: sessionID, run: { ...run, status: "running" } });
  send("message.start", { round: 1 });
  send("message.delta", { delta: "临时流式草稿，正在核对资料…" });
  if (stoppable) {
    await new Promise((resolve) => response.once("close", () => {
      run.status = "failed"; run.error_code = "agent_canceled";
      history.at(-1).status = "failed";
      resolve();
    }));
    return;
  }
  // Keep the connection open briefly so the browser must render the stream.
  await new Promise((resolve) => setTimeout(resolve, 100));
  if (suspended) send("run.suspended", { run_id: runID, run, prompt: prompt(sessionID, runID) });
  else send("run.completed", { run_id: runID, run, answer: "流式终态答案（与持久消息不同）", citations: [] });
  response.end();
}
async function handle(request, response) {
  const url = new URL(request.url, "http://fixture.invalid");
  const route = url.pathname;
  const method = request.method;
  requests.push({ method, path: route, query: url.search });
  if (assets[route] && method === "GET") {
    const [name, type] = assets[route];
    // Read on every request: screenshots and checks always use current source files.
    const body = await readFile(path.join(root, "internal/webui/assets", name));
    response.writeHead(200, { "Content-Type": type, "Content-Security-Policy": csp,
      "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store" });
    response.end(body);
    return;
  }
  if (route === "/favicon.ico") { response.writeHead(204); response.end(); return; }
  if (route.startsWith("/api/v1/auth/browser/") && method === "POST") {
    assert.equal(request.headers["x-magworkspace-browser"], "1");
    assert.equal(request.headers.origin, origin);
    const kind = route.slice("/api/v1/auth/browser/".length);
    if (kind === "logout") {
      response.writeHead(204, { "Cache-Control": "no-store", "Set-Cookie": "magworkspace_refresh=; Path=/api/v1/auth/browser; HttpOnly; SameSite=Strict; Max-Age=0" });
      response.end(); return;
    }
    if (kind === "login") {
      const body = await readJSON(request);
      assert.equal(body.email, user.email);
      assert.equal(body.password, "fixture-password-123");
    } else if (kind === "refresh") {
      if (rejectAuthentication || !request.headers.cookie?.includes("magworkspace_refresh=fixture-refresh")) {
        failure(response, 401, "unauthorized"); return;
      }
    } else throw new Error("unexpected browser auth endpoint " + kind);
    json(response, { user, workspaces, token: "fixture-access", expires_at: "2099-01-01T00:00:00Z" }, 200, {},
      { "Set-Cookie": "magworkspace_refresh=fixture-refresh; Path=/api/v1/auth/browser; HttpOnly; SameSite=Strict" });
    return;
  }
  if (!route.startsWith("/api/v1/") || rejectAuthentication || request.headers.authorization !== "Bearer fixture-access") {
    failure(response, 401, "unauthorized"); return;
  }
  if (route === "/api/v1/config") { json(response, { max_file_size_mb: 50, max_file_size_bytes: 50 * 1024 * 1024 }); return; }
  if (route === "/api/v1/auth/me") { json(response, { user, workspaces }); return; }
  const match = /^\/api\/v1\/workspaces\/(owner|viewer)(.*)$/.exec(route);
  if (match) {
    const [, workspace, suffix] = match;
    if (suffix === "/documents" && method === "GET") {
      const items = workspace === "owner" ? documents : documents.slice(0, 1);
      const page = Number(url.searchParams.get("page") || 1), pageSize = Number(url.searchParams.get("page_size") || 10);
      if (await sendDelayedBody(response, route, "application/json", JSON.stringify({ success: true,
        data: items.slice((page - 1) * pageSize, page * pageSize), total: items.length, page, page_size: pageSize }))) return;
      list(response, items, url); return;
    }
    if (suffix === "/search" && method === "POST") {
      const body = await readJSON(request);
      assert.equal(body.query, "资料依据");
      assert.equal(body.mode, "hybrid");
      json(response, { hits: [{ title: maliciousTitle, chunk: { document_id: "literal", content: maliciousBody } }] }); return;
    }
    const documentMatch = /^\/documents\/([^/]+)(.*)$/.exec(suffix);
    if (documentMatch && method === "GET") {
      const [, documentID, resource] = documentMatch;
      const document = documents.find((item) => item.id === documentID);
      if (resource === "/versions/missing-baseline/content") { failure(response, 503, "object_unavailable"); return; }
      if (document && !resource) { json(response, document); return; }
      if (document && resource.endsWith("/index")) { json(response, { status: document.status }); return; }
      if (document && resource === "/versions") {
        list(response, [{ id: document.current_version_id, number: 1, filename: "notes.md", size_bytes: Buffer.byteLength(maliciousBody), created_at: timestamp }], url); return;
      }
      if (document && resource.endsWith("/content")) {
        if (await sendDelayedBody(response, route, "text/plain; charset=utf-8", maliciousBody)) return;
        response.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" }); response.end(maliciousBody); return;
      }
    }
    if (suffix === "/sessions") {
      if (method === "GET") { list(response, sessions, url); return; }
      if (method === "POST") {
        const session = { id: `session-${sessions.length + 1}`, title: (await readJSON(request)).title };
        sessions.unshift(session); messages.set(session.id, []); json(response, session, 201); return;
      }
    }
    const sessionMatch = /^\/sessions\/([^/]+)(.*)$/.exec(suffix);
    if (sessionMatch) {
      const [, sessionID, resource] = sessionMatch;
      const session = sessions.find((item) => item.id === sessionID);
      if (session) {
        if (!resource && method === "GET") { json(response, session); return; }
        if (!resource && method === "PATCH") { session.title = (await readJSON(request)).title; json(response, session); return; }
        if (!resource && method === "DELETE") { sessions.splice(sessions.indexOf(session), 1); messages.delete(sessionID); response.writeHead(204); response.end(); return; }
        if (resource === "/messages" && method === "GET") { list(response, messages.get(sessionID), url); return; }
        if (resource === "/messages" && method === "POST") {
          const { content } = await readJSON(request);
          if (content === "请求应当被拒绝并保留草稿") { failure(response, 409, "conflict"); return; }
          assert.equal(request.headers.accept, "text/event-stream");
          await streamMessage(response, sessionID, content); return;
        }
        const sourceMatch = /^\/runs\/([^/]+)\/(citations|evidence\/[^/]+)$/.exec(resource);
        if (sourceMatch && method === "GET") {
          const evidence = { evidence_id: evidenceID, title: "团队知识手册.pdf", document_id: "guide", version_id: "v-guide", start_byte: 0, end_byte: 150 };
          if (sourceMatch[2] === "citations") json(response, { citations: [{ evidence }] });
          else if (sourceMatch[2] === "evidence/" + evidenceID) json(response, { evidence: { ...evidence, content: sourceText } });
          else failure(response, 404, "not_found");
          return;
        }
        const recovery = /^\/runs\/([^/]+)\/recover$/.exec(resource);
        if (recovery && method === "POST") {
          const run = runs.get(recovery[1]);
          assert.equal(run.status, "failed"); run.status = "completed"; delete run.error_code;
          const history = messages.get(sessionID);
          history.at(-1).status = "completed";
          history.push({ role: "assistant", content: richAnswer, run_id: run.id, status: "completed" });
          response.writeHead(200, { "Content-Type": "text/event-stream" });
          response.end(`event: run.started\ndata: ${JSON.stringify({ run_id: run.id })}\n\nevent: run.completed\ndata: ${JSON.stringify({ answer: richAnswer, run_id: run.id })}\n\n`);
          return;
        }
        const runMatch = /^\/runs\/([^/]+)(\/prompt)?$/.exec(resource);
        if (runMatch && method === "GET") {
          const run = runs.get(runMatch[1]);
          if (run && !runMatch[2]) { json(response, run); return; }
          if (run?.status === "suspended") { json(response, prompt(sessionID, run.id)); return; }
          if (run) { failure(response, 404, "not_found"); return; }
        }
      }
    }
  }
  unexpected.push(`${method} ${route}`);
  failure(response, 404, "not_found");
}
async function check(name, action) {
  try {
    await action();
    results.checks.push({ name, status: "passed" });
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    results.checks.push({ name, status: "failed", error: error.message });
    throw error;
  }
}
async function idle(page) {
  await page.waitForFunction(() => document.querySelector("#shell").getAttribute("aria-busy") === "false");
}
async function login(page) {
  await page.goto(origin);
  await page.locator("#auth-form").waitFor({ state: "visible" });
  await page.locator('#auth-form input[name="email"]').fill(user.email);
  await page.locator('#auth-form input[name="password"]').fill("fixture-password-123");
  await page.locator("#auth-submit").click();
  await page.locator("#shell").waitFor({ state: "visible" });
  await idle(page);
}
async function noOverflow(page) {
  const dimensions = await page.evaluate(() => ({ viewport: innerWidth, body: document.body.scrollWidth, root: document.documentElement.scrollWidth }));
  assert.ok(dimensions.body <= dimensions.viewport + 1 && dimensions.root <= dimensions.viewport + 1, JSON.stringify(dimensions));
}
async function screenshot(page, filename) {
  await page.screenshot({ path: path.join(output, filename), fullPage: true });
  results.screenshots.push(filename);
}
async function literalContent(page) {
  assert.equal(await page.locator("#search-results h2").textContent(), maliciousTitle);
  assert.equal(await page.locator("#search-results pre").textContent(), maliciousBody);
  assert.equal(await page.locator("#search-results img, #search-results script").count(), 0);
  assert.equal(await page.evaluate(() => window.__injected), undefined);
}
const countMessagePosts = () => requests.filter((entry) => entry.method === "POST" && entry.path.endsWith("/messages")).length;

try {
  await mkdir(output, { recursive: true });
  for (const [name] of Object.values(assets)) {
    results.source_sha256[name] = createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex");
  }
  const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || "playwright");
  server = createServer((request, response) => {
    handle(request, response).catch((error) => {
      unexpected.push(error.message);
      if (!response.headersSent) failure(response, 500, "fixture_error");
      else response.destroy(error);
    });
  });
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({ headless: true,
    ...(process.env.MAGWORKSPACE_BROWSER_CHANNEL ? { channel: process.env.MAGWORKSPACE_BROWSER_CHANNEL } : {}),
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1024 }, locale: "zh-CN", timezoneId: "Asia/Shanghai" });
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  const pageErrors = [];
  const externalRequests = [];
  await context.route("**/*", async (route) => {
    if (new URL(route.request().url()).origin !== origin) {
      externalRequests.push(route.request().url()); await route.abort();
    } else await route.continue();
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));

  await check("login renders current assets with CSP and synthetic account", async () => {
    const response = await page.goto(origin);
    assert.equal(response.headers()["content-security-policy"], csp);
    await screenshot(page, "desktop-login.png");
    await login(page);
    assert.match(await page.locator("#identity").textContent(), /演示用户/);
    const storage = await page.evaluate(() => ({ local: { ...localStorage }, session: { ...sessionStorage }, cookie: document.cookie }));
    assert.equal(Object.keys(storage.local).length, 0);
    assert.ok(!JSON.stringify(storage.session).includes("fixture-access"));
    assert.ok(!JSON.stringify(storage.session).includes("fixture-refresh"));
    assert.ok(!storage.cookie.includes("magworkspace_refresh"));
    const cookie = (await context.cookies(origin + "/api/v1/auth/browser/refresh")).find((item) => item.name === "magworkspace_refresh");
    assert.equal(cookie.httpOnly, true);
    assert.equal(cookie.sameSite, "Strict");
    assert.equal(cookie.path, "/api/v1/auth/browser");
  });
  await check("page reload restores cookie login and selected workspace", async () => {
    await page.locator("#workspace").selectOption("viewer"); await idle(page);
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#workspace").inputValue(), "viewer");
    assert.equal(await page.locator("#show-upload").isDisabled(), true);
    await page.locator("#workspace").selectOption("owner"); await idle(page);
  });
  await check("temporary startup auth failure keeps cookie and retry restores login", async () => {
    await page.route("**/api/v1/auth/browser/refresh", async (route) => {
      await route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ success: false, error: { code: "unavailable" } }) });
    }, { times: 1 });
    await page.reload(); await page.locator("#retry-auth").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#auth-form").isVisible(), false);
    assert.ok((await context.cookies(origin + "/api/v1/auth/browser/refresh")).some((item) => item.name === "magworkspace_refresh"));
    await page.locator("#retry-auth").click();
    await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
  });
  await check("document totals, current index states and malicious title are literal", async () => {
    assert.equal(await page.locator("#doc-total").textContent(), "3");
    assert.deepEqual(await page.locator("#documents [role=status]").allTextContents(), ["可检索", "正在处理", "处理失败"]);
    const statusBackgrounds = await page.locator("#documents [role=status]").evaluateAll((elements) => elements.map((element) => getComputedStyle(element).backgroundColor));
    assert.equal(new Set(statusBackgrounds).size, 3, `index status backgrounds must differ: ${statusBackgrounds}`);
    assert.equal(await page.locator("#documents .document-title").last().textContent(), maliciousTitle);
    assert.equal(await page.locator("#documents img, #documents script").count(), 0);
    await noOverflow(page); await screenshot(page, "desktop-documents.png");
  });
  await check("upload opens and cancels without a document POST", async () => {
    await page.locator("#show-upload").click(); await idle(page);
    assert.equal(await page.locator("#upload-dialog").evaluate((element) => element.open), true);
    assert.match(await page.locator("#upload-limit").textContent(), /50 MB/);
    await page.locator("#close-upload").click(); await idle(page);
    assert.equal(await page.locator("#upload-dialog").evaluate((element) => element.open), false);
    assert.equal(requests.filter((entry) => entry.method === "POST" && entry.path.endsWith("/documents")).length, 0);
  });
  await check("content search and clear preserve untrusted text", async () => {
    await page.getByRole("textbox", { name: "检索知识库" }).fill("资料依据");
    await page.locator('#search-form button[type="submit"]').click(); await idle(page);
    await literalContent(page);
    assert.equal(await page.locator("#documents").isVisible(), false);
    assert.equal(await page.locator("#doc-total").textContent(), "1");
    assert.equal(await page.locator("#doc-columns").isVisible(), false);
    await page.locator("#refresh-docs").click(); await idle(page);
    assert.equal(await page.locator("#doc-total").textContent(), "1");
    assert.equal(await page.locator("#doc-columns").isVisible(), false);
    await page.locator("#clear-search").click(); await idle(page);
    assert.equal(await page.locator("#documents").isVisible(), true);
    assert.equal(await page.locator("#doc-total").textContent(), "3");
    assert.equal(await page.locator("#doc-columns").isVisible(), true);
    assert.equal(await page.locator("#search-results").textContent(), "");
    assert.equal(await page.getByRole("textbox", { name: "检索知识库" }).inputValue(), "");
  });
  await check("raw document preview remains literal text", async () => {
    await page.locator("#documents .document-title").last().click(); await idle(page);
    await page.getByRole("button", { name: "查看正文", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#detail-body pre").textContent(), maliciousBody);
    assert.equal(await page.locator("#detail-body img, #detail-body script").count(), 0);
    await page.locator("#close-detail").click(); await idle(page);
  });
  await check("workspace switch disables upload for viewer and restores owner", async () => {
    await page.locator("#workspace").selectOption("viewer"); await idle(page);
    assert.equal(await page.locator("#role").textContent(), "只读成员");
    assert.equal(await page.locator("#show-upload").isDisabled(), true);
    assert.equal(await page.locator('#upload-form input[name="file"]').isDisabled(), true);
    await page.locator("#workspace").selectOption("owner"); await idle(page);
    assert.equal(await page.locator("#show-upload").isEnabled(), true);
  });
  await check("new session and suggestion fill composer without sending", async () => {
    await page.locator("#nav-chat").click(); await idle(page);
    assert.equal(await page.locator("#send").isDisabled(), true);
    await page.locator("#new-session").click(); await idle(page);
    await page.locator("#detail-body input").fill("前端验收会话");
    await page.locator('#detail-body button[type="submit"]').click(); await idle(page);
    assert.equal(await page.locator("#session-title").textContent(), "前端验收会话");
    const posts = countMessagePosts();
    await page.locator("[data-suggestion]").first().click();
    assert.match(await page.locator("#query").inputValue(), /概览当前知识库/);
    assert.equal(countMessagePosts(), posts);
  });
  await check("rejected message POST preserves draft and never retries", async () => {
    const content = "请求应当被拒绝并保留草稿";
    const posts = countMessagePosts();
    await page.locator("#query").fill(content); await page.locator("#send").click(); await idle(page);
    assert.equal(await page.locator("#query").inputValue(), content);
    assert.equal(countMessagePosts(), posts + 1);
    assert.match(await page.locator("#notice").textContent(), /未自动重试/);
    await page.locator("#refresh-chat").click(); await idle(page);
    assert.equal(countMessagePosts(), posts + 1);
  });
  await check("SSE completion reconciles durable history and clears accepted input", async () => {
    const posts = countMessagePosts();
    await page.locator("#query").fill("请依据资料整理关键要点");
    await page.locator("#send").click();
    await page.locator("#draft").waitFor({ state: "visible" });
    await idle(page);
    assert.equal(await page.locator("#messages .assistant > div").textContent(), durableAnswer);
    assert.equal(await page.locator("#messages img, #messages script").count(), 0);
    assert.equal(await page.locator("#query").inputValue(), "");
    assert.equal(await page.locator("#draft").isVisible(), false);
    assert.equal(countMessagePosts(), posts + 1);
    await noOverflow(page); await screenshot(page, "desktop-chat.png");
  });
  await check("Markdown answers, inline sources, copy and mobile drawer", async () => {
    const history = messages.get(sessions[0].id);
    history[0].content = "请根据团队知识手册，整理四个实践阶段和推荐环境，附来源。";
    history.at(-1).content = richAnswer;
    await page.locator("#refresh-chat").click(); await idle(page);
    const answer = page.locator("#messages .assistant .message-content");
    assert.equal(await answer.locator("h3").first().textContent(), "从资料到可执行的计划");
    assert.equal(await answer.locator("li").count(), 4);
    assert.equal(await answer.locator("tbody tr").count(), 2);
    assert.equal(await answer.locator("pre code").textContent(), "free -h");
    assert.equal(await answer.locator("button.citation-mark").count(), 2);
    assert.deepEqual(await answer.locator("button.citation-mark").allTextContents(), ["1", "1"]);
    assert.ok(!(await answer.textContent()).includes(evidenceID));
    await page.evaluate(() => { navigator.clipboard.writeText = async (value) => { window.copiedAnswer = value; }; });
    await page.getByRole("button", { name: "复制回答", exact: true }).click(); await idle(page);
    assert.equal(await page.evaluate(() => window.copiedAnswer), richAnswer);
    await page.evaluate(() => { document.querySelector('.conversation-scroll').scrollTop = 0; });
    await screenshot(page, "desktop-answer.png");
    await answer.locator("button.citation-mark").first().click(); await idle(page);
    assert.equal(await page.locator("#detail").evaluate((e) => e.classList.contains("source-drawer")), true);
    assert.equal(await page.locator(".source-passage").textContent(), sourceText);
    assert.ok(requests.some((r) => r.path.endsWith('/evidence/' + evidenceID)));
    await screenshot(page, "desktop-sources.png");
    await page.locator("#close-detail").click(); await idle(page);
    await page.setViewportSize({ width: 390, height: 844 });
    await noOverflow(page);
    const composer = await page.locator("#chat-form").boundingBox();
    assert.ok(composer.y >= 0 && composer.y + composer.height <= 844, JSON.stringify(composer));
    await page.evaluate(() => { const s = document.querySelector('.conversation-scroll'); s.scrollTop = 0; s.dispatchEvent(new Event('scroll')); });
    await page.locator("#jump-latest").waitFor({ state: "visible" });
    await screenshot(page, "mobile-answer.png");
    await page.locator("#jump-latest").click();
    await page.waitForFunction(() => { const s = document.querySelector('.conversation-scroll'); return s.scrollHeight - s.scrollTop - s.clientHeight < 10; });
    await answer.locator("button.citation-mark").last().click(); await idle(page);
    await noOverflow(page); await screenshot(page, "mobile-sources.png");
    await page.locator("#close-detail").click(); await idle(page);
    await page.setViewportSize({ width: 1440, height: 1024 });
  });
  await check("renderer keeps HTML and unsafe links literal; unverified citation cannot read a source", async () => {
    const history = messages.get(sessions[0].id);
    const unknown = 'f'.repeat(32);
    const unsafe = "# \n**加粗**\n\n" + maliciousBody + "\n[危险链接](javascript:window.__injected=4)\n[E:" + unknown + "]";
    history.at(-1).content = unsafe;
    await page.locator("#refresh-chat").click(); await idle(page);
    assert.equal(await page.locator("#messages img, #messages script, #messages a[href^='javascript:']").count(), 0);
    assert.equal(await page.evaluate(() => window.__injected), undefined);
    await page.locator("#messages .citation-mark").click(); await idle(page);
    assert.match(await page.locator("#notice").textContent(), /不在已保存的回答来源中/);
    assert.ok(!requests.some((r) => r.path.endsWith('/evidence/' + unknown)));
    history.at(-1).content = richAnswer;
    await page.locator("#refresh-chat").click(); await idle(page);
  });
  await check("source drawer retains inline numbering even when saved source list differs", async () => {
    const history = messages.get(sessions[0].id);
    history.at(-1).content = '[E:' + 'b'.repeat(32) + ']\n[E:' + evidenceID + ']';
    await page.locator("#refresh-chat").click(); await idle(page);
    await page.locator('#messages .citation-mark').last().click(); await idle(page);
    assert.equal(await page.locator('.source-number').textContent(), '来源 2');
    await page.getByRole('button', { name: '← 查看全部来源', exact: true }).click(); await idle(page);
    assert.equal(await page.locator('.source-number').textContent(), '来源 2');
    await page.locator('#close-detail').click();
    history.at(-1).content = richAnswer;
    await page.locator("#refresh-chat").click(); await idle(page);
  });
  await check("failed run has a recoverable explanation and preserves its original question", async () => {
    const history = messages.get(sessions[0].id);
    const run = { id: 'failed-turn', status: 'failed', error_code: 'agent_failed' };
    runs.set(run.id, run);
    history.push({ role: 'user', content: '请继续核对推荐配置。', run_id: run.id, status: 'failed' });
    await page.locator("#refresh-chat").click(); await idle(page);
    assert.match(await page.locator("#run-state").textContent(), /你的问题已保留/);
    assert.equal(await page.locator("#run-state details").evaluate((e) => e.open), false);
    await screenshot(page, "desktop-recovery.png");
    const posts = countMessagePosts(), count = history.length;
    await page.getByRole("button", { name: "尝试恢复原运行", exact: true }).click(); await idle(page);
    assert.equal(countMessagePosts(), posts);
    assert.equal(history.length, count + 1);
    assert.equal(history.at(-1).run_id, run.id);
    assert.equal(await page.locator("#run-state").textContent(), "");
    assert.equal(await page.locator("#send").isEnabled(), true);
  });
  await check("IME does not submit; stop clears draft without resending or inventing a saved answer", async () => {
    const posts = countMessagePosts(), history = messages.get(sessions[0].id), count = history.length;
    await page.locator("#query").fill("测试停止生成");
    await page.locator("#query").dispatchEvent("keydown", { key: "Enter", ctrlKey: true, isComposing: true });
    assert.equal(countMessagePosts(), posts);
    await page.locator("#query").press("Control+Enter");
    await page.waitForFunction(() => document.querySelector('#draft').textContent.includes('临时流式草稿'));
    assert.equal(await page.locator('#draft button.citation-mark').count(), 0);
    await page.locator("#stop").click(); await idle(page);
    assert.equal(await page.locator("#draft").textContent(), "");
    assert.equal(countMessagePosts(), posts + 1);
    assert.equal(history.length, count + 1);
    assert.equal(history.at(-1).role, "user");
    await page.locator("#refresh-chat").click(); await idle(page);
    assert.match(await page.locator("#run-state").textContent(), /生成已停止/);
    assert.equal(await page.getByRole("button", { name: "尝试恢复原运行", exact: true }).count(), 0);
    assert.equal(await page.locator("#send").isEnabled(), true);
  });
  await check("session rename and confirmed delete update UI and fixture", async () => {
    await page.locator("#rename-session").click(); await idle(page);
    await page.locator("#detail-body input").fill("已重命名的会话");
    await page.locator('#detail-body button[type="submit"]').click(); await idle(page);
    assert.equal(await page.locator("#session-title").textContent(), "已重命名的会话");
    assert.match(await page.locator("#sessions").textContent(), /已重命名的会话/);
    await page.locator("#delete-session").click(); await idle(page);
    await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#session-total").textContent(), "0");
    assert.equal(await page.locator("#send").isDisabled(), true);
    assert.equal(sessions.length, 0);
  });
  await check("suspended approval cannot approve when fixed baseline fails", async () => {
    await page.locator("#new-session").click(); await idle(page);
    await page.locator("#detail-body input").fill("审批失败路径");
    await page.locator('#detail-body button[type="submit"]').click(); await idle(page);
    await page.locator("#query").fill("测试审批基线失败");
    await page.locator("#send").click(); await idle(page);
    assert.match(await page.locator("#prompt").textContent(), /基线读取失败，不能批准/);
    assert.equal(await page.getByRole("button", { name: "批准这次写入", exact: true }).isDisabled(), true);
    assert.equal(await page.getByRole("button", { name: "拒绝这次写入", exact: true }).isEnabled(), true);
    assert.match(await page.locator("#prompt").textContent(), /window.__injected=2/);
    assert.equal(await page.locator("#prompt img, #prompt script").count(), 0);
    assert.equal(await page.locator("#send").isDisabled(), true);
    await noOverflow(page); await screenshot(page, "desktop-approval.png");
  });
  await check("page reload restores selected session and its pending approval", async () => {
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#chat-view").isVisible(), true);
    assert.equal(await page.locator("#session-title").textContent(), "审批失败路径");
    assert.match(await page.locator("#prompt").textContent(), /基线读取失败，不能批准/);
    assert.equal(await page.getByRole("button", { name: "批准这次写入", exact: true }).isDisabled(), true);
  });
  await check("older history keeps latest suspended run and approval visible", async () => {
    const history = messages.get(sessions[0].id);
    runs.set("older-run", { id: "older-run", status: "completed" });
    history.unshift(...Array.from({ length: 20 }, (_, index) => ({
      id: `older-${index}`, role: index % 2 ? "assistant" : "user", content: `较早消息 ${index + 1}`,
      run_id: "older-run", status: "completed",
    })));
    await page.locator("#refresh-chat").click(); await idle(page);
    await page.locator("#message-pages").getByRole("button", { name: "上一页", exact: true }).click(); await idle(page);
    assert.match(await page.locator("#messages").textContent(), /较早消息 1/);
    assert.match(await page.locator("#prompt").textContent(), /审阅文档变更/);
    assert.equal(await page.getByRole("button", { name: "批准这次写入", exact: true }).isDisabled(), true);
    assert.equal(await page.locator("#send").isDisabled(), true);
    assert.equal(requests.filter((entry) => entry.path.endsWith("/messages")).at(-1).query, "?page=2&page_size=20");
  });
  await check("full session page and composer remain reachable on laptop height", async () => {
    for (let index = 0; index < 10; index++) {
      const session = { id: `additional-${index}`, title: `知识整理会话 ${index + 1}` };
      sessions.push(session); messages.set(session.id, []);
    }
    await page.setViewportSize({ width: 1440, height: 768 });
    await page.locator("#nav-chat").click(); await idle(page);
    assert.equal(await page.locator("#sessions button").count(), 10);
    const bounds = await page.evaluate(() => {
      const rect = (selector) => {
        const { top, bottom } = document.querySelector(selector).getBoundingClientRect();
        return { top, bottom };
      };
      return { layout: rect(".chat-layout"), sessions: rect(".sessions"), pager: rect("#session-pages"), composer: rect("#chat-form") };
    });
    assert.ok(bounds.composer.bottom <= bounds.layout.bottom + 1, JSON.stringify(bounds));
    assert.ok(bounds.pager.bottom <= bounds.sessions.bottom + 1, JSON.stringify(bounds));
    await page.locator("#session-pages").getByRole("button", { name: "下一页", exact: true }).click(); await idle(page);
    await page.locator("#session-pages").getByRole("button", { name: "上一页", exact: true }).click(); await idle(page);
    await noOverflow(page); await screenshot(page, "laptop-chat.png");
    await page.setViewportSize({ width: 1440, height: 1024 });
  });
  await check("mobile documents, search and approval fit viewport", async () => {
    const mobile = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 1, locale: "zh-CN", timezoneId: "Asia/Shanghai" });
    const mobilePage = await mobile.newPage();
    mobilePage.setDefaultTimeout(8000);
    mobilePage.on("pageerror", (error) => pageErrors.push(error.message));
    await login(mobilePage); await noOverflow(mobilePage);
    await screenshot(mobilePage, "mobile-documents.png");
    await mobilePage.getByRole("textbox", { name: "检索知识库" }).fill("资料依据");
    await mobilePage.locator('#search-form button[type="submit"]').click(); await idle(mobilePage);
    await literalContent(mobilePage); await noOverflow(mobilePage);
    await mobilePage.locator("#clear-search").click(); await idle(mobilePage);
    await mobilePage.locator("#nav-chat").click(); await idle(mobilePage);
    await mobilePage.locator("#sessions button").first().click(); await idle(mobilePage);
    assert.equal(await mobilePage.getByRole("button", { name: "批准这次写入", exact: true }).isDisabled(), true);
    await noOverflow(mobilePage); await screenshot(mobilePage, "mobile-chat.png");
    assert.equal(await mobilePage.evaluate(() => window.__injected), undefined);
    await mobile.close();
  });
  for (const kind of ["text", "json", "401"]) {
    await check(`cross-tab logout prevents delayed private ${kind} body from restoring DOM`, async () => {
      await page.locator("#nav-docs").click(); await idle(page);
      const second = await context.newPage();
      second.setDefaultTimeout(8000);
      second.on("pageerror", (error) => pageErrors.push(error.message));
      await second.goto(origin); await second.locator("#shell").waitFor({ state: "visible" }); await idle(second);
      let route = "/api/v1/workspaces/owner/documents";
      if (kind !== "json") {
        await page.locator("#documents .document-title").last().click(); await idle(page);
        route += "/literal/versions/v-literal/content";
      }
      const release = delayNextBody(route, kind === "401" ? 401 : 200);
      try {
        const receivedHeaders = page.waitForResponse((response) => new URL(response.url()).pathname === route);
        if (kind !== "json") await page.getByRole("button", { name: "查看正文", exact: true }).click();
        else await page.locator("#refresh-docs").click();
        assert.equal((await receivedHeaders).status(), kind === "401" ? 401 : 200);
        assert.equal(await page.locator("#shell").getAttribute("aria-busy"), "true");
        await second.locator("#logout").click(); await idle(second);
        await page.locator("#auth-form").waitFor({ state: "visible" });
        const refreshes = requests.filter((entry) => entry.path === "/api/v1/auth/browser/refresh").length;
        // Releasing after the logout exercises body completion, beyond headers.
        release(); await idle(page);
        assert.equal(requests.filter((entry) => entry.path === "/api/v1/auth/browser/refresh").length, refreshes, "late response must not renew a logged-out session");
        assert.equal(await page.locator("#shell").isVisible(), false);
        assert.equal(await page.locator("#auth-form").isVisible(), true);
        assert.equal(await page.locator("#auth-restoring").isVisible(), false);
        assert.equal(await page.locator("#detail").evaluate((element) => element.open), false);
        for (const id of ["documents", "detail-body", "messages", "sessions", "identity", "workspace"])
          assert.equal(await page.locator(`#${id}`).textContent(), "", `delayed ${kind} restored ${id} after logout`);
        if (kind === "401") assert.match(await page.locator("#notice").textContent(), /登录状态已变化/);
      } finally {
        release(); await second.close();
      }
      await login(page);
    });
  }
  await check("401 returns to login and clears private DOM content", async () => {
    await page.locator("#nav-docs").click(); await idle(page);
    await page.locator("#documents .document-title").last().click(); await idle(page);
    rejectAuthentication = true;
    await page.getByRole("button", { name: "查看正文", exact: true }).click();
    await page.locator("#auth").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#shell").isVisible(), false);
    for (const id of ["messages", "prompt", "run-state", "documents", "sessions", "search-results", "detail-body", "identity", "workspace"])
      assert.equal(await page.locator(`#${id}`).textContent(), "", `private content survived in ${id}`);
    assert.equal(await page.locator("#query").inputValue(), "");
    assert.equal(await page.locator("#detail").evaluate((element) => element.open), false);
    assert.equal(await page.locator("#upload-dialog").evaluate((element) => element.open), false);
  });
  await check("logout clears cookie and remains logged out after reload", async () => {
    rejectAuthentication = false;
    await context.clearCookies();
    await login(page);
    await page.locator("#logout").click(); await idle(page);
    await page.locator("#auth-form").waitFor({ state: "visible" });
    assert.equal((await context.cookies(origin + "/api/v1/auth/browser/refresh")).some((item) => item.name === "magworkspace_refresh"), false);
    await page.reload(); await page.locator("#auth-form").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#shell").isVisible(), false);
    assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
  });
  await check("no JavaScript errors, external requests or unhandled fixture routes", async () => {
    assert.deepEqual(pageErrors, []);
    assert.deepEqual(externalRequests, []);
    assert.deepEqual(unexpected, []);
    assert.equal(await page.evaluate(() => window.__injected), undefined);
    for (const [name] of Object.values(assets))
      assert.equal(createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex"), results.source_sha256[name], `${name} changed during browser checks; rerun against stable sources`);
  });
  results.status = "passed";
} catch (error) {
  results.status = "failed";
  results.error = error.stack || error.message;
  process.stderr.write(`${results.error}\n`);
  process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  if (server) await new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); });
  results.finished_at = new Date().toISOString();
  results.request_counts = {
    total: requests.length,
    message_posts: countMessagePosts(),
    unexpected: unexpected.length,
  };
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, "browser-fixture-results.json"), JSON.stringify(results, null, 2) + "\n");
}
