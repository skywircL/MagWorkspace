// Real browser against the disposable acceptance stack. Creates synthetic data;
// uploads, indexing and edits use the real backend. This script makes no model calls.
// Run after smoke-workbench.py to also inspect its model citations and write receipt.
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const acceptance = process.env.MAGWORKSPACE_ACCEPTANCE_DIR;
if (!acceptance) throw new Error("MAGWORKSPACE_ACCEPTANCE_DIR must identify the disposable acceptance stack");
const privateState = JSON.parse(await readFile(path.join(acceptance, "private-state.json"), "utf8"));
assert.match(privateState.project, /^mw-web-[a-f0-9]+$/);
const backend = privateState.base_url;
const origin = process.env.MAGWORKSPACE_BROWSER_BASE_URL || backend;
for (const value of [backend, origin]) {
  const url = new URL(value);
  assert.equal(url.protocol, "http:");
  assert.equal(url.hostname, "127.0.0.1");
  assert.equal(url.pathname, "/");
}
const output = path.join(root, "artifacts/acceptance/018");
const label = process.env.MAGWORKSPACE_BROWSER_LABEL || "";
assert.match(label, /^[a-z0-9-]*$/);
const marker = randomBytes(6).toString("hex");
const filename = `browser-${marker}.md`;
const title = `真实浏览器联调 ${marker}`;
const original = `# 浏览器联调资料\n核验编号：${marker}\n青岚项目住宿上限为每晚 426 元。\n申请须在结束后 17 天内提交。\n`;
const updated = original.replace("426", "581");
const credentials = { name: "真实浏览器验收", email: `browser-${marker}@example.test`, password: randomBytes(24).toString("hex") };
const privateValues = [credentials.email, credentials.password];
const results = {
  fixture: false, scope: "Current frontend with disposable real MySQL, MinIO, Tika, Elasticsearch and Ollama; no new chat-model calls",
  started_at: new Date().toISOString(), backend, browser_origin: origin,
  checks: [], screenshots: [], source_sha256: {},
};
let browser;
let account;
let context;
let page;
let documentID;
let originalVersion;
let currentVersion;
let workspacePath;
const pageErrors = [];
const externalRequests = [];
const messagePosts = [];

function safeError(error, stack = false) {
  let value = (stack && error.stack) || error.message;
  for (const secret of privateValues) if (secret) value = value.split(secret).join("[redacted]");
  return value;
}
async function check(name, action) {
  try {
    await action();
    results.checks.push({ name, status: "passed" });
    process.stdout.write(`PASS ${name}\n`);
  } catch (error) {
    results.checks.push({ name, status: "failed", error: safeError(error) });
    throw error;
  }
}
async function idle(page) {
  await page.waitForFunction(() => document.querySelector("#shell").getAttribute("aria-busy") === "false");
}
async function screenshot(page, name) {
  if (label) name = label + "-" + name;
  await page.screenshot({ path: path.join(output, name), fullPage: true });
  results.screenshots.push(name);
}
async function api(resource, { raw = false, token = account.token } = {}) {
  const response = await context.request.get(origin + resource, { headers: { Authorization: `Bearer ${token}` } });
  assert.equal(response.status(), 200, `GET ${resource}`);
  return raw ? response.text() : (await response.json()).data;
}
async function ready(page) {
  const deadline = Date.now() + 360000;
  while (Date.now() < deadline) {
    await page.locator("#refresh-docs").click(); await idle(page);
    const row = page.locator("#documents article").filter({ has: page.getByRole("button", { name: title, exact: true }) });
    const label = await row.locator("[role=status]").textContent();
    if (label === "可检索") return;
    assert.notEqual(label, "处理失败", "real indexing failed");
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error("real indexing did not become ready within six minutes");
}
async function assertPrivateStorage(page) {
  const storage = await page.evaluate(() => ({ local: { ...localStorage }, session: { ...sessionStorage }, cookies: document.cookie }));
  assert.equal(Object.keys(storage.local).length, 0, "no localStorage authentication data");
  const values = JSON.stringify(storage.session);
  assert.ok(!values.includes(account.token));
  assert.ok(!/refresh_token|access_token|password/i.test(values));
  assert.ok(!storage.cookies.includes("magworkspace_refresh"), "HttpOnly refresh cookie is unreadable to page JS");
  const cookie = (await context.cookies(origin + "/api/v1/auth/browser/refresh")).find((item) => item.name === "magworkspace_refresh");
  assert.ok(cookie, "browser session cookie exists");
  assert.equal(cookie.httpOnly, true);
  assert.equal(cookie.sameSite, "Strict");
  assert.equal(cookie.path, "/api/v1/auth/browser");
}
async function login(page, privateAccount) {
  await page.goto(origin);
  await page.locator("#auth-form").waitFor({ state: "visible" });
  await page.locator('#auth-form input[name="email"]').fill(privateAccount.email);
  await page.locator('#auth-form input[name="password"]').fill(privateAccount.password);
  await page.locator("#auth-submit").click();
  await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
}

try {
  await mkdir(output, { recursive: true });
  for (const name of ["index.html", "app.js", "core.js", "style.css"])
    results.source_sha256[name] = createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex");
  const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || "playwright");
  browser = await chromium.launch({ headless: true,
    ...(process.env.MAGWORKSPACE_BROWSER_CHANNEL ? { channel: process.env.MAGWORKSPACE_BROWSER_CHANNEL } : {}),
  });
  context = await browser.newContext({ viewport: { width: 1440, height: 1024 }, locale: "zh-CN", timezoneId: "Asia/Shanghai" });
  await context.route("**/*", async (route) => {
    if (new URL(route.request().url()).origin !== origin) {
      externalRequests.push(route.request().url()); await route.abort();
    } else await route.continue();
  });
  page = await context.newPage();
  page.setDefaultTimeout(15000);
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("request", (request) => {
    if (request.method() === "POST" && new URL(request.url()).pathname.endsWith("/messages")) messagePosts.push(request.url());
  });
  await check("served assets match the current frontend sources", async () => {
    for (const [name, digest] of Object.entries(results.source_sha256)) {
      const response = await context.request.get(origin + (name === "index.html" ? "/" : "/" + name));
      assert.equal(response.status(), 200);
      assert.equal(createHash("sha256").update(await response.body()).digest("hex"), digest, `${name}: rebuild the acceptance backend or use the current dev proxy`);
    }
  });
  await check("real browser registration creates authenticated personal workspace", async () => {
    await page.goto(origin); await page.locator("#auth-form").waitFor({ state: "visible" });
    await page.locator("#auth-toggle").click(); await idle(page);
    for (const [key, value] of Object.entries(credentials)) await page.locator(`#auth-form input[name="${key}"]`).fill(value);
    const response = page.waitForResponse((item) => new URL(item.url()).pathname === "/api/v1/auth/browser/register");
    await page.locator("#auth-submit").click();
    const registered = await response; assert.equal(registered.status(), 201);
    account = (await registered.json()).data;
    privateValues.push(account.token, account.refresh_token);
    assert.ok(!Object.hasOwn(account, "refresh_token"), "refresh secret is never returned to browser JS");
    workspacePath = "/api/v1/workspaces/" + account.workspaces[0].id;
    await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.match(await page.locator("#identity").textContent(), /真实浏览器验收/);
    await assertPrivateStorage(page);
  });
  await check("real UI upload persists exact bytes and indexes through actual services", async () => {
    await page.locator("#show-upload").click(); await idle(page);
    await page.locator('#upload-form input[name="title"]').fill(title);
    await page.locator('#upload-form input[name="file"]').setInputFiles({ name: filename, mimeType: "text/markdown", buffer: Buffer.from(original) });
    await page.locator('#upload-form button[type="submit"]').click(); await idle(page);
    assert.equal(await page.locator("#upload-dialog").evaluate((element) => element.open), false);
    const documents = await api(workspacePath + "/documents");
    const document = documents.find((item) => item.title === title);
    assert.ok(document); documentID = document.id; originalVersion = document.current_version_id;
    assert.equal(await api(`${workspacePath}/documents/${documentID}/versions/${originalVersion}/content`, { raw: true }), original);
    await ready(page); await screenshot(page, "live-documents-ready.png");
  });
  await check("lexical vector and hybrid searches return real indexed upload and source", async () => {
    for (const mode of ["lexical", "vector", "hybrid"]) {
      await page.getByRole("textbox", { name: "检索知识库" }).fill("青岚项目住宿");
      await page.locator('#search-form select[name="mode"]').selectOption(mode);
      await page.locator('#search-form button[type="submit"]').click(); await idle(page);
      assert.match(await page.locator("#search-results").textContent(), /426/);
      assert.match(await page.locator("#search-results").textContent(), new RegExp(marker));
    }
    await page.getByRole("button", { name: "查看来源文档", exact: true }).first().click(); await idle(page);
    await page.getByRole("button", { name: "查看正文", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#detail-body pre").textContent(), original);
    await screenshot(page, "live-original-preview.png");
  });
  await check("human edit creates new immutable version through real UI", async () => {
    await page.getByRole("textbox", { name: "文档正文", exact: true }).fill(updated);
    await page.getByRole("button", { name: "保存新版本", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#detail-body h2").count(), 2);
    const document = await api(`${workspacePath}/documents/${documentID}`);
    currentVersion = document.current_version_id; assert.notEqual(currentVersion, originalVersion);
    assert.equal(await api(`${workspacePath}/documents/${documentID}/versions/${currentVersion}/content`, { raw: true }), updated);
    const old = page.locator("#detail-body .card").filter({ has: page.getByRole("heading", { name: `版本 1 · ${filename}`, exact: true }) });
    await old.getByRole("button", { name: "查看正文", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#detail-body pre").textContent(), original);
    assert.equal(await page.getByRole("button", { name: "保存新版本", exact: true }).count(), 0);
    assert.equal(await api(`${workspacePath}/documents/${documentID}/versions/${originalVersion}/content`, { raw: true }), original);
    await screenshot(page, "live-immutable-history.png");
    await page.locator("#close-detail").click(); await idle(page);
    await page.locator("#clear-search").click(); await idle(page);
    await ready(page);
    await page.getByRole("textbox", { name: "检索知识库" }).fill("青岚项目住宿");
    await page.locator('#search-form button[type="submit"]').click(); await idle(page);
    assert.match(await page.locator("#search-results").textContent(), /581/);
    assert.ok(!(await page.locator("#search-results").textContent()).includes("426"));
    await page.locator("#clear-search").click(); await idle(page);
  });
  const sessionTitle = "浏览器创建与刷新验收 " + marker;
  await check("real session create rename and refresh preserve authenticated selection", async () => {
    await page.locator("#nav-chat").click(); await idle(page);
    await page.locator("#new-session").click(); await idle(page);
    await page.locator("#detail-body input").fill(sessionTitle);
    await page.locator('#detail-body button[type="submit"]').click(); await idle(page);
    await page.locator("#rename-session").click(); await idle(page);
    await page.locator("#detail-body input").fill(sessionTitle + " 已改名");
    await page.locator('#detail-body button[type="submit"]').click(); await idle(page);
    const workspace = await page.locator("#workspace").inputValue();
    await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#workspace").inputValue(), workspace);
    assert.equal(await page.locator("#session-title").textContent(), sessionTitle + " 已改名");
    assert.equal(await page.locator("#chat-view").isVisible(), true);
    await assertPrivateStorage(page); await screenshot(page, "live-refresh-restored-session.png");
  });
  await check("cookie session restores in a new tab without JavaScript token persistence", async () => {
    const second = await context.newPage();
    await second.goto(origin); await second.locator("#shell").waitFor({ state: "visible" }); await idle(second);
    assert.match(await second.locator("#identity").textContent(), /真实浏览器验收/);
    await assertPrivateStorage(second); await second.close();
  });
  await check("real session deletion and logout clear session and stay logged out on refresh", async () => {
    await page.locator("#delete-session").click(); await idle(page);
    await page.getByRole("button", { name: "确认删除", exact: true }).click(); await idle(page);
    assert.equal(await page.locator("#session-total").textContent(), "0");
    await page.locator("#logout").click(); await idle(page);
    await page.locator("#auth-form").waitFor({ state: "visible" });
    assert.equal((await context.cookies(origin + "/api/v1/auth/browser/refresh")).some((item) => item.name === "magworkspace_refresh"), false);
    await page.reload(); await page.locator("#auth-form").waitFor({ state: "visible" }); await idle(page);
    assert.equal(await page.locator("#shell").isVisible(), false);
    assert.equal(await page.locator("#identity").textContent(), "");
    assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
  });
  const privateBrowser = path.join(acceptance, "private-browser.json");
  let liveAccount;
  try { liveAccount = JSON.parse(await readFile(privateBrowser, "utf8")); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  if (liveAccount) {
    privateValues.push(liveAccount.email, liveAccount.password);
    await check("existing real model answer citations and committed approval receipt render in UI", async () => {
      await login(page, liveAccount);
      assert.equal(await page.locator("#workspace").inputValue(), liveAccount.workspace_id, "model-fixture login selects its actual workspace");
      assert.ok((await page.locator("#identity").textContent()).includes(liveAccount.email), "model-fixture user is authenticated");
      await page.locator("#nav-chat").click(); await idle(page);
      assert.ok(await page.locator("#sessions button").count() > 0, "model-fixture session list has loaded");
      assert.equal(await page.locator("#chat-view").isVisible(), true, "model-fixture chat navigation is visible");
      assert.ok((await page.locator("#sessions button").allTextContents()).includes("核对住宿标准"), "model-fixture expected session title is present");
      await page.locator("#sessions").getByRole("button", { name: /核对住宿标准$/ }).click(); await idle(page);
      assert.match(await page.locator("#messages .assistant").textContent(), /417/);
      await page.getByRole("button", { name: "查看引用", exact: true }).click(); await idle(page);
      assert.match(await page.locator("#detail-title").textContent(), /固定版本/);
      assert.match(await page.locator("#detail-body").textContent(), /417/);
      await page.getByRole("button", { name: "读取片段", exact: true }).first().click(); await idle(page);
      await screenshot(page, "live-model-fixed-citation.png");
      await page.locator("#close-detail").click(); await idle(page);
      await page.locator("#sessions").getByRole("button", { name: /审批更新差旅制度$/ }).click(); await idle(page);
      assert.match(await page.locator("#run-state").textContent(), /文档写入已提交/);
      assert.match(await page.locator("#run-state").textContent(), /版本 2/);
      await screenshot(page, "live-model-approval-receipt.png");
      await page.reload(); await page.locator("#shell").waitFor({ state: "visible" }); await idle(page);
      assert.equal(await page.locator("#session-title").textContent(), "审批更新差旅制度");
      assert.match(await page.locator("#run-state").textContent(), /文档写入已提交/);
    });
  } else results.checks.push({ name: "existing real model answer citations and committed approval receipt render in UI", status: "not_run", reason: "smoke-workbench private fixture is not available" });
  await check("no browser errors external requests new model POSTs or source changes", async () => {
    assert.deepEqual(pageErrors, []); assert.deepEqual(externalRequests, []); assert.deepEqual(messagePosts, []);
    for (const [name, digest] of Object.entries(results.source_sha256))
      assert.equal(createHash("sha256").update(await readFile(path.join(root, "internal/webui/assets", name))).digest("hex"), digest, `${name} changed during browser checks`);
  });
  results.status = "passed";
} catch (error) {
  results.status = "failed"; results.error = safeError(error, true);
  if (page) await screenshot(page, "live-failure.png").catch(() => {});
  process.stderr.write(results.error + "\n"); process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  results.finished_at = new Date().toISOString();
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, `browser-live${label ? "-" + label : ""}-results.json`), JSON.stringify(results, null, 2) + "\n");
}
