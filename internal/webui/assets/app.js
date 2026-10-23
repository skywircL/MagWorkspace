import { consumeEvents, changedLines, validateUpload, indexPresentation, renderAnswer } from "./core.js";
const $ = (id) => document.getElementById(id);
const state = {
  auth: null,
  memoryEnabled: null,
  userID: "",
  spaces: [],
  wid: "",
  knowledgeBases: [],
  kid: "",
  sid: "",
  rid: "",
  docPage: 1,
  docTotal: 0,
  sessionPage: 1,
  messagePage: 1,
  busy: false,
  controller: null,
  promptID: "",
  register: false,
  runStatus: "",
};
let authEpoch = 0;
let searchRevision = 0;
let documentsRevision = 0;
// Draft preferences only. The backend owns the immutable scope of an accepted Run.
const chatScopes = new Map();
let chatScopeDraft = null;
let chatMention = null;
let queryComposing = false;
const chatDocuments = new Map();
const authChannel = typeof BroadcastChannel === "function" ? new BroadcastChannel("magworkspace-auth") : null;
let refreshing = null,
  noticeTimer;
const enc = encodeURIComponent;
const wp = () => "/api/v1/workspaces/" + enc(state.wid);
const kp = () => wp() + "/knowledge-bases/" + enc(state.kid);
const sp = () => wp() + "/sessions/" + enc(state.sid);
const rp = () => sp() + "/runs/" + enc(state.rid);
function node(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== undefined) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
function note(text) {
  $("notice").textContent = text;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => ($("notice").textContent = ""), 9000);
}
function button(text, fn, cls) {
  const b = node("button", text, cls);
  b.type = "button";
  b.addEventListener("click", () => act(fn));
  return b;
}
function busy(on) {
  state.busy = on;
  document.querySelectorAll("button,input,select,textarea").forEach((e) => {
    if (on) {
      e.dataset.wasDisabled = String(e.disabled);
      e.disabled = true;
    } else if (e.dataset.wasDisabled !== undefined) {
      e.disabled = e.dataset.wasDisabled === "true";
      delete e.dataset.wasDisabled;
    }
  });
  $("activity").hidden = !on;
  $("shell").setAttribute("aria-busy", String(on));
  if (!on) syncAvailability();
  $("jump-latest").disabled = false;
  $("stop").disabled = false;
  $("stop").hidden = !state.controller;
}
async function act(fn) {
  if (state.busy) return;
  busy(true);
  try {
    await fn();
  } catch (e) {
    note(e.message || "操作失败，请刷新状态后检查结果");
  } finally {
    busy(false);
    rememberNavigation();
  }
}
function bind(id, fn) {
  $(id).addEventListener("click", () => act(fn));
}
function form(id, fn) {
  $(id).addEventListener("submit", (e) => {
    e.preventDefault();
    const data = new FormData(e.target);
    act(() => fn(data));
  });
}
async function errorResponse(r) {
  let code = "http_" + r.status;
  try {
    code = (await r.json()).error?.code || code;
  } catch {}
  const labels = {
    unauthorized: "登录已过期，请重新登录",
    conflict: "状态已改变或有运行尚未结束，请刷新后核对；未自动重试",
    forbidden: "当前角色没有此操作权限",
    not_found: "资源不存在或你已无权访问",
    agent_unavailable: "Agent 尚未配置",
    registration_disabled: "注册已关闭",
    invalid_request: "输入格式不正确或超过限制",
    rate_limited: "请求过于频繁，请稍后再试",
  };
  return new Error(labels[code] || "操作未完成：" + code);
}
function guardResponse(response, epoch) {
  // Headers can arrive before logout while a private response body is still
  // downloading. Check again after every asynchronous body read.
  for (const method of ["json", "text", "blob", "arrayBuffer"]) {
    const read = response[method].bind(response);
    response[method] = async (...args) => {
      const value = await read(...args);
      if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
      return value;
    };
  }
  return response;
}
async function browserAuth(action, data = {}) {
  const epoch = authEpoch;
  const response = await fetch("/api/v1/auth/browser/" + action, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", "X-Magworkspace-Browser": "1" },
    body: JSON.stringify(data),
  });
  const r = guardResponse(response, epoch);
  if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
  if (!r.ok) {
    const error = await errorResponse(r);
    if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
    error.status = r.status;
    throw error;
  }
  return r.status === 204 ? null : (await r.json()).data;
}
async function authLock(fn) {
  return navigator.locks ? navigator.locks.request("magworkspace-auth", fn) : fn();
}
function announceCredentials() {
  if (state.auth && state.userID) authChannel?.postMessage({
    kind: "credentials", userID: state.userID,
    token: state.auth.token, expires_at: state.auth.expires_at,
  });
}
async function renew(force = false) {
  if (!force && (!state.auth || Date.parse(state.auth.expires_at) > Date.now() + 30000)) return;
  if (!refreshing) {
    const epoch = authEpoch;
    refreshing = authLock(async () => {
      try {
        const credentials = await browserAuth("refresh");
        if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
        state.auth = credentials;
        announceCredentials();
      } catch (error) {
        if (error.status === 401) resetAuth();
        throw error;
      }
    }).finally(() => { refreshing = null; });
  }
  await refreshing;
}
async function request(
  path,
  { method = "GET", data, raw = false, signal, stream = false, retried = false } = {},
) {
  await renew();
  const epoch = authEpoch;
  const headers = {};
  if (state.auth) headers.Authorization = "Bearer " + state.auth.token;
  if (data !== undefined && !(data instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    data = JSON.stringify(data);
  }
  if (stream) headers.Accept = "text/event-stream";
  const r = guardResponse(await fetch(path, { method, headers, body: data, signal, credentials: "same-origin" }), epoch);
  if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
  if (stream && r.headers.get("X-Run-ID"))
    state.rid = r.headers.get("X-Run-ID");
  if (!r.ok) {
    const error = await errorResponse(r);
    if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
    error.status = r.status;
    if (r.status === 401 && state.auth && !retried) {
      await renew(true);
      // Only read requests are safe to repeat; never replay a business mutation.
      if (method === "GET") return request(path, { method, raw, signal, stream, retried: true });
      throw Error("登录状态已更新，请核对结果后再次操作；未自动重试");
    }
    if (r.status === 401) resetAuth();
    throw error;
  }
  if (raw || stream) return r;
  if (r.status === 204) return null;
  return r.json();
}
async function api(path, method = "GET", data) {
  return (await request(path, { method, data }))?.data;
}
function empty(target, text) {
  target.replaceChildren(node("p", text, "empty"));
}
function pager(id, result, onPage) {
  const e = $(id);
  e.replaceChildren();
  const max = Math.max(1, Math.ceil(result.total / result.page_size));
  if (max === 1) return;
  const prev = button("上一页", () => onPage(result.page - 1));
  prev.disabled = result.page <= 1;
  const next = button("下一页", () => onPage(result.page + 1));
  next.disabled = result.page >= max;
  e.append(
    prev,
    node("small", `${result.page} / ${max} · ${result.total} 条`),
    next,
  );
}
function syncAvailability() {
  const hasSpace = Boolean(state.wid);
  const hasKnowledgeBase = hasSpace && Boolean(state.kid);
  $("knowledge-base").disabled = !hasSpace || !state.knowledgeBases.length;
  $("new-knowledge-base").hidden = !manageKnowledgeBases();
  $("new-knowledge-base").disabled = !hasSpace;
  for (const id of ["edit-knowledge-base", "delete-knowledge-base"]) {
    $(id).hidden = !manageKnowledgeBases();
    $(id).disabled = !hasKnowledgeBase;
  }
  for (const id of ["memory-refresh", "memory-export", "memory-clear", "memory-status"])
    $(id).disabled = !hasSpace;
  $("memory-toggle").disabled = !hasSpace || state.memoryEnabled === null;
  $("show-upload").disabled = !hasKnowledgeBase || !writable();
  $("upload-form").querySelectorAll("input,button").forEach((e) => {
    e.disabled = !hasKnowledgeBase || !writable();
  });
  $("new-session").disabled = !hasSpace;
  $("refresh-docs").disabled = !hasSpace;
  $("search-form").querySelectorAll("input,select,button").forEach((e) => {
    e.disabled = !hasKnowledgeBase;
  });
  const hasSession = hasSpace && Boolean(state.sid);
  $("rename-session").disabled = !hasSession;
  $("delete-session").disabled = !hasSession;
  $("refresh-chat").disabled = !hasSession;
  $("send").disabled = !hasSession || ["running", "suspended"].includes(state.runStatus) || missingChatResources().length > 0;
  $("query").disabled = !hasSession;
  $("composer-hint").textContent = state.runStatus === "suspended"
    ? "请先答复或处理上方审批，再继续提问"
    : state.runStatus === "running" ? "Agent 正在处理，完成后可继续提问" : "Ctrl / ⌘ + Enter 发送 · Enter 换行";
  $("query").placeholder = hasSession
    ? "输入 @ 选择知识库或文档，再提出你的问题…"
    : "先新建或选择一个会话，开始与知识对话";
  renderChatScope();
  if ($("chat-scope-dialog").open) renderChatScopeSummary();
}
function showView(view) {
  $("shell").classList.toggle("chat-mode", view === "chat");
  for (const name of ["docs", "chat", "memory"]) {
    $(name + "-view").hidden = view !== name;
    $("nav-" + name).classList.toggle("active", view === name);
    $("nav-" + name).setAttribute("aria-current", view === name ? "page" : "false");
  }
  $("page-title").textContent = { docs: "文档知识库", chat: "Agent 会话", memory: "个人记忆" }[view];
  $("page-description").textContent = {
    docs: "让资料井然有序，让每个答案有据可循。",
    chat: "输入 @ 选择知识库或文档，查找资料、整理思路，或继续深入追问。",
    memory: "管理你的资料、偏好与长期事项，让后续对话更贴合你。",
  }[view];
  history.replaceState(null, "", "#" + view);
}

function showDetail(title) {
  $("detail").classList.remove("source-drawer");
  $("detail-title").textContent = title;
  $("detail-body").replaceChildren();
  if (!$("detail").open) $("detail").showModal();
  return $("detail-body");
}
function editDialog(title, initial, save) {
  const body = showDetail(title),
    f = node("form"),
    label = node("label", title),
    input = node("input");
  input.value = initial;
  input.required = true;
  input.setAttribute("aria-label", title);
  label.append(input);
  const submit = node("button", "保存", "primary");
  submit.type = "submit";
  f.append(label, submit);
  body.append(f);
  f.addEventListener("submit", (event) => {
    event.preventDefault();
    const value = input.value.trim();
    if (value)
      act(async () => {
        await save(value);
      });
  });
  input.focus();
}
function confirmDialog(title, message, confirm) {
  const body = showDetail(title);
  body.append(node("p", message), button("确认删除", confirm, "danger"));
}
function writable() {
  return ["owner", "admin", "contributor"].includes(
    state.spaces.find((x) => x.id === state.wid)?.role,
  );
}
function manageKnowledgeBases() {
  return ["owner", "admin"].includes(state.spaces.find((w) => w.id === state.wid)?.role);
}
function roleLabel() {
  return { owner: "所有者", admin: "管理员", contributor: "协作者", viewer: "只读成员" }[
    state.spaces.find((w) => w.id === state.wid)?.role
  ] || "暂无空间";
}
function clearSearch() {
  searchRevision++;
  $("search-results").replaceChildren();
  $("search-summary").textContent = "";
  $("clear-search").hidden = true;
  $("documents").hidden = false;
  $("doc-pages").hidden = false;
  $("doc-columns").hidden = false;
  $("doc-list-heading").textContent = "全部文档";
  $("doc-total").textContent = state.docTotal;
  $("search-form").reset();
}
function resetAuth() {
  authEpoch++;
  searchRevision++;
  documentsRevision++;
  state.controller?.abort();
  state.auth = null;
  state.userID = "";
  try { sessionStorage.removeItem("magworkspace.navigation"); } catch {}
  $("auth-restoring").hidden = true;
  state.spaces = [];
  state.knowledgeBases = [];
  chatScopes.clear();
  chatDocuments.clear();
  closeMentionPicker();
  chatScopeDraft = null;
  $("chat-scope-dialog").close();
  $("chat-scope-options").replaceChildren();
  $("chat-scope-chips").replaceChildren();
  state.kid = "";
  state.memoryEnabled = null;
  state.wid = state.sid = state.rid = state.runStatus = "";
  $("shell").hidden = true;
  $("auth").hidden = false;
  for (const id of ["messages", "prompt", "run-state", "draft", "documents", "sessions", "search-results", "detail-body", "identity", "workspace", "memory-list", "knowledge-base", "knowledge-base-description", "upload-knowledge-base"])
    $(id).replaceChildren();
  $("query").value = "";
  $("search-form").reset();
  $("upload-form").reset();
  $("detail").close();
  $("upload-dialog").close();
}
async function uploadLimits() {
  const limits = await api("/api/v1/config");
  validateUpload({ name: "check", size: 0 }, limits);
  $("upload-limit").textContent =
    `支持 TXT、Markdown、PDF、DOCX · 单文件最大 ${limits.max_file_size_mb} MB`;
  return limits;
}
async function loadSpaces() {
  await uploadLimits();
  const me = await api("/api/v1/auth/me");
  state.userID = me.user.id;
  state.spaces = me.workspaces || [];
  $("identity").textContent = me.user.name + " · " + me.user.email;
  const select = $("workspace");
  select.replaceChildren();
  for (const w of state.spaces) {
    const o = node("option", w.name);
    o.value = w.id;
    select.append(o);
  }
  if (!state.spaces.some((w) => w.id === state.wid))
    state.wid = state.spaces[0]?.id || "";
  select.value = state.wid;
  $("role").textContent = roleLabel();
  $("workspace-name").textContent = state.spaces.find((w) => w.id === state.wid)?.name || "工作空间";
}
async function switchSpace(preferredKnowledgeBase = "") {
  chatScopes.clear();
  chatDocuments.clear();
  closeMentionPicker();
  $("chat-scope-dialog").close();
  state.knowledgeBases = [];
  state.kid = "";
  renderKnowledgeBases();
  resetDocumentView();
  state.memoryEnabled = null;
  $("memory-list").replaceChildren();
  $("memory-count").textContent = "0";
  $("memory-status").value = "active";
  $("memory-toggle").textContent = "读取设置中…";
  $("memory-setting-note").textContent = "";
  state.sid = "";
  state.rid = "";
  state.docPage = state.sessionPage = state.messagePage = 1;
  state.promptID = "";
  state.runStatus = "";
  state.docTotal = 0;
  $("query").value = "";
  $("chat-welcome").hidden = false;
  $("doc-total").textContent = "0";
  $("session-total").textContent = "0";
  $("messages").replaceChildren();
  $("prompt").replaceChildren();
  $("run-state").replaceChildren();
  $("session-title").textContent = "选择或创建会话";
  clearSearch();
  if (state.wid) {
    await loadKnowledgeBases(preferredKnowledgeBase);
    await loadDocs();
    await loadSessions();
    if (!$("memory-view").hidden) await loadMemories();
  } else {
    empty($("documents"), "创建一个空间，开始收藏资料。");
    $("sessions").replaceChildren();
  }
  $("role").textContent = roleLabel();
  $("workspace-name").textContent = state.spaces.find((w) => w.id === state.wid)?.name || "工作空间";
}
function rememberNavigation() {
  if (!state.auth || !state.userID) return;
  try {
    sessionStorage.setItem("magworkspace.navigation", JSON.stringify({
      userID: state.userID, wid: state.wid, kid: state.kid, sid: state.sid,
      chatScopes: Object.fromEntries(chatScopes),
      view: !$("memory-view").hidden ? "memory" : $("chat-view").hidden ? "docs" : "chat",
    }));
  } catch { /* Browser storage restrictions do not prevent using the workspace. */ }
}
function savedNavigation() {
  try {
    const saved = JSON.parse(sessionStorage.getItem("magworkspace.navigation"));
    if (saved?.userID === state.userID && typeof saved.wid === "string" && typeof saved.sid === "string"
        && saved.wid.length < 100 && saved.sid.length < 100) return saved;
  } catch {}
  return null;
}
async function enterWorkspace() {
  await loadSpaces();
  const saved = savedNavigation();
  if (saved && state.spaces.some((w) => w.id === saved.wid)) {
    state.wid = saved.wid;
    $("workspace").value = state.wid;
  }
  $("auth-restoring").hidden = true;
  $("auth").hidden = true;
  $("shell").hidden = false;
  const view = saved?.view || location.hash.slice(1);
  showView(["chat", "memory"].includes(view) ? view : "docs");
  await switchSpace(typeof saved?.kid === "string" ? saved.kid : "");
  if (saved?.wid === state.wid && saved.chatScopes && typeof saved.chatScopes === "object" && !Array.isArray(saved.chatScopes)) {
    for (const [sid, scope] of Object.entries(saved.chatScopes)) {
      if (!sid || sid.length >= 100) continue;
      // Invalid or obsolete draft metadata must not widen the resource range.
      chatScopes.set(sid, savedChatScope(scope));
    }
  }
  if (saved?.sid && saved.wid === state.wid) {
    try {
      const session = await api(wp() + "/sessions/" + enc(saved.sid));
      state.sid = session.id;
      await refreshChatDocuments();
      $("session-title").textContent = session.title;
      await loadMessages(true);
      await loadSessions();
    } catch (error) {
      if (!state.auth) throw error;
      state.sid = state.rid = state.runStatus = "";
      note("原会话暂不可访问，可从会话列表重新选择。");
    }
  }
  announceCredentials();
}
async function restoreLogin() {
  const epoch = authEpoch;
  $("auth-restoring").hidden = false;
  $("auth").hidden = true;
  $("shell").hidden = true;
  $("restore-message").textContent = "正在恢复登录状态…";
  $("retry-auth").hidden = true;
  $("use-login").hidden = true;
  try {
    await renew(true);
    await enterWorkspace();
  } catch (error) {
    if (epoch !== authEpoch) return;
    if (error.status === 401) { resetAuth(); return; }
    $("auth").hidden = true;
    $("shell").hidden = true;
    $("auth-restoring").hidden = false;
    $("restore-message").textContent = "暂时无法恢复登录，请检查后端连接后重试。";
    $("retry-auth").hidden = false;
    $("use-login").hidden = false;
  }
}
form("auth-form", async (f) => {
  const data = { email: f.get("email"), password: f.get("password") };
  if (state.register) data.name = f.get("name");
  await authLock(async () => {
    state.auth = await browserAuth(state.register ? "register" : "login", data);
  });
  $("auth-form").reset();
  await enterWorkspace();
});
bind("auth-toggle", () => {
  state.register = !state.register;
  $("name-label").hidden = !state.register;
  $("auth-form").elements.name.required = state.register;
  $("auth-form").elements.password.autocomplete = state.register ? "new-password" : "current-password";
  $("auth-submit").textContent = state.register ? "创建账号" : "登录工作台";
  $("auth-toggle").textContent = state.register
    ? "已有账号？登录"
    : "还没有账号？注册";
});
bind("logout", async () => {
  await authLock(() => browserAuth("logout"));
  authChannel?.postMessage({ kind: "logout" });
  resetAuth();
});
$("workspace").addEventListener("change", () =>
  act(async () => {
    state.wid = $("workspace").value;
    await loadSpaces();
    await switchSpace();
  }),
);
bind("new-workspace", () =>
  editDialog("新空间名称", "", async (name) => {
    const w = await api("/api/v1/workspaces", "POST", { name });
    state.wid = w.id;
    await loadSpaces();
    await switchSpace();
    $("detail").close();
  }),
);
bind("nav-docs", async () => {
  showView("docs");
  await loadKnowledgeBases();
  await loadDocs();
});
bind("nav-chat", async () => {
  showView("chat");
  await loadSessions();
  if (state.sid) await loadMessages();
});
bind("nav-memory", async () => { showView("memory"); await loadMemories(); });
bind("memory-refresh", loadMemories);
$("memory-status").addEventListener("change", () => act(loadMemories));
bind("memory-toggle", async () => {
  await api(wp() + "/memory-settings", "PUT", { enabled: !state.memoryEnabled });
  await loadMemories();
});
bind("memory-export", () => download(wp() + "/memories/export", "personal-memories.json"));
bind("memory-clear", async () => {
  if (!confirm("忘记当前空间中你的全部记忆？内容将不再用于召回，遗忘记录会保留，且不能重新添加相同内容。历史对话不会删除。")) return;
  await api(wp() + "/memories", "DELETE", { confirm: true });
  await loadMemories();
  note("已忘记当前空间中的全部个人记忆。");
});

const memoryKinds = { profile: "个人资料", preference: "表达偏好", fact: "稳定事实", task: "长期事项" };
const memoryOrigins = { manual: "手动纠正或历史保存", explicit: "聊天明确保存", automatic: "对话自动提取" };
async function loadMemories() {
  if (!state.wid) return;
  const base = wp(), status = $("memory-status").value;
  state.memoryEnabled = null;
  $("memory-list").replaceChildren();
  $("memory-count").textContent = "0";
  const [settings, items] = await Promise.all([
    api(base + "/memory-settings"), api(base + "/memories?status=" + enc(status)),
  ]);
  if (base !== wp()) return;
  state.memoryEnabled = settings.enabled;
  $("memory-toggle").textContent = settings.enabled ? "记忆已开启 · 点击关闭" : "记忆已关闭 · 点击开启";
  $("memory-toggle").setAttribute("aria-pressed", String(settings.enabled));
  $("memory-setting-note").textContent = settings.enabled ? "已开启对话采集和召回；后台保存可能稍有延迟" : "已停止采集和召回；仍可纠正、遗忘与导出";
  $("memory-count").textContent = String(items.length);
  if (!items.length) { empty($("memory-list"), "暂无此类记忆。开启后，完成对话会触发后台提取；也可以在聊天中说“请记住：…”。"); return; }
  for (const item of items) {
    const row = node("article", undefined, "memory-row"), body = node("div", undefined, "memory-copy");
    const meta = node("div", undefined, "memory-meta");
    meta.append(node("small", memoryKinds[item.kind] || item.kind), node("span", memoryOrigins[item.origin] || "历史保存", "memory-origin"));
    body.append(meta, node("p", item.content, "memory-content"));
    body.append(node("small", [item.topic && "主题：" + item.topic, "重要度 " + item.importance, item.expires_at && "有效至 " + new Date(item.expires_at * 1000).toLocaleString(), "使用 " + item.use_count + " 次"].filter(Boolean).join(" · ")));
    const actions = node("div", undefined, "memory-actions");
    if (item.status === "active") actions.append(button("纠正", () => memoryEditor(item), "small quiet"));
    if (item.status !== "archived") actions.append(button("遗忘", async () => {
      if (!confirm("遗忘这条记忆及相同内容的副本？相同内容将不能重新添加。")) return;
      await api(base + "/memories/" + enc(item.id), "DELETE"); await loadMemories();
    }, "small quiet danger"));
    row.append(body, actions); $("memory-list").append(row);
  }
}
function memoryEditor(item) {
  if (!item || item.status !== "active") return;
  const base = wp(), body = showDetail("纠正个人记忆"), formNode = node("form");
  const fields = {};
  const field = (name, title, input) => {
    const label = node("label", title); input.name = name; fields[name] = input; label.append(input); formNode.append(label); return input;
  };
  const kind = node("select");
  for (const [value, text] of Object.entries(memoryKinds)) { const option = node("option", text); option.value = value; kind.append(option); }
  kind.value = item.kind; field("kind", "类别", kind);
  const content = field("content", "记忆内容（最多 500 字）", node("textarea")); content.rows = 4; content.required = true; content.maxLength = 500; content.value = item.content;
  const topic = field("topic", "主题（可选，同主题新内容替代旧记忆）", node("input")); topic.maxLength = 500; topic.value = item.topic || "";
  const importance = field("importance", "重要度（1–5）", node("input")); importance.type = "number"; importance.min = "1"; importance.max = "5"; importance.required = true; importance.value = String(item.importance);
  const expiry = field("expiry", "到期时间（可选，使用本地时间）", node("input")); expiry.type = "datetime-local";
  if (item.expires_at) { const d = new Date(item.expires_at * 1000); expiry.value = new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16); }
  formNode.append(node("p", "纠正不准确或已变化的内容，修改会保留替代历史。记忆不会授予文档访问权限。", "muted"));
  const save = node("button", "保存纠正", "primary"); save.type = "submit"; formNode.append(save);
  formNode.addEventListener("submit", (event) => {
    event.preventDefault();
    act(async () => {
      if (base !== wp()) throw Error("工作空间已变化，请重新打开记忆。");
      const expires = expiry.value ? new Date(expiry.value).getTime() / 1000 : null;
      if (expires !== null && (!Number.isFinite(expires) || expires <= Date.now() / 1000)) throw Error("到期时间必须晚于现在。");
      await api(base + "/memories/" + enc(item.id), "PUT", {
        kind: kind.value, content: content.value, topic: topic.value,
        importance: Number(importance.value), expires_at: expires,
        ...(item.source_session_id ? { source_session_id: item.source_session_id } : {}),
      });
      $("detail").close(); await loadMemories(); note("个人记忆已保存。");
    });
  });
  body.append(formNode);
}
bind("close-detail", () => $("detail").close());
function renderKnowledgeBases() {
  const select = $("knowledge-base");
  select.replaceChildren();
  if (!state.knowledgeBases.length) {
    const option = node("option", "暂无知识库");
    option.value = "";
    select.append(option);
  }
  for (const knowledgeBase of state.knowledgeBases) {
    const option = node("option", knowledgeBase.name + (knowledgeBase.is_default ? " · 默认" : ""));
    option.value = knowledgeBase.id;
    select.append(option);
  }
  select.value = state.kid;
  const current = state.knowledgeBases.find((item) => item.id === state.kid);
  $("knowledge-base-description").textContent = current?.description || (current
    ? "此页的文档、上传和检索仅限当前知识库。"
    : manageKnowledgeBases() ? "创建一个知识库，开始分类整理资料。" : "当前空间暂无知识库，请联系管理员创建。");
  $("upload-knowledge-base").textContent = current ? "上传至：" + current.name : "请先选择知识库";
  renderChatScope();
}
function emptyChatScope() {
  return { knowledge_base_ids: [], document_ids: [] };
}
function copyChatScope(scope) {
  return scope === null ? null : { knowledge_base_ids: [...scope.knowledge_base_ids], document_ids: [...scope.document_ids] };
}
function savedChatScope(value) {
  if (value === null) return null;
  const validIDs = (ids, limit) => Array.isArray(ids) && ids.length <= limit && ids.every((id) => typeof id === "string" && /^[a-f0-9]{32}$/.test(id));
  if (!value || !validIDs(value.knowledge_base_ids, 20) || !validIDs(value.document_ids, 100)) return emptyChatScope();
  return { knowledge_base_ids: [...new Set(value.knowledge_base_ids)], document_ids: [...new Set(value.document_ids)] };
}
function currentChatScope() {
  return chatScopes.get(state.sid) ?? null;
}
function normalizeChatScope(scope) {
  if (scope === null) return null;
  return { knowledge_base_ids: [...new Set(scope.knowledge_base_ids)], document_ids: [...new Set(scope.document_ids)]
    .filter((id) => !scope.knowledge_base_ids.includes(chatDocuments.get(id)?.knowledge_base_id)) };
}
function missingChatResources(scope = currentChatScope()) {
  if (scope === null) return [];
  return [...scope.knowledge_base_ids.filter((id) => !state.knowledgeBases.some((kb) => kb.id === id)),
    ...scope.document_ids.filter((id) => chatDocuments.has(id) && !chatDocuments.get(id))];
}
async function refreshChatDocuments(scope = currentChatScope()) {
  const workspace = state.wid, epoch = authEpoch;
  for (const id of scope?.document_ids || []) {
    let doc;
    try { doc = await api(wp() + "/documents/" + enc(id)); }
    catch (error) { if (![403, 404].includes(error.status)) throw error; doc = null; }
    if (workspace !== state.wid || epoch !== authEpoch) return;
    chatDocuments.set(id, doc);
  }
  renderChatScope();
}
function setChatScope(scope) {
  chatScopes.set(state.sid, normalizeChatScope(scope));
  renderChatScope();
  rememberNavigation();
}
function chatScopeLocked() {
  return state.busy || !state.sid || ["running", "suspended"].includes(state.runStatus);
}
function renderChatScope() {
  const scope = currentChatScope(), missing = missingChatResources(scope);
  const locked = chatScopeLocked(), count = (scope?.knowledge_base_ids.length || 0) + (scope?.document_ids.length || 0);
  const trigger = $("chat-knowledge-scope");
  trigger.textContent = scope === null ? "@ 全部知识库" : count ? `@ 已选 ${count} 项资料` : "@ 不使用知识库";
  trigger.disabled = locked;
  const chips = $("chat-scope-chips");
  chips.replaceChildren();
  for (const kind of ["knowledge_base", "document"]) for (const id of scope?.[kind + "_ids"] || []) {
    const item = kind === "knowledge_base" ? state.knowledgeBases.find((kb) => kb.id === id) : chatDocuments.get(id);
    const missing = kind === "knowledge_base" ? !item : chatDocuments.has(id) && !item;
    const name = item?.name || item?.title || (kind === "knowledge_base" ? "知识库已不可用" : missing ? "文档已不可用" : "文档待核对");
    const chip = button((kind === "knowledge_base" ? "▤ " : "▧ ") + name + " ×", () => {
      const next = copyChatScope(currentChatScope());
      next[kind + "_ids"] = next[kind + "_ids"].filter((value) => value !== id);
      setChatScope(next);
    }, "chat-scope-chip" + (missing ? " unavailable" : ""));
    chip.dataset.kind = kind; chip.dataset.id = id;
    chip.title = name;
    chip.setAttribute("aria-label", "移除" + (kind === "knowledge_base" ? "知识库：" : "文档：") + name);
    chip.disabled = locked;
    chips.append(chip);
  }
  $("chat-scope-hint").textContent = ["running", "suspended"].includes(state.runStatus)
    ? "当前运行及其确认沿用发送时的资料范围；结束后可调整下次提问。"
    : missing.length ? "已选资料已删除或不可访问，请移除或重新选择后发送。"
    : scope === null ? "输入 @ 选择知识库或文档；未选择时，可使用当前空间的全部知识库。"
    : count ? "使用所选的整个知识库及单独文档；同库文档已包含在全库范围内。"
    : "下次提问不使用知识库中的文档；仍可使用对话与个人记忆。";
  $("chat-scope-hint").classList.toggle("scope-error", missing.length > 0);
  if (locked) closeMentionPicker();
}
function renderChatScopeSummary() {
  if (!chatScopeDraft) return;
  const { mode, scope } = chatScopeDraft, missing = missingChatResources(scope);
  const count = scope.knowledge_base_ids.length + scope.document_ids.length;
  $("chat-scope-summary").textContent = mode === "all" ? "可使用当前空间的全部知识库，包括之后新增的知识库。"
    : mode === "none" ? "不读取、检索或修改知识库文档。对话和个人记忆仍可使用。"
    : missing.length ? `有 ${missing.length} 项已选资料不可用，请取消选择。`
    : `已选 ${scope.knowledge_base_ids.length} / 20 个知识库、${scope.document_ids.length} / 100 份文档` + (count ? " · 全库包含同库文档" : " · 请至少选择一项");
  $("chat-scope-apply").disabled = mode === "selected" && (!count || missing.length > 0);
}
function pickerIsCurrent(picker) {
  return (picker === chatScopeDraft || picker === chatMention) && picker.workspace === state.wid && picker.session === state.sid;
}
function pickerPrefix(picker) { return picker === chatScopeDraft ? "chat-scope" : "chat-mention"; }
function pickerScope(picker) { return picker === chatScopeDraft ? picker.scope : currentChatScope() || emptyChatScope(); }
function resourceOptions(picker) {
  if (picker.tab === "document") return [...(picker.docs || []), ...pickerScope(picker).document_ids
    .filter((id) => chatDocuments.has(id) && !chatDocuments.get(id) && !(picker.docs || []).some((doc) => doc.id === id))
    .map((id) => ({ id, title: "文档已不可用", missing: true }))];
  const query = picker.query.toLocaleLowerCase();
  const scope = pickerScope(picker);
  return [...state.knowledgeBases, ...scope.knowledge_base_ids.filter((id) => !state.knowledgeBases.some((kb) => kb.id === id))
    .map((id) => ({ id, name: "知识库已不可用", missing: true }))]
    .filter((kb) => kb.missing || !query || (kb.name + " " + (kb.description || "")).toLocaleLowerCase().includes(query));
}
function chooseResource(picker, item, checked = true) {
  if (chatScopeLocked() || !pickerIsCurrent(picker)) return;
  const kind = picker.tab, key = kind + "_ids", scope = copyChatScope(pickerScope(picker));
  if (kind === "document" && scope.knowledge_base_ids.includes(item.knowledge_base_id)) return;
  if (checked && !scope[key].includes(item.id) && scope[key].length >= (kind === "document" ? 100 : 20)) { note("最多选择 20 个知识库及 100 份单独文档。"); return; }
  scope[key] = checked ? [...new Set([...scope[key], item.id])] : scope[key].filter((id) => id !== item.id);
  if (kind === "document" && !item.missing) chatDocuments.set(item.id, item);
  if (picker === chatScopeDraft) {
    picker.scope = normalizeChatScope(scope);
    renderChatScopeOptions();
    $("chat-scope-options").querySelector(`input[value="${item.id}"]`)?.focus();
  } else {
    setChatScope(scope);
    const input = $("query");
    input.setRangeText("", picker.start, picker.end, "end");
    closeMentionPicker();
    input.focus();
    syncAvailability();
  }
}
function renderResourcePicker(picker) {
  if (!pickerIsCurrent(picker)) return;
  const prefix = pickerPrefix(picker), target = $(prefix + "-options"), pages = $(prefix + "-pages");
  target.replaceChildren(); pages.replaceChildren();
  $(prefix + "-tabs").querySelectorAll("button").forEach((tab) => {
    const active = tab.dataset.kind === picker.tab;
    tab.setAttribute("aria-selected", String(active)); tab.classList.toggle("active", active);
  });
  if (picker.loading || picker.error) { empty(target, picker.error || "正在查找文档…"); return; }
  const scope = pickerScope(picker), options = resourceOptions(picker);
  picker.active = Math.min(picker.active || 0, Math.max(0, options.length - 1));
  options.forEach((item, index) => {
    const kind = picker.tab, covered = kind === "document" && scope.knowledge_base_ids.includes(item.knowledge_base_id);
    const selected = covered || scope[kind + "_ids"].includes(item.id);
    const label = node(picker === chatScopeDraft ? "label" : "button", undefined, "chat-scope-option");
    label.dataset.kind = kind; label.dataset.id = item.id;
    const text = node("span"), kb = state.knowledgeBases.find((kb) => kb.id === item.knowledge_base_id);
    text.append(node("strong", item.name || item.title), node("small", kind === "document"
      ? item.missing ? "请取消此项后重新选择" : (kb?.name || "知识库") + (covered ? " · 已包含在所选知识库中" : selected ? " · 已选择" : " · 单独文档")
      : item.missing ? "请取消此项后重新选择" : item.description || "整个知识库"));
    const disabled = covered || (!selected && scope[kind + "_ids"].length >= (kind === "document" ? 100 : 20));
    if (picker === chatScopeDraft) {
      const input = node("input"); input.type = "checkbox"; input.value = item.id;
      input.checked = selected; input.disabled = disabled;
      input.addEventListener("change", () => chooseResource(picker, item, input.checked));
      label.append(input, text);
    } else {
      label.type = "button"; label.id = "mention-option-" + index; label.setAttribute("role", "option");
      label.setAttribute("aria-selected", String(index === picker.active)); label.classList.toggle("active", index === picker.active);
      label.disabled = disabled || item.missing;
      label.addEventListener("mousedown", (event) => event.preventDefault());
      label.addEventListener("click", () => chooseResource(picker, item)); label.append(text);
    }
    target.append(label);
  });
  if (!options.length) empty(target, picker.query ? "没有匹配的" + (picker.tab === "document" ? "文档" : "知识库") : "当前空间暂无" + (picker.tab === "document" ? "文档" : "知识库"));
  if (picker !== chatScopeDraft) {
    if (options.length) $("query").setAttribute("aria-activedescendant", "mention-option-" + picker.active);
    else $("query").removeAttribute("aria-activedescendant");
  }
  if (picker.tab === "document" && picker.total > 20) {
    const max = Math.ceil(picker.total / 20);
    for (const [label, next, disabled] of [["上一页", picker.page - 1, picker.page <= 1], ["下一页", picker.page + 1, picker.page >= max]]) {
      const b = node("button", label, "small"); b.type = "button"; b.disabled = disabled;
      b.addEventListener("click", () => { picker.page = next; loadPickerDocuments(picker); }); pages.append(b);
    }
    pages.append(node("small", `${picker.page} / ${max} · ${picker.total} 份`));
  }
}
async function loadPickerDocuments(picker) {
  const revision = ++picker.revision, epoch = authEpoch;
  picker.loading = true; picker.error = ""; renderResourcePicker(picker);
  try {
    const result = await request(wp() + `/documents?keyword=${enc(picker.query)}&page=${picker.page}&page_size=20`);
    if (!pickerIsCurrent(picker) || revision !== picker.revision || epoch !== authEpoch) return;
    picker.docs = result.data || []; picker.total = result.total || 0;
    for (const doc of picker.docs) chatDocuments.set(doc.id, doc);
    picker.loading = false; renderResourcePicker(picker); renderChatScope();
  } catch (error) {
    if (!pickerIsCurrent(picker) || revision !== picker.revision || epoch !== authEpoch) return;
    picker.loading = false; picker.error = error.message; renderResourcePicker(picker);
  }
}
function changePickerTab(picker, tab) {
  if (!picker || !pickerIsCurrent(picker)) return;
  picker.revision++; clearTimeout(picker.timer);
  picker.tab = tab; picker.page = 1; picker.active = 0; picker.loading = false; picker.error = "";
  if (tab === "document") loadPickerDocuments(picker); else renderResourcePicker(picker);
}
function updatePickerQuery(picker, query) {
  picker.query = query; picker.page = 1; picker.active = 0; picker.error = ""; picker.revision++;
  clearTimeout(picker.timer);
  if (picker.tab === "document") {
    picker.loading = true; renderResourcePicker(picker);
    picker.timer = setTimeout(() => loadPickerDocuments(picker), 180);
  } else renderResourcePicker(picker);
}
function newPicker(scope = null) {
  return { scope: copyChatScope(scope) || emptyChatScope(), tab: "knowledge_base", query: "", docs: [], page: 1, total: 0,
    active: 0, revision: 0, workspace: state.wid, session: state.sid, loading: false, error: "" };
}
function renderChatScopeOptions() {
  if (!chatScopeDraft) return;
  document.querySelectorAll('[name="chat-scope-mode"]').forEach((input) => { input.checked = input.value === chatScopeDraft.mode; });
  $("chat-scope-picker").hidden = chatScopeDraft.mode !== "selected";
  renderResourcePicker(chatScopeDraft); renderChatScopeSummary();
}
bind("chat-knowledge-scope", async () => {
  if (!state.sid || ["running", "suspended"].includes(state.runStatus)) return;
  closeMentionPicker();
  await loadKnowledgeBases(); await refreshChatDocuments();
  const scope = currentChatScope();
  chatScopeDraft = { ...newPicker(scope), mode: scope === null ? "all" : scope.knowledge_base_ids.length + scope.document_ids.length ? "selected" : "none" };
  $("chat-scope-search").value = ""; renderChatScopeOptions(); $("chat-scope-dialog").showModal();
});
document.querySelectorAll('[name="chat-scope-mode"]').forEach((input) => input.addEventListener("change", () => {
  if (state.busy || !chatScopeDraft) return;
  chatScopeDraft.mode = input.value; renderChatScopeOptions();
}));
$("chat-scope-search").addEventListener("input", () => { if (chatScopeDraft) updatePickerQuery(chatScopeDraft, $("chat-scope-search").value.trim()); });
$("chat-scope-dialog").addEventListener("close", () => { clearTimeout(chatScopeDraft?.timer); chatScopeDraft = null; });
bind("chat-scope-cancel", () => $("chat-scope-dialog").close());
bind("chat-scope-refresh", async () => {
  await loadKnowledgeBases(); await refreshChatDocuments(chatScopeDraft?.scope);
  if (chatScopeDraft?.tab === "document") await loadPickerDocuments(chatScopeDraft); else renderChatScopeOptions();
});
bind("chat-scope-apply", () => {
  if (!chatScopeDraft) return;
  const { mode, scope } = chatScopeDraft;
  if (mode === "selected" && (!(scope.knowledge_base_ids.length + scope.document_ids.length) || missingChatResources(scope).length)) return;
  setChatScope(mode === "all" ? null : mode === "none" ? emptyChatScope() : scope);
  $("chat-scope-dialog").close();
});
for (const prefix of ["chat-scope", "chat-mention"]) $(prefix + "-tabs").querySelectorAll("button").forEach((tab) => {
  tab.addEventListener("click", () => { if (!state.busy) changePickerTab(prefix === "chat-scope" ? chatScopeDraft : chatMention, tab.dataset.kind); });
});
function closeMentionPicker() {
  clearTimeout(chatMention?.timer); chatMention = null;
  $("chat-mention-picker").hidden = true; $("chat-mention-options").replaceChildren(); $("chat-mention-pages").replaceChildren();
  $("query").setAttribute("aria-expanded", "false"); $("query").removeAttribute("aria-activedescendant");
}
function updateMentionPicker() {
  if (queryComposing || chatScopeLocked()) { closeMentionPicker(); return; }
  const input = $("query"), end = input.selectionStart;
  const match = /(?:^|[^a-zA-Z0-9_.+%/\-])@([^\s@]{0,100})$/.exec(input.value.slice(0, end));
  if (!match || input.selectionEnd !== end) { closeMentionPicker(); return; }
  const start = end - match[1].length - 1;
  if (!chatMention || chatMention.start !== start) {
    closeMentionPicker(); chatMention = { ...newPicker(), start, end };
    const picker = chatMention;
    loadKnowledgeBases().then(() => { if (pickerIsCurrent(picker)) renderResourcePicker(picker); }).catch((error) => {
      if (pickerIsCurrent(picker)) { picker.error = error.message; renderResourcePicker(picker); }
    });
  }
  chatMention.end = end;
  $("chat-mention-picker").hidden = false; $("query").setAttribute("aria-expanded", "true");
  updatePickerQuery(chatMention, match[1]);
}
$("query").addEventListener("input", updateMentionPicker);
$("query").addEventListener("click", updateMentionPicker);
$("query").addEventListener("compositionstart", () => { queryComposing = true; closeMentionPicker(); });
$("query").addEventListener("compositionend", () => { queryComposing = false; updateMentionPicker(); });
$("query").addEventListener("keydown", (event) => {
  if (!chatMention || queryComposing || event.isComposing || event.keyCode === 229) return;
  if (event.key === "Escape") { event.preventDefault(); closeMentionPicker(); return; }
  if (["ArrowDown", "ArrowUp"].includes(event.key)) {
    event.preventDefault();
    const options = [...$("chat-mention-options").querySelectorAll('button:not(:disabled)')];
    if (!options.length) return;
    const current = options.findIndex((option) => option.getAttribute("aria-selected") === "true");
    const next = (current + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length;
    chatMention.active = Number(options[next].id.replace("mention-option-", ""));
    renderResourcePicker(chatMention); $("mention-option-" + chatMention.active)?.scrollIntoView({ block: "nearest" });
  } else if (event.key === "Enter" && !event.ctrlKey && !event.metaKey) {
    event.preventDefault();
    $("mention-option-" + chatMention.active)?.click();
  }
});
$("chat-mention-close").addEventListener("click", closeMentionPicker);
document.addEventListener("pointerdown", (event) => {
  if (event.target !== $("query") && !$("chat-mention-picker").contains(event.target)) closeMentionPicker();
});
function resetDocumentView() {
  documentsRevision++;
  state.docPage = 1;
  state.docTotal = 0;
  clearSearch();
  $("documents").replaceChildren();
  $("doc-pages").replaceChildren();
}
async function loadKnowledgeBases(preferred = state.kid) {
  const workspace = state.wid;
  if (!workspace) return;
  const knowledgeBases = await api(wp() + "/knowledge-bases");
  if (workspace !== state.wid) return;
  state.knowledgeBases = knowledgeBases || [];
  const selected = state.knowledgeBases.find((item) => item.id === preferred)
    || state.knowledgeBases.find((item) => item.is_default) || state.knowledgeBases[0];
  const next = selected?.id || "";
  if (next !== state.kid) resetDocumentView();
  state.kid = next;
  renderKnowledgeBases();
}
$("knowledge-base").addEventListener("change", () => act(async () => {
  const id = $("knowledge-base").value;
  if (!state.knowledgeBases.some((item) => item.id === id)) return;
  state.kid = id;
  resetDocumentView();
  renderKnowledgeBases();
  $("detail").close();
  $("upload-dialog").close();
  await loadDocs();
}));
function editKnowledgeBase(knowledgeBase = null) {
  if (!manageKnowledgeBases()) throw Error("当前角色没有管理知识库的权限");
  const workspace = state.wid, base = wp() + "/knowledge-bases";
  const body = showDetail(knowledgeBase ? "编辑知识库" : "新建知识库");
  const formNode = node("form"), nameLabel = node("label", "知识库名称"), name = node("input");
  name.name = "name"; name.required = true; name.maxLength = 100;
  name.value = knowledgeBase?.name || ""; name.setAttribute("aria-label", "知识库名称");
  nameLabel.append(name);
  const descriptionLabel = node("label", "说明（可选）"), description = node("textarea");
  description.name = "description"; description.rows = 3; description.maxLength = 2000;
  description.value = knowledgeBase?.description || ""; description.setAttribute("aria-label", "知识库说明");
  descriptionLabel.append(description);
  const submit = node("button", knowledgeBase ? "保存知识库" : "创建知识库", "primary");
  submit.type = "submit";
  formNode.append(nameLabel, descriptionLabel, submit); body.append(formNode);
  formNode.addEventListener("submit", (event) => {
    event.preventDefault();
    act(async () => {
      if (workspace !== state.wid || !manageKnowledgeBases()) throw Error("空间或权限已变化，请重新打开表单");
      if (!name.value.trim()) throw Error("请输入知识库名称");
      const result = await api(base + (knowledgeBase ? "/" + enc(knowledgeBase.id) : ""), knowledgeBase ? "PATCH" : "POST", {
        name: name.value.trim(), description: description.value.trim(),
      });
      $("detail").close();
      await loadKnowledgeBases(result.id);
      await loadDocs();
      note(knowledgeBase ? "知识库已更新。" : "知识库已创建，可以上传资料。");
    });
  });
  name.focus();
}
bind("new-knowledge-base", () => editKnowledgeBase());
bind("edit-knowledge-base", () => {
  const current = state.knowledgeBases.find((item) => item.id === state.kid);
  if (current) editKnowledgeBase(current);
});
bind("delete-knowledge-base", () => {
  if (!manageKnowledgeBases()) throw Error("当前角色没有删除知识库的权限");
  const current = state.knowledgeBases.find((item) => item.id === state.kid), base = kp();
  if (!current) return;
  confirmDialog("删除知识库", `确定删除知识库“${current.name}”？其中全部文档、历史版本及来源引用将不可访问，原文件和检索索引将在后台清理，无法恢复。此操作不会删除工作空间、会话或个人记忆。`, async () => {
    await api(base, "DELETE");
    $("detail").close();
    state.knowledgeBases = state.knowledgeBases.filter((item) => item.id !== current.id);
    state.kid = "";
    resetDocumentView();
    renderKnowledgeBases();
    await loadKnowledgeBases();
    await loadDocs();
    note("知识库已删除，相关文件和索引将在后台清理。会话和个人记忆已保留。");
  });
});
async function loadDocs() {
  const workspace = state.wid, knowledgeBase = state.kid, revision = ++documentsRevision;
  if (!workspace || !knowledgeBase) {
    resetDocumentView();
    empty($("documents"), !workspace ? "创建一个空间，开始收藏资料。" : manageKnowledgeBases()
      ? "还没有知识库。点击“新建知识库”，再上传你的第一份资料。" : "当前空间暂无知识库，请联系管理员创建。");
    return;
  }
  const base = wp();
  const result = await request(kp() + `/documents?page=${state.docPage}&page_size=10`);
  if (revision !== documentsRevision || workspace !== state.wid || knowledgeBase !== state.kid) return;
  if (!result.data?.length && result.total > 0 && state.docPage > 1) {
    state.docPage--;
    return loadDocs();
  }
  state.docTotal = result.total;
  if ($("clear-search").hidden) $("doc-total").textContent = result.total;
  const target = $("documents");
  target.replaceChildren();
  if (!result.data?.length) {
    empty(target, writable() ? "知识库还空着。上传第一份资料，让知识在这里积累。" : "当前知识库还没有文档，请联系协作者添加资料。");
  }
  // Index reads are independent. Render metadata immediately and check this page together.
  await Promise.allSettled((result.data || []).map(async (d) => {
    const card = node("article", undefined, "document-row");
    const icon = node("div", "文档", "file-icon");
    icon.setAttribute("aria-hidden", "true");
    const meta = node("div", undefined, "document-meta");
    meta.append(
      button(d.title, () => documentDetail(d.id), "document-title"),
      node("small", "更新于 " + new Date(d.updated_at).toLocaleString("zh-CN")),
    );
    const badge = node("span", "检查索引…", "pill status-pending");
    badge.setAttribute("role", "status");
    card.append(icon, meta, badge, button("查看详情 →", () => documentDetail(d.id), "text-button"));
    target.append(card);
    try {
      const job = await api(base + `/documents/${enc(d.id)}/versions/${enc(d.current_version_id)}/index`);
      if (revision !== documentsRevision || workspace !== state.wid || knowledgeBase !== state.kid) return;
      const status = indexPresentation(job);
      badge.textContent = status.label;
      badge.className = "pill " + status.className;
      if (job.error_code) badge.title = job.error_code;
    } catch (e) {
      if (revision !== documentsRevision || workspace !== state.wid || knowledgeBase !== state.kid) return;
      badge.textContent = "索引不可用";
      badge.className = "pill status-unavailable";
      badge.title = e.message;
    }
  }));
  if (revision !== documentsRevision || workspace !== state.wid || knowledgeBase !== state.kid) return;
  pager("doc-pages", result, async (p) => {
    state.docPage = p;
    await loadDocs();
  });
}
bind("refresh-docs", async () => { await loadKnowledgeBases(); await loadDocs(); });
bind("show-upload", () => {
  if (!writable()) throw Error("当前角色没有上传权限");
  if (!state.kid) throw Error("请先选择知识库");
  $("upload-dialog").showModal();
});
bind("close-upload", () => $("upload-dialog").close());
bind("clear-search", clearSearch);
form("upload-form", async (f) => {
  if (!state.wid || !state.kid) throw Error("请先选择知识库");
  const workspace = state.wid, knowledgeBase = state.kid, base = kp();
  const file = f.get("file");
  validateUpload(file, await uploadLimits());
  const data = new FormData();
  data.set("file", file);
  if (f.get("title").trim()) data.set("title", f.get("title").trim());
  await api(base + "/documents", "POST", data);
  if (workspace !== state.wid || knowledgeBase !== state.kid) return;
  $("upload-form").reset();
  $("upload-dialog").close();
  clearSearch();
  state.docPage = 1;
  await loadDocs();
  note("上传成功，后台正在处理文档。");
});
async function download(path, filename) {
  const r = await request(path, { raw: true });
  const url = URL.createObjectURL(await r.blob());
  const a = node("a");
  a.href = url;
  a.download = filename;
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}
async function documentDetail(id, page = 1) {
  const base = wp() + "/documents/" + enc(id);
  const d = await api(base);
  const versions = await request(base + `/versions?page=${page}&page_size=10`);
  const body = showDetail(d.title);
  body.append(node("small", "文档 ID：" + d.id));
  if (writable()) {
    const actions = node("div", undefined, "toolbar");
    actions.append(
      button("修改标题", () =>
        editDialog("文档标题", d.title, async (title) => {
          await api(base, "PATCH", { title });
          await documentDetail(id, page);
          await loadDocs();
        }),
      ),
      button(
        "删除文档",
        () =>
          confirmDialog(
            "删除文档",
            "删除后文档及全部历史版本立即不可访问，原文件和检索索引将在后台清理，无法恢复。请确认目标：" +
              d.title,
            async () => {
              await api(base, "DELETE");
              clearSearch();
              $("detail").close();
              await loadDocs();
              note("文档已删除，相关文件和索引将在后台清理。");
            },
          ),
        "danger",
      ),
    );
    body.append(actions);
    const replace = node("form", undefined, "toolbar"),
      input = node("input");
    input.type = "file";
    input.required = true;
    input.setAttribute("aria-label", "替换文档文件");
    replace.append(input, node("button", "上传新版本"));
    replace.addEventListener("submit", (e) => {
      e.preventDefault();
      act(async () => {
        const file = input.files[0];
        validateUpload(file, await uploadLimits());
        const f = new FormData();
        f.set("file", file);
        f.set("base_version_id", d.current_version_id);
        await api(base + "/versions", "POST", f);
        clearSearch();
        await documentDetail(id);
        await loadDocs();
      });
    });
    body.append(replace);
  }
  for (const v of versions.data || []) {
    const card = node("div", undefined, "card");
    card.append(
      node(
        "h2",
        `版本 ${v.number} · ${v.filename}${v.id === d.current_version_id ? " · 当前版本" : ""}`,
      ),
      node(
        "small",
        `${v.size_bytes} 字节 · ${new Date(v.created_at).toLocaleString()} · ${v.id}`,
      ),
    );
    const path = base + "/versions/" + enc(v.id),
      actions = node("div", undefined, "toolbar");
    actions.append(
      button("下载原文", () => download(path + "/content", v.filename)),
    );
    if (/\.(txt|md|markdown)$/i.test(v.filename)) {
      actions.append(
        button("查看正文", async () => {
          const text = await (
            await request(path + "/content", { raw: true })
          ).text();
          const preview = showDetail(`${d.title} · 版本 ${v.number}`);
          preview.append(node("pre", text));
          if (
            writable() &&
            v.id === d.current_version_id &&
            v.size_bytes <= 1024 * 1024
          ) {
            const label = node("label", "编辑全文（保存为新版本）"),
              edit = node("textarea");
            edit.rows = 12;
            edit.value = text;
            edit.setAttribute("aria-label", "文档正文");
            label.append(edit);
            preview.append(
              label,
              button(
                "保存新版本",
                async () => {
                  await api(base + "/content", "PUT", {
                    base_version_id: v.id,
                    content: edit.value,
                  });
                  clearSearch();
                  await documentDetail(id);
                  await loadDocs();
                  note("新版本已保存，等待索引。");
                },
                "primary",
              ),
            );
          }
        }),
      );
    }
    if (writable() && v.id === d.current_version_id)
      actions.append(
        button("重试索引", async () => {
          await api(path + "/index", "POST", {});
          note("已提交索引任务。");
          await loadDocs();
        }),
      );
    card.append(actions);
    body.append(card);
  }
  const pages = node("div", undefined, "pager");
  if (page > 1)
    pages.append(button("更新的版本", () => documentDetail(id, page - 1)));
  if (page * 10 < versions.total)
    pages.append(button("更早的版本", () => documentDetail(id, page + 1)));
  body.append(pages);
}
form("search-form", async (f) => {
  if (!state.wid || !state.kid) throw Error("请先选择知识库");
  const workspace = state.wid, knowledgeBase = state.kid, revision = ++searchRevision;
  const result = await api(kp() + "/search", "POST", {
    query: f.get("query"),
    mode: f.get("mode"),
    limit: 10,
  });
  if (revision !== searchRevision || workspace !== state.wid || knowledgeBase !== state.kid) return;
  const target = $("search-results");
  target.replaceChildren();
  $("documents").hidden = true;
  $("doc-pages").hidden = true;
  $("doc-columns").hidden = true;
  $("clear-search").hidden = false;
  $("doc-list-heading").textContent = "检索结果";
  $("doc-total").textContent = result.hits?.length || 0;
  $("search-summary").textContent = `“${f.get("query")}” · ${result.hits?.length || 0} 个片段（最多 10 个）`;
  if (!result.hits?.length)
    target.append(node("p", "没有命中，请确认索引已完成。", "muted"));
  for (const hit of result.hits || []) {
    const c = hit.chunk;
    const card = node("article", undefined, "card search-hit");
    card.append(
      node("h2", hit.title || c.title || "文档片段"),
      node("pre", c.content),
      button("查看来源文档", () => documentDetail(c.document_id)),
    );
    target.append(card);
  }
});
async function loadSessions() {
  if (!state.wid) return;
  const result = await request(
    wp() + `/sessions?page=${state.sessionPage}&page_size=10`,
  );
  if (!result.data?.length && result.total > 0 && state.sessionPage > 1) {
    state.sessionPage--;
    return loadSessions();
  }
  $("session-total").textContent = result.total;
  $("sessions").replaceChildren();
  if (!result.data?.length) empty($("sessions"), "还没有会话，点击上方新建。");
  for (const s of result.data || [])
    $("sessions").append(
      button(
        s.title,
        async () => {
          closeMentionPicker();
          state.sid = s.id;
          await refreshChatDocuments();
          state.rid = "";
          state.messagePage = 1;
          state.runStatus = "";
          $("query").value = "";
          $("session-title").textContent = s.title;
          await loadMessages(true);
          await loadSessions();
        },
        s.id === state.sid ? "selected" : "",
      ),
    );
  pager("session-pages", result, async (p) => {
    state.sessionPage = p;
    await loadSessions();
  });
}
bind("new-session", () =>
  editDialog("会话名称", "新会话", async (title) => {
    const s = await api(wp() + "/sessions", "POST", { title });
    state.sid = s.id;
    state.rid = "";
    state.messagePage = 1;
    state.sessionPage = 1;
    state.runStatus = "";
    $("query").value = "";
    showView("chat");
    $("session-title").textContent = s.title;
    await loadSessions();
    await loadMessages();
    $("detail").close();
  }),
);
bind("rename-session", () => {
  if (!state.sid) return;
  editDialog("会话名称", $("session-title").textContent, async (title) => {
    const session = await api(sp(), "PATCH", { title });
    $("session-title").textContent = session.title;
    await loadSessions();
    $("detail").close();
  });
});
bind("delete-session", () => {
  if (!state.sid) return;
  confirmDialog("删除会话", "此会话及其消息将被删除。请确认目标：" + $("session-title").textContent, async () => {
    await api(sp(), "DELETE");
    chatScopes.delete(state.sid);
    state.sid = state.rid = state.runStatus = "";
    $("session-title").textContent = "选择或创建会话";
    $("query").value = "";
    for (const id of ["messages", "prompt", "run-state", "message-pages"]) $(id).replaceChildren();
    $("chat-welcome").hidden = false;
    await loadSessions();
    $("detail").close();
  });
});
document.querySelectorAll("[data-suggestion]").forEach((b) => {
  b.addEventListener("click", () => {
    if (state.busy) return;
    if (!state.sid) { note("先点击「新建会话」，再选择你想完成的任务。"); return; }
    $("query").value = b.dataset.suggestion;
    $("query").focus();
  });
});
$("query").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey) && !queryComposing && !e.isComposing && e.keyCode !== 229 && !$("send").disabled) {
    e.preventDefault();
    $("chat-form").requestSubmit();
  }
});
async function showCitations(rid, selectedID = "", referenceOrder = []) {
  const path = sp() + "/runs/" + enc(rid), workspace = wp();
  const { citations: list = [] } = await api(path + "/citations");
  const sourceOrder = [...referenceOrder];
  for (const item of list) if (!sourceOrder.includes(item.evidence.evidence_id)) sourceOrder.push(item.evidence.evidence_id);
  list.sort((a, b) => sourceOrder.indexOf(a.evidence.evidence_id) - sourceOrder.indexOf(b.evidence.evidence_id));
  const selected = selectedID ? list.find((item) => item.evidence.evidence_id === selectedID) : null;
  if (selectedID && !selected) throw Error("这条引用不在已保存的回答来源中，请刷新会话后核对。");
  const full = selected ? await api(path + "/evidence/" + enc(selectedID)) : null;
  const body = showDetail(selected ? "引用来源" : `回答来源 · ${list.length}`);
  $("detail").classList.add("source-drawer");
  body.append(node("p", "引用对应回答时使用的文档版本，可核对原文或下载留存。", "source-intro"));
  if (selected) body.append(button("← 查看全部来源", () => showCitations(rid, "", referenceOrder), "text-button"));
  for (const item of selected ? [selected] : list) {
    const e = item.evidence;
    const card = node("article", undefined, "card source-card");
    card.append(node("span", "来源 " + (sourceOrder.indexOf(e.evidence_id) + 1), "source-number"), node("h2", e.title));
    const content = node("pre", full?.evidence.content || e.content || "点击读取片段，核对回答依据。", "source-passage");
    card.append(content);
    const metadata = node("details", undefined, "source-locator");
    metadata.append(node("summary", "版本与片段定位"), node("small", `版本 ${e.version_id} · 抽取文本字节 ${e.start_byte}–${e.end_byte}（非 PDF 页码）`));
    card.append(metadata);
    if (!full) card.append(button("读取片段", async () => {
      const result = await api(path + "/evidence/" + enc(e.evidence_id));
      content.textContent = result.evidence.content;
    }, "quiet"));
    card.append(button("下载该版本", async () => {
      const base = workspace + `/documents/${enc(e.document_id)}/versions/${enc(e.version_id)}`;
      const version = await api(base);
      return download(base + "/content", version.filename);
    }, "quiet"));
    body.append(card);
  }
  if (!list.length) body.append(node("p", "本回答没有已提交的文档引用。"));
}
function updateScrollButton() {
  const scroller = document.querySelector(".conversation-scroll");
  $("jump-latest").hidden = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 120;
}
function scrollConversation() {
  const scroller = document.querySelector(".conversation-scroll");
  scroller.scrollTop = scroller.scrollHeight;
  updateScrollButton();
}
document.querySelector(".conversation-scroll").addEventListener("scroll", updateScrollButton, { passive: true });
$("jump-latest").addEventListener("click", scrollConversation);
function runMemoryStatus(runID) {
  const box = node("p", undefined, "message-memory");
  box.hidden = true;
  box.setAttribute("aria-live", "polite");
  const base = sp(), epoch = authEpoch;
  const current = () => epoch === authEpoch && base === sp() && box.isConnected;
  const refresh = async () => {
    try {
      const result = await api(base + "/runs/" + enc(runID) + "/memory");
      if (!current()) return;
      const pending = ["waiting", "queued", "running"].includes(result.status);
      const count = Number.isInteger(result.saved_count) && result.saved_count > 0 ? result.saved_count : 0;
      const labels = {
        waiting: "记忆等待处理", queued: "记忆已排队", running: "正在整理本轮记忆",
        done: count ? `已保存 ${count} 条记忆` : "本轮未新增长期记忆",
        failed: "记忆处理失败，回答已保留", cancelled: "本轮记忆采集已取消",
      };
      if (!labels[result.status]) { box.hidden = true; return; }
      box.replaceChildren(node("span", labels[result.status]));
      if (pending) box.append(button("刷新记忆状态", refresh, "text-button"));
      if (result.status === "done" && count) box.append(button("查看个人记忆", async () => {
        showView("memory"); await loadMemories();
      }, "text-button"));
      box.hidden = false;
    } catch (error) {
      if (!current()) return;
      if (error.status === 404) { box.hidden = true; return; }
      box.replaceChildren(node("span", "暂时无法读取记忆状态"), button("刷新记忆状态", refresh, "text-button"));
      box.hidden = false;
    }
  };
  // One read per rendered answer. Manual refresh avoids polling after navigation.
  void refresh();
  return box;
}
function messageCard(m) {
  const assistant = m.role === "assistant";
  const card = node("article", undefined, "message " + (assistant ? "assistant" : "user"));
  const header = node("header", undefined, "message-heading");
  header.append(node("span", assistant ? "✦" : "你", "message-avatar"), node("strong", assistant ? "Agent" : "你"));
  if (m.created_at && Number.isFinite(Date.parse(m.created_at))) {
    const time = node("time", new Date(m.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }));
    time.dateTime = m.created_at; header.append(time);
  }
  const content = node("div", undefined, "message-content" + (assistant ? " answer-markdown" : ""));
  card.append(header, content);
  if (assistant) {
    const references = [];
    const count = renderAnswer(content, m.content, { onCitation: (id) => act(() => showCitations(m.run_id, id, references)) });
    for (const mark of content.querySelectorAll(".citation-mark")) if (!references.includes(mark.dataset.evidence)) references.push(mark.dataset.evidence);
    const actions = node("footer", undefined, "message-actions");
    actions.append(button("复制回答", async () => {
      await navigator.clipboard.writeText(m.content);
      note("回答已复制");
    }, "text-button"), button(count ? `查看引用 · ${count}` : "查看引用", () => showCitations(m.run_id, "", references), "text-button"));
    card.append(actions);
    if (m.status === "completed" && m.run_id) card.append(runMemoryStatus(m.run_id));
  } else content.textContent = m.content;
  return card;
}
async function loadMessages(latest = false) {
  if (!state.sid) return;
  let result = await request(
    sp() + `/messages?page=${state.messagePage}&page_size=20`,
  );
  if (latest && result.total > 20) {
    state.messagePage = Math.ceil(result.total / 20);
    result = await request(
      sp() + `/messages?page=${state.messagePage}&page_size=20`,
    );
  }
  const target = $("messages");
  $("chat-welcome").hidden = Boolean(result.data?.length);
  target.replaceChildren();
  $("prompt").replaceChildren();
  $("run-state").replaceChildren();
  state.promptID = "";
  for (const m of result.data || []) target.append(messageCard(m));

  pager("message-pages", result, async (p) => {
    state.messagePage = p;
    state.rid = "";
    await loadMessages();
  });
  let last = result.data?.at(-1);
  // Browsing older messages must not hide a newer suspended/running Run.
  if (result.page * result.page_size < result.total) {
    const tail = await request(sp() + `/messages?page=${Math.ceil(result.total / 20)}&page_size=20`);
    last = tail.data?.at(-1);
  }
  state.rid = last?.run_id || "";
  state.runStatus = "";
  if (state.rid) await inspectRun();
  if (latest) scrollConversation(); else updateScrollButton();
}
async function inspectRun() {
  if (!state.rid) return;
  const run = await api(rp());
  state.runStatus = run.status;
  const recoverable = run.status === "failed" && ["agent_failed", "interrupted", "lease_expired"].includes(run.error_code);
  const status = node("section", undefined, "run-notice " + (run.status === "failed" ? "run-failed" : ""));
  $("run-state").replaceChildren();
  if (run.status !== "completed") {
    const labels = {
      running: ["Agent 正在处理问题", "本次运行沿用发送时的知识库范围。刷新状态可查看最新结果，请勿重复发送。"],
      failed: [run.error_code === "agent_canceled" ? "生成已停止" : "这次回答尚未完成", recoverable
        ? "你的问题已保留。恢复将沿用原运行的知识库范围；也可调整范围后发送新问题。"
        : "你的问题已保留，可以继续发送新问题。"],
      suspended: ["等待你的确认", "完成下方答复或审批后，Agent 将沿用本次运行原有的知识库范围继续处理。"],
    };
    const [title, description] = labels[run.status] || ["运行状态待确认", "请刷新状态后查看结果。"];
    status.append(node("strong", title), node("p", description));
    if (run.error_code) {
      const details = node("details");
      details.append(node("summary", "错误详情"), node("code", run.error_code)); status.append(details);
    }
    $("run-state").append(status);
  }
  if (recoverable)
    status.append(
      button("尝试恢复原运行", () => stream(rp() + "/recover", {}), "quiet"),
    );
  if (run.status === "suspended") {
    const p = await api(rp() + "/prompt");
    await renderPrompt(p);
  } else {
    try {
      const p = await api(rp() + "/prompt");
      if (p.approval?.result) {
        const receipt = node("div", "文档写入已提交；索引异步更新。", "card");
        const result = p.approval.result;
        receipt.append(
          node(
            "p",
            `${result.document.title} · 版本 ${result.version.number} · ${result.version.size_bytes} 字节`,
          ),
          button("查看已写入文档", () => documentDetail(result.document.id)),
        );
        $("run-state").append(receipt);
      }
    } catch {
      /* A run without prompts normally returns 404. */
    }
  }
}
async function renderPrompt(p) {
  state.promptID = p.id;
  const box = node("section", undefined, "prompt");
  $("prompt").replaceChildren(box);
  box.append(
    node(
      "h2",
      p.kind === "tool_approval" ? "审阅文档变更" : "Agent 需要你补充信息",
    ),
    node("p", p.question),
    node(
      "small",
      "状态：" +
        p.status +
        " · 到期：" +
        new Date(p.expires_at).toLocaleString(),
    ),
  );
  if (p.status !== "pending") return;
  const path = rp() + "/prompts/" + enc(p.id);
  if (p.kind === "tool_approval") {
    const a = p.approval,
      args = a.arguments;
    box.append(
      node(
        "p",
        `${a.tool === "create_document" ? "新建" : "更新"}：${a.title} / ${a.filename}`,
      ),
      node(
        "small",
        `目标：${a.document_id || "新文档"} · 基线：${a.base_version_id || "无"} · 新正文 ${a.content_bytes} 字节`,
      ),
    );
    if (a.tool === "create_document") {
      const targetID = a.knowledge_base_id || args?.knowledge_base_id;
      const target = state.knowledgeBases.find((item) => item.id === targetID);
      box.append(node("p", targetID
        ? `目标知识库：${target?.name || "名称暂不可用"} · ID：${targetID}`
        : "目标知识库：执行时的默认知识库（未指定固定知识库 ID）。", "approval-target"));
    }
    if (!args || typeof args.content !== "string")
      throw Error("审批内容不可用，请刷新状态");
    let before = "";
    let reviewable = true;
    if (a.base_version_id) {
      try {
        before = await (
          await request(
            wp() +
              `/documents/${enc(a.document_id)}/versions/${enc(a.base_version_id)}/content`,
            { raw: true },
          )
        ).text();
      } catch (e) {
        reviewable = false;
        box.append(node("p", "基线读取失败，不能批准：" + e.message, "error"));
      }
    }
    const diff = changedLines(before, args.content),
      comparison = node("div", undefined, "comparison");
    for (const [key, title, cls] of [
      ["before", "修改前（固定基线）", "removed"],
      ["after", "修改后（完整正文）", "added"],
    ]) {
      const col = node("div");
      col.append(node("h2", title));
      const pre = node("pre");
      for (const line of diff[key])
        pre.append(
          node("div", line.text || "\u200b", line.changed ? cls : undefined),
        );
      col.append(pre);
      comparison.append(col);
    }
    box.append(
      comparison,
      node("small", "标色为变化区段；批准仅授权此份提案，保存为独立新版本。"),
    );
    const approve = button(
      "批准这次写入",
      () => stream(path + "/approve", { approved: true }),
      "primary",
    );
    approve.disabled = !reviewable;
    box.append(
      approve,
      button("拒绝这次写入", () =>
        stream(path + "/approve", { approved: false }),
      ),
    );
  } else {
    const input = node("textarea");
    input.rows = 2;
    input.setAttribute("aria-label", "答复 Agent");
    for (const option of p.options || [])
      box.append(
        button(option, () => stream(path + "/answer", { answer: option })),
      );
    box.append(
      input,
      button(
        "提交答复",
        async () => {
          if (!input.value.trim()) throw Error("请输入答复");
          await stream(path + "/answer", { answer: input.value });
        },
        "primary",
      ),
    );
  }
  box.append(
    button(
      "取消整个等待",
      async () => {
        await api(path + "/cancel", "POST", {});
        await loadMessages(true);
      },
      "danger",
    ),
  );
}
async function stream(path, data) {
  const epoch = authEpoch;
  state.controller = new AbortController();
  $("stop").hidden = false;
  $("draft").hidden = false;
  const draftHeader = node("header", undefined, "message-heading");
  const draftStatus = node("small", "正在连接 Agent…", "stream-status");
  draftStatus.setAttribute("role", "status");
  draftHeader.append(node("span", "✦", "message-avatar"), node("strong", "Agent"), draftStatus);
  const draftBody = node("div", undefined, "message-content answer-markdown");
  $("draft").replaceChildren(draftHeader, draftBody);
  $("chat-welcome").hidden = true;
  let draftText = "", paint = 0, userShown = false;
  const scheduleDraft = () => {
    if (paint) return;
    paint = requestAnimationFrame(() => {
      paint = 0;
      if (epoch !== authEpoch || !state.controller) return;
      const scroller = document.querySelector(".conversation-scroll");
      const follow = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 120;
      renderAnswer(draftBody, draftText);
      if (follow) scrollConversation(); else updateScrollButton();
    });
  };
  $("prompt").replaceChildren();
  scrollConversation();
  let terminal = false, accepted = false;
  try {
    const r = await request(path, {
      method: "POST",
      data,
      stream: true,
      signal: state.controller.signal,
    });
    accepted = true;
    terminal = await consumeEvents(r.body, async (name, payload) => {
      if (epoch !== authEpoch) throw Error("登录状态已变化，请刷新页面");
      const scroller = document.querySelector(".conversation-scroll");
      const follow = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 100;
      if (name === "run.started") {
        state.rid = payload.run_id;
        draftStatus.textContent = "正在准备回答…";
        if (data.content && !userShown) {
          $("messages").append(messageCard({ role: "user", content: data.content }));
          userShown = true;
        }
      }
      if (name === "message.start" || name === "message.discard") {
        draftText = "";
        draftStatus.textContent = name === "message.discard" ? "正在重新组织回答…" : "正在准备回答…";
        scheduleDraft();
      }
      if (name === "message.delta") {
        draftStatus.textContent = "正在生成回答…";
        draftText += payload.delta;
        scheduleDraft();
      }
      if (name === "run.completed") { draftText = payload.answer; draftStatus.textContent = "正在保存回答…"; scheduleDraft(); }
      if (name === "run.failed") note("这次回答尚未完成，正在查询运行状态。");
      if (follow) scrollConversation();
    });
    if (!terminal) note("连接已结束，正在查询已保存状态；不会自动重发。");
  } catch (e) {
    note(
      e.name === "AbortError"
        ? "已停止生成，正在查询运行状态。"
        : e.message + "；正在检查已保存状态。",
    );
  } finally {
    cancelAnimationFrame(paint);
    state.controller = null;
    $("stop").hidden = true;
    $("draft").hidden = true;
    $("draft").textContent = ""; // Always reconcile durable history after a delivery failure; never retry POST.
    await loadMessages(true);
  }
  return accepted;
}
$("stop").addEventListener("click", () => state.controller?.abort());
form("chat-form", async () => {
  if (!state.sid) throw Error("请先创建或选择会话");
  if (["running", "suspended"].includes(state.runStatus)) throw Error("请先完成当前运行");
  const content = $("query").value;
  if (!content.trim()) return;
  closeMentionPicker();
  const session = state.sid, workspace = state.wid;
  await loadKnowledgeBases(); await refreshChatDocuments();
  if (session !== state.sid || workspace !== state.wid) throw Error("会话已变化，请重新发送");
  const scope = normalizeChatScope(currentChatScope());
  if (missingChatResources(scope).length) throw Error("已选资料已删除或不可访问，请重新选择后发送");
  const accepted = await stream(sp() + "/messages", {
    content, ...(scope === null ? {} : scope),
  });
  if (accepted) $("query").value = "";
});
bind("refresh-chat", async () => {
  if (state.sid) await loadMessages(true);
});

bind("retry-auth", restoreLogin);
bind("use-login", () => {
  $("auth-restoring").hidden = true;
  $("auth").hidden = false;
});
authChannel?.addEventListener("message", ({ data }) => {
  if (data?.kind === "logout") { resetAuth(); return; }
  if (data?.kind !== "credentials" || !state.auth) return;
  if (state.userID && data.userID !== state.userID) {
    resetAuth();
    note("账号已在其他页面切换，请重新登录。");
  } else if (typeof data.token === "string" && typeof data.expires_at === "string") {
    state.auth = { token: data.token, expires_at: data.expires_at };
  }
});
act(restoreLogin);
