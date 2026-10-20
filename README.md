# MagWorkspace

个人文档与知识库工作台，包含 Go + Gin 后端和同源内嵌 Web 工作台。已实现用户、Workspace / 成员权限、私有会话和独立 Magent Harness 接入，以及文档不可变版本、异步处理、混合检索和运行内可信引用。基础设施统一使用 **Docker Compose**。

可观测支持结构化日志、分阶段指标与可选 OpenTelemetry，Docker Jaeger/Prometheus 的入口见[可观测文档](docs/observability.md)。检索评测通过真实上传/索引/检索入口运行，使用完整 SciFact 和统一业务集格式，支持断点恢复、逐query结果与配对比较；命令和统计口径见[检索评测](docs/retrieval-evaluation.md)，实际完成状态见[迭代009](docs/iterations/009-evaluation-observability.md)。

个人长期记忆保存于 MySQL，按用户与 Workspace 隔离，跨 Session 和服务重启生效。成功对话后由后台自动提取用户陈述；聊天中的“请记住：…”直接进入保存流程，不调用提取模型。侧栏「个人记忆」支持纠正、开启/关闭、查看历史、导出和清空，无需手动新增。回答下方可查看记忆处理状态；自动提取使用现有 Harness 模型并产生额外调用，召回本身不调用模型。资料与偏好作为有界背景，事实与事项按当前问题召回，模型注入前回查当前权威状态。接口及遗忘边界见[个人记忆 API](docs/api.md#个人长期记忆)。

消息接口支持 SSE：添加 `Accept: text/event-stream` 可接收 Harness 原生文本增量；默认仍返回 JSON。实现与验证记录见 [迭代 002](docs/iterations/002-sse.md)。

文档支持上传、标题修改、TXT/Markdown 在线编辑、历史版本与下载，内容更新通过当前版本 ID 检测冲突并自动排队索引。原始文件默认最大 50 MB，通过 `MAX_FILE_SIZE_MB` 配置（1–512，1 MB = 1,048,576 字节，修改后重启），在线正文最大 1 MiB，存入 MinIO；MySQL 仅负责业务状态与元数据。调用示例见 [文档 API](docs/api.md#文档与不可变版本)，更新验收见 [迭代 007](docs/iterations/007-document-updates.md)。

## 启动

需要 Docker，以及相邻的 `../Magent Harness` 源码目录；不会修改该目录。Go 后端和 FastAPI Harness 分为两个容器，运行时无需在宿主机安装 Go 或 Python。

```bash
python3 scripts/init-env.py
docker compose --profile app up -d --build
```

`init-env.py` 为 MySQL / MinIO / Qdrant / Redis 和内部 Harness API 生成随机凭据，遇到已有 `.env` 会停止。升级已有环境时须补齐 `.env.example` 的 `MAGENT_SERVICE_API_KEY`（至少32字符随机值）；不能用模型 API Key 代替。模型相关环境只注入 Harness 容器，Go 仅持服务通信凭据。配置说明见[Harness HTTP 服务](docs/harness-http.md)。

首次启动会下载本地 bge-m3 模型；后续复用模型卷。macOS Docker Ollama 使用 CPU，索引速度受资源影响。`GET /healthz` 检查 Go 进程，`GET /readyz` 检查数据库并报告 Redis 状态；Harness 的健康端点默认在 `http://127.0.0.1:18090/healthz`。独立健康不代表模型调用已验证。

启动后打开 `http://127.0.0.1:8080/`（自定义 HTTP_PORT 时使用对应端口）。前端采用知识库列表与私有会话双入口，包含空间切换、索引状态、上传弹窗、全文检索和会话管理。前端无需 Node 或独立构建服务；支持注册/登录、空间、文档上传/版本/编辑/索引重试/检索、私有 SSE 会话、引用、提问答复及文档写审批。审批并排展示固定基线和完整拟写正文，批准后以写入回执确认结果。访问令牌仅在页面内存，刷新凭据使用 HttpOnly Cookie；刷新页面和新标签页可恢复登录，同一标签页会恢复所选空间/会话与待审批任务。退出会撤销服务器会话；断线不会自动重发写请求。

Compose 的 `harness` 服务包含 Python/FastAPI 并只读挂载 Harness 源码，`backend` 仅包含 Go 程序。因此 app profile 在 Agent 未启用时也需要有效的 Harness 路径。应用使用独立 MySQL 账号，不使用 root。停止容器用 `docker compose --profile app down`，数据卷保留。

快速调整前端时，保持后端运行，在项目根执行：

```bash
python3 scripts/dev-webui.py --backend http://127.0.0.1:8080 --port 5173
```

访问 `http://127.0.0.1:5173/`，编辑 `internal/webui/assets/` 后刷新即可看到变化；无需 npm 安装或重编译 Go。开发入口只监听本地回环地址，并同源转发 API 与 SSE；生产入口仍由 Go embed 打包，更新内嵌资源需重新构建。会话支持改名、删除与 Ctrl/⌘+Enter 发送，示例任务只填入输入框；发送被拒绝时保留草稿。

## 文档处理与存储职责

当前默认使用 Qdrant 内置中文 BM25、稠密向量和原生 RRF。隔离真实栈验收见[迭代023](docs/iterations/023-qdrant-native.md)，统计更新与生命周期设计见[决策017](docs/decisions/017-qdrant-native-retrieval.md)。

| 服务 | 唯一职责 |
|---|---|
| MySQL | 用户、Workspace/权限、私有会话与提问状态、个人长期记忆与墓碑、文档及版本元数据、索引任务状态与发布记录、来源关系及待清理对象记录 |
| MinIO | 原始文件、不可变解析正文与分块清单、待答复提问的私有执行快照 |
| Redis | 可重建的索引任务分发/去重、短期处理进度与 SSE 事件补发；不是权限或业务状态权威 |
| Qdrant | 内置多语言 BM25、向量索引与 RRF，可由原文重建 |
| Ollama + bge-m3 | 本地生成 1024 维向量 |
| Tika | 在受限容器中解析 PDF / DOCX；TXT / Markdown 在 Go 内处理 |

上传成功即排队，查询版本 `/index` 观察 `queued/running/ready/failed`。检索支持 `lexical/vector/hybrid`，默认 hybrid；只返回当前版本的已发布索引，引用固定版本与分块。失败任务通过同一版本 POST `/index` 重试。详见 [API](docs/api.md#文档处理与检索)。

解析上限 1 MiB UTF-8 文本、每块 800 字符、重叠 100 字符；不支持 OCR。任意格式仍可保存，无法解析的版本会保留原文并将索引标记失败。关闭 `DOCUMENT_INDEXING_ENABLED` 可停止自动入队与检索，但文档原文仍需要 MinIO。不要同时变更正在使用的模型标签内容；更换模型或配置后生成独立 profile，旧版本需显式重新提交索引。

MySQL 没有分块正文表或全文索引。历史内联内容字段仅用于升级兼容，新服务写入空值并保存 MinIO 对象引用。外部搜索内容不会直接当成可信引用；返回前重新核验数据库权限和发布记录，并校验 MinIO 清单摘要。服务职责、清理与恢复限制见 [存储与索引决策](docs/decisions/005-processing-storage.md)。

## Agent 模式

`.env` 中默认 `MAGENT_MODE` 为空：用户、Workspace、会话管理可用，发消息返回 503。

- `MAGENT_MODE=demo`：真实运行 Harness，但模型是确定性脚本，用来本地联调，不调用 LLM。
- `MAGENT_MODE=live`：设置 `MAGENT_BASE_URL`、`MAGENT_API_KEY`、`MAGENT_MODEL`，经兼容 Chat Completions 的接口调用模型。Base URL 使用服务商 API 根地址（DeepSeek 默认为 `https://api.deepseek.com`），适配器自动追加 `/chat/completions`。已使用真实 DeepSeek `deepseek-flash` 验证 SSE；普通 JSON 的兼容性验证仍使用本地模拟 provider。

live SSE 与 live JSON 均使用 Harness 的 `DeepSeekChatModel`，上游需要兼容其 `thinking: disabled`、`stream_options.include_usage`、`max_tokens` 请求字段及工具调用。JSON 会聚合执行结果，SSE 转发公开正文增量；两者均不输出原始 reasoning 或内部摘要。

工作台不会自动加载 Harness 目录中的环境文件。复用其配置时，将 `DEEPSEEK_API_KEY / DEEPSEEK_MODEL / DEEPSEEK_BASE_URL` 分别映射到 `MAGENT_API_KEY / MAGENT_MODEL / MAGENT_BASE_URL`，并设置 `MAGENT_MODE=live`。若 Harness 未配置模型或地址，当前原生适配器默认值为 `deepseek-flash` 和 `https://api.deepseek.com`；工作台仍需显式配置，不能沿用其他服务商的旧地址。

模型窗口、输出预留和私有摘要预算通过 Harness 服务环境显式配置，JSON/SSE 共用安全压缩与硬预算。授权历史按完整回合有界加载，普通聊天原文保持；运行内 checkpoint 可经既有日志恢复。配置及限制见[长上下文管理](docs/context.md)。

修改模型/预算配置后更新 `harness` 容器；模式、服务凭据或通信地址变更时同时更新 `backend`。Magent 提供 `search_knowledge`、`read_document`、`list_documents`、`search_conversations` 和 `ask_user`；Go 根据认证身份和 Run 文档范围处理请求。关闭索引时目录、历史会话和提问仍可用。`ask_user` 持久暂停，用户答复后恢复同一 Run；详见 [工具与提问 API](docs/api.md#agent-工具与用户提问)。新增 `create_document` / `update_document`，每次写入均通过原生 tool_approval 持久暂停，批准后追加不可变版本；详见[文档写入与审批](docs/api.md#agent-文档写入与审批)。支持小型TXT/Markdown原文分页、JSON/SSE批准或拒绝及持久写入回执；前端已接入完整审批展示。

回答使用 `[E:<32位证据ID>]` 引用本次 Run 实际获得的固定版本原文；JSON 和 SSE `run.completed` 均返回 `citations`，可按其中的 `passage_url` 获取原文片段，按 `source_url` 下载固定版本。引用读取重新鉴权并校验 MinIO 清单。文档换头不会改变旧引用，软删除后不可读取。定位为抽取文本的 UTF-8 字节区间，暂不提供 PDF 页码/高亮区域。

消息请求可带 `document_ids`：省略或 null 表示当前有权读取的 Workspace，`[]` 表示无文档，非空数组限制到指定文档（最多100个、不重复）。来源限制同时用于工具、最终提交和后续模型历史；旧回答从已删除或超出范围来源派生时，过滤发送给模型的回合，数据库历史保留。demo 模式用真实工具结果生成确定性引用，仍不调用模型。详见 [可信引用 API](docs/api.md#可信检索与引用)。

## 最小调用

```bash
curl -s http://127.0.0.1:8080/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com","password":"choose-a-long-password"}'
```

响应包含 `token`、`refresh_token`、`user` 和自动创建的个人 `workspaces`。随后使用响应中的真实 ID：

```bash
export TOKEN='<token>' WORKSPACE_ID='<workspace id>'
curl -s "http://127.0.0.1:8080/api/v1/workspaces/$WORKSPACE_ID/sessions" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"title":"我的第一段会话"}'
export SESSION_ID='<session id>'
curl -s "http://127.0.0.1:8080/api/v1/workspaces/$WORKSPACE_ID/sessions/$SESSION_ID/messages" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"content":"你好，请介绍一下你能做什么。"}'
```

需要流式输出时使用 `curl -N`：

```bash
curl -N "http://127.0.0.1:8080/api/v1/workspaces/$WORKSPACE_ID/sessions/$SESSION_ID/messages" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' -d '{"content":"请继续说明。"}'
```

live 模式转发上游实际文本增量；demo 仅用于验证流协议，不调用模型。浏览器使用 `fetch` 读取响应流，事件契约及断连处理见 [API 文档](docs/api.md#sse-文本会话)。

## 维护

- [API 与权限](docs/api.md)
- [项目架构](docs/architecture.md)
- [代码规范](docs/code-standards.md)、[验证命令](docs/testing.md)
- [本轮验收记录](docs/iterations/001-workspaces-sessions.md)
- [SSE 验收记录](docs/iterations/002-sse.md)
- [文档版本验收记录](docs/iterations/003-documents-versions.md)
- [处理与检索验收记录](docs/iterations/004-processing-indexes.md)
- [可信引用验收记录](docs/iterations/005-trusted-retrieval.md)
- [存储与 Agent 边界决策](docs/decisions/001-backend-boundaries.md)

基础工作台与维护流程见[迭代017](docs/iterations/017-web-workbench.md)，最新真实后端联调及刷新登录恢复见[迭代018](docs/iterations/018-real-web-session.md)，此前真实全链路验收见[迭代015](docs/iterations/015-minimal-web-live.md)。当前前端不含成员管理、PDF 内嵌预览及文档范围选择；这些已有后端能力可继续通过 API 使用（PDF 使用下载）。SSO / 邮件验证尚未实现。对外发布前仍需 HTTPS、备份恢复和部署策略；本地默认只监听回环地址。


### Redis

默认 Compose 已启用 Redis；运行 `scripts/init-env.py` 创建新配置时会生成独立密码。已有 `.env` 需设置 `REDIS_PASSWORD`，本机开发使用 `REDIS_ADDR=127.0.0.1:16379`，Compose backend 自动连接 `redis:6379`。端口仅绑定回环，`REDIS_PORT` 可调整。Redis AOF 保存在独立 `redis-data` 卷。

索引状态接口增加可选 `progress`（stage/chunks/updated_at），前端展示真实处理阶段。`GET .../sessions/{sessionID}/runs/{runID}/events` 为认证只读 SSE 订阅，接受 POST SSE 提供的 `Last-Event-ID: generation:sequence`；无游标从首条读取。409 表示缓存不完整或执行已更换，503 表示 Redis 不可用，应读取现有 Run/消息。事件保留30分钟、最多4096条。订阅不会重新执行 Agent，原 POST 断连仍取消执行。前端沿用显式恢复按钮，不自动重跑。`/readyz` 的 `redis` 字段可为 ready/degraded/disabled，数据库正常时降级仍可服务。
