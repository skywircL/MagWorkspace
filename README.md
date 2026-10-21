# MagWorkspace

个人文档与知识库工作台，包含 Go + Gin 后端和同源内嵌 Web 工作台。已实现用户、Workspace / 成员权限、私有会话和独立 Magent Harness 接入，以及文档不可变版本、异步处理、混合检索、二阶段重排和运行内可信引用。基础设施统一使用 **Docker Compose**。

可观测支持结构化日志、分阶段指标与可选 OpenTelemetry，Docker Jaeger/Prometheus 的入口见[可观测文档](docs/observability.md)。检索评测通过真实上传/索引/检索入口运行，使用完整 SciFact 和统一业务集格式，支持断点恢复、逐query结果与配对比较；命令和统计口径见[检索评测](docs/retrieval-evaluation.md)，实际完成状态见[迭代009](docs/iterations/009-evaluation-observability.md)。

个人长期记忆保存于 MySQL，按用户与 Workspace 隔离，跨 Session 和服务重启生效。成功对话后由后台自动提取用户陈述；聊天中的“请记住：…”直接进入保存流程，不调用提取模型。侧栏「个人记忆」支持纠正、开启/关闭、查看历史、导出和清空，无需手动新增。回答下方可查看记忆处理状态；自动提取使用现有 Harness 模型并产生额外调用，召回本身不调用模型。资料与偏好作为有界背景，事实与事项按当前问题召回，模型注入前回查当前权威状态。接口及遗忘边界见[个人记忆 API](docs/api.md#个人长期记忆)。

消息接口支持 SSE：添加 `Accept: text/event-stream` 可接收 Harness 原生文本增量；默认仍返回 JSON。实现与验证记录见 [迭代 002](docs/iterations/002-sse.md)。

文档支持上传、标题修改、TXT/Markdown 在线编辑、历史版本与下载，内容更新通过当前版本 ID 检测冲突并自动排队索引。原始文件默认最大 50 MB，通过 `MAX_FILE_SIZE_MB` 配置（1–512，1 MB = 1,048,576 字节，修改后重启），在线正文最大 1 MiB，原文件存入 MinIO；MySQL 保存业务状态、元数据及索引发布后的父子分块正文。调用示例见 [文档 API](docs/api.md#文档与不可变版本)，更新验收见 [迭代 007](docs/iterations/007-document-updates.md)。

## 启动

需要 Docker，以及相邻的 `../Magent Harness` 源码目录；不会修改该目录。Go 后端和 FastAPI Harness 分为两个容器，运行时无需在宿主机安装 Go 或 Python。

```bash
python3 scripts/init-env.py
docker compose --profile app up -d --build
```

`init-env.py` 为 MySQL / MinIO / Qdrant / Redis、内部 Harness API 和本地重排服务生成随机凭据，遇到已有 `.env` 会停止。升级已有环境时须补齐 `.env.example` 的 `MAGENT_SERVICE_API_KEY` 和 `RERANK_API_KEY`（各至少32字符随机值）；不能用聊天模型 API Key 代替。聊天模型凭据只注入 Harness 容器，Go 持内部服务通信凭据。配置说明见[Harness HTTP 服务](docs/harness-http.md)。

首次启动会下载本地 bge-m3 向量模型及独立的多语言 mMARCO MiniLM 重排模型；后续复用各自模型卷。默认重排模型下载约493 MB（470 MiB），慢速网络需要等待。macOS Docker 两类模型均使用 CPU，索引和搜索速度受资源影响。`GET /healthz` 检查 Go 进程，`GET /readyz` 检查数据库并报告 Redis 状态；Harness 的健康端点默认在 `http://127.0.0.1:18090/healthz`，重排模型的加载健康端点为 `http://127.0.0.1:18091/health`。Go 就绪不代表重排模型已经加载；下载和验证方式见[重排运行时](docker/reranker/README.md)。

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
| MySQL | 用户、Workspace/权限、私有会话与提问状态、个人长期记忆与墓碑、文档及版本元数据、父子分块正文及关系、索引任务状态与发布记录、来源关系及待清理对象记录 |
| MinIO | 原始文件、私有执行快照与运行日志 |
| Redis | Asynq 索引执行与去重、短期处理进度和 SSE 补发；可靠投递与重试预算由 MySQL outbox 持有 |
| Qdrant | 子块的内置多语言 BM25、向量索引与 RRF，可由原文重建 |
| Ollama + bge-m3 | 为子块和查询本地生成 1024 维向量 |
| TEI + mMARCO MiniLM | 对已召回且重新鉴权的子块进行相关性评分；不扫描全库，不写索引 |
| Tika | 在受限容器中解析 PDF / DOCX；TXT / Markdown 在 Go 内处理 |

上传成功即排队，查询版本 `/index` 观察 `queued/running/ready/failed`。检索支持 `lexical/vector/hybrid`，默认 hybrid；只返回当前版本的已发布索引，引用固定版本与分块。失败任务通过同一版本 POST `/index` 重试。详见 [API](docs/api.md#文档处理与检索)。

解析上限 1 MiB UTF-8 文本；唯一策略 `CHUNKING_STRATEGY=parent_child_v1`，结构优先切父块（最多4096 Unicode字符、无重叠），再在父内切子块（384字符、语义重叠最多76），总计最多8192块。父子正文、偏移、摘要和章节路径保存在MySQL，只有子块送入Qdrant；检索与重排按子块评分、同父合并，再返回完整父正文及其引用身份。阅读和引用回查不下载MinIO清单、不重新分块。原文件继续保存在MinIO；不支持OCR。旧策略拒绝启动，旧测试数据按本轮授权清理，重新上传即可按新策略处理。实现与验收见[父子分块迭代](docs/iterations/036-parent-child-chunks.md)。无法解析的版本保留原文并标记失败。关闭 `DOCUMENT_INDEXING_ENABLED` 可停止自动入队与检索；原文件下载仍需要MinIO。更换embedding或解析配置生成独立profile，旧版本需要显式重新提交索引。

MySQL `chunks.content` 保存父子正文，全部分块与索引 ready 状态在同一事务发布；MySQL 不实现文档全文索引。外部搜索结果仅作候选 ID 提示，返回前从 MySQL 回读正文，重新核验权限、版本、发布 generation、摘要与父子关系。旧单层索引与清单格式不兼容；本轮测试文档及派生数据清理保留账号、Workspace、聊天和长期记忆，旧文档来源的聊天引用失效。服务职责、清理与兼容边界见[父子分块决策](docs/decisions/026-parent-child-relational-chunks.md)。

## 召回与重排

Compose 默认启用本地重排：关键词与向量子候选经 RRF 融合后，先可信回读，再构造“当前文档标题＋章节路径＋清洗后的子块正文”，调用重排模型，完成阈值筛选、综合评分和 MMR 去冗余。同父保留最强子块，再选择不同父块 Top K；检索 API 与 Agent 工具共用实现。章节路径独立保存，评分输入不改写正文；返回的父引用使用真实父块 ID、字节与摘要。

默认最多给模型20个候选，可配置到200；混合RRF先除以两路第一的理论得分 `2/61`，重排不再按候选最大分缩放；模型相关性分数、该检索分数与来源权重按0.6/0.3/0.1合成，MMR使用正文的中文词项Jaccard相似度。模型失败或超时退回已验证的召回结果；模型成功但相关性过低可以返回空结果，两者在API诊断中明确区分。接口字段、阈值与分数含义见[检索API](docs/api.md#文档处理与检索)。

| 配置 | 默认值及用途 |
|---|---|
| `RERANK_ENABLED` | Compose/新 `.env` 为 `true`；直接运行未配置环境的 Go 程序为 `false` |
| `RERANK_URL` | Go 本地开发默认 `http://127.0.0.1:18091/rerank`；Compose 使用内部 `reranker:8091/rerank` |
| `RERANK_API_KEY` | 至少32字符；与独立容器共享的内部认证凭据 |
| `RERANK_MODEL` / `RERANK_MODEL_REVISION` | `cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` / `1427fd652930e4ba29e8149678df786c240d8825` |
| `RERANK_MAX_CANDIDATES` | `20`，允许 `1..200`；增加前须按运行硬件测量延迟 |
| `RERANK_THRESHOLD` | `0.2`，允许 `0..1`；不是答案正确概率 |
| `RERANK_MAX_PASSAGE_RUNES` | `2048`，模型输入文本字符预算，允许 `128..8192` |
| `RERANK_MAX_INPUT_TOKENS` | `1024`，TEI批次token预算；单对query/passage还受模型窗口限制，默认模型实际最多512 tokens |
| `RERANK_REQUEST_TIMEOUT` | `30s`，允许 `1s..1m`；还受当前请求剩余时间限制 |

更换重排模型或这些参数只改变请求时的 `retrieval_policy_id`，不改变持久索引 profile，无需重新生成 embedding。分块策略切换仍遵守上文重新索引规则。API可传 `"rerank": false` 作同索引基线比较，前端与Agent默认使用服务配置。新评测按policy冻结并追踪模型降级，不能续写旧无policy的成绩；本轮进度与实际验收状态见[迭代032](docs/iterations/032-retrieval-reranking.md)。

## Agent 模式

`.env` 中默认 `MAGENT_MODE` 为空：用户、Workspace、会话管理可用，发消息返回 503。

- `MAGENT_MODE=demo`：真实运行 Harness，但模型是确定性脚本，用来本地联调，不调用 LLM。
- `MAGENT_MODE=live`：设置 `MAGENT_BASE_URL`、`MAGENT_API_KEY`、`MAGENT_MODEL`，经兼容 Chat Completions 的接口调用模型。Base URL 使用服务商 API 根地址（DeepSeek 默认为 `https://api.deepseek.com`），适配器自动追加 `/chat/completions`。已使用真实 DeepSeek `deepseek-flash` 验证 SSE；普通 JSON 的兼容性验证仍使用本地模拟 provider。

live SSE 与 live JSON 均使用 Harness 的 `DeepSeekChatModel`，上游需要兼容其 `thinking: disabled`、`stream_options.include_usage`、`max_tokens` 请求字段及工具调用。JSON 会聚合执行结果，SSE 转发公开正文增量；两者均不输出原始 reasoning 或内部摘要。

工作台不会自动加载 Harness 目录中的环境文件。复用其配置时，将 `DEEPSEEK_API_KEY / DEEPSEEK_MODEL / DEEPSEEK_BASE_URL` 分别映射到 `MAGENT_API_KEY / MAGENT_MODEL / MAGENT_BASE_URL`，并设置 `MAGENT_MODE=live`。若 Harness 未配置模型或地址，当前原生适配器默认值为 `deepseek-flash` 和 `https://api.deepseek.com`；工作台仍需显式配置，不能沿用其他服务商的旧地址。

模型窗口、输出预留和私有摘要预算通过 Harness 服务环境显式配置，JSON/SSE 共用安全压缩与硬预算。授权历史按完整回合有界加载，普通聊天原文保持；运行内 checkpoint 可经既有日志恢复。配置及限制见[长上下文管理](docs/context.md)。

修改模型/预算配置后更新 `harness` 容器；模式、服务凭据或通信地址变更时同时更新 `backend`。Magent 提供 `search_knowledge`、`read_document`、`list_documents`、`search_conversations` 和 `ask_user`；Go 根据认证身份和 Run 文档范围处理请求。关闭索引时目录、历史会话和提问仍可用。`ask_user` 持久暂停，用户答复后恢复同一 Run；详见 [工具与提问 API](docs/api.md#agent-工具与用户提问)。新增 `create_document` / `update_document`，每次写入均通过原生 tool_approval 持久暂停，批准后追加不可变版本；详见[文档写入与审批](docs/api.md#agent-文档写入与审批)。支持小型TXT/Markdown原文分页、JSON/SSE批准或拒绝及持久写入回执；前端已接入完整审批展示。

回答使用 `[E:<32位证据ID>]` 引用本次 Run 实际获得的固定版本原文；JSON 和 SSE `run.completed` 均返回 `citations`，可按其中的 `passage_url` 获取原文片段，按 `source_url` 下载固定版本。引用读取重新鉴权并校验 MySQL 中固定 generation 的正文与摘要。新策略下文档换头不会改变历史父引用，软删除后不可读取。定位为抽取文本的 UTF-8 字节区间，暂不提供 PDF 页码/高亮区域。

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

索引采用事务 outbox + Asynq：原文版本与投递意图同时提交，Redis 队列丢失可重建，重复投递不会覆盖已发布结果。临时故障默认自动重试3次，可用 `INDEX_MAX_RETRIES=0..10` 调整；耗尽后保留失败原因，仍可在文档详情手动重试。Redis 暂时不可用时上传意图保留，恢复后继续执行。只在未设置 `REDIS_ADDR` 的开发入口使用数据库轮询。恢复流程与边界见[可靠索引投递](docs/decisions/025-index-outbox-asynq.md)。

索引状态接口增加可选 `progress`（stage/chunks/updated_at），前端展示真实处理阶段。`GET .../sessions/{sessionID}/runs/{runID}/events` 为认证只读 SSE 订阅，接受 POST SSE 提供的 `Last-Event-ID: generation:sequence`；无游标从首条读取。409 表示缓存不完整或执行已更换，503 表示 Redis 不可用，应读取现有 Run/消息。事件保留30分钟、最多4096条。订阅不会重新执行 Agent，原 POST 断连仍取消执行。前端沿用显式恢复按钮，不自动重跑。`/readyz` 的 `redis` 字段可为 ready/degraded/disabled，数据库正常时降级仍可服务。
