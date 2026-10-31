# 中文合成重排开发集与留出集 v1

本目录的地点、制度、数字和问题全部为本次新编写的虚构内容，不能用于实际业务。24 篇短文、18 条查询仅用于验证评测闭环和观察特定小样本；它不是生产业务集、外部独立标注或统计显著性证据，也不衡量生成答案或 Agent 任务成功率。

开发集包含档案阅览、实验设备、样本保管三个主题；留出集包含自行车维修、博物馆拍摄、社区地块三个不同主题。各有 12 篇文档、6 条有答案查询和 3 条无答案查询。相似文档中的角色、对象、时间和数量构成困难负例；无答案问题刻意使用相近领域但原文未规定的收费、规格和服务信息。全文全部进入同一临时索引，所有查询都检索完整语料。

`fixture.json` 中的 `relevant` 是文档级二元标注，空列表代表无答案；`hard_negatives` 为明确相近但不能回答该问题的文档。存在两个正确文档的题目须按全部正例计算召回率。标注是作者预先写定的，没有人工标注一致性审计，不能称为高质量业务标注。

离线校验不会访问网络或创建账号：

```bash
python3 -B scripts/eval-reranking-holdout.py --validate-only
python3 -B -m unittest scripts.test_reranking_holdout -v
```

真实检索模式要求已启动隔离应用栈和该后端实际使用的模型服务。模型服务认证密钥仅从 `RERANKER_TEST_API_KEY` 环境变量读取，不能放在命令参数或产物中。必须核对 `--reranker-info-url` 对应后端实际配置的模型端点；公共检索 API 没有暴露应用二进制、embedding digest 或可直接核验的端点绑定。

```bash
python3 -B scripts/eval-reranking-holdout.py \
  --base-url http://127.0.0.1:18080 \
  --reranker-info-url http://127.0.0.1:18091/info \
  --expected-model BAAI/bge-reranker-base \
  --output artifacts/evaluations/zh-reranking-holdout-v1
```

应用配置须在开始前固定为 10 个重排候选、阈值 0.2；模型和配置在整个运行中不得切换。脚本仅使用公开检索请求的 `rerank:false/true`，不会更改服务器全局设置。逐响应校验 threshold=0.2、实际评分候选不超过 10、固定 profile/policy 和来源版本；检查 `/info` 的实际模型/revision/设备及限制前后一致。模型错误、关闭重排、配置变化或来源变化使运行失败，不把回退结果算作成功重排。

开发集选择规则在模型请求前写入 `fixture.freeze.json`：重排的 nDCG@5、Recall@5 和无答案误召回率均不得变差，且 nDCG 至少增加 0.02 或无答案误召回率至少降低 0.25，warm HTTP p95 不超过 5000ms，才选重排。否则保留 baseline。这些门槛是本合成练习的预注册选择规则，不是项目的生产 SLO。

开发集选择和策略 ID 写入 `decision.freeze.json` 后才发出第一条留出查询。如果选重排，留出集仅比较 baseline 与已选配置；如果保留 baseline，留出只测 baseline，不再试用已被拒绝的候选。留出结果用于接受或拒绝本次候选，不允许据此重新选参数。默认每个 query/variant 一次首次观测和一次 warm 重复，质量分母只使用首次观测；warm 重复仅描述延迟与运行状态，不能冒充额外独立样本。首次请求也不是冷启动测量，因为导入索引可能已预热模型。

结果分别报告 nDCG/Recall/Hit/MRR@5、无答案查询中返回任意文档的比例、困难负例返回数量和首次/warm p50/p95。无答案误召回只是检索层代理指标，不是生成幻觉率。小样本顺序计时没有控制宿主负载，不能证明因果性能提升。

`status=completed` 仅表示完整实验和清理完成。策略结论另见 `conclusion`：开发集保留 baseline、留出确认重排或留出未确认重排。脚本拒绝覆盖或续跑已有产物；看过留出结果后另开目录重跑仍不是新的独立留出实验。每次注册临时账号并上传临时 Workspace，结束删除该 Workspace；由于没有删除账号 API，账号需随隔离栈清理。脚本不调用聊天或付费模型，不配置基础设施。

只通过 `--validate-only`、离线测试或单独 `/rerank` 服务测试，均不能声称此检索评测已执行。实际执行记录应引用包含来源核验、模型冻结、两阶段结果和清理状态的 `results.json`。
