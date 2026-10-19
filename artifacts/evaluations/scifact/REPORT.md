# SciFact 全量检索评测报告

状态：**SciFact 完整链路验收完成；真实业务集尚未运行；端到端因果加速尚未证明。**

完整编排于 2026-10-16T12:57:57.461243+00:00 完成。四组结果各包含全部 300 条 test 查询，均成功、无失败、无查询截断。Hybrid 取得最高平均检索质量；并行执行策略保持了全部排名和分数，但本轮顺序测量不足以证明普遍加速。

## 评测口径

- **5183 篇文档、12933 个 chunk** 经真实 HTTP、MinIO、异步索引、Elasticsearch、本地 Ollama 链路处理，全部 ready，使用同一 profile。
- 本次只计分 **300 条 test 查询、339 条正相关标注**。queries.jsonl 其余 809 条 train 查询未参与计分；方法查询前固定，没有根据 test 调参，本轮未执行 train/dev 参数选择实验。
- 每次最多返回 50 个 chunk；按文档保留第一次命中去重。未标注文档按不相关计分。Recall@k 是逐查询召回比例的宏平均；Hit@5 是前 5 个不同文档中至少有一篇正例的查询比例。
- **Hit@5 是从冻结 JSONL 和 test.tsv 派生的补充指标，未修改已冻结 CLI 或内置指标。** [top5-metrics.json](top5-metrics.json) 绑定了标注文件及四份排名文件的原始字节 SHA256，并保留公式、查询数、命中数与派生区间，支持离线复核。

官方文件摘要和 corpus ID 不变。固定策略 beir-title-text-cc-space-v1 将不被生产 parser 接受的 Cc 字符替为空格，仅影响 **2 篇、5 字符**，通过真实 PUT /content 创建新不可变版本，保留旧来源。策略、原文件与规范化语料摘要进入 manifest，详见[规范化证据](../../../.cache/iteration009-control-normalization.json)。

## 全量结果

数值按全部 300 条 test 查询计分；表中显示 6 位小数，完整精度见各 summary JSON。

| 执行 / 检索模式 | nDCG@10 | Recall@5 | Recall@10 | Recall@20 | Hit@5 | MRR@10 | MAP@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline / lexical | 0.639405 | 0.720667 | 0.762167 | 0.812389 | 0.746667 | 0.609665 | 0.593084 |
| baseline / vector | 0.662562 | 0.722944 | 0.793000 | 0.840333 | 0.743333 | 0.631717 | 0.614267 |
| baseline / hybrid | 0.688555 | 0.741833 | 0.824444 | 0.882889 | 0.766667 | 0.652894 | 0.639891 |
| optimized / hybrid | 0.688555 | 0.741833 | 0.824444 | 0.882889 | 0.766667 | 0.652894 | 0.639891 |

前 5 个文档中至少命中一篇正例的查询数，依次为 **224/300、223/300、230/300、230/300**。因此当前 hybrid 的全量 Query Recall@5 为 **74.1833%**，Hit@5 为 **76.6667%**。

独立公式复算与 summary 的内置 7 项指标完全一致。四组排名均为 300 个唯一 test query ID；状态全部 ok，实际服务 execution 分别为 baseline、baseline、baseline、optimized；提交查询摘要、未截断标记及文档去重顺序通过复核。

当前 chunk 深度仍构成文档检索深度的上界：vector/hybrid 只有查询 971 在 50 个 chunk 去重后得到 18 篇文档，其 4 篇正例均已返回，Recall@20=1；其余查询至少有 20 篇不同文档。该限制应在不同业务语料上重新检查。

## 配对比较与准确结论

对相同 300 条查询做 paired percentile bootstrap，10000 次重采样，seed=42。以下为候选 hybrid 减去相应基线的差值与 95% 区间；区间未做多重比较校正。显示 8 位小数，完整精度见[hybrid 对 lexical](hybrid-vs-lexical.json)、[hybrid 对 vector](hybrid-vs-vector.json)；Hit@5 补充区间见 [top5-metrics.json](top5-metrics.json)。

| 指标 | hybrid − lexical：差值 [95% CI] | hybrid − vector：差值 [95% CI] |
|---|---|---|
| nDCG@10 | +0.04914961 [0.02366168, 0.07610181] | +0.02599264 [-0.00004005, 0.05119636] |
| Recall@5 | +0.02116667 [-0.01383333, 0.05683333] | +0.01888889 [-0.01816667, 0.05466806] |
| Recall@10 | +0.06227778 [0.02816667, 0.09722500] | +0.03144444 [-0.00244444, 0.06766667] |
| Recall@20 | +0.07050000 [0.04050000, 0.10266667] | +0.04255556 [0.01011111, 0.07645000] |
| Hit@5（补充） | +0.02000000 [-0.01666667, 0.05666667] | +0.02333333 [-0.01333333, 0.06000000] |
| MRR@10 | +0.04322884 [0.01355625, 0.07410582] | +0.02117725 [-0.00869362, 0.05079504] |
| MAP@10 | +0.04680741 [0.01821500, 0.07679884] | +0.02562407 [-0.00367718, 0.05438678] |
| Precision@10 | +0.00833333 [0.00433333, 0.01266667] | +0.00333333 [-0.00066667, 0.00766667] |

Hybrid 相比 lexical 的 nDCG@10、Recall@10/20、MRR@10、MAP@10 和 Precision@10 区间均不含 0，支持本次数据上的提升。相比 vector，Recall@20 区间不含 0；**nDCG@10 区间仍跨 0，不能声称已证明 nDCG 显著优于 vector**。Recall@5 和补充 Hit@5 对两路的区间都跨 0，均值更高不等于已证明稳定提升。

优化策略仅重叠执行独立的 lexical 分支和 embedding→vector 分支，保持既有融合、可信回读与最终权限检查。**300/300 查询的完整文档排名均相同，完整 chunk ID、顺序和 score 也逐项相同。** 内置 7 个质量指标及补充 Hit@5 的差值都是 0，配对区间为 [0, 0]。这是本次冻结条件下的结果保持证据，不是所有数据上的等价性证明。参见[优化配对比较](parallel-comparison.json)。

## 延迟与可观测证据

HTTP 客户端 wall-clock 延迟，不包含批次前后全量索引核验时间，未排除冷启动或预热请求。

| 执行 / 检索模式 | p50（ms） | p95（ms） |
|---|---:|---:|
| baseline / lexical | 96.952833 | 121.410215 |
| baseline / vector | 287.502124 | 1012.998029 |
| baseline / hybrid | 340.628417 | 891.884468 |
| optimized / hybrid | 346.082896 | 689.034873 |

优化组 p50 比基线 hybrid 高约 1.60%，p95 低约 22.74%。两次批次顺序执行，基线每个 query 的模式顺序固定为 lexical→vector→hybrid，缓存温度、模型预热、系统负载和执行顺序未做随机化或交叉控制。**这些延迟只能描述本轮观测，不能作为普遍或因果加速结论。**

[运行可观测证据](runtime-verification.json) 中的优化组记录：

- 全部 300 条 optimized 排名的 request_id 均与服务日志关联；Prometheus backend up=1，retrieval.search success=300。
- 从 Jaeger 实际读取并核验 3 条抽样 optimized trace，包含 scope、lexical、embed、vector、merge、resolve、hydrate、filter、search 等阶段。
- 三条样本中 lexical 与 embed 时间分别重叠 **13.108 ms、8.123 ms、18.358 ms**，证明本次运行中并行分支实际发生。采样跨度重叠不等同于端到端加速，也不能代表全部查询的重叠分布。

## 冻结、恢复与可复核范围

[最终环境](environment.json) 已生成，并通过与[冻结环境](environment.freeze.json) 的一致性检查：83 个源码/构建输入、应用 binary、评测器、模型 digest、corpus/profile 和物理索引均绑定摘要。模型为 bge-m3:567m（F16、1024 维）；索引为 1 primary、0 replica、100ms refresh；查询前合并为单 segment _4th（generation 6245，12933 docs、0 deleted），两批 UUID/segment 形状相同，详见[索引准备](index-preparation.json)。完整摘要保留在原始 JSON，不在此重复。

每份 summary 绑定 manifest 与原始排名 SHA256；固定内容摘要、文档头/确定性标题和全部 5183 个 ready index 的 ID/generation/profile/source SHA 均经严格核验。

切换优化服务时 Docker Hub 匿名 token EOF 中断了冗余构建，随后用本地冻结 image、compose --no-build --pull never 恢复，binary/source 摘要保持一致。基线排名未经改写而复用，恢复时再次核验全索引；最终基线 summary 时间对应最近一次恢复核验，不代表查询重跑。参见[镜像恢复证据](recovery.json)。

本次已跑通可恢复的完整检索评测、结果保持校验以及日志→trace→指标关联。真实业务文档、查询与人工标注尚未提供，**业务场景未验证**；仓库中的 synthetic 中文模板仅验证格式和流程，不能作为业务通过证据。检索评测也不包含回答生成正确性评测。
