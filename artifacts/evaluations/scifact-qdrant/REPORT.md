# Qdrant SciFact 全量评测

全部 5,183 篇语料、300 条 test 查询；三路实际 execution 均为 qdrant_native。完整环境核对凭据为 environment.json。

| 模式 | Recall@5 | Hit@5 | nDCG@10 | 失败查询 |
|---|---:|---:|---:|---:|
| lexical | 0.710000 | 0.733333 | 0.639900 | 0 |
| vector | 0.722944 | 0.743333 | 0.662562 | 0 |
| hybrid | 0.747722 | 0.770000 | 0.694130 | 0 |

Hit@5 按前五个不同文档是否命中至少一篇正例计算，失败查询计零，分母为全部 300 条 query。质量差异与区间见 hybrid-vs-lexical.json 和 hybrid-vs-vector.json；顺序延迟不是因果加速证据。业务集尚未运行。
