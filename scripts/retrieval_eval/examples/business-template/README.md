# 中文业务集格式模板（synthetic）

本目录是完全虚构的合成数据，只演示 BEIR 文件格式与 train/dev/test 分离。文档中的金额、时限和操作均为编造，不能用于实际业务。运行本目录只验证格式或流程，不能据此声称真实业务场景评测通过。

实际业务集应由有权限的业务人员提供文档、真实查询与人工相关性标注，保留独立的测试集；参数选择使用 train/dev，最终方案固定后再评估 test。当前模板只有 3 篇文档和 6 个查询，不能验证优化有效性。

| 文件 | 字段 |
|---|---|
| `corpus.jsonl` | 每行 `_id`、`title`、`text`，一篇业务文档一个稳定字符串 ID |
| `queries.jsonl` | 每行 `_id`、`text`，所有 split 的查询放在同一文件 |
| `qrels/train.tsv`、`qrels/dev.tsv`、`qrels/test.tsv` | TSV 表头 `query-id`、`corpus-id`、`score`；0 为不相关，正整数为相关等级 |

不同 split 使用不同 query ID；不要把调参过程中读过的测试查询重新标作未见测试集。评测以文档为单位，chunk 命中按对应文档第一次出现的顺序去重；未标注文档按不相关计分。正式业务集应事先确定标注范围和相关性标准，并复核同义查询与重复文档，避免把缺少标注当成检索错误。

在项目根目录验证此合成模板：

```bash
python3 -m scripts.retrieval_eval validate \
  --dataset scripts/retrieval_eval/examples/business-template --split test
```

接入真实业务集时复制格式并替换全部合成内容，使用新的输入目录、state 和结果 label。保留每次产物中的输入 SHA256、索引 profile、检索配置和覆盖率；出现失败查询时，先修复失败再进行可验收的比较。
