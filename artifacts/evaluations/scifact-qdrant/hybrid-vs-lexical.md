# Paired retrieval comparison

Paired queries: 300; bootstrap samples: 10000; seed: 42.

| Metric | Candidate − baseline | Bootstrap 95% CI |
|---|---:|---|
| nDCG@10 | 0.054229 | [0.027213, 0.082442] |
| Recall@5 | 0.037722 | [0.000500, 0.076335] |
| Recall@10 | 0.055611 | [0.025667, 0.087168] |
| Recall@20 | 0.060278 | [0.032443, 0.090503] |
| MRR@10 | 0.050308 | [0.017945, 0.083810] |
| MAP@10 | 0.054577 | [0.023332, 0.086949] |
| Precision@10 | 0.007667 | [0.004000, 0.011667] |

Document ranking parity: False; chunk ranking parity: False.

Observed execution: {"baseline": "qdrant_native", "candidate": "qdrant_native"}

Corpus verification: {"baseline": "indexes", "candidate": "indexes"}

Latency (ms): {"baseline": {"mean": 89.24047681968659, "p50": 88.34787500381935, "p95": 102.46969789732249}, "candidate": {"mean": 179.00053984553475, "p50": 171.06545850401744, "p95": 225.45919613912702}}

Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate.
