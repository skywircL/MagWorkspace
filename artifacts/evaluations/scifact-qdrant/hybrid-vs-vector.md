# Paired retrieval comparison

Paired queries: 300; bootstrap samples: 10000; seed: 42.

| Metric | Candidate − baseline | Bootstrap 95% CI |
|---|---:|---|
| nDCG@10 | 0.031568 | [0.005439, 0.056986] |
| Recall@5 | 0.024778 | [-0.011667, 0.061667] |
| Recall@10 | 0.033278 | [-0.002000, 0.071833] |
| Recall@20 | 0.030611 | [-0.000946, 0.063444] |
| MRR@10 | 0.026131 | [-0.004226, 0.055236] |
| MAP@10 | 0.032650 | [0.003338, 0.061022] |
| Precision@10 | 0.003000 | [-0.001333, 0.007667] |

Document ranking parity: False; chunk ranking parity: False.

Observed execution: {"baseline": "qdrant_native", "candidate": "qdrant_native"}

Corpus verification: {"baseline": "indexes", "candidate": "indexes"}

Latency (ms): {"baseline": {"mean": 165.4211390427857, "p50": 156.95722900272813, "p95": 225.49203365633733}, "candidate": {"mean": 179.00053984553475, "p50": 171.06545850401744, "p95": 225.45919613912702}}

Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate.
