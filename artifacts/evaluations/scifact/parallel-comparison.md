# Paired retrieval comparison

Paired queries: 300; bootstrap samples: 10000; seed: 42.

| Metric | Candidate − baseline | Bootstrap 95% CI |
|---|---:|---|
| nDCG@10 | 0.000000 | [0.000000, 0.000000] |
| Recall@5 | 0.000000 | [0.000000, 0.000000] |
| Recall@10 | 0.000000 | [0.000000, 0.000000] |
| Recall@20 | 0.000000 | [0.000000, 0.000000] |
| MRR@10 | 0.000000 | [0.000000, 0.000000] |
| MAP@10 | 0.000000 | [0.000000, 0.000000] |
| Precision@10 | 0.000000 | [0.000000, 0.000000] |

Document ranking parity: True; chunk ranking parity: True.

Observed execution: {"baseline": "baseline", "candidate": "optimized"}

Corpus verification: {"baseline": "indexes", "candidate": "indexes"}

Latency (ms): {"baseline": {"mean": 415.1235251263036, "p50": 340.6284165030229, "p95": 891.8844683990756}, "candidate": {"mean": 400.105802493005, "p50": 346.08289600146236, "p95": 689.034872601042}}

Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate.
