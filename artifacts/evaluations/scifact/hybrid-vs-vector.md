# Paired retrieval comparison

Paired queries: 300; bootstrap samples: 10000; seed: 42.

| Metric | Candidate − baseline | Bootstrap 95% CI |
|---|---:|---|
| nDCG@10 | 0.025993 | [-0.000040, 0.051196] |
| Recall@5 | 0.018889 | [-0.018167, 0.054668] |
| Recall@10 | 0.031444 | [-0.002444, 0.067667] |
| Recall@20 | 0.042556 | [0.010111, 0.076450] |
| MRR@10 | 0.021177 | [-0.008694, 0.050795] |
| MAP@10 | 0.025624 | [-0.003677, 0.054387] |
| Precision@10 | 0.003333 | [-0.000667, 0.007667] |

Document ranking parity: False; chunk ranking parity: False.

Observed execution: {"baseline": "baseline", "candidate": "baseline"}

Corpus verification: {"baseline": "indexes", "candidate": "indexes"}

Latency (ms): {"baseline": {"mean": 388.72171662267647, "p50": 287.5021244981326, "p95": 1012.9980288496882}, "candidate": {"mean": 415.1235251263036, "p50": 340.6284165030229, "p95": 891.8844683990756}}

Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate.
