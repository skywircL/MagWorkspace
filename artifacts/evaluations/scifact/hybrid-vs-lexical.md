# Paired retrieval comparison

Paired queries: 300; bootstrap samples: 10000; seed: 42.

| Metric | Candidate − baseline | Bootstrap 95% CI |
|---|---:|---|
| nDCG@10 | 0.049150 | [0.023662, 0.076102] |
| Recall@5 | 0.021167 | [-0.013833, 0.056833] |
| Recall@10 | 0.062278 | [0.028167, 0.097225] |
| Recall@20 | 0.070500 | [0.040500, 0.102667] |
| MRR@10 | 0.043229 | [0.013556, 0.074106] |
| MAP@10 | 0.046807 | [0.018215, 0.076799] |
| Precision@10 | 0.008333 | [0.004333, 0.012667] |

Document ranking parity: False; chunk ranking parity: False.

Observed execution: {"baseline": "baseline", "candidate": "baseline"}

Corpus verification: {"baseline": "indexes", "candidate": "indexes"}

Latency (ms): {"baseline": {"mean": 100.38135573617183, "p50": 96.9528334971983, "p95": 121.4102152021951}, "candidate": {"mean": 415.1235251263036, "p50": 340.6284165030229, "p95": 891.8844683990756}}

Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate.
