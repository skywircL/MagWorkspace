# Retrieval evaluation

Label: qdrant · Mode: lexical

| Metric | Value |
|---|---:|
| nDCG@10 | 0.639900 |
| Recall@5 | 0.710000 |
| Recall@10 | 0.770667 |
| Recall@20 | 0.810667 |
| MRR@10 | 0.607540 |
| MAP@10 | 0.592341 |
| Precision@10 | 0.084333 |

Coverage: 300/300 successful; 0 failed.

Latency ms: {"mean": 89.24047681968659, "p50": 88.34787500381935, "p95": 102.46969789732249}

Unjudged documents are nonrelevant; failed queries remain in the metric denominator.
Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.
Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.

Observed server execution: qdrant_native.

Corpus verification: indexes; before=passed, after=passed.
