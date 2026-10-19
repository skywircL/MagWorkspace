# Retrieval evaluation

Label: qdrant · Mode: hybrid

| Metric | Value |
|---|---:|
| nDCG@10 | 0.694130 |
| Recall@5 | 0.747722 |
| Recall@10 | 0.826278 |
| Recall@20 | 0.870944 |
| MRR@10 | 0.657848 |
| MAP@10 | 0.646917 |
| Precision@10 | 0.092000 |

Coverage: 300/300 successful; 0 failed.

Latency ms: {"mean": 179.00053984553475, "p50": 171.06545850401744, "p95": 225.45919613912702}

Unjudged documents are nonrelevant; failed queries remain in the metric denominator.
Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.
Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.

Observed server execution: qdrant_native.

Corpus verification: indexes; before=passed, after=passed.
