# Retrieval evaluation

Label: qdrant · Mode: vector

| Metric | Value |
|---|---:|
| nDCG@10 | 0.662562 |
| Recall@5 | 0.722944 |
| Recall@10 | 0.793000 |
| Recall@20 | 0.840333 |
| MRR@10 | 0.631717 |
| MAP@10 | 0.614267 |
| Precision@10 | 0.089000 |

Coverage: 300/300 successful; 0 failed.

Latency ms: {"mean": 165.4211390427857, "p50": 156.95722900272813, "p95": 225.49203365633733}

Unjudged documents are nonrelevant; failed queries remain in the metric denominator.
Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.
Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.

Observed server execution: qdrant_native.

Corpus verification: indexes; before=passed, after=passed.
