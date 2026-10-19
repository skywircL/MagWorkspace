# Retrieval evaluation

Label: baseline · Mode: lexical

| Metric | Value |
|---|---:|
| nDCG@10 | 0.639405 |
| Recall@5 | 0.720667 |
| Recall@10 | 0.762167 |
| Recall@20 | 0.812389 |
| MRR@10 | 0.609665 |
| MAP@10 | 0.593084 |
| Precision@10 | 0.084000 |

Coverage: 300/300 successful; 0 failed.

Latency ms: {"mean": 100.38135573617183, "p50": 96.9528334971983, "p95": 121.4102152021951}

Unjudged documents are nonrelevant; failed queries remain in the metric denominator.
Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.
Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.

Observed server execution: baseline.

Corpus verification: indexes; before=passed, after=passed.
