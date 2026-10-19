# Retrieval evaluation

Label: baseline · Mode: hybrid

| Metric | Value |
|---|---:|
| nDCG@10 | 0.688555 |
| Recall@5 | 0.741833 |
| Recall@10 | 0.824444 |
| Recall@20 | 0.882889 |
| MRR@10 | 0.652894 |
| MAP@10 | 0.639891 |
| Precision@10 | 0.092333 |

Coverage: 300/300 successful; 0 failed.

Latency ms: {"mean": 415.1235251263036, "p50": 340.6284165030229, "p95": 891.8844683990756}

Unjudged documents are nonrelevant; failed queries remain in the metric denominator.
Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.
Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.

Observed server execution: baseline.

Corpus verification: indexes; before=passed, after=passed.
