"""Document-level BEIR-style ranking metrics (missing queries are never ignored)."""
import math
import random
import statistics

from .data import EvaluationError

METRIC_NAMES = ("nDCG@10", "Recall@5", "Recall@10", "Recall@20", "MRR@10", "MAP@10", "Precision@10")


def deduplicate_documents(hits, reverse_mapping):
    ranking, seen = [], set()
    for hit in hits:
        try:
            document_id = hit["chunk"]["document_id"]
            corpus_id = reverse_mapping[document_id]
        except (KeyError, TypeError):
            raise EvaluationError("search returned a document outside the evaluation mapping") from None
        if corpus_id not in seen:
            seen.add(corpus_id)
            ranking.append(corpus_id)
    return ranking


def query_metrics(ranking, judgments):
    if len(ranking) != len(set(ranking)):
        raise EvaluationError("rankings must be deduplicated at document level")
    relevant = {doc for doc, grade in judgments.items() if grade > 0}
    if not relevant:
        raise EvaluationError("query has no positive judgment")
    # BEIR/pytrec_eval ndcg_cut uses the relevance grade itself as gain.
    dcg = sum(judgments.get(doc, 0) / math.log2(rank + 2) for rank, doc in enumerate(ranking[:10]))
    ideal = sum(grade / math.log2(rank + 2) for rank, grade in
                enumerate(sorted(judgments.values(), reverse=True)[:10]))
    found, precisions, reciprocal = 0, [], 0.0
    for rank, doc in enumerate(ranking[:10], 1):
        if doc in relevant:
            found += 1
            precisions.append(found / rank)
            if not reciprocal:
                reciprocal = 1 / rank
    result = {"nDCG@10": dcg / ideal if ideal else 0.0,
              "MRR@10": reciprocal, "MAP@10": sum(precisions) / len(relevant),
              "Precision@10": found / 10}
    for cutoff in (5, 10, 20):
        result[f"Recall@{cutoff}"] = len(relevant.intersection(ranking[:cutoff])) / len(relevant)
    return result


def percentile(values, fraction):
    if not values:
        return None
    data = sorted(values)
    position = (len(data) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(data) - 1)
    return data[low] + (data[high] - data[low]) * (position - low)


def score_records(records, qrels):
    indexed = {}
    for record in records:
        qid = record.get("query_id")
        if qid not in qrels or qid in indexed:
            raise EvaluationError("rankings contain unknown or duplicate query IDs")
        ranking = record.get("ranking")
        latency = record.get("latency_ms")
        if (record.get("status") not in ("ok", "error") or not isinstance(ranking, list) or
                any(not isinstance(doc, str) or not doc for doc in ranking) or
                not isinstance(latency, (int, float)) or isinstance(latency, bool) or
                not math.isfinite(latency) or latency < 0):
            raise EvaluationError("ranking status, document IDs, or latency is invalid")
        indexed[qid] = record
    if set(indexed) != set(qrels):
        raise EvaluationError(f"query coverage incomplete: {len(indexed)}/{len(qrels)}")
    errors = sum(r.get("status") != "ok" for r in indexed.values())
    per_query = {}
    for qid, judgments in qrels.items():
        record = indexed[qid]
        # Explicit failed queries score zero and remain in the denominator.
        per_query[qid] = query_metrics(record.get("ranking", []) if record.get("status") == "ok" else [], judgments)
    aggregate = {key: statistics.mean(value[key] for value in per_query.values()) for key in METRIC_NAMES}
    latencies = [r["latency_ms"] for r in indexed.values() if r.get("status") == "ok"]
    return {"metrics": aggregate, "coverage": {"expected": len(qrels), "recorded": len(indexed),
            "successful": len(qrels) - errors, "failed": errors, "complete": errors == 0},
            "latency_ms": {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95),
                           "mean": statistics.mean(latencies) if latencies else None},
            "per_query": per_query}


def paired_compare(baseline, candidate, qrels, samples=10000, seed=42):
    if samples < 100:
        raise EvaluationError("bootstrap samples must be at least 100")
    before, after = score_records(baseline, qrels), score_records(candidate, qrels)
    if not before["coverage"]["complete"] or not after["coverage"]["complete"]:
        raise EvaluationError("comparison requires complete, error-free paired runs")
    qids = sorted(qrels)
    randomizer = random.Random(seed)
    differences = {metric: [after["per_query"][qid][metric] - before["per_query"][qid][metric]
                            for qid in qids] for metric in METRIC_NAMES}
    bootstrap = {metric: [] for metric in METRIC_NAMES}
    for _ in range(samples):
        indices = randomizer.choices(range(len(qids)), k=len(qids))
        for metric in METRIC_NAMES:
            bootstrap[metric].append(sum(differences[metric][i] for i in indices) / len(indices))
    return {"queries": len(qids), "bootstrap_samples": samples, "seed": seed,
            "delta_candidate_minus_baseline": {metric: {"mean": statistics.mean(differences[metric]),
                "ci95": [percentile(bootstrap[metric], .025), percentile(bootstrap[metric], .975)]}
                for metric in METRIC_NAMES},
            "latency_ms": {"baseline": before["latency_ms"], "candidate": after["latency_ms"]},
            "latency_caveat": "Sequential runs can differ in cache warmth/load/order; latency is descriptive, not a controlled causal estimate."}
