#!/usr/bin/env python3
"""Select rerank off/on on synthetic dev, then evaluate a frozen holdout once.

Uses an isolated application's authenticated upload/index/search API, never chat.
Only its temporary Workspace is removed; the temporary account remains. All
fixture facts are invented. This is not a business benchmark or production SLO.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parent
HELPER = ROOT / "smoke-reranking.py"
SPEC = importlib.util.spec_from_file_location("reranking_holdout_http", HELPER)
http = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(http)
CheckError, require, digest, save = http.CheckError, http.require, http.digest, http.save
FIXTURE_PATH = ROOT / "retrieval_eval/examples/reranking-holdout/fixture.json"
PROTOCOL = {
    "schema": 1, "mode": "hybrid", "limit": 5,
    "arms": {"baseline": {"rerank": False}, "rerank": {"rerank": True}},
    "selection": {"min_ndcg_gain": 0.02, "min_no_answer_reduction": 0.25,
                  "max_warm_p95_ms": 5000, "quality_regression_tolerance": 1e-12},
    "expected_threshold": 0.2, "max_candidates": 10,
    "quality": "First response only; binary document qrels, deduplicated at document level.",
    "no_answer": "Fraction of no-answer queries returning any document at API limit=5.",
    "split": "Dev and holdout have disjoint topic, document, query ID and query-text sets.",
    "order": "Alternate baseline/rerank first by query; warm passes reverse first-pass order.",
    "holdout": "Freeze dev choice before first holdout request; no re-selection on holdout.",
    "limits": [
        "Synthetic authored mini-set, not real business queries, independently annotated data or a statistical guarantee.",
        "Only off/on is selected; model, candidate budget and threshold are fixed before dev.",
        "No-answer returned_any is a retrieval false-return proxy, not answer hallucination or user satisfaction.",
        "First pass is not a cold-start measurement: indexing may already warm models.",
        "Warm HTTP p50/p95 are descriptive, with small sequential samples and uncontrolled host load.",
        "Policy and document/index identities are checked; the HTTP API does not expose application binary or embedding digests.",
        "The operator must bind the supplied /info URL to the reranker used by this isolated backend.",
        "A second run after viewing holdout cannot be presented as a fresh independent holdout.",
    ],
}


def load_fixture(path=FIXTURE_PATH):
    fixture = json.loads(Path(path).read_text(encoding="utf-8"))
    require(fixture.get("schema") == 1 and fixture.get("synthetic") is True,
            "fixture must be explicitly synthetic schema 1")
    documents, queries = fixture.get("documents", []), fixture.get("queries", [])
    require(documents and queries, "fixture requires documents and queries")
    mapping = {row["id"]: row for row in documents}
    require(len(mapping) == len(documents), "duplicate document IDs")
    require(len({row["id"] for row in queries}) == len(queries), "duplicate query IDs")
    require(len({row["text"].strip() for row in queries}) == len(queries), "duplicate query text")
    topics = {split: {row["topic"] for row in documents if row["split"] == split}
              for split in ("dev", "holdout")}
    require(all(topics.values()) and not topics["dev"] & topics["holdout"], "topic leakage across splits")
    for row in documents:
        require(row["split"] in topics and row["title"].strip() and row["text"].strip(),
                "invalid document split or content")
    for row in queries:
        relevant, negatives = row["relevant"], row["hard_negatives"]
        require(row["split"] in topics and row["topic"] in topics[row["split"]]
                and row["text"].strip(), "invalid query split or topic")
        require(len(relevant) == len(set(relevant)) and len(negatives) == len(set(negatives))
                and negatives and not set(relevant) & set(negatives), "invalid relevance/negative judgments")
        for document_id in relevant + negatives:
            require(document_id in mapping and mapping[document_id]["split"] == row["split"]
                    and mapping[document_id]["topic"] == row["topic"], "judgment crosses split/topic")
    for split in topics:
        cases = [row for row in queries if row["split"] == split]
        require(any(row["relevant"] for row in cases) and any(not row["relevant"] for row in cases),
                "each split requires answerable and no-answer cases")
    old_text = {row["text"] for row in http.FIXTURE["documents"] + http.FIXTURE["queries"]
                + http.FIXTURE["no_answer"]}
    require(not old_text & {row["text"] for row in documents + queries}, "fixture repeats old smoke examples")
    return fixture


def fixture_counts(fixture):
    return {split: {"documents": sum(row["split"] == split for row in fixture["documents"]),
                    "answerable": sum(row["split"] == split and bool(row["relevant"]) for row in fixture["queries"]),
                    "no_answer": sum(row["split"] == split and not row["relevant"] for row in fixture["queries"])}
            for split in ("dev", "holdout")}


def source_hashes(fixture_path):
    return {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in
            (("evaluator", Path(__file__).resolve()), ("http_helpers", HELPER),
             ("fixture", Path(fixture_path).resolve()))}


def model_info(url, key, expected_model):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme in ("http", "https") and parsed.netloc and parsed.path == "/info"
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
            "invalid reranker info URL")
    require(len(key) >= 32 and "\n" not in key and "\r" not in key, "RERANKER_TEST_API_KEY must be at least 32 characters")
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + key})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), http.NoRedirect())
    try:
        with opener.open(request, timeout=15) as response:
            require(response.status == 200, "reranker info HTTP status is not 200")
            raw = response.read(65537)
            require(len(raw) <= 65536, "reranker info exceeds size limit")
            info = json.loads(raw)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        raise CheckError("reranker info unavailable or malformed") from None
    require(isinstance(info, dict) and info.get("model_id") == expected_model
            and isinstance(info.get("model_sha"), str) and bool(info["model_sha"])
            and isinstance(info.get("model_type"), dict) and "reranker" in info["model_type"]
            and info.get("auto_truncate") is True, "loaded model identity is invalid or unexpected")
    for field in ("max_input_length", "max_batch_tokens", "max_client_batch_size"):
        require(type(info.get(field)) is int and info[field] > 0, "invalid reranker limits")
    require(info["max_input_length"] <= info["max_batch_tokens"]
            and info["max_client_batch_size"] >= PROTOCOL["max_candidates"], "reranker limits incompatible with protocol")
    result = {key: info[key] for key in ("model_id", "model_sha", "max_input_length", "max_batch_tokens",
                                       "max_client_batch_size", "auto_truncate")}
    for field in ("device", "requested_device"):
        if field in info:
            require(isinstance(info[field], str) and len(info[field]) <= 32, "invalid model device metadata")
            result[field] = info[field]
    return result


def latency_summary(rows):
    values = [row["latency_ms"] for row in rows]
    return {"samples": len(values), "p50": http.percentile(values, .5) if values else None,
            "p95": http.percentile(values, .95) if values else None}


def summarize(records, variants):
    summary = {}
    for variant in variants:
        rows = [row for row in records if row["variant"] == variant]
        first = [row for row in rows if row["pass"] == 0]
        positives = [row for row in first if row["answerable"]]
        negatives = [row for row in first if not row["answerable"]]
        require(positives and negatives, "missing answerable/no-answer observations")
        summary[variant] = {
            "answerable_queries": len(positives), "no_answer_queries": len(negatives),
            "metrics": {metric: statistics.mean(row["metrics"][metric] for row in positives)
                        for metric in ("Recall@5", "Hit@5", "MRR@5", "nDCG@5")},
            "no_answer_false_return_rate": statistics.mean(bool(row["ranking"]) for row in negatives),
            "hard_negative_queries_at_5": sum(bool(row["hard_negative_hits"]) for row in first),
            "first_pass_latency_ms": latency_summary(first),
            "warm_latency_ms": latency_summary([row for row in rows if row["pass"] > 0]),
            "outcomes": dict(Counter(row["rerank"]["outcome"] for row in rows)),
        }
    return summary


def candidate_gates(summary):
    baseline, candidate = summary["baseline"], summary["rerank"]
    rule = PROTOCOL["selection"]
    tolerance = rule["quality_regression_tolerance"]
    ndcg = candidate["metrics"]["nDCG@5"] - baseline["metrics"]["nDCG@5"]
    recall = candidate["metrics"]["Recall@5"] - baseline["metrics"]["Recall@5"]
    fewer_false = baseline["no_answer_false_return_rate"] - candidate["no_answer_false_return_rate"]
    warm_p95 = candidate["warm_latency_ms"]["p95"]
    return {"ndcg_non_regression": ndcg >= -tolerance, "recall_non_regression": recall >= -tolerance,
            "no_answer_non_regression": fewer_false >= -tolerance,
            "meaningful_gain": ndcg >= rule["min_ndcg_gain"] or fewer_false >= rule["min_no_answer_reduction"],
            "warm_latency_budget": warm_p95 is not None and warm_p95 <= rule["max_warm_p95_ms"]}


def select_on_dev(summary):
    gates = candidate_gates(summary)
    return {"selected": "rerank" if all(gates.values()) else "baseline", "dev_gates": gates,
            "rule_sha256": digest(PROTOCOL["selection"]), "dev_summary_sha256": digest(summary)}


def check_response(response, variant, mapping, profile, policies):
    value = http.validate_response(response, variant, mapping, profile, policies)
    require(response.get("execution") == "qdrant_native", "unexpected retrieval execution")
    if variant == "baseline":
        require(value["rerank"]["outcome"] == "disabled", "baseline did not explicitly disable reranking")
    else:
        diagnostic = value["rerank"]
        require(diagnostic.get("threshold") == PROTOCOL["expected_threshold"]
                and type(diagnostic.get("candidate_count")) is int
                and 0 < diagnostic["candidate_count"] <= PROTOCOL["max_candidates"],
                "rerank candidate/threshold configuration differs from fixed protocol")
    return value


def run_split(client, workspace, fixture, split, variants, mapping, profile, policies, warm_repeats, records, checkpoint):
    cases = [row for row in fixture["queries"] if row["split"] == split]
    for pass_number in range(warm_repeats + 1):
        for number, case in enumerate(cases):
            order = variants if (number + pass_number) % 2 == 0 else list(reversed(variants))
            for variant in order:
                started = time.perf_counter()
                response = client.request("POST", workspace + "/search",
                                          {"query": case["text"], "mode": "hybrid", "limit": 5,
                                           "rerank": variant == "rerank"})
                elapsed = (time.perf_counter() - started) * 1000
                value = check_response(response, variant, mapping, profile, policies)
                value.update(query_id=case["id"], variant=variant, split=split,
                             answerable=bool(case["relevant"]), latency_ms=elapsed)
                value["pass"] = pass_number
                value["hard_negative_hits"] = sorted(set(value["ranking"][:5]) & set(case["hard_negatives"]))
                if case["relevant"]:
                    value["metrics"] = http.quality(value["ranking"], case["relevant"])
                records.append(value)
                checkpoint()
    return summarize(records, variants)


def run(client, output, fixture_path=FIXTURE_PATH, info_reader=None, wait_timeout=900, poll_interval=1, warm_repeats=1):
    require(1 <= warm_repeats <= 3, "warm repeats must be 1..3")
    require(info_reader is not None, "model identity reader is required")
    fixture, output = load_fixture(fixture_path), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    require(not any(output.iterdir()), "output must be empty; holdout runs cannot be resumed or overwritten")
    frozen = {"fixture": fixture, "fixture_sha256": digest(fixture), "protocol": PROTOCOL,
              "protocol_sha256": digest(PROTOCOL), "source_sha256": source_hashes(fixture_path),
              "split_sha256": {split: digest([row for row in fixture["queries"] if row["split"] == split])
                               for split in ("dev", "holdout")}, "warm_repeats": warm_repeats}
    save(output / "fixture.freeze.json", frozen)
    report = {"schema": 1, "status": "running", "fixture_sha256": frozen["fixture_sha256"],
              "dev": [], "holdout": [], "holdout_started": False, "cleanup": "not_created",
              "limits": PROTOCOL["limits"], "retrieval_policy_ids": {}}
    workspace = None
    checkpoint = lambda: save(output / "results.json", report)
    try:
        model = info_reader()
        save(output / "model.freeze.json", model)
        report["model"] = model
        account = client.request("POST", "/auth/register", {
            "name": "Synthetic rerank holdout", "email": "holdout-" + uuid.uuid4().hex + "@example.test",
            "password": uuid.uuid4().hex}, expected=201)
        require(isinstance(account, dict) and isinstance(account.get("token"), str) and account["token"]
                and len(account.get("workspaces", [])) == 1, "invalid registration response")
        client.token = account["token"]
        workspace = "/workspaces/" + account["workspaces"][0]["id"]
        mapping = {}
        for document in fixture["documents"]:
            uploaded, source_sha = client.upload(workspace, document)
            mapping[document["id"]] = {"document_id": uploaded["document"]["id"],
                                        "version_id": uploaded["version"]["id"], "source_sha256": source_sha}
        outstanding, profile, deadline = dict(mapping), None, time.monotonic() + wait_timeout
        while outstanding:
            for key, entry in list(outstanding.items()):
                job = client.request("GET", workspace + "/documents/" + entry["document_id"]
                                     + "/versions/" + entry["version_id"] + "/index")
                require(job.get("status") != "failed", "fixture indexing failed")
                if job.get("status") == "ready":
                    require(profile is None or profile == job.get("profile_id"), "mixed index profiles")
                    profile = job["profile_id"]
                    entry.update(index_id=job["id"], generation=job["generation"])
                    del outstanding[key]
            if outstanding:
                require(time.monotonic() < deadline, "fixture indexing timed out")
                time.sleep(poll_interval)
        http.verify_indexes(client, workspace, mapping, profile)
        save(output / "index.freeze.json", {"profile_id": profile, "mapping": mapping})
        report["corpus_verification_before"] = "passed"
        policies = report["retrieval_policy_ids"]
        report["dev_summary"] = run_split(client, workspace, fixture, "dev", ["baseline", "rerank"],
                                           mapping, profile, policies, warm_repeats, report["dev"], checkpoint)
        require(info_reader() == model, "model changed during dev")
        http.verify_indexes(client, workspace, mapping, profile)
        decision = select_on_dev(report["dev_summary"])
        decision["retrieval_policy_ids"] = dict(policies)
        save(output / "decision.freeze.json", decision)
        report["decision"] = decision
        report["holdout_started"] = True
        checkpoint()
        variants = ["baseline", "rerank"] if decision["selected"] == "rerank" else ["baseline"]
        report["holdout_summary"] = run_split(client, workspace, fixture, "holdout", variants,
                                               mapping, profile, policies, warm_repeats, report["holdout"], checkpoint)
        require(json.loads((output / "decision.freeze.json").read_text()) == decision,
                "frozen dev decision changed during holdout")
        require(info_reader() == model, "model changed during holdout")
        http.verify_indexes(client, workspace, mapping, profile)
        require(source_hashes(fixture_path) == frozen["source_sha256"] and digest(load_fixture(fixture_path)) == frozen["fixture_sha256"],
                "evaluation source or fixture changed during run")
        report.update(status="completed", corpus_verification_after="passed")
        if decision["selected"] == "rerank":
            report["holdout_gates"] = candidate_gates(report["holdout_summary"])
            report["conclusion"] = "rerank_confirmed_on_synthetic_holdout" if all(report["holdout_gates"].values()) else "rerank_not_confirmed_on_holdout_no_retuning"
        else:
            report["conclusion"] = "retain_baseline_rerank_rejected_on_dev"
    except CheckError as error:
        report.update(status="failed", error=str(error))
    except (KeyError, TypeError, ValueError, OSError):
        report.update(status="failed", error="invalid API schema, model metadata or local artifact access")
    finally:
        if workspace:
            try:
                client.request("DELETE", workspace, expected=204)
                report["cleanup"] = "temporary_workspace_deleted_temporary_account_remains"
            except Exception:
                report.update(status="failed", cleanup="failed", cleanup_error="temporary Workspace deletion failed")
        checkpoint()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-only", action="store_true", help="validate fixture offline; makes no HTTP requests")
    parser.add_argument("--base-url", help="isolated app origin")
    parser.add_argument("--output", type=Path, help="new empty artifact directory")
    parser.add_argument("--reranker-info-url", help="the backend's actual model service /info URL")
    parser.add_argument("--expected-model", default="BAAI/bge-reranker-base")
    parser.add_argument("--wait-timeout", type=int, default=900)
    parser.add_argument("--warm-repeats", type=int, choices=(1, 2, 3), default=1)
    args = parser.parse_args()
    try:
        fixture = load_fixture()
        if args.validate_only:
            print(json.dumps({"status": "validated_offline_only", "fixture_sha256": digest(fixture),
                              "counts": fixture_counts(fixture)}, ensure_ascii=False, sort_keys=True))
            return 0
        require(args.base_url and args.output and args.reranker_info_url,
                "live mode requires --base-url, --output and --reranker-info-url")
        require(1 <= args.wait_timeout <= 7200, "wait timeout must be 1..7200 seconds")
        reader = lambda: model_info(args.reranker_info_url, os.environ.get("RERANKER_TEST_API_KEY", ""), args.expected_model)
        report = run(http.Client(args.base_url, timeout=125), args.output, info_reader=reader,
                     wait_timeout=args.wait_timeout, warm_repeats=args.warm_repeats)
        print(json.dumps({key: report[key] for key in ("status", "cleanup", "conclusion", "error") if key in report}))
        return 0 if report["status"] == "completed" else 1
    except (CheckError, OSError, ValueError, KeyError, TypeError) as error:
        print("FAIL: " + (str(error) if isinstance(error, CheckError) else "invalid fixture or local artifact access"))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
