"""python3 -m scripts.retrieval_eval --help"""
import argparse
import json
import sys
from pathlib import Path

from .api import APIClient, atomic_json
from .data import EvaluationError, dataset_info, load_dataset
from .metrics import score_records
from .runner import compare_runs, comparison_markdown, evaluate, ingest, load_records, summary_markdown


def parser():
    result = argparse.ArgumentParser(description="Strict document-level BEIR evaluation through MagWorkspace HTTP")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("validate", "ingest", "run", "score", "compare"):
        command = commands.add_parser(name)
        command.add_argument("--dataset", required=True, help="BEIR directory containing corpus.jsonl, queries.jsonl, qrels/")
        command.add_argument("--split", default="test", help="qrels split; never tune using test judgments")
        if name in ("ingest", "run"):
            command.add_argument("--base-url", required=True)
            command.add_argument("--token-env", default="MAGWORKSPACE_EVAL_TOKEN")
            command.add_argument("--auth-file", help="owner-only JSON {token,refresh_token}; auto-refreshes; keep outside artifacts")
            command.add_argument("--state", required=True, help="resumable mapping JSON, contains no credentials")
            command.add_argument("--request-timeout", default=45, type=float)
        if name == "ingest":
            command.add_argument("--workers", default=2, type=int)
            command.add_argument("--wait-timeout", default=21600, type=float)
            command.add_argument("--poll-interval", default=2, type=float)
        if name == "run":
            command.add_argument("--modes", default="lexical,vector,hybrid")
            command.add_argument("--label", required=True, help="e.g. baseline or optimized; immutable manifest identity")
            command.add_argument("--output", default="artifacts/evaluations")
            command.add_argument("--chunk-limit", default=50, type=int)
            command.add_argument("--query-policy", choices=("fail", "truncate_utf8"), default="fail")
            command.add_argument("--retry-errors", action="store_true", help="retry recorded failures; retain originals in failed-attempts.jsonl")
            command.add_argument("--verify-indexes", action=argparse.BooleanOptionalAction, default=True,
                                 help="default: verify every frozen index before/after querying; --no-verify-indexes checks heads only")
            command.add_argument("--expected-execution", choices=("baseline", "optimized", "qdrant_native"),
                                 help="require server-reported execution strategy; otherwise freeze first observed strategy")
            command.add_argument("--environment-file", help="non-credential frozen source/runtime/model JSON; must remain identical on resume")
        if name == "score":
            command.add_argument("--rankings", required=True)
            command.add_argument("--output", required=True, help="summary JSON path, Markdown written beside it")
        if name == "compare":
            command.add_argument("--baseline", required=True)
            command.add_argument("--candidate", required=True)
            command.add_argument("--output", required=True)
            command.add_argument("--bootstrap-samples", default=10000, type=int)
            command.add_argument("--seed", default=42, type=int)
    return result


def main(argv=None):
    arguments = parser().parse_args(argv)
    try:
        dataset = load_dataset(arguments.dataset, arguments.split)
        if arguments.command == "validate":
            print(json.dumps(dataset_info(dataset), ensure_ascii=False, indent=2))
            return 0
        if arguments.command in ("ingest", "run"):
            client = APIClient(arguments.base_url, arguments.token_env, arguments.auth_file, arguments.request_timeout)
            if arguments.command == "ingest":
                result = ingest(client, dataset, arguments.state, arguments.workers, arguments.wait_timeout, arguments.poll_interval)
                print(json.dumps(result, sort_keys=True))
                return 0
            summaries = evaluate(client, dataset, arguments.state, arguments.output,
                                 arguments.modes.split(","), arguments.label, arguments.chunk_limit, arguments.query_policy,
                                 retry_errors=arguments.retry_errors, verify_indexes=arguments.verify_indexes,
                                 expected_execution=arguments.expected_execution, environment_file=arguments.environment_file)
            print(json.dumps({mode: {"metrics": item["metrics"], "coverage": item["coverage"], "latency_ms": item["latency_ms"]}
                              for mode, item in summaries.items()}, sort_keys=True))
            return 0 if all(s["coverage"]["complete"] for s in summaries.values()) else 1
        if arguments.command == "score":
            result = score_records(load_records(arguments.rankings), dataset["qrels"])
            result["dataset"] = dataset_info(dataset)
            atomic_json(arguments.output, result)
            Path(arguments.output).with_suffix(".md").write_text(summary_markdown(result), encoding="utf-8")
            return 0 if result["coverage"]["complete"] else 1
        result = compare_runs(arguments.baseline, arguments.candidate, dataset, arguments.bootstrap_samples, arguments.seed)
        atomic_json(arguments.output, result)
        Path(arguments.output).with_suffix(".md").write_text(comparison_markdown(result), encoding="utf-8")
        print(json.dumps({"queries": result["queries"], "ranking_parity": result["ranking_parity"], "latency_ms": result["latency_ms"]}, sort_keys=True))
        return 0
    except EvaluationError as error:
        print("evaluation failed: " + str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError):
        print("evaluation failed: invalid/missing local data, configuration, or unexpected API schema; no remote body logged", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
