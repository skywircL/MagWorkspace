#!/usr/bin/env python3
"""An isolated, resumable local benchmark stack; never reads application .env."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".cache" / "retrieval-evaluation-qdrant"


def private_write(path, text):
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write(text)


def runtime():
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
    envfile = RUNTIME / "stack.env"
    if not envfile.exists():
        values = {
            "COMPOSE_PROJECT_NAME": "mw-eval-" + secrets.token_hex(5),
            "MYSQL_PASSWORD": secrets.token_hex(24), "MYSQL_ROOT_PASSWORD": secrets.token_hex(24),
            "MINIO_ACCESS_KEY": secrets.token_hex(12), "MINIO_SECRET_KEY": secrets.token_hex(24),
            "MAGENT_SERVICE_API_KEY": secrets.token_hex(32), "QDRANT_API_KEY": secrets.token_hex(24), "REDIS_PASSWORD": secrets.token_hex(24),
            "MINIO_BUCKET": "magworkspace", "MAGENT_MODE": "", "MAGENT_API_KEY": "",
            "DOCUMENT_INDEXING_ENABLED": "true", "EMBEDDING_MODEL": "bge-m3:567m",
            "EMBEDDING_DIMENSIONS": "1024", "INDEX_WORKERS": "1", "INDEX_JOB_TIMEOUT": "15m",
            "METRICS_ADDR": "0.0.0.0:9091", "TRACING_ENABLED": "false",
            "MAGWORKSPACE_MODEL_CACHE_VOLUME": os.environ.get("MAGWORKSPACE_MODEL_CACHE_VOLUME", "magworkspace_ollama-models"),
        }
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", values["MAGWORKSPACE_MODEL_CACHE_VOLUME"]):
            raise ValueError("invalid model cache volume name")
        for key in ("MAGENT_SERVICE_PORT", "REDIS_PORT", "MYSQL_PORT", "MINIO_PORT", "MINIO_CONSOLE_PORT", "HTTP_PORT", "SEARCH_PORT","QDRANT_PORT", "OLLAMA_PORT", "TIKA_PORT", "METRICS_PORT", "JAEGER_PORT", "OTLP_HTTP_PORT", "PROMETHEUS_PORT"):
            values[key] = "0"
        private_write(envfile, "".join(f"{key}={value}\n" for key, value in values.items()))
    values = dict(line.split("=", 1) for line in envfile.read_text().splitlines() if line and not line.startswith("#"))
    # Upgrade private runtime configuration without touching frozen benchmark evidence.
    if "REDIS_PASSWORD" not in values:
        values.update(REDIS_PASSWORD=secrets.token_hex(24), REDIS_PORT="0")
        private_write(envfile, "".join(f"{key}={value}\n" for key, value in values.items()))
    if "MAGENT_SERVICE_API_KEY" not in values:
        values.update(MAGENT_SERVICE_API_KEY=secrets.token_hex(32), MAGENT_SERVICE_PORT="0")
        private_write(envfile, "".join(f"{key}={value}\n" for key, value in values.items()))
    override = RUNTIME / "override.yaml"
    return envfile, override, values


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("up", "up-observability", "status", "down", "register"))
    p.add_argument("--infra-only", action="store_true")
    p.add_argument("--observability", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument("--execution", choices=("qdrant_native",))
    p.add_argument("--remove-data", action="store_true", help="delete this benchmark project's synthetic data; preserves model cache")
    args = p.parse_args()
    envfile, override, values = runtime()
    settings_path = RUNTIME / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {"observability": False, "execution": "qdrant_native"}
    if args.observability is not None:
        settings["observability"] = args.observability
    if args.action == "up-observability":
        settings["observability"] = True
    if args.execution:
        settings["execution"] = args.execution
    if settings["execution"] not in ("qdrant_native",) or type(settings["observability"]) is not bool:
        raise ValueError("invalid evaluation stack settings")
    private_write(settings_path, json.dumps(settings) + "\n")
    private_write(override, """services:
  backend:
    environment:
      INDEX_WORKERS: '1'
      INDEX_JOB_TIMEOUT: 15m
      EMBEDDING_REQUEST_TIMEOUT: 5m
      RETRIEVAL_EXECUTION: qdrant_native
  ollama:
    environment:
      OLLAMA_KEEP_ALIVE: 24h
volumes:
  ollama-models:
    external: true
    name: ${MAGWORKSPACE_MODEL_CACHE_VOLUME}
""")
    env = dict(os.environ, **values)
    command = ["docker", "compose", "--env-file", str(envfile), "--project-name", values["COMPOSE_PROJECT_NAME"], "--profile", "app", "-f", str(ROOT / "compose.yaml")]
    if settings["observability"]:
        command += ["-f", str(ROOT / "compose.observability.yaml"), "--profile", "observability"]
    command += ["-f", str(override)]

    def compose(*parts, capture=False):
        return subprocess.run(command + list(parts), env=env, cwd=ROOT, check=True,
                              text=True, stdout=subprocess.PIPE if capture else None)

    if args.action == "up":
        compose("config", "--quiet")
        subprocess.run(["docker", "volume", "create", values["MAGWORKSPACE_MODEL_CACHE_VOLUME"]], check=True, stdout=subprocess.DEVNULL)
        compose("up", "-d", "--build", "--wait", "mysql", "minio", "qdrant", "tika", "ollama")
        compose("run", "--rm", "ollama-model")
        if not args.infra_only:
            compose("up", "-d", "--build", "backend")
            if settings["observability"]:
                compose("up", "-d", "jaeger", "prometheus")
    elif args.action == "up-observability":
        compose("config", "--quiet")
        compose("up", "-d", "jaeger", "prometheus")
    elif args.action == "down":
        parts = ["down", "--remove-orphans"]
        if args.remove_data:
            parts.append("--volumes")
        compose(*parts)
        if args.remove_data:
            print("Removed only this evaluation stack's synthetic data; local state/auth must be recreated before ingestion.")
        return
    elif args.action == "register":
        import urllib.request
        endpoints = json.loads((RUNTIME / "endpoints.json").read_text())
        authfile = RUNTIME / "scifact-auth.json"
        if authfile.exists():
            print("Private evaluation credential file already exists; use the resumable CLI.")
            return
        payload = {"name": "Retrieval evaluation", "email": "evaluation-" + secrets.token_hex(8) + "@example.test", "password": secrets.token_urlsafe(24)}
        request = urllib.request.Request(endpoints["backend:8080"] + "/api/v1/auth/register", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            auth = json.load(response)["data"]
        private_write(authfile, json.dumps({"token": auth["token"], "refresh_token": auth["refresh_token"]}) + "\n")
        print("Created a dedicated evaluation identity; private credentials saved under .cache/retrieval-evaluation-qdrant.")
        return
    endpoints = {}
    for service, port in (("backend", 8080), ("backend", 9091), ("ollama", 11434), ("qdrant", 6333), ("jaeger", 16686), ("prometheus", 9090)):
        result = subprocess.run(command + ["port", service, str(port)], env=env, cwd=ROOT, text=True, capture_output=True)
        if result.returncode == 0 and result.stdout.strip():
            endpoints[f"{service}:{port}"] = "http://" + result.stdout.strip().splitlines()[0]
    private_write(RUNTIME / "endpoints.json", json.dumps(endpoints, indent=2) + "\n")
    # Keep the first assigned application port across strategy rebuilds. It is
    # part of the evaluator's frozen API origin, unlike infrastructure ports.
    if values.get("HTTP_PORT") == "0" and "backend:8080" in endpoints:
        import urllib.parse
        values["HTTP_PORT"] = str(urllib.parse.urlsplit(endpoints["backend:8080"]).port)
        private_write(envfile, "".join(f"{key}={value}\n" for key, value in values.items()))
    if args.action == "up" and not args.infra_only:
        import time
        import urllib.request
        for _ in range(60):
            try:
                with urllib.request.urlopen(endpoints["backend:8080"] + "/readyz", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            time.sleep(1)
        else:
            raise ValueError("evaluation backend did not become ready")
    print(json.dumps({"project": values["COMPOSE_PROJECT_NAME"], "endpoints": endpoints}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError:
        print("Evaluation stack command failed; credentials are retained privately for resume.", file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, KeyError):
        print("Invalid or unavailable evaluation runtime configuration; no credentials logged.", file=sys.stderr)
        sys.exit(1)
