#!/bin/sh
# Real infrastructure smoke in its own project; never reads application .env.
set -eu
cd "$(dirname "$0")/.."
stack="mw-index-test-$(python3 -c 'import secrets; print(secrets.token_hex(5))')"
envfile=$(mktemp)
override=$(mktemp)
cleanup_state=$(mktemp)
chmod 600 "$envfile" "$override" "$cleanup_state"
python3 - "$envfile" <<'PY'
import secrets, sys
values={"MYSQL_PASSWORD":secrets.token_hex(24),"MYSQL_ROOT_PASSWORD":secrets.token_hex(24),
        "MINIO_ACCESS_KEY":secrets.token_hex(12),"MINIO_SECRET_KEY":secrets.token_hex(24),
        "MAGENT_SERVICE_API_KEY":secrets.token_hex(32),"MINIO_BUCKET":"magworkspace","MAGENT_MODE":"demo","MAGENT_API_KEY":"","QDRANT_API_KEY":secrets.token_hex(24),"REDIS_PASSWORD":secrets.token_hex(24),
        "CLEANUP_QDRANT_STORES":"[]","CLEANUP_MINIO_STORES":"[]",
        "RERANK_ENABLED":"false","DOCUMENT_INDEXING_ENABLED":"true","CHUNKING_STRATEGY":"parent_child_v1","EMBEDDING_MODEL":"bge-m3:567m","EMBEDDING_DIMENSIONS":"1024"}
for key in ("MAGENT_SERVICE_PORT", "REDIS_PORT", "MYSQL_PORT","MINIO_PORT","MINIO_CONSOLE_PORT","HTTP_PORT","SEARCH_PORT","QDRANT_PORT","OLLAMA_PORT","TIKA_PORT"):
    values[key]="0"
with open(sys.argv[1],"w") as f:
    for k,v in values.items():f.write(k+"="+v+"\n")
PY
set -a
. "$envfile"
set +a
# Preserve the downloaded model in the application's cache. Only synthetic
# MySQL/MinIO/Qdrant data uses disposable project volumes.
model_cache="${MAGWORKSPACE_MODEL_CACHE_VOLUME:-magworkspace_ollama-models}"
export MAGWORKSPACE_MODEL_CACHE_VOLUME="$model_cache"
cat > "$override" <<'YAML'
volumes:
  ollama-models:
    external: true
    name: ${MAGWORKSPACE_MODEL_CACHE_VOLUME}
YAML
# Optional copied source context with a local Go module cache, for offline
# builds. It changes only this disposable test project, not application images.
if [ -n "${MAGWORKSPACE_TEST_BACKEND_IMAGE:-}" ] || [ -n "${MAGWORKSPACE_TEST_HARNESS_IMAGE:-}" ]; then
  # Use a previously verified local build when registry metadata is offline.
  # Both images are required, so neither service silently uses stale code.
  : "${MAGWORKSPACE_TEST_BACKEND_IMAGE:?Both prebuilt images are required}"
  : "${MAGWORKSPACE_TEST_HARNESS_IMAGE:?Both prebuilt images are required}"
  docker image inspect "$MAGWORKSPACE_TEST_BACKEND_IMAGE" >/dev/null
  docker image inspect "$MAGWORKSPACE_TEST_HARNESS_IMAGE" >/dev/null
  python3 - "$override" "$MAGWORKSPACE_TEST_BACKEND_IMAGE" "$MAGWORKSPACE_TEST_HARNESS_IMAGE" <<'PY'
import json, sys
with open(sys.argv[1], "a") as output:
    output.write("\nservices:\n")
    for service, image in zip(("backend", "harness"), sys.argv[2:]):
        output.write("  " + service + ":\n    build: !reset null\n    image: " + json.dumps(image) + "\n    pull_policy: never\n")
PY
elif [ -n "${MAGWORKSPACE_TEST_BACKEND_CONTEXT:-}" ]; then
  python3 - "$override" "$MAGWORKSPACE_TEST_BACKEND_CONTEXT" <<'PY'
import json, pathlib, sys
context = pathlib.Path(sys.argv[2]).resolve(strict=True)
if not (context / "Dockerfile").is_file():
    raise SystemExit("test backend context must contain a Dockerfile")
with open(sys.argv[1], "a") as output:
    output.write("\nservices:\n  backend:\n    build:\n      context: " + json.dumps(str(context)) + "\n")
PY
fi
compose() { docker compose --env-file "$envfile" --project-name "$stack" --profile app -f compose.yaml -f "$override" "$@"; }
cleanup() { compose down --volumes --remove-orphans >/dev/null 2>&1 || true; rm -f "$envfile" "$override" "$cleanup_state"; }
trap cleanup EXIT HUP INT TERM
compose config --quiet
docker volume create "$model_cache" >/dev/null
compose up -d --build --wait mysql minio qdrant tika ollama
compose run --rm ollama-model
compose up -d --build backend
port=$(compose port backend 8080 | sed 's/.*://')
tries=0
until curl --fail --silent "http://127.0.0.1:$port/readyz" >/dev/null; do
 tries=$((tries+1))
 if [ "$tries" -ge 60 ]; then echo 'Application did not become ready' >&2; compose logs --tail 20 backend; exit 1; fi
 sleep 1
done
minio_port=$(compose port minio 9000 | sed 's/.*://')
tika_port=$(compose port tika 9998 | sed 's/.*://')
qdrant_port=$(compose port qdrant 6333 | sed 's/.*://')
export MAGWORKSPACE_TEST_QDRANT_URL="http://127.0.0.1:$qdrant_port"
export MAGWORKSPACE_TEST_QDRANT_API_KEY="$QDRANT_API_KEY"
export MAGWORKSPACE_TEST_MINIO_URL="http://127.0.0.1:$minio_port"
export MAGWORKSPACE_TEST_MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY"
export MAGWORKSPACE_TEST_MINIO_SECRET_KEY="$MINIO_SECRET_KEY"
export MAGWORKSPACE_TEST_TIKA_URL="http://127.0.0.1:$tika_port"
export GOMODCACHE="${GOMODCACHE:-$PWD/.cache/gomod}"
export GOCACHE="${GOCACHE:-$PWD/.cache/gobuild}"
go test ./internal/blob ./internal/indexing -run '^(TestMinIOLive|TestParserRealTika|TestQdrantLiveNativeChineseBM25RRFAndMutations)$' -count=1 -v
python3 scripts/smoke-indexing.py "http://127.0.0.1:$port"

python3 scripts/smoke-redis.py "http://127.0.0.1:$port"

# Exercise resource disposal through the packaged server after index generation
# is disabled. Only this disposable project's synthetic resource is adjusted.
python3 scripts/smoke-cleanup.py prepare --base-url "http://127.0.0.1:$port" --project "$stack" --env-file "$envfile" --compose-file compose.yaml --compose-file "$override" --state "$cleanup_state"
export DOCUMENT_INDEXING_ENABLED=false
compose up -d --no-build --no-deps backend
port=$(compose port backend 8080 | sed 's/.*://')
tries=0
until curl --fail --silent "http://127.0.0.1:$port/readyz" >/dev/null; do
 tries=$((tries+1))
 if [ "$tries" -ge 60 ]; then echo 'Cleanup-only backend did not become ready' >&2; exit 1; fi
 sleep 1
done
python3 scripts/smoke-cleanup.py verify --base-url "http://127.0.0.1:$port" --project "$stack" --env-file "$envfile" --compose-file compose.yaml --compose-file "$override" --state "$cleanup_state" --expect-indexing false
