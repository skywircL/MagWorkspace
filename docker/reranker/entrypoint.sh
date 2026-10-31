#!/bin/sh
set -eu

# The inference endpoint is private even when somebody publishes a Docker port.
# Never put the key into argv or print the environment on startup failure.
API_KEY=${API_KEY:-}
if [ "${#API_KEY}" -lt 32 ]; then
    echo "reranker: API_KEY must contain at least 32 characters" >&2
    exit 1
fi

# Keep one selected device for the whole model; MPS cannot silently fall back.
export PYTORCH_ENABLE_MPS_FALLBACK=0
export HF_HUB_DISABLE_PROGRESS_BARS=1
export HF_HUB_DISABLE_TELEMETRY=1
export TOKENIZERS_PARALLELISM=false
export PYTHONDONTWRITEBYTECODE=1
exec python3 /opt/reranker/service.py
