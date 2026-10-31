# Linux amd64/arm64 CPU by default; select a CUDA wheel index at build time.
FROM python:3.12.12-slim-bookworm

ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
COPY docker/reranker/requirements.txt /opt/reranker/requirements.txt
RUN pip install --no-cache-dir torch==2.8.0 --index-url "${TORCH_INDEX_URL}" \
    && pip install --no-cache-dir -r /opt/reranker/requirements.txt

COPY docker/reranker/service.py /opt/reranker/service.py
COPY docker/reranker/entrypoint.sh /usr/local/bin/magworkspace-reranker
RUN chmod 0555 /usr/local/bin/magworkspace-reranker

ENV MODEL_ID=BAAI/bge-reranker-base \
    REVISION=2cfc18c9415c912f9d8155881c133215df768a70 \
    HUGGINGFACE_HUB_CACHE=/data \
    CROSS_ENCODER_DEVICE="" \
    PORT=8091 \
    DTYPE=float32 \
    MAX_CLIENT_BATCH_SIZE=200 \
    MAX_CONCURRENT_REQUESTS=200 \
    MAX_BATCH_REQUESTS=4 \
    MAX_BATCH_TOKENS=1024 \
    PAYLOAD_LIMIT=2000000 \
    MAX_HTTP_REQUESTS=16 \
    OMP_NUM_THREADS=4 \
    AUTO_TRUNCATE=true \
    TOKENIZERS_PARALLELISM=false \
    HF_HUB_DISABLE_TELEMETRY=1 \
    HF_HUB_DISABLE_PROGRESS_BARS=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTORCH_ENABLE_MPS_FALLBACK=0

EXPOSE 8091
HEALTHCHECK --interval=10s --timeout=5s --start-period=10m --retries=3 \
    CMD python3 -c 'import os,urllib.request; urllib.request.urlopen("http://127.0.0.1:"+os.environ.get("PORT","8091")+"/health",timeout=4).close()' || exit 1
ENTRYPOINT ["/usr/local/bin/magworkspace-reranker"]
CMD []
