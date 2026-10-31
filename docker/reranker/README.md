# Cross-encoder reranker runtime

The runtime uses PyTorch 2.8.0 and Transformers 4.57.1 to serve the MIT-licensed
`BAAI/bge-reranker-base` Chinese/English cross-encoder, pinned to revision
`2cfc18c9415c912f9d8155881c133215df768a70`. It loads only safetensors, disables
remote model code, and runs in float32 inference mode. This is separate from the
`bge-m3` embedding model used by Ollama. The HTTP contract remains compatible with
the application's former TEI client.

- `GET /health` becomes available after model loading and a real inference warmup.
- `POST /rerank` requires `Authorization: Bearer <API_KEY>`.
- Request: `{"query":"...", "texts":["..."], "truncate":true,
  "raw_scores":false, "return_text":false}`.
- Response: `[{"index":0,"score":0.9}, ...]` for every input, sorted by score.
  Scores use sigmoid normalization; they are not answer-correctness probabilities.
- `truncate:false` rejects overlong pairs; `truncate:true` truncates the pair to
  the effective limit. Omission uses `AUTO_TRUNCATE` (default `true`).
- `raw_scores:true` returns unnormalized logits. `return_text:true` includes the
  original candidate in each result's `text` field; the default is `false`.
- `GET /info` requires authentication and reports model/revision, actual limits,
  `device` (`cuda`, `mps`, or `cpu`) and `requested_device` (empty means automatic).

The default service has a 2,000,000-byte body limit, 200 candidate limit, 200
in-flight pair permits, four pairs per model batch, and a 1024-token batch budget.
The default model supports at most **512 tokens per query/passage pair**, including
the query and special tokens. The batch budget does not increase this model
limit. A lower batch budget also lowers the pair limit. The batch budget counts
**padded tokens** (`longest pair × number of pairs`), so four long pairs are split
into smaller model batches. `MAX_CONCURRENT_REQUESTS` counts **pairs**, including
queued requests; exceeding it returns 429. Model inference is serialized and
waiting for that lock is limited to 30 seconds (503 on timeout).
`MAX_HTTP_REQUESTS` defaults to 16; surplus connections receive 503 instead of
creating unbounded threads. HTTP reads have a 30-second timeout. Client cancellation
bounds the caller's wait; already admitted inference can finish in the service.
The application uses a smaller 20-candidate cap.

The backend must enforce its own timeout and fail back to the verified retrieval
results on transport/model errors. A successful low relevance score is different
from a transport failure.

The entrypoint requires an API key of 32–4096 printable non-whitespace ASCII
characters. Model exceptions and request bodies never enter logs or error
responses. Startup failure reports only the error class (or a fixed configuration
message); it never prints the key or upstream exception text.

## Device selection

`CROSS_ENCODER_DEVICE` is read by the inference process:

| Value | Behavior |
| --- | --- |
| Empty/unset | First available device in CUDA → MPS → CPU order |
| `cuda` | Require CUDA; startup fails if unavailable |
| `mps` | Require a built and available MPS backend; startup fails if unavailable |
| `cpu` | Always use CPU, even if an accelerator is available |

Model loading/warmup failures never silently switch devices. MPS per-operation
CPU fallback is disabled. `/info.device` reports the selected device; an empty
request is not evidence that GPU execution was selected.

The default Docker image supports Linux amd64 and arm64 with CPU PyTorch wheels.
Docker Desktop's Linux VM cannot access Apple's MPS backend: `mps` must fail
there, and automatic selection uses CPU. For NVIDIA Linux hosts, build with
`--build-arg TORCH_INDEX_URL=https://download.pytorch.org/whl/cu126`, expose the GPU
with Docker's NVIDIA runtime (`--gpus all`), and set `CROSS_ENCODER_DEVICE=cuda`.
The host's driver must support the chosen wheel; GPU availability is checked at
startup, not inferred from the environment variable.

For native Apple Silicon inference, use an isolated Python 3.12 environment:

```sh
python3.12 -m venv /tmp/magworkspace-reranker-venv
/tmp/magworkspace-reranker-venv/bin/pip install torch==2.8.0
/tmp/magworkspace-reranker-venv/bin/pip install -r docker/reranker/requirements.txt
# Supply API_KEY securely through the process environment (never command arguments).
CROSS_ENCODER_DEVICE=mps /tmp/magworkspace-reranker-venv/bin/python docker/reranker/service.py
```

Point the backend's reranker URL at this process (`http://127.0.0.1:8091/rerank`
for a native backend, `http://host.docker.internal:8091/rerank` from Docker Desktop).
Only the inference process needs the device setting. Keep the same API key on
the backend and service. Native invocation is optional; the standard project
infrastructure continues to use Compose.

## Model download

Weights and tokenizer files total approximately 1.14 GB. The cache is a persistent
Docker volume; model files are not baked into the image or installed on the host.
Transformers downloads missing files when it starts. For a slow connection, the standalone
`scripts/download-reranker_service.py` can run in a disposable Python container
with the cache mounted at `/data`. It resumes `.part` files and checks the
published LFS SHA256 or Git blob digest before publishing the pinned snapshot.
Do not run this downloader concurrently with the runtime's initial download into the
same cache.

The downloader accepts `--model owner/name --revision <40-character-commit>`
for another public model with the same inference files. Each model/revision uses
its own cache directory; downloading another model preserves the existing one.
Set both `MODEL_ID` and `REVISION` on the service to select the verified snapshot.

## Validation

Offline transfer integrity and startup authentication:

```sh
python3 scripts/test_reranker_service.py
python3 -B -m unittest scripts.test_cross_encoder_service -v
```

For an already loaded local service, provide `RERANKER_TEST_API_KEY` through the
environment, then run:

```sh
python3 scripts/smoke-reranker_service.py --url http://127.0.0.1:18091
```

This checks loaded model identity, real Chinese relevance ordering, sigmoid
scores, authentication, and request bounds. Add `--benchmark 20 50 200 --timeout
600` to measure CPU latency with 800-character synthetic passages. This benchmark
measures service latency, not retrieval Recall/Hit.

For reproducible CPU sizing across long Chinese, mixed code/Chinese, and short
passages, use `scripts/benchmark-reranker_service.py`. For example:

```sh
python3 scripts/benchmark-reranker_service.py \
  --distribution chinese_800 --candidates 20 50 --timeout 30 \
  --output /tmp/reranker-latency.json
```

The generated passages are fixed synthetic data and contain no user documents.
The script records a timeout and stops adding work after one occurs. Stop or
restart the owned test service before another probe if a request timed out.
These measurements establish latency limits, not retrieval accuracy; changing
the token cap or candidate count also requires a labelled retrieval evaluation.

### Historical TEI sizing (not results for the current runtime/model)

On the development machine, the previous TEI Docker ARM64 CPU inference with two CPUs, float32,
four pairs per batch and a 1024-token batch budget produced these measurements
on 2026-10-20:

| Model | Fixed synthetic input | Observed duration |
| --- | --- | --- |
| mMiniLMv2 (previous model) | 4 short Chinese candidates | 0.294 s |
| mMiniLMv2 (previous model) | 20 Chinese candidates × 800 characters | 21.892 s |
| mMiniLMv2 (previous model) | 50 Chinese candidates × 800 characters | timed out at 30.020 s |
| mMiniLMv2 (previous model) | 16 Chinese candidates × 80 characters | 2.249 s |
| BGE reranker v2 m3 | 50 Chinese candidates × 800 characters | timed out at 60.032 s |

The mMiniLMv2 service used at most 1.831 GiB of cgroup memory, including file cache,
in this probe and reported zero OOM kills. Its 512-token pair limit truncates the
long inputs; the timings do not establish PDF answer quality. The BGE model's
verified cache can be retained for a more capable inference host. No live user
documents were used in these service probes. The BGE row is an earlier sizing
probe with repeated passages; it is not an identical-corpus model comparison.

These old measurements do not establish the new BGE-base latency, quality, or
GPU behavior. The offline tests use a fake model through the real HTTP/token
batching path; they prove API and selection rules, not GPU availability or model
relevance. Run the live smoke command on each intended device for that evidence.

Upstream references:
[pinned model](https://huggingface.co/BAAI/bge-reranker-base/tree/2cfc18c9415c912f9d8155881c133215df768a70),
[PyTorch wheel variants](https://pytorch.org/get-started/previous-versions/),
[MPS availability](https://docs.pytorch.org/docs/stable/notes/mps.html),
[Transformers tokenization](https://huggingface.co/docs/transformers/v4.57.1/main_classes/tokenizer).
