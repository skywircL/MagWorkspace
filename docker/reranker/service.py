#!/usr/bin/env python3
"""Private, bounded cross-encoder HTTP runtime; model imports happen at startup."""
from __future__ import annotations

import contextlib
import hmac
import json
import logging
import math
import os
import re
import sys
import threading
import warnings
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL_ID = "BAAI/bge-reranker-base"
REVISION = "2cfc18c9415c912f9d8155881c133215df768a70"


class ConfigurationError(ValueError):
    """Messages are static and safe for startup diagnostics."""


class RequestError(Exception):
    def __init__(self, status: int, message: str):
        self.status, self.message = status, message


def integer(env, name, default, minimum, maximum):
    try:
        value = int(env.get(name, str(default)))
    except (TypeError, ValueError):
        raise ConfigurationError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} is outside its supported range")
    return value


@dataclass(frozen=True)
class Settings:
    api_key: str
    model_id: str = MODEL_ID
    revision: str = REVISION
    requested_device: str = ""
    host: str = "127.0.0.1"
    port: int = 8091
    max_client_batch_size: int = 200
    max_concurrent_requests: int = 200  # Pair permits, including queued requests.
    max_batch_requests: int = 4
    max_batch_tokens: int = 1024
    payload_limit: int = 2_000_000
    max_http_requests: int = 16
    auto_truncate: bool = True
    dtype: str = "float32"

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        key = env.get("API_KEY", "")
        if not 32 <= len(key) <= 4096 or any(ord(c) < 33 or ord(c) > 126 for c in key):
            raise ConfigurationError("API_KEY must contain at least 32 printable ASCII characters (maximum 4096)")
        model = env.get("MODEL_ID", MODEL_ID)
        revision = env.get("REVISION", REVISION)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", model):
            raise ConfigurationError("MODEL_ID must be a public Hub owner/name")
        if not re.fullmatch(r"[a-f0-9]{40}", revision):
            raise ConfigurationError("REVISION must be a pinned 40-character commit")
        device = env.get("CROSS_ENCODER_DEVICE", "").strip().lower()
        if device not in {"", "cuda", "mps", "cpu"}:
            raise ConfigurationError("CROSS_ENCODER_DEVICE must be empty, cuda, mps, or cpu")
        auto = env.get("AUTO_TRUNCATE", "true").lower()
        if auto not in {"true", "false"}:
            raise ConfigurationError("AUTO_TRUNCATE must be true or false")
        if env.get("DTYPE", "float32") != "float32":
            raise ConfigurationError("DTYPE must be float32")
        host = env.get("HOST", "127.0.0.1")
        if host not in {"127.0.0.1", "0.0.0.0"}:
            raise ConfigurationError("HOST must be 127.0.0.1 or 0.0.0.0")
        return cls(
            api_key=key, model_id=model, revision=revision, requested_device=device,
            host=host,
            port=integer(env, "PORT", 8091, 1, 65535),
            max_client_batch_size=integer(env, "MAX_CLIENT_BATCH_SIZE", 200, 1, 200),
            max_concurrent_requests=integer(env, "MAX_CONCURRENT_REQUESTS", 200, 1, 200),
            max_batch_requests=integer(env, "MAX_BATCH_REQUESTS", 4, 1, 200),
            max_batch_tokens=integer(env, "MAX_BATCH_TOKENS", 1024, 8, 102400),
            payload_limit=integer(env, "PAYLOAD_LIMIT", 2_000_000, 1024, 2_000_000),
            max_http_requests=integer(env, "MAX_HTTP_REQUESTS", 16, 2, 64),
            auto_truncate=auto == "true",
        )


def select_device(torch, requested: str) -> str:
    if requested not in {"", "cuda", "mps", "cpu"}:
        raise ConfigurationError("CROSS_ENCODER_DEVICE must be empty, cuda, mps, or cpu")
    if requested == "cpu":
        return "cpu"
    cuda = bool(torch.cuda.is_available())
    mps_backend = getattr(torch.backends, "mps", None)
    mps = bool(mps_backend and mps_backend.is_built() and mps_backend.is_available())
    if requested:
        if (requested == "cuda" and cuda) or (requested == "mps" and mps):
            return requested
        raise ConfigurationError(f"requested {requested} device is unavailable")
    return "cuda" if cuda else "mps" if mps else "cpu"


class CrossEncoder:
    def __init__(self, settings, torch, tokenizer, model, device):
        self.settings, self.torch = settings, torch
        self.tokenizer, self.model, self.device = tokenizer, model, device
        limit = min(512, settings.max_batch_tokens)
        token_limit = getattr(tokenizer, "model_max_length", None)
        if isinstance(token_limit, int) and token_limit > 0:
            limit = min(limit, token_limit)
        positions = getattr(model.config, "max_position_embeddings", None)
        if isinstance(positions, int) and positions > 0:
            # RoBERTa positions start after the padding index (514 -> 512).
            offset = (getattr(model.config, "pad_token_id", 1) + 1
                      if getattr(model.config, "model_type", "") in {"roberta", "xlm-roberta"} else 0)
            limit = min(limit, positions - offset)
        if limit < 8 or getattr(model.config, "num_labels", 0) != 1:
            raise ConfigurationError("model must be a single-logit cross-encoder with at least 8 input tokens")
        self.max_input_length = limit

    @classmethod
    def load(cls, settings):
        # No secrets in upstream diagnostics, download progress, or warnings.
        logging.disable(logging.CRITICAL)
        warnings.filterwarnings("ignore")
        os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        from transformers.utils import logging as transformer_logging
        from huggingface_hub.utils import disable_progress_bars
        transformer_logging.set_verbosity_error()
        disable_progress_bars()
        device = select_device(torch, settings.requested_device)
        common = {"revision": settings.revision, "trust_remote_code": False,
                  "cache_dir": os.environ.get("HUGGINGFACE_HUB_CACHE") or None}
        tokenizer = AutoTokenizer.from_pretrained(settings.model_id, use_fast=True, **common)
        model = AutoModelForSequenceClassification.from_pretrained(
            settings.model_id, use_safetensors=True, torch_dtype=torch.float32, **common,
        )
        # An unavailable or broken explicitly selected device never falls back.
        model.to(device)
        model.eval()
        encoder = cls(settings, torch, tokenizer, model, device)
        warmup = encoder.score("health", ["health"], True)
        if len(warmup) != 1 or not math.isfinite(warmup[0]):
            raise ConfigurationError("model warmup produced invalid scores")
        return encoder

    def score(self, query, texts, truncate):
        encoded = []
        for text in texts:
            pair = self.tokenizer(query, text, truncation=truncate,
                                  max_length=self.max_input_length if truncate else None,
                                  padding=False, return_attention_mask=True, verbose=False)
            length = len(pair["input_ids"])
            if length > self.max_input_length:
                raise RequestError(422, "query/passage pair exceeds the token limit")
            if not length:
                raise RequestError(422, "empty tokenized pair")
            encoded.append(pair)
        scores = []
        start = 0
        with self.torch.inference_mode():
            while start < len(encoded):
                end, longest = start, 0
                while end < len(encoded) and end - start < self.settings.max_batch_requests:
                    next_longest = max(longest, len(encoded[end]["input_ids"]))
                    if next_longest * (end - start + 1) > self.settings.max_batch_tokens:
                        break
                    longest, end = next_longest, end + 1
                if end == start:
                    raise RequestError(422, "pair exceeds the batch token limit")
                batch = self.tokenizer.pad(encoded[start:end], padding=True, return_tensors="pt")
                batch = {name: tensor.to(self.device) for name, tensor in batch.items()}
                logits = self.model(**batch).logits
                if tuple(logits.shape) != (end - start, 1):
                    raise RuntimeError("invalid cross-encoder output shape")
                scores.extend(logits.float().cpu().reshape(-1).tolist())
                start = end
        return scores


def sigmoid(value):
    return 1 / (1 + math.exp(-value)) if value >= 0 else math.exp(value) / (1 + math.exp(value))


class Application:
    def __init__(self, settings, encoder):
        self.settings, self.encoder = settings, encoder
        self._permits = threading.Lock()
        self._active_pairs = 0
        self._model_lock = threading.Lock()

    def info(self):
        cfg = self.settings
        return {"model_id": cfg.model_id, "model_sha": cfg.revision, "model_type": {"reranker": {}},
                "max_input_length": self.encoder.max_input_length,
                "max_batch_tokens": cfg.max_batch_tokens, "max_batch_requests": cfg.max_batch_requests,
                "max_client_batch_size": cfg.max_client_batch_size,
                "max_concurrent_requests": cfg.max_concurrent_requests,
                "auto_truncate": cfg.auto_truncate, "dtype": cfg.dtype,
                "device": self.encoder.device, "requested_device": cfg.requested_device}

    def rerank(self, payload):
        if not isinstance(payload, dict) or set(payload) - {"query", "texts", "truncate", "raw_scores", "return_text"}:
            raise RequestError(422, "invalid request fields")
        query, texts = payload.get("query"), payload.get("texts")
        if not isinstance(query, str) or not query.strip() or not isinstance(texts, list):
            raise RequestError(422, "query and texts are required")
        if not texts:
            raise RequestError(400, "texts must not be empty")
        if len(texts) > self.settings.max_client_batch_size:
            raise RequestError(422, "too many candidate texts")
        if any(not isinstance(text, str) or not text.strip() for text in texts):
            raise RequestError(422, "candidate texts must be nonempty strings")
        try:
            query.encode("utf-8")
            for text in texts:
                text.encode("utf-8")
        except UnicodeError:
            raise RequestError(422, "invalid text encoding") from None
        flags = {name: payload.get(name, default) for name, default in
                 (("truncate", self.settings.auto_truncate), ("raw_scores", False), ("return_text", False))}
        if any(type(value) is not bool for value in flags.values()):
            raise RequestError(422, "request flags must be booleans")
        with self._permits:
            if self._active_pairs + len(texts) > self.settings.max_concurrent_requests:
                raise RequestError(429, "inference capacity exceeded")
            self._active_pairs += len(texts)
        try:
            if not self._model_lock.acquire(timeout=30):
                raise RequestError(503, "inference queue deadline exceeded")
            try:
                scores = self.encoder.score(query, texts, flags["truncate"])
            finally:
                self._model_lock.release()
            if len(scores) != len(texts) or any(type(x) not in (int, float) or not math.isfinite(x) for x in scores):
                raise RuntimeError("invalid model scores")
            rows = [{"index": i, "score": score if flags["raw_scores"] else sigmoid(score),
                     **({"text": texts[i]} if flags["return_text"] else {})}
                    for i, score in enumerate(scores)]
            return sorted(rows, key=lambda row: (-row["score"], row["index"]))
        finally:
            with self._permits:
                self._active_pairs -= len(texts)


class Handler(BaseHTTPRequestHandler):
    server_version = "CrossEncoder"
    sys_version = ""

    def setup(self):
        self.request.settimeout(30)
        super().setup()

    def log_message(self, *_args):
        pass

    def _reply(self, status, value):
        body = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _authorized(self):
        values = self.headers.get_all("Authorization", [])
        expected = "Bearer " + self.server.application.settings.api_key
        if len(values) != 1 or not hmac.compare_digest(values[0].encode("utf-8"), expected.encode("utf-8")):
            raise RequestError(401, "unauthorized")

    def do_GET(self):
        self._dispatch(False)

    def do_POST(self):
        self._dispatch(True)

    def _dispatch(self, post):
        try:
            app = self.server.application
            if self.path == "/health" and not post:
                self._reply(200, {"status": "ok"})
                return
            self._authorized()
            if self.path == "/info" and not post:
                self._reply(200, app.info())
                return
            if self.path != "/rerank" or not post:
                raise RequestError(404, "not found")
            if self.headers.get("Transfer-Encoding") is not None:
                raise RequestError(400, "transfer encoding is unsupported")
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
                raise RequestError(400, "content length is required")
            length = int(lengths[0])
            if length > app.settings.payload_limit:
                raise RequestError(413, "request body exceeds limit")
            if self.headers.get_content_type() != "application/json":
                raise RequestError(415, "application/json is required")
            body = self.rfile.read(length)
            if len(body) != length:
                raise RequestError(400, "incomplete request body")
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeError, RecursionError):
                raise RequestError(400, "invalid JSON") from None
            self._reply(200, app.rerank(payload))
        except RequestError as exc:
            self._safe_error(exc.status, exc.message)
        except (BrokenPipeError, ConnectionError, TimeoutError):
            self.close_connection = True
        except Exception:
            # Provider text and tracebacks may contain document or key material.
            self._safe_error(503, "inference unavailable")

    def _safe_error(self, status, message):
        with contextlib.suppress(OSError):
            self._reply(status, {"error": message})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, application):
        self.application = application
        self._http_slots = threading.BoundedSemaphore(application.settings.max_http_requests)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self._http_slots.acquire(blocking=False):
            with contextlib.suppress(OSError):
                request.settimeout(1)
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._http_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._http_slots.release()

    def handle_error(self, request, client_address):
        pass  # Never emit BaseServer's traceback (request or model data).


def main():
    try:
        settings = Settings.from_env()
        encoder = CrossEncoder.load(settings)
        server = Server((settings.host, settings.port), Application(settings, encoder))
    except ConfigurationError as exc:
        print(f"reranker: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"reranker: startup failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(f"reranker: ready on {encoder.device}", file=sys.stderr, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
