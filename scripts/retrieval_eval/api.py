"""Bounded public-API client. No response body or credentials in errors/logs."""
import json
import http.client
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from .data import EvaluationError


def atomic_json(path, value, private=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600 if private else 0o644)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class APIError(EvaluationError):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(f"API request failed: HTTP {status}, code {code}")


class APIClient:
    def __init__(self, base_url, token_env="MAGWORKSPACE_EVAL_TOKEN", auth_file=None, timeout=45):
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise EvaluationError("base URL must be an HTTP(S) origin without credentials, query, or fragment")
        self.base = base_url.rstrip("/")
        if not self.base.endswith("/api/v1"):
            self.base += "/api/v1"
        self.timeout, self.auth_file = timeout, Path(auth_file) if auth_file else None
        self.lock = threading.Lock()
        self.response_context = threading.local()
        self.token = os.environ.get(token_env, "")
        if self.auth_file:
            if self.auth_file.stat().st_mode & 0o077:
                raise EvaluationError("auth file must have owner-only permissions (chmod 600)")
            with open(self.auth_file, encoding="utf-8") as stream:
                credentials = json.load(stream)
            self.token = credentials.get("token", "")
        if not self.token:
            raise EvaluationError("set token environment variable or supply an owner-only auth file")

    def _request(self, method, path, data, media, token):
        self.response_context.request_id = None
        headers = {"Content-Type": media, "Authorization": "Bearer " + token}
        if data is not None and not isinstance(data, bytes):
            data = json.dumps(data, ensure_ascii=False).encode("utf-8")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        request = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            response = opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            response = error
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
            raise EvaluationError("API network failure; inspect server state before resuming uploads") from None
        with response:
            request_id = response.headers.get("X-Request-ID", "")
            self.response_context.request_id = request_id if len(request_id) == 32 and all(c in "0123456789abcdef" for c in request_id) else None
            try:
                raw = response.read((8 << 20) + 1)
            except (TimeoutError, OSError, http.client.HTTPException):
                raise EvaluationError("API response read failed; no response body logged") from None
            if len(raw) > (8 << 20):
                raise EvaluationError("API response exceeds 8 MiB limit")
            try:
                envelope = json.loads(raw) if raw else {}
            except (ValueError, UnicodeError):
                raise EvaluationError("API response was not JSON") from None
            if response.status >= 300:
                error_data = envelope.get("error") if isinstance(envelope, dict) else None
                code = error_data.get("code", "unknown") if isinstance(error_data, dict) else "unknown"
                if not isinstance(code, str) or len(code) > 100 or not code.replace("_", "").isalnum():
                    code = "unknown"
                raise APIError(response.status, code)
            if response.status != 204 and (not isinstance(envelope, dict) or envelope.get("success") is not True or "data" not in envelope):
                raise EvaluationError("API response envelope is invalid")
            return envelope

    def request(self, method, path, data=None, media="application/json", envelope=False):
        token = self.token
        try:
            result = self._request(method, path, data, media, token)
        except APIError as error:
            if error.status != 401 or not self.auth_file:
                raise
            with self.lock:
                if self.token == token:
                    with open(self.auth_file, encoding="utf-8") as stream:
                        credentials = json.load(stream)
                    refresh = credentials.get("refresh_token")
                    if not refresh:
                        raise EvaluationError("expired credential; auth file has no refresh token") from None
                    renewed = self._request("POST", "/auth/refresh", {"refresh_token": refresh}, "application/json", "")["data"]
                    atomic_json(self.auth_file, {"token": renewed["token"], "refresh_token": renewed["refresh_token"]}, private=True)
                    self.token = renewed["token"]
            result = self._request(method, path, data, media, self.token)
        return result if envelope else result.get("data")

    def upload(self, workspace, title, content):
        boundary = uuid.uuid4().hex
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="title"\r\n\r\n{title}\r\n'
                f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="corpus.txt"\r\n'
                'Content-Type: text/plain\r\n\r\n').encode("utf-8") + content + f'\r\n--{boundary}--\r\n'.encode()
        return self.request("POST", f"/workspaces/{workspace}/documents", body, f"multipart/form-data; boundary={boundary}")

    @property
    def last_request_id(self):
        return getattr(self.response_context, "request_id", None)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
