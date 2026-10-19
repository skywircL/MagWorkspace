"""Bind callback destinations to operator configuration, never model input."""
from __future__ import annotations

import os
import re
import urllib.parse


def callback_origin() -> str:
    origin = os.environ.get("MAGENT_CALLBACK_ORIGIN", "")
    if not origin:
        return ""
    url = urllib.parse.urlsplit(origin)
    if (
        url.scheme not in ("http", "https") or not url.hostname
        or url.username is not None or url.password is not None
        or url.path or url.query or url.fragment
        or not re.fullmatch(r"[a-zA-Z0-9.:-]+", url.netloc)
        or url.port is None or not 1 <= url.port <= 65535
        or origin != f"{url.scheme}://{url.hostname}:{url.port}"
    ):
        raise ValueError("invalid callback origin configuration")
    return origin


def validate_callback(value: object, path: str) -> dict:
    if (
        not isinstance(value, dict) or set(value) != {"url", "token"}
        or not isinstance(value["url"], str) or not isinstance(value["token"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["token"])
    ):
        raise ValueError("invalid callback endpoint")
    origin = callback_origin()
    if origin:
        if value["url"] != origin + path:
            raise ValueError("invalid callback endpoint")
        return value
    url = urllib.parse.urlsplit(value["url"])
    if (
        url.scheme != "http" or url.hostname != "127.0.0.1"
        or url.username is not None or url.password is not None
        or url.port is None or not 1 <= url.port <= 65535
        or value["url"] != f"http://127.0.0.1:{url.port}{path}"
    ):
        raise ValueError("invalid callback endpoint")
    return value
