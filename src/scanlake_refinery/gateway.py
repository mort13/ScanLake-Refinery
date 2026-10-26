"""HTTP client for ScanLake Gateway, the only service the Refinery talks to.

Raw data:     GET /api/files            (list, cursor-paged)
              GET /api/files/<key>      (download)
Derived data: GET /api/derived          (list)
              PUT /api/derived/<key>    (upload; needs a key with scope "derived:write")
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import httpx

log = logging.getLogger(__name__)

Area = Literal["files", "derived"]

RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRY_AFTER_S = 300.0


class GatewayError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemoteObject:
    key: str
    size: int
    uploaded: str
    etag: str | None

    @property
    def fingerprint(self) -> str:
        """Changes whenever the object is re-uploaded."""
        return f"{self.etag or ''}|{self.size}|{self.uploaded}"


class GatewayClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 60.0,
        max_retries: int = 5,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key, "User-Agent": "scanlake-refinery"},
            timeout=timeout,
            transport=transport,
        )
        self._max_retries = max_retries
        self._sleep = sleep

    def __enter__(self) -> GatewayClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def list_objects(
        self, area: Area = "files", prefix: str | None = None
    ) -> Iterator[RemoteObject]:
        cursor: str | None = None
        while True:
            params: dict[str, str] = {"limit": "1000"}
            if prefix:
                params["prefix"] = prefix
            if cursor:
                params["cursor"] = cursor
            body = self._request("GET", f"/api/{area}", params=params).json()
            for o in body["objects"]:
                yield RemoteObject(
                    key=o["key"],
                    size=int(o["size"]),
                    uploaded=str(o["uploaded"]),
                    etag=o.get("etag"),
                )
            cursor = body.get("cursor")
            if not body.get("truncated") or not cursor:
                return

    def download(self, key: str, dest: Path, area: Area = "files") -> int:
        """Download `key` to `dest` atomically and return the number of bytes written."""
        resp = self._request("GET", f"/api/{area}/{quote(key, safe='/')}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_bytes(resp.content)
        os.replace(tmp, dest)
        return len(resp.content)

    def upload(self, key: str, data: bytes, content_type: str) -> dict:
        resp = self._request(
            "PUT",
            f"/api/derived/{quote(key, safe='/')}",
            content=data,
            headers={
                "Content-Type": content_type,
                "X-Content-SHA256": hashlib.sha256(data).hexdigest(),
            },
        )
        return resp.json()

    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        for attempt in range(self._max_retries + 1):
            last = attempt == self._max_retries
            try:
                resp = self._http.request(method, url, **kwargs)
            except httpx.TransportError as e:
                if last:
                    raise GatewayError(f"{method} {url}: {e}") from e
                delay = _backoff(attempt)
                log.warning("%s %s failed (%s), retrying in %.0fs", method, url, e, delay)
                self._sleep(delay)
                continue

            if resp.status_code in RETRY_STATUSES and not last:
                delay = _retry_after(resp) or _backoff(attempt)
                if delay > MAX_RETRY_AFTER_S:
                    raise GatewayError(
                        f"{method} {url}: HTTP {resp.status_code}, "
                        f"Retry-After {delay:.0f}s is too long"
                    )
                log.warning(
                    "%s %s returned %d, retrying in %.0fs", method, url, resp.status_code, delay
                )
                self._sleep(delay)
                continue

            if resp.is_error:
                raise GatewayError(f"{method} {url}: HTTP {resp.status_code} {_error_text(resp)}")
            return resp
        raise AssertionError("unreachable")


def _backoff(attempt: int) -> float:
    return float(min(2**attempt, 60))


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _error_text(resp: httpx.Response) -> str:
    try:
        return str(resp.json().get("error", resp.text))
    except ValueError:
        return resp.text[:200]
