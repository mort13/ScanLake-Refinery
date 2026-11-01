"""Run notifications for ScanLake FrontDesk, the public landing page at https://scanlake.rocks.

`refinery run` reports when it starts, finishes and fails, so the page can show a running state
and the latest totals:

    POST {FRONTDESK_URL}/api/ingest/refinery   Authorization: Bearer {FRONTDESK_TOKEN}   -> 204
    {"event": "started" | "finished" | "failed", "runId": ..., "at": ISO-8601 UTC,
     "totals": {"scans", "sessions", "users"} (finished only), "error": ... (failed only)}

Notifications are best effort: errors are logged as warnings and never fail a run. Nothing is sent
unless both FRONTDESK_URL and FRONTDESK_TOKEN are set.
"""

from __future__ import annotations

import logging
from typing import TypedDict

import httpx

from .state import utcnow

log = logging.getLogger(__name__)

TIMEOUT_S = 5.0
MAX_ERROR_CHARS = 500


class Totals(TypedDict):
    scans: int
    sessions: int
    users: int


class FrontDesk:
    def __init__(
        self,
        base_url: str | None,
        token: str | None,
        *,
        timeout: float = TIMEOUT_S,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.enabled = bool(base_url and token)
        self._url = f"{(base_url or '').rstrip('/')}/api/ingest/refinery"
        self._token = token
        self._timeout = timeout
        self._transport = transport

    def started(self, run_id: str) -> None:
        self._send("started", run_id)

    def finished(self, run_id: str, totals: Totals) -> None:
        self._send("finished", run_id, totals=dict(totals))

    def failed(self, run_id: str, error: str) -> None:
        self._send("failed", run_id, error=error[:MAX_ERROR_CHARS])

    def _send(self, event: str, run_id: str, **extra: object) -> None:
        if not self.enabled:
            return
        try:
            at = utcnow().isoformat(timespec="milliseconds").replace("+00:00", "Z")
            with httpx.Client(timeout=self._timeout, transport=self._transport) as http:
                resp = http.post(
                    self._url,
                    json={"event": event, "runId": run_id, "at": at, **extra},
                    headers={
                        "Authorization": f"Bearer {self._token}",
                        "User-Agent": "scanlake-refinery",
                    },
                )
        except Exception as e:
            log.warning("FrontDesk %s notification failed: %s", event, e)
            return
        if resp.is_success:
            log.debug("FrontDesk %s notification sent", event)
        else:
            log.warning("FrontDesk %s notification failed: HTTP %d", event, resp.status_code)
