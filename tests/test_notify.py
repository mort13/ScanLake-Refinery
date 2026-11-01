import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from conftest import FRONTDESK_TOKEN, FRONTDESK_URL

from scanlake_refinery.config import load_settings
from scanlake_refinery.notify import MAX_ERROR_CHARS, FrontDesk

RUN_ID = "20261101T120000Z-abc123"
TOTALS = {"scans": 3, "sessions": 2, "users": 1}


def make(frontdesk, url=FRONTDESK_URL, token=FRONTDESK_TOKEN) -> FrontDesk:
    return FrontDesk(url, token, transport=httpx.MockTransport(frontdesk.handler))


def notify_all(fd: FrontDesk) -> None:
    fd.started(RUN_ID)
    fd.finished(RUN_ID, TOTALS)
    fd.failed(RUN_ID, "GatewayError: boom")


@pytest.mark.parametrize(
    "url, token", [(None, FRONTDESK_TOKEN), (FRONTDESK_URL, None), (None, None), ("", "")]
)
def test_disabled_without_url_or_token(frontdesk, url, token):
    fd = make(frontdesk, url, token)
    notify_all(fd)

    assert not fd.enabled
    assert frontdesk.requests == []


def test_settings():
    s = load_settings({"FRONTDESK_URL": "https://scanlake.rocks/", "FRONTDESK_TOKEN": "t"})
    assert (s.frontdesk_url, s.frontdesk_token) == ("https://scanlake.rocks", "t")

    for env in ({}, {"FRONTDESK_URL": "", "FRONTDESK_TOKEN": ""}):
        s = load_settings(env)
        assert (s.frontdesk_url, s.frontdesk_token) == (None, None)


@pytest.mark.parametrize("url", [FRONTDESK_URL, FRONTDESK_URL + "/"])
def test_request(frontdesk, url):
    make(frontdesk, url).started(RUN_ID)

    (request,) = frontdesk.requests
    assert request.method == "POST"
    assert str(request.url) == "https://frontdesk.test/api/ingest/refinery"
    assert request.headers["Authorization"] == f"Bearer {FRONTDESK_TOKEN}"
    assert request.headers["Content-Type"] == "application/json"
    assert request.extensions["timeout"] == dict.fromkeys(("connect", "read", "write", "pool"), 5.0)

    body = json.loads(request.content)
    assert body == {"event": "started", "runId": RUN_ID, "at": body["at"]}
    assert body["at"].endswith("Z")
    at = datetime.fromisoformat(body["at"])
    assert at.utcoffset() == timedelta(0)
    assert abs(datetime.now(UTC) - at) < timedelta(minutes=1)


def test_bodies(frontdesk):
    notify_all(make(frontdesk))
    make(frontdesk).failed(RUN_ID, "x" * (MAX_ERROR_CHARS + 100))

    started, finished, failed, long_failure = frontdesk.events()
    assert finished == {
        "event": "finished",
        "runId": RUN_ID,
        "at": finished["at"],
        "totals": TOTALS,
    }
    assert failed == {
        "event": "failed",
        "runId": RUN_ID,
        "at": failed["at"],
        "error": "GatewayError: boom",
    }
    assert long_failure["error"] == "x" * MAX_ERROR_CHARS


@pytest.mark.parametrize(
    "error, message",
    [
        (httpx.ConnectError("connection refused"), "connection refused"),
        (httpx.ReadTimeout("timed out"), "timed out"),
        (RuntimeError("unexpected"), "unexpected"),
    ],
)
def test_errors_are_logged_not_raised(frontdesk, caplog, error, message):
    frontdesk.error = error
    notify_all(make(frontdesk))

    assert len(frontdesk.requests) == 3
    for event in ("started", "finished", "failed"):
        assert f"FrontDesk {event} notification failed: {message}" in caplog.text


@pytest.mark.parametrize("status", [500, 503, 401, 301])
def test_http_errors_are_logged_not_raised(frontdesk, caplog, status):
    frontdesk.status = status
    make(frontdesk).finished(RUN_ID, TOTALS)

    assert f"FrontDesk finished notification failed: HTTP {status}" in caplog.text
