import httpx
import pytest
from conftest import API_KEY, FRONTDESK_TOKEN, FRONTDESK_URL, USER

from scanlake_refinery import cli
from scanlake_refinery.gateway import GatewayClient
from scanlake_refinery.lock import RunLock
from scanlake_refinery.notify import FrontDesk

INGEST = ("POST", "/api/ingest/refinery")


@pytest.fixture
def env(monkeypatch, tmp_path, gateway, frontdesk):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GATEWAY_URL", "https://gateway.test")
    monkeypatch.setenv("GATEWAY_API_KEY", API_KEY)
    monkeypatch.setenv("REFINERY_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("REFINERY_PUBLISH", "true")
    monkeypatch.setenv("FRONTDESK_URL", FRONTDESK_URL)
    monkeypatch.setenv("FRONTDESK_TOKEN", FRONTDESK_TOKEN)
    monkeypatch.setattr(
        cli,
        "make_client",
        lambda s: GatewayClient(
            s.gateway_url, s.require_api_key(), transport=httpx.MockTransport(gateway.handler)
        ),
    )
    monkeypatch.setattr(
        cli,
        "make_frontdesk",
        lambda s: FrontDesk(
            s.frontdesk_url, s.frontdesk_token, transport=httpx.MockTransport(frontdesk.handler)
        ),
    )
    frontdesk.log = gateway.requests
    return tmp_path / "data"


def test_run_end_to_end(env, gateway, capsys):
    assert cli.main(["run"]) == 0

    derived = sorted(gateway.derived)
    assert [k for k in derived if not k.startswith(("results/", "monthly/"))] == [
        "consolidated/compositions.parquet",
        "consolidated/confidences.parquet",
        "consolidated/positions.parquet",  # empty: the fixtures hold no positions
        "consolidated/scans.parquet",
        "manifest.json",
    ]
    assert "results/deposit_materials.parquet" in derived
    # Both fixture months (March and June 2026) are closed by now.
    assert [k for k in derived if k.startswith("monthly/")] == [
        f"monthly/2026/{month}/{USER}/{table}.parquet"
        for month in ("03", "06")
        for table in ("compositions", "confidences", "scans")
    ]

    assert cli.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "6 tracked" in out
    assert "run          ok" in out


def test_run_without_publish_keeps_results_local(env, gateway, frontdesk):
    assert cli.main(["run", "--no-publish"]) == 0

    assert gateway.derived == {}
    assert (env / "out" / "manifest.json").is_file()
    assert [e["event"] for e in frontdesk.events()] == ["started", "finished"]


def test_run_notifies_frontdesk(env, gateway, frontdesk, capsys):
    assert cli.main(["run"]) == 0

    started, finished = frontdesk.events()
    assert started == {"event": "started", "runId": started["runId"], "at": started["at"]}
    # Three captures in two sessions, all by user u1 (see conftest).
    assert finished == {
        "event": "finished",
        "runId": started["runId"],
        "at": finished["at"],
        "totals": {"scans": 3, "sessions": 2, "users": 1},
    }
    # started goes out before the first Gateway call, finished after the manifest upload.
    assert gateway.requests[0] == INGEST
    assert gateway.requests[-2:] == [("PUT", "/api/derived/manifest.json"), INGEST]

    cli.main(["status"])
    assert f"{started['runId']}  run          ok" in capsys.readouterr().out


def test_failed_run_notifies_frontdesk(env, gateway, frontdesk):
    gateway.fail_next = [401]

    assert cli.main(["run"]) == 1
    started, failed = frontdesk.events()
    assert failed == {
        "event": "failed",
        "runId": started["runId"],
        "at": failed["at"],
        "error": "GatewayError: GET /api/files: HTTP 401 busy",
    }


def test_unreachable_frontdesk_does_not_fail_the_run(env, frontdesk, caplog):
    frontdesk.error = httpx.ConnectError("connection refused")

    assert cli.main(["run", "--no-publish"]) == 0
    assert "FrontDesk started notification failed: connection refused" in caplog.text
    assert "FrontDesk finished notification failed: connection refused" in caplog.text


def test_only_run_notifies_frontdesk(env, frontdesk):
    assert cli.main(["sync"]) == 0
    assert cli.main(["consolidate"]) == 0

    assert frontdesk.requests == []


def test_failed_run_is_recorded(env, gateway, capsys):
    gateway.fail_next = [401] * 1

    assert cli.main(["sync"]) == 1
    cli.main(["status"])
    assert "sync         failed" in capsys.readouterr().out


def test_missing_api_key_is_a_clean_error(env, monkeypatch, caplog):
    monkeypatch.delenv("GATEWAY_API_KEY")

    assert cli.main(["sync"]) == 1
    assert "GATEWAY_API_KEY is not set" in caplog.text


def test_concurrent_run_is_refused(env):
    with RunLock(env / ".refinery.lock"):
        assert cli.main(["consolidate"]) == cli.EX_TEMPFAIL
