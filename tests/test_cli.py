import httpx
import pytest
from conftest import API_KEY, USER

from scanlake_refinery import cli
from scanlake_refinery.gateway import GatewayClient
from scanlake_refinery.lock import RunLock


@pytest.fixture
def env(monkeypatch, tmp_path, gateway):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GATEWAY_URL", "https://gateway.test")
    monkeypatch.setenv("GATEWAY_API_KEY", API_KEY)
    monkeypatch.setenv("REFINERY_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("REFINERY_PUBLISH", "true")
    monkeypatch.setattr(
        cli,
        "make_client",
        lambda s: GatewayClient(
            s.gateway_url, s.require_api_key(), transport=httpx.MockTransport(gateway.handler)
        ),
    )
    return tmp_path / "data"


def test_run_end_to_end(env, gateway, capsys):
    assert cli.main(["run"]) == 0

    derived = sorted(gateway.derived)
    assert [k for k in derived if not k.startswith(("results/", "monthly/"))] == [
        "consolidated/compositions.parquet",
        "consolidated/confidences.parquet",
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


def test_run_without_publish_keeps_results_local(env, gateway):
    assert cli.main(["run", "--no-publish"]) == 0

    assert gateway.derived == {}
    assert (env / "out" / "manifest.json").is_file()


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
