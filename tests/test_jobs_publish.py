import json

import pytest
from conftest import RAW_FILES, write_raw

from scanlake_refinery import jobs, publish
from scanlake_refinery.consolidate import consolidate


@pytest.fixture
def consolidated(con, tmp_path):
    raw = tmp_path / "raw"
    write_raw(raw, RAW_FILES, tmp_path / "fixtures")
    consolidate(con, raw)
    return con


def test_scan_counts(consolidated):
    summary = jobs.compute(consolidated)

    assert summary["scan_counts"] == 3
    assert consolidated.execute(
        "SELECT CAST(day AS VARCHAR), system, deposit, rocks, clusters, total_mass "
        "FROM results_scan_counts ORDER BY ALL"
    ).fetchall() == [
        ("2026-03-28", "Stanton", "Quantanium", 1, 1, 12000),
        ("2026-03-28", "Stanton", "copper", 1, 1, 9000),
        ("2026-06-24", "Nyx", "Taranite", 1, 1, 1000),
    ]


def test_stale_result_tables_are_dropped(consolidated):
    consolidated.execute("CREATE TABLE results_removed_job AS SELECT 1 AS x")

    jobs.compute(consolidated)

    assert (
        consolidated.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'results_removed_job'"
        ).fetchone()[0]
        == 0
    )


def test_export_is_deterministic(consolidated, settings):
    jobs.compute(consolidated)

    first = publish.export(consolidated, settings.out_dir, "run-1")
    second = publish.export(consolidated, settings.out_dir, "run-2")

    assert [f["sha256"] for f in first] == [f["sha256"] for f in second]
    assert [f["key"] for f in first] == [
        "consolidated/scans.parquet",
        "consolidated/compositions.parquet",
        "consolidated/confidences.parquet",
        "results/scan_counts.parquet",
    ]
    manifest = json.loads((settings.out_dir / "manifest.json").read_text())
    assert manifest["run_id"] == "run-2"
    assert manifest["files"] == second


def test_upload_sends_only_changed_files_and_manifest_last(consolidated, client, gateway, settings):
    jobs.compute(consolidated)
    publish.export(consolidated, settings.out_dir, "run-1")

    first = publish.upload(consolidated, client, settings.out_dir)
    assert len(first["uploaded"]) == 4
    assert gateway.uploads()[-1] == "manifest.json"
    assert (
        gateway.derived["consolidated/scans.parquet"]
        == (settings.out_dir / "consolidated/scans.parquet").read_bytes()
    )

    gateway.requests.clear()
    publish.export(consolidated, settings.out_dir, "run-2")
    second = publish.upload(consolidated, client, settings.out_dir)
    assert second == {"uploaded": [], "unchanged": 4}
    assert gateway.uploads() == ["manifest.json"]

    gateway.requests.clear()
    consolidated.execute("UPDATE scans SET mass = 1 WHERE capture_id = 'b1'")
    publish.export(consolidated, settings.out_dir, "run-3")
    third = publish.upload(consolidated, client, settings.out_dir)
    assert third["uploaded"] == ["consolidated/scans.parquet"]
    assert gateway.uploads() == ["consolidated/scans.parquet", "manifest.json"]
