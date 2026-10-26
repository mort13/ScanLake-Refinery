import json
from datetime import date

import pytest
from conftest import RAW_FILES, USER, write_raw

from scanlake_refinery import jobs, publish
from scanlake_refinery.consolidate import consolidate

# June is still open: only the March fixtures get monthly files.
JUNE = date(2026, 6, 30)
JULY = date(2026, 7, 1)
OTHER_USER = "0f0f0f0f-0000-4000-8000-000000000000"
OTHER_RAW = f"2026/03/30/{OTHER_USER}/33333333-3333-4333-8333-333333333333"
RESULT_KEYS = [
    "results/deposit_materials.parquet",
    "results/deposit_materials_by_region.parquet",
    "results/highscores.parquet",
    "results/location_stats.parquet",
    "results/location_stats_by_region.parquet",
    "results/scan_counts.parquet",
    "results/user_stats.parquet",
]


@pytest.fixture
def consolidated(con, tmp_path):
    raw = tmp_path / "raw"
    write_raw(raw, RAW_FILES, tmp_path / "fixtures")
    write_raw(
        raw,
        {
            f"{OTHER_RAW}/scans_batch001.parquet": """
                SELECT * FROM (VALUES
                    ('s3', 'u2', 'Doc', '', '2026-03-30T20:00:00.000Z', 'd1', 'c9', 'Pyro',
                     'Bloom', 'none', 'none', 'Iron', 5000::INTEGER, 10::INTEGER, 1.0, 1.0)
                ) t(session_id, user_id, user_name, org, "timestamp", capture_id, cluster_id,
                    system, gravity_well, region, place, deposit, mass, resistance, instability,
                    volume)
            """,
            f"{OTHER_RAW}/compositions_batch001.parquet": """
                SELECT * FROM (VALUES ('d1', 0::INTEGER, 'Iron', 100.0, 400::INTEGER, 1.0, 1.0))
                    t(capture_id, mat_index, type, amount, quality, material_volume,
                      min_confidence)
            """,
        },
        tmp_path / "fixtures",
    )
    consolidate(con, raw)
    return con


def test_scan_counts(consolidated):
    summary = jobs.compute(consolidated)

    assert summary["scan_counts"] == 4
    assert consolidated.execute(
        "SELECT CAST(day AS VARCHAR), system, deposit, rocks, clusters, total_mass "
        "FROM results_scan_counts ORDER BY ALL"
    ).fetchall() == [
        ("2026-03-28", "Stanton", "Copper", 1, 1, 9000),
        ("2026-03-28", "Stanton", "Quantanium", 1, 1, 12000),
        ("2026-03-30", "Pyro", "Iron", 1, 1, 5000),
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

    first = publish.export(consolidated, settings.out_dir, "run-1", today=JUNE)
    second = publish.export(consolidated, settings.out_dir, "run-2", today=JUNE)

    assert [f["sha256"] for f in first] == [f["sha256"] for f in second]
    assert [f["key"] for f in first] == [
        "consolidated/scans.parquet",
        "consolidated/compositions.parquet",
        "consolidated/confidences.parquet",
        *RESULT_KEYS,
        f"monthly/2026/03/{OTHER_USER}/scans.parquet",
        f"monthly/2026/03/{USER}/scans.parquet",
        f"monthly/2026/03/{OTHER_USER}/compositions.parquet",
        f"monthly/2026/03/{USER}/compositions.parquet",
        f"monthly/2026/03/{USER}/confidences.parquet",
    ]
    manifest = json.loads((settings.out_dir / "manifest.json").read_text())
    assert manifest["run_id"] == "run-2"
    assert manifest["files"] == second


def test_monthly_files_split_closed_months_by_user(consolidated, settings):
    files = publish.export(consolidated, settings.out_dir, "run-1", today=JULY)

    monthly = {f["key"]: f["rows"] for f in files if f["key"].startswith("monthly/")}
    assert monthly == {
        f"monthly/2026/03/{USER}/scans.parquet": 2,
        f"monthly/2026/03/{USER}/compositions.parquet": 4,
        f"monthly/2026/03/{USER}/confidences.parquet": 2,
        f"monthly/2026/03/{OTHER_USER}/scans.parquet": 1,
        f"monthly/2026/03/{OTHER_USER}/compositions.parquet": 1,
        f"monthly/2026/06/{USER}/scans.parquet": 1,
        f"monthly/2026/06/{USER}/compositions.parquet": 2,
        f"monthly/2026/06/{USER}/confidences.parquet": 1,
    }
    path = (settings.out_dir / f"monthly/2026/03/{OTHER_USER}/scans.parquet").as_posix()
    assert consolidated.execute(
        f"SELECT capture_id, deposit, source_key FROM read_parquet('{path}')"
    ).fetchall() == [("d1", "Iron", f"{OTHER_RAW}/scans_batch001.parquet")]


def test_files_no_longer_produced_are_removed(consolidated, settings):
    publish.export(consolidated, settings.out_dir, "run-1", today=JULY)
    june = settings.out_dir / f"monthly/2026/06/{USER}"
    assert (june / "scans.parquet").is_file()

    files = publish.export(consolidated, settings.out_dir, "run-2", today=JUNE)

    assert not (settings.out_dir / "monthly/2026/06").exists()
    assert not any(f["key"].startswith("monthly/2026/06/") for f in files)


def test_upload_sends_only_changed_files_and_manifest_last(consolidated, client, gateway, settings):
    jobs.compute(consolidated)
    files = publish.export(consolidated, settings.out_dir, "run-1", today=JUNE)

    first = publish.upload(consolidated, client, settings.out_dir)
    assert len(first["uploaded"]) == len(files)
    assert gateway.uploads()[-1] == "manifest.json"
    assert (
        gateway.derived["consolidated/scans.parquet"]
        == (settings.out_dir / "consolidated/scans.parquet").read_bytes()
    )

    gateway.requests.clear()
    publish.export(consolidated, settings.out_dir, "run-2", today=JUNE)
    second = publish.upload(consolidated, client, settings.out_dir)
    assert second == {"uploaded": [], "unchanged": len(files)}
    assert gateway.uploads() == ["manifest.json"]

    gateway.requests.clear()
    # b1 lives in June, which is still open: no monthly file changes.
    consolidated.execute("UPDATE scans SET mass = 1 WHERE capture_id = 'b1'")
    publish.export(consolidated, settings.out_dir, "run-3", today=JUNE)
    third = publish.upload(consolidated, client, settings.out_dir)
    assert third["uploaded"] == ["consolidated/scans.parquet"]
    assert gateway.uploads() == ["consolidated/scans.parquet", "manifest.json"]
