import pytest
from conftest import CURRENT, CURRENT_FILES, LEGACY, RAW_FILES, write_raw

from scanlake_refinery import consolidate as consolidate_module
from scanlake_refinery.consolidate import consolidate


@pytest.fixture
def raw_dir(tmp_path):
    raw = tmp_path / "raw"
    write_raw(raw, RAW_FILES, tmp_path / "fixtures")
    return raw


def rows(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def test_counts_and_canonical_types(con, raw_dir):
    summary = consolidate(con, raw_dir)

    assert (summary["scans"], summary["compositions"], summary["confidences"]) == (3, 6, 3)
    assert summary["clusters"] == 2
    types = dict(
        rows(con, "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = 'scans'")
    )
    assert types["mass"] == "INTEGER"
    assert types["resistance"] == "INTEGER"
    assert types["timestamp"] == "TIMESTAMP WITH TIME ZONE"
    assert types["source_key"] == "VARCHAR"


def test_timestamps_parse_in_both_formats(con, raw_dir):
    consolidate(con, raw_dir)

    got = dict(
        rows(con, "SELECT capture_id, strftime(\"timestamp\", '%Y-%m-%dT%H:%M:%S.%g') FROM scans")
    )
    assert got == {
        "a1": "2026-03-28T11:18:29.000",
        "a2": "2026-03-28T11:19:01.000",
        "b1": "2026-06-24T09:04:12.345",
    }


def test_legacy_confidences_come_from_inline_scan_columns(con, raw_dir):
    summary = consolidate(con, raw_dir)

    assert summary["excluded_legacy_confidences"] == 1
    assert summary["inline_confidence_files"] == 1
    assert rows(
        con, "SELECT capture_id, deposit_conf, volume_conf FROM confidences ORDER BY 1"
    ) == [
        ("a1", 0.9, 0.5),
        ("a2", 0.9, 0.9),
        ("b1", 1.0, 1.0),
    ]


def test_missing_min_confidence_becomes_null(con, raw_dir):
    consolidate(con, raw_dir)

    assert rows(
        con, "SELECT capture_id, count(min_confidence) FROM compositions GROUP BY 1 ORDER BY 1"
    ) == [("a1", 0), ("a2", 0), ("b1", 2)]


def test_lineage_and_trimmed_names(con, raw_dir):
    consolidate(con, raw_dir)

    assert rows(con, "SELECT deposit, source_key, batch FROM scans WHERE capture_id = 'a1'") == [
        ("copper", f"{LEGACY}/scans_batch001.parquet", 1)
    ]


def test_latest_batch_wins_per_capture(con, raw_dir, tmp_path):
    write_raw(
        raw_dir,
        {
            f"{CURRENT}/scans_batch002.parquet": CURRENT_FILES[
                f"{CURRENT}/scans_batch001.parquet"
            ].replace("1000::INTEGER", "2000::INTEGER"),
            f"{CURRENT}/compositions_batch002.parquet": """
                SELECT * FROM (VALUES ('b1', 0::INTEGER, 'Taranite', 100.0, 650::INTEGER, 5.0, 1.0))
                    t(capture_id, mat_index, type, amount, quality, material_volume, min_confidence)
            """,
        },
        tmp_path / "fixtures",
    )

    consolidate(con, raw_dir)

    assert rows(con, "SELECT mass, batch FROM scans WHERE capture_id = 'b1'") == [(2000, 2)]
    # All minerals come from the newest file containing the capture; stale rows don't leak in.
    assert rows(con, "SELECT mat_index, amount FROM compositions WHERE capture_id = 'b1'") == [
        (0, 100.0)
    ]


def test_upload_time_from_sync_state_beats_path_order(con, raw_dir, tmp_path):
    key2 = f"{CURRENT}/scans_batch002.parquet"
    write_raw(
        raw_dir,
        {
            key2: CURRENT_FILES[f"{CURRENT}/scans_batch001.parquet"].replace(
                "1000::INTEGER", "2000::INTEGER"
            )
        },
        tmp_path / "fixtures",
    )
    # batch001 was re-uploaded after batch002.
    con.executemany(
        "INSERT INTO sync_state VALUES (?, 0, NULL, ?, '', now(), FALSE)",
        [
            [f"{CURRENT}/scans_batch001.parquet", "2026-06-25T00:00:00.000Z"],
            [key2, "2026-06-24T10:00:00.000Z"],
        ],
    )

    consolidate(con, raw_dir)

    assert rows(con, "SELECT mass FROM scans WHERE capture_id = 'b1'") == [(1000,)]


def test_files_deleted_in_bucket_are_excluded(con, raw_dir):
    con.execute(
        "INSERT INTO sync_state VALUES (?, 0, NULL, '2026-03-28T12:00:00Z', '', now(), TRUE)",
        [f"{LEGACY}/scans_batch001.parquet"],
    )

    summary = consolidate(con, raw_dir)

    assert summary["excluded_deleted_remote"] == 1
    assert rows(con, "SELECT capture_id FROM scans") == [("b1",)]


def test_empty_mirror_gives_empty_tables(con, tmp_path):
    summary = consolidate(con, tmp_path / "nothing")

    assert (summary["scans"], summary["compositions"], summary["confidences"]) == (0, 0, 0)
    assert "deposit" in [r[0] for r in rows(con, "DESCRIBE scans")]


def test_rebuild_is_idempotent(con, raw_dir):
    first = consolidate(con, raw_dir)
    second = consolidate(con, raw_dir)

    assert first == second


def test_aliases_fix_spelling(con, raw_dir, monkeypatch):
    monkeypatch.setattr(consolidate_module, "DEPOSIT_ALIASES", {"copper": "Copper"})
    monkeypatch.setattr(consolidate_module, "MINERAL_ALIASES", {"inert materials": "Inert"})

    consolidate(con, raw_dir)

    assert rows(con, "SELECT DISTINCT deposit FROM scans ORDER BY 1") == [
        ("Copper",),
        ("Quantanium",),
        ("Taranite",),
    ]
    assert rows(con, "SELECT count(*) FROM compositions WHERE type = 'Inert'") == [(3,)]
