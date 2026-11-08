"""Ship positions (format v3): consolidation, publishing and the cluster distance jobs."""

from datetime import date

import pytest
from conftest import CURRENT, LEGACY, RAW_FILES, USER, write_raw

from scanlake_refinery import jobs, publish
from scanlake_refinery.consolidate import consolidate

COLUMNS = (
    "t(capture_id, zone_index, zone, pos_x, pos_y, pos_z, cam_dir_x, cam_dir_y, cam_dir_z, "
    "confidence)"
)


def positions_sql(*rows: tuple) -> str:
    values = ", ".join(
        f"('{c}', {i}::INTEGER, '{z}', {x}::DOUBLE, {y}::DOUBLE, {zz}::DOUBLE, "
        f"-155.0::DOUBLE, 45.0::DOUBLE, -179.0::DOUBLE, 0.95::DOUBLE)"
        for c, i, z, x, y, zz in rows
    )
    return f"SELECT * FROM (VALUES {values}) {COLUMNS}"


# b1 (Keeger Ring): ship, asteroid field, an unreadable line (index 2, dropped by the client),
# solar system, root. zz has no scan.
B1_POSITIONS = positions_sql(
    ("b1", 0, "MISC_Prospector_1", 1, 2, 3),
    ("b1", 1, " ab_mine_nyx_1 ", 1200.5, -300, 40),
    ("b1", 3, "SolarSystem_906804811333", 9e9, 1e8, 0),
    ("b1", 4, "Root", 5e10, 0, 0),
    ("zz", 0, "Root", 0, 0, 0),
)
# a1 (Daymar): no SolarSystem or Root line, so its last line is the local zone.
A1_POSITIONS = positions_sql(
    ("a1", 0, "MISC_Prospector_1", 1, 2, 3), ("a1", 1, "OOC_Stanton_2b", 7, 8, 9)
)


@pytest.fixture
def raw_dir(tmp_path):
    raw = tmp_path / "raw"
    write_raw(raw, RAW_FILES, tmp_path / "fixtures")
    write_raw(raw, {f"{CURRENT}/positions_batch001.parquet": B1_POSITIONS}, tmp_path / "fixtures")
    return raw


def rows(con, sql):
    return con.execute(sql).fetchall()


def test_positions_are_consolidated_with_their_local_zone(con, raw_dir):
    summary = consolidate(con, raw_dir)

    assert (summary["positions"], summary["captures_with_position"]) == (4, 1)
    assert rows(
        con,
        "SELECT capture_id, zone_index, zone, pos_x, cam_dir_x, confidence, is_local "
        "FROM positions ORDER BY capture_id, zone_index",
    ) == [
        ("b1", 0, "MISC_Prospector_1", 1.0, -155.0, 0.95, False),
        ("b1", 1, "ab_mine_nyx_1", 1200.5, -155.0, 0.95, True),  # trimmed, not capitalised
        ("b1", 3, "SolarSystem_906804811333", 9e9, -155.0, 0.95, False),
        ("b1", 4, "Root", 5e10, -155.0, 0.95, False),
    ]
    types = dict(
        rows(con, "SELECT column_name, data_type FROM duckdb_columns() "
                  "WHERE table_name = 'positions'")
    )  # fmt: skip
    assert types["zone_index"] == "INTEGER"
    assert types["pos_x"] == "DOUBLE"
    assert types["is_local"] == "BOOLEAN"
    assert rows(con, "SELECT DISTINCT source_key FROM positions") == [
        (f"{CURRENT}/positions_batch001.parquet",)
    ]


def test_newest_positions_file_wins(con, raw_dir, tmp_path):
    newer = positions_sql(
        ("b1", 0, "SolarSystem_906804811333", 1, 1, 1), ("b1", 1, "Root", 2, 2, 2)
    )
    write_raw(raw_dir, {f"{CURRENT}/positions_batch002.parquet": newer}, tmp_path / "fixtures")

    consolidate(con, raw_dir)

    # All lines come from the newest file holding the scan: the old index 3 and 4 do not leak in.
    # Its top line is a system zone, so that line is the local zone.
    assert rows(con, "SELECT zone_index, zone, is_local, batch FROM positions ORDER BY 1") == [
        (0, "SolarSystem_906804811333", True, 2),
        (1, "Root", False, 2),
    ]


def test_local_zone_without_a_system_line_is_the_last_line(con, raw_dir, tmp_path):
    write_raw(
        raw_dir, {f"{LEGACY}/positions_batch001.parquet": A1_POSITIONS}, tmp_path / "fixtures"
    )

    consolidate(con, raw_dir)

    assert rows(
        con, "SELECT zone_index, is_local FROM positions WHERE capture_id = 'a1' ORDER BY 1"
    ) == [(0, False), (1, True)]


def test_files_deleted_in_bucket_take_their_positions_along(con, raw_dir):
    con.execute(
        "INSERT INTO sync_state VALUES (?, 0, NULL, '2026-06-24T12:00:00Z', '', now(), TRUE)",
        [f"{CURRENT}/positions_batch001.parquet"],
    )

    summary = consolidate(con, raw_dir)

    assert summary["positions"] == 0


def test_mirror_without_positions_gives_an_empty_table(con, tmp_path):
    raw = tmp_path / "raw"
    write_raw(raw, RAW_FILES, tmp_path / "fixtures")

    summary = consolidate(con, raw)

    assert summary["positions"] == 0
    assert "is_local" in [r[0] for r in rows(con, "DESCRIBE positions")]


def test_positions_are_published_like_the_other_tables(con, raw_dir, tmp_path, settings):
    write_raw(
        raw_dir, {f"{LEGACY}/positions_batch001.parquet": A1_POSITIONS}, tmp_path / "fixtures"
    )
    consolidate(con, raw_dir)

    files = {
        f["key"]: f["rows"]
        for f in publish.export(con, settings.out_dir, "run-1", today=date(2026, 7, 1))
    }

    assert files["consolidated/positions.parquet"] == 6
    assert files[f"monthly/2026/03/{USER}/positions.parquet"] == 2
    assert files[f"monthly/2026/06/{USER}/positions.parquet"] == 4


# The distance jobs, on hand-made scans and positions.

BLOOM = ("Pyro", "Bloom", None)
KEEGER = ("Nyx", "Keeger Ring", None)
SHIP = (1.0, 2.0, 3.0)  # the ship's own frame: every cluster would be 0 m apart in it


def located(capture, cluster, location, deposit, local, local_xyz, system, system_xyz, root_xyz):
    """A scan and its four Zone lines: ship, local zone, solar system, root."""
    scan = [capture, cluster, "s1", *location, deposit]
    lines = [
        [capture, 0, "MISC_Prospector_1", *SHIP, False],
        [capture, 1, local, *local_xyz, True],
        [capture, 2, system, *system_xyz, False],
        [capture, 3, "Root", *root_xyz, False],
    ]
    return scan, lines


PYRO, NYX = "SolarSystem_1", "SolarSystem_2"
LOCATED = [
    # Bloom: three clusters on the planet, 3000, 4000 and 5000 m apart in the planet's frame.
    # Their solar system coordinates disagree (the planet turns) and must not be used.
    located("r1", "c1", BLOOM, "Iron", "OOC_Pyro_Bloom", (0, 0, 0), PYRO, (1e9, 0, 0),
            (2e10, 0, 0)),
    located("r2", "c1", BLOOM, "Iron", "OOC_Pyro_Bloom", (200, 0, 0), PYRO, (1e9, 5e5, 0),
            (2e10, 5e5, 0)),
    located("r3", "c2", BLOOM, "Iron", "OOC_Pyro_Bloom", (100, 3000, 0), PYRO, (1e9, 9e5, 0),
            (2e10, 9e5, 0)),
    located("r4", "c3", BLOOM, "Quartz", "OOC_Pyro_Bloom", (100, 0, 4000), PYRO, (1e9, 0, 3e5),
            (2e10, 0, 3e5)),
    # Keeger Ring: k1 and k2 in different asteroid fields, 7000 m apart in the solar system (the
    # root frame says 9000 and loses: it is not the innermost shared frame). k3, without a
    # deposit, shares k2's field and is 600 m from it.
    located("k1", "c4", KEEGER, "Ice", "ab_mine_1", (0, 0, 0), NYX, (0, 0, 0), (5e10, 0, 0)),
    located("k2", "c5", KEEGER, "Ice", "ab_mine_2", (0, 0, 0), NYX, (0, 0, 7000),
            (5e10, 0, 9000)),
    located("k3", "c6", KEEGER, None, "ab_mine_2", (0, 600, 0), NYX, (0, 600, 7000),
            (5e10, 600, 9000)),
]  # fmt: skip
UNLOCATED = [["r5", "c7", "s1", *BLOOM, "Iron"]]  # no position: not counted


@pytest.fixture
def distances(con, tmp_path):
    consolidate(con, tmp_path / "empty")
    con.executemany(
        "INSERT INTO scans (capture_id, cluster_id, session_id, system, gravity_well, region, "
        "deposit) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [scan for scan, _ in LOCATED] + UNLOCATED,
    )
    con.executemany(
        "INSERT INTO positions (capture_id, zone_index, zone, pos_x, pos_y, pos_z, is_local) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [line for _, lines in LOCATED for line in lines],
    )
    jobs.compute(con, only=["cluster_distances", "cluster_distances_by_region"])
    return con


DISTANCE_COLUMNS = (
    "deposit, location_clusters, location_cluster_pairs, location_avg_cluster_distance_m, "
    "location_median_cluster_distance_m, location_avg_nearest_cluster_m, "
    "location_median_nearest_cluster_m, deposit_clusters, deposit_cluster_pairs, "
    "deposit_avg_cluster_distance_m, deposit_median_cluster_distance_m, "
    "deposit_avg_nearest_cluster_m, deposit_median_nearest_cluster_m"
)


def test_clusters_on_a_planet_are_measured_in_its_frame(distances):
    assert rows(
        distances,
        f"SELECT {DISTANCE_COLUMNS} FROM results_cluster_distances "
        "WHERE gravity_well = 'Bloom' ORDER BY deposit",
    ) == [
        # Pairs 3000, 4000, 5000; nearest 3000 (c1), 3000 (c2), 4000 (c3).
        ("Iron", 3, 3, 4000.0, 4000.0, 3333.3, 3000.0, 2, 1, 3000.0, 3000.0, 3000.0, 3000.0),
        ("Quartz", 3, 3, 4000.0, 4000.0, 3333.3, 3000.0, 1, 0, None, None, None, None),
    ]


def test_clusters_in_different_fields_meet_in_the_solar_system(distances):
    assert rows(
        distances,
        f"SELECT {DISTANCE_COLUMNS} FROM results_cluster_distances "
        "WHERE gravity_well = 'Keeger Ring' ORDER BY deposit NULLS LAST",
    ) == [
        # Pairs 7000 (k1-k2), 600 (k2-k3), 7025.7 (k1-k3); nearest 7000, 600, 600.
        ("Ice", 3, 3, 4875.2, 7000.0, 2733.3, 600.0, 2, 1, 7000.0, 7000.0, 7000.0, 7000.0),
        (None, 3, 3, 4875.2, 7000.0, 2733.3, 600.0, 1, 0, None, None, None, None),
    ]


def test_region_split_keeps_the_region(distances):
    assert rows(
        distances,
        "SELECT system, gravity_well, region, deposit, deposit_clusters "
        "FROM results_cluster_distances_by_region ORDER BY ALL NULLS LAST",
    ) == [
        ("Nyx", "Keeger Ring", None, "Ice", 2),
        ("Nyx", "Keeger Ring", None, None, 1),
        ("Pyro", "Bloom", None, "Iron", 2),
        ("Pyro", "Bloom", None, "Quartz", 1),
    ]
