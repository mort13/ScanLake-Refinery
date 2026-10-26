from datetime import datetime, timedelta

import pytest

from scanlake_refinery import jobs
from scanlake_refinery.consolidate import consolidate

BLOOM = ("Pyro", "Bloom", None)
THETA = ("Nyx", "Keeger Ring", "PSS Theta")
KEEGER = ("Nyx", "Keeger Ring", None)
CLIO = ("Stanton", "Clio", None)


def scan(capture, session, user, start, seconds, location, deposit, mass):
    user_id, user_name = user
    ts = (start + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S+00")
    return [capture, "c", session, user_id, user_name, ts, *location, deposit, mass]


MORT = ("u1", "Mort13")
MORT_ALT = ("u2", "mort13")  # same person, second user ID, newer spelling
DOC = ("u3", "Doc")
JUNE_1, JUNE_2 = datetime(2026, 6, 1, 10), datetime(2026, 6, 2, 10)
JUNE_3, JUNE_4 = datetime(2026, 6, 3, 10), datetime(2026, 6, 4, 10)

SCANS = [
    # Bloom: 1 min apart, then a 15 min pause.
    scan("r1", "s1", MORT, JUNE_1, 0, BLOOM, "Iron", 1000),
    scan("r2", "s1", MORT, JUNE_1, 60, BLOOM, "Iron", 3000),
    scan("r3", "s1", MORT, JUNE_1, 120, BLOOM, "Quartz", 2000),
    scan("r4", "s1", MORT, JUNE_1, 1020, BLOOM, "Quartz", 500),  # no composition
    # Keeger Ring: two scans near PSS Theta, then one without region.
    scan("k1", "s2", MORT_ALT, JUNE_2, 0, THETA, "Ice", 1000),
    scan("k2", "s2", MORT_ALT, JUNE_2, 30, THETA, "Ice", 1000),
    scan("k3", "s2", MORT_ALT, JUNE_2, 90, KEEGER, "Ice", 1000),
    # Clio: a 30-scan session at 20 s and a faster 29-scan session that is too short to rank.
    *[scan(f"s3-{i}", "s3", DOC, JUNE_3, 20 * i, CLIO, "Copper", 100) for i in range(30)],
    *[scan(f"s4-{i}", "s4", DOC, JUNE_4, 10 * i, CLIO, "Copper", 100) for i in range(29)],
]
COMPOSITIONS = [
    ["r1", 0, "Iron", 65.0, 500],
    ["r1", 1, "Gold", 5.0, 800],
    ["r1", 2, "Inert Materials", 30.0, 0],
    ["r2", 0, "Iron", 60.0, 700],
    ["r2", 1, "Iron", 10.0, 300],  # the same mineral twice in one rock
    ["r2", 2, "Inert Materials", 30.0, 0],
    ["r3", 0, "Quartz", 50.0, 1518],  # out-of-range quality
    ["r3", 1, "Inert Materials", 50.0, 0],
]


@pytest.fixture
def results(con, tmp_path):
    consolidate(con, tmp_path / "empty")
    con.executemany(
        'INSERT INTO scans (capture_id, cluster_id, session_id, user_id, user_name, "timestamp", '
        "system, gravity_well, region, deposit, mass) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        SCANS,
    )
    con.executemany(
        "INSERT INTO compositions (capture_id, mat_index, type, amount, quality) "
        "VALUES (?, ?, ?, ?, ?)",
        COMPOSITIONS,
    )
    jobs.compute(con)
    return con


def rows(con, sql):
    return con.execute(sql).fetchall()


def test_deposit_shares_and_material_distribution(results):
    assert rows(
        results,
        "SELECT deposit, material, location_scans, deposit_scans, deposit_share_pct, "
        "composition_scans, material_scans, presence_pct, avg_share_pct, "
        "avg_share_when_present_pct FROM results_deposit_materials "
        "WHERE gravity_well = 'Bloom' ORDER BY deposit, material",
    ) == [
        ("Iron", "Gold", 4, 2, 50.0, 2, 1, 50.0, 2.5, 5.0),
        ("Iron", "Inert Materials", 4, 2, 50.0, 2, 2, 100.0, 30.0, 30.0),
        ("Iron", "Iron", 4, 2, 50.0, 2, 2, 100.0, 67.5, 67.5),
        ("Quartz", "Inert Materials", 4, 2, 50.0, 1, 1, 100.0, 50.0, 50.0),
        ("Quartz", "Quartz", 4, 2, 50.0, 1, 1, 100.0, 50.0, 50.0),
    ]


def test_material_mass_and_quality(results):
    assert rows(
        results,
        "SELECT material, total_material_mass, quality_avg, quality_weighted_avg, quality_min, "
        "quality_median, quality_max FROM results_deposit_materials "
        "WHERE gravity_well = 'Bloom' ORDER BY deposit, material",
    ) == [
        ("Gold", 50.0, 800.0, 800.0, 800, 800, 800),
        # Inert material carries no quality.
        ("Inert Materials", 1200.0, None, None, None, None, None),
        # Entries 500 @ 650, 700 @ 1800, 300 @ 300 (mass).
        ("Iron", 2750.0, 500.0, 609.09, 300, 500, 700),
        ("Inert Materials", 1000.0, None, None, None, None, None),
        # 1518 is no valid quality.
        ("Quartz", 1000.0, None, None, None, None, None),
    ]


def test_deposit_without_compositions_keeps_one_row(results):
    assert rows(
        results,
        "SELECT deposit, material, deposit_scans, composition_scans, presence_pct "
        "FROM results_deposit_materials WHERE gravity_well = 'Clio'",
    ) == [("Copper", None, 59, 0, None)]


def test_region_split(results):
    assert rows(
        results,
        "SELECT region, deposit, location_scans, deposit_share_pct "
        "FROM results_deposit_materials_by_region WHERE gravity_well = 'Keeger Ring' "
        "ORDER BY region NULLS LAST",
    ) == [("PSS Theta", "Ice", 2, 100.0), (None, "Ice", 1, 100.0)]
    assert rows(
        results,
        "SELECT location_scans FROM results_deposit_materials WHERE gravity_well = 'Keeger Ring'",
    ) == [(3,)]


def test_location_pace_skips_pauses_and_location_changes(results):
    assert rows(
        results,
        "SELECT gravity_well, scans, sessions, users, gaps_counted, avg_gap_seconds "
        "FROM results_location_stats ORDER BY gravity_well",
    ) == [
        ("Bloom", 4, 1, 1, 2, 60.0),
        ("Clio", 59, 2, 1, 57, round((29 * 20 + 28 * 10) / 57, 2)),
        ("Keeger Ring", 3, 1, 1, 2, 45.0),
    ]
    assert rows(
        results,
        "SELECT region, scans, gaps_counted, avg_gap_seconds FROM results_location_stats_by_region "
        "WHERE gravity_well = 'Keeger Ring' ORDER BY region NULLS LAST",
    ) == [("PSS Theta", 2, 1, 30.0), (None, 1, 0, None)]


def test_user_stats_group_names_case_insensitively(results):
    assert rows(
        results,
        "SELECT user_name, user_ids, sessions, scans, biggest_scan_mass, most_scans_in_session, "
        "best_session_avg_gap_seconds FROM results_user_stats ORDER BY scans DESC",
    ) == [
        ("Doc", 1, 2, 59, 100, 30, 20.0),
        ("mort13", 2, 2, 7, 3000, 4, None),
    ]


def test_highscores_list_each_users_best(results):
    assert rows(
        results,
        'SELECT category, "rank", user_name, value, session_id, capture_id, detail '
        'FROM results_highscores ORDER BY category, "rank"',
    ) == [
        ("biggest_scan", 1, "mort13", 3000.0, "s1", "r2", "Iron, Pyro / Bloom"),
        # Doc's rocks all weigh 100; the earliest one counts.
        ("biggest_scan", 2, "Doc", 100.0, "s3", "s3-0", "Copper, Stanton / Clio"),
        # s4 is faster but has only 29 scans; Mort's sessions are too short.
        ("fastest_session_avg_scan_time", 1, "Doc", 20.0, "s3", None, "30 scans"),
        ("most_scans_in_session", 1, "Doc", 30.0, "s3", None, None),
        ("most_scans_in_session", 2, "mort13", 4.0, "s1", None, None),
        ("most_scans_total", 1, "Doc", 59.0, None, None, None),
        ("most_scans_total", 2, "mort13", 7.0, None, None, None),
    ]
