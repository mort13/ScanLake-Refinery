"""SQL building blocks shared by the result jobs. Not a job itself.

Timestamps are the moment a scan was saved in Capture, so "scan time" is the time between two
saves of the same session. A gap longer than PAUSE_SECONDS counts as a break and is left out.
"""

from __future__ import annotations

PAUSE_SECONDS = 600
MIN_SESSION_SCANS = 30
TOP_N = 10
INERT = "Inert Materials"

LOCATION = ["system", "gravity_well"]
LOCATION_BY_REGION = ["system", "gravity_well", "region"]

# Users are told apart by name, case-insensitively; one person may own several user IDs.
USER_KEY = "coalesce(lower(nullif(trim(user_name), '')), user_id)"
# The most recent spelling of a user's name.
USER_NAME = "arg_max(coalesce(nullif(trim(user_name), ''), user_id), \"timestamp\")"


def scan_gaps(keys: list[str]) -> str:
    """Every scan plus `user_key`, `gap_seconds` since the previous scan of its session,
    `counted` (the gap is no pause), `same_location` (the previous scan had the same `keys`) and
    `new_cluster` (the previous scan was in another cluster)."""
    same = " AND ".join(f"{k} IS NOT DISTINCT FROM lag({k}) OVER w" for k in keys)
    return f"""
        SELECT *, gap_seconds <= {PAUSE_SECONDS} AS counted FROM (
            SELECT *,
                {USER_KEY} AS user_key,
                date_diff('millisecond', lag("timestamp") OVER w, "timestamp") / 1000.0
                    AS gap_seconds,
                {same} AS same_location,
                cluster_id IS DISTINCT FROM lag(cluster_id) OVER w AS new_cluster
            FROM scans
            WINDOW w AS (PARTITION BY session_id ORDER BY "timestamp", capture_id)
        )
    """


# One row per composition entry with its scan. A rock can list the same mineral more than once:
# usually a small high-quality fraction first and the bulk second. `entry` numbers a mineral's
# entries in a rock in the order they are listed (1, 2, ...).
# Quality is meaningless for inert material and out of range for a few old OCR misreads.
ROCK_MATERIALS = f"""
    SELECT s.*, c.mat_index, c.type AS material, c.amount, c.material_volume,
        row_number() OVER (PARTITION BY c.capture_id, c.type ORDER BY c.mat_index) AS entry,
        s.mass * c.amount / 100 AS material_mass,
        CASE WHEN c.type = '{INERT}' OR c.quality NOT BETWEEN 0 AND 1000 THEN NULL
            ELSE c.quality END AS quality
    FROM compositions c JOIN scans s USING (capture_id)
"""

# Quality bands 0-100, 101-200, ..., 901-1000.
QUALITY_BANDS = [(0, 100)] + [(lo, lo + 99) for lo in range(101, 1000, 100)]


def quality_band_sums() -> str:
    """Aggregates over ROCK_MATERIALS rows that quality_band_pcts() turns into percentages."""
    sums = [
        "sum(material_volume) FILTER (quality IS NOT NULL) AS rated_scu",
        "count(quality) AS rated_entries",
    ]
    for lo, hi in QUALITY_BANDS:
        sums.append(f"sum(material_volume) FILTER (quality BETWEEN {lo} AND {hi}) AS scu_{lo}_{hi}")
        sums.append(f"count(*) FILTER (quality BETWEEN {lo} AND {hi}) AS entries_{lo}_{hi}")
    return ",\n".join(sums)


def quality_band_pcts(alias: str) -> str:
    """Share of the SCU, then of the entries, in each quality band; NULL without a quality."""
    scu = [
        f"round(100 * coalesce({alias}.scu_{lo}_{hi}, 0) / nullif({alias}.rated_scu, 0), 2) "
        f"AS quality_{lo}_{hi}_scu_pct"
        for lo, hi in QUALITY_BANDS
    ]
    count = [
        f"round(100 * {alias}.entries_{lo}_{hi} / nullif({alias}.rated_entries, 0), 2) "
        f"AS quality_{lo}_{hi}_count_pct"
        for lo, hi in QUALITY_BANDS
    ]
    return ",\n".join(scu + count)


def columns(keys: list[str], alias: str) -> str:
    return ", ".join(f"{alias}.{k}" for k in keys)


def same(keys: list[str], left: str, right: str) -> str:
    """Join condition on `keys` that treats NULL (e.g. no region) as a value."""
    return " AND ".join(f"{left}.{k} IS NOT DISTINCT FROM {right}.{k}" for k in keys)
