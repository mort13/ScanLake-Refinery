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
    `counted` (the gap is no pause) and `same_location` (the previous scan had the same `keys`)."""
    same = " AND ".join(f"{k} IS NOT DISTINCT FROM lag({k}) OVER w" for k in keys)
    return f"""
        SELECT *, gap_seconds <= {PAUSE_SECONDS} AS counted FROM (
            SELECT *,
                {USER_KEY} AS user_key,
                date_diff('millisecond', lag("timestamp") OVER w, "timestamp") / 1000.0
                    AS gap_seconds,
                {same} AS same_location
            FROM scans
            WINDOW w AS (PARTITION BY session_id ORDER BY "timestamp", capture_id)
        )
    """


# One row per composition entry with its scan. A rock can list the same mineral more than once.
# Quality is meaningless for inert material and out of range for a few old OCR misreads.
ROCK_MATERIALS = f"""
    SELECT s.*, c.mat_index, c.type AS material, c.amount,
        s.mass * c.amount / 100 AS material_mass,
        CASE WHEN c.type = '{INERT}' OR c.quality NOT BETWEEN 0 AND 1000 THEN NULL
            ELSE c.quality END AS quality
    FROM compositions c JOIN scans s USING (capture_id)
"""


def columns(keys: list[str], alias: str) -> str:
    return ", ".join(f"{alias}.{k}" for k in keys)


def same(keys: list[str], left: str, right: str) -> str:
    """Join condition on `keys` that treats NULL (e.g. no region) as a value."""
    return " AND ".join(f"{left}.{k} IS NOT DISTINCT FROM {right}.{k}" for k in keys)
