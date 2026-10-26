from . import job
from ._shared import LOCATION, LOCATION_BY_REGION, USER_KEY, scan_gaps


def location_stats_sql(keys: list[str]) -> str:
    """Totals per location and the pace of scanning there: gaps between consecutive scans of one
    session at the same location, pauses left out."""
    return f"""
        SELECT {", ".join(keys)},
            count(*) AS scans,
            count(DISTINCT cluster_id) AS clusters,
            count(DISTINCT session_id) AS sessions,
            count(DISTINCT {USER_KEY}) AS users,
            count(DISTINCT deposit) AS deposits,
            sum(mass) AS total_mass,
            round(avg(mass), 2) AS avg_mass,
            min("timestamp") AS first_scan_at,
            max("timestamp") AS last_scan_at,
            count(*) FILTER (counted AND same_location) AS gaps_counted,
            round(avg(gap_seconds) FILTER (counted AND same_location), 2) AS avg_gap_seconds,
            round(median(gap_seconds) FILTER (counted AND same_location), 2)
                AS median_gap_seconds
        FROM ({scan_gaps(keys)})
        GROUP BY ALL
    """


@job("location_stats", "Scans, sessions, users and scan pace per gravity well")
def location_stats(con) -> str:
    return location_stats_sql(LOCATION)


@job("location_stats_by_region", "location_stats, split by region within a gravity well")
def location_stats_by_region(con) -> str:
    return location_stats_sql(LOCATION_BY_REGION)
