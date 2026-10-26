from . import job
from ._shared import LOCATION, MIN_SESSION_SCANS, USER_NAME, scan_gaps


@job("user_stats", "Totals and personal bests per user (by name, case-insensitive)")
def user_stats(con) -> str:
    return f"""
        WITH
        gaps AS ({scan_gaps(LOCATION)}),
        session_bests AS (
            SELECT user_key,
                max(scans) AS most_scans_in_session,
                min(avg_gap_seconds) FILTER (scans >= {MIN_SESSION_SCANS})
                    AS best_session_avg_gap_seconds
            FROM (
                SELECT user_key, session_id, count(*) AS scans,
                    avg(gap_seconds) FILTER (counted) AS avg_gap_seconds
                FROM gaps GROUP BY ALL
            )
            GROUP BY ALL
        )
        SELECT
            {USER_NAME} AS user_name,
            count(DISTINCT user_id) AS user_ids,
            count(DISTINCT session_id) AS sessions,
            count(*) AS scans,
            count(DISTINCT cluster_id) AS clusters,
            sum(mass) AS total_mass,
            max(mass) AS biggest_scan_mass,
            any_value(most_scans_in_session) AS most_scans_in_session,
            round(any_value(best_session_avg_gap_seconds), 2) AS best_session_avg_gap_seconds,
            round(avg(gap_seconds) FILTER (counted), 2) AS avg_gap_seconds,
            min("timestamp") AS first_scan_at,
            max("timestamp") AS last_scan_at
        FROM gaps JOIN session_bests USING (user_key)
        GROUP BY user_key
    """
