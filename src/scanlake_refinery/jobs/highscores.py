from . import job
from ._shared import LOCATION, MIN_SESSION_SCANS, TOP_N, USER_NAME, scan_gaps


@job("highscores", "Top users per category; each user's best entry, ties to the earlier one")
def highscores(con) -> str:
    return f"""
        WITH
        gaps AS ({scan_gaps(LOCATION)}),
        users AS (
            SELECT user_key, {USER_NAME} AS user_name FROM gaps GROUP BY user_key
        ),
        sessions AS (
            SELECT user_key, session_id,
                count(*) AS scans,
                avg(gap_seconds) FILTER (counted) AS avg_gap_seconds,
                max("timestamp") AS ended_at
            FROM gaps GROUP BY ALL
        ),
        entries AS (
            SELECT 'fastest_session_avg_scan_time' AS category, TRUE AS lower_wins, user_key,
                avg_gap_seconds AS value, 'seconds' AS unit, session_id,
                CAST(NULL AS VARCHAR) AS capture_id, ended_at AS achieved_at,
                concat(scans, ' scans') AS detail
            FROM sessions
            WHERE scans >= {MIN_SESSION_SCANS} AND avg_gap_seconds IS NOT NULL
            UNION ALL
            SELECT 'biggest_scan', FALSE, user_key, mass, 'kg', session_id, capture_id,
                "timestamp", concat_ws(', ', deposit, concat_ws(' / ', system, gravity_well))
            FROM gaps WHERE mass IS NOT NULL
            UNION ALL
            SELECT 'most_scans_in_session', FALSE, user_key, scans, 'scans', session_id, NULL,
                ended_at, NULL
            FROM sessions
            UNION ALL
            SELECT 'most_scans_total', FALSE, user_key, count(*), 'scans', NULL, NULL,
                max("timestamp"), NULL
            FROM gaps GROUP BY user_key
        ),
        best AS (
            SELECT *, CASE WHEN lower_wins THEN value ELSE -value END AS score FROM entries
            QUALIFY row_number() OVER (
                PARTITION BY category, user_key
                ORDER BY CASE WHEN lower_wins THEN value ELSE -value END, achieved_at
            ) = 1
        ),
        ranked AS (
            SELECT *,
                row_number() OVER (PARTITION BY category ORDER BY score, achieved_at, user_key)
                    AS "rank"
            FROM best
        )
        SELECT category, "rank", u.user_name, round(value, 2) AS value, unit, session_id,
            capture_id, achieved_at, detail
        FROM ranked JOIN users u USING (user_key)
        WHERE "rank" <= {TOP_N}
    """
