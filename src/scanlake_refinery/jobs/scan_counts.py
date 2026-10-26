from . import job


@job("scan_counts", "Rocks, clusters and sessions per day, location and deposit")
def scan_counts(con) -> str:
    return """
        SELECT
            CAST("timestamp" AS DATE)     AS day,
            system,
            gravity_well,
            deposit,
            count(*)                      AS rocks,
            count(DISTINCT cluster_id)    AS clusters,
            count(DISTINCT session_id)    AS sessions,
            sum(mass)                     AS total_mass
        FROM scans
        GROUP BY ALL
        ORDER BY ALL
    """
