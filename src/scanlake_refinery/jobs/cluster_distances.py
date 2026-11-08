"""Distances between the clusters at a location, from the ship positions of their scans.

A cluster's position is the mean ship position over its scans (the ship is close to the rock it
scans), in every zone frame from the scan's local zone outward: the asteroid field, planet or
station, then SolarSystem and Root. Two clusters are compared in the innermost frame both have:
the planet's own frame for two clusters on its surface (that frame turns with the planet), the
solar system's for clusters in two different asteroid fields. Lines above the local zone (the
ship itself) are never used: in the ship's frame every cluster sits at the ship.

A cluster's deposit is the most common deposit of its rocks. Clusters without a deposit count
for the location but are never "the same deposit" as another cluster.
"""

from . import job
from ._shared import LOCATION, LOCATION_BY_REGION, columns, same


def cluster_distances_sql(keys: list[str]) -> str:
    """One row per location and deposit. The location_* columns cover every two clusters at the
    location and repeat on each of its rows; the deposit_* columns cover every two clusters of
    that deposit. Per set: clusters with a position, the pairs measured, the mean and median
    distance over all pairs, and the mean and median distance from each cluster to its nearest
    other cluster. Distances are in metres."""
    k = ", ".join(keys)
    a = columns(keys, "a")
    return f"""
        WITH
        frames AS (
            -- Every scan's position in each frame from its local zone outward (depth 0 = local).
            SELECT p.capture_id, p.zone, p.zone_index - l.zone_index AS depth,
                p.pos_x, p.pos_y, p.pos_z
            FROM positions p JOIN positions l ON l.capture_id = p.capture_id AND l.is_local
            WHERE p.zone_index >= l.zone_index
                AND p.pos_x IS NOT NULL AND p.pos_y IS NOT NULL AND p.pos_z IS NOT NULL
        ),
        clusters AS (
            SELECT {k}, cluster_id, mode(deposit) AS deposit
            FROM scans
            WHERE cluster_id IS NOT NULL AND capture_id IN (SELECT capture_id FROM frames)
            GROUP BY ALL
        ),
        centres AS (
            SELECT {columns(keys, "s")}, s.cluster_id, f.zone, min(f.depth) AS depth,
                avg(f.pos_x) AS x, avg(f.pos_y) AS y, avg(f.pos_z) AS z
            FROM scans s JOIN frames f USING (capture_id)
            WHERE s.cluster_id IS NOT NULL
            GROUP BY ALL
        ),
        pairs AS (
            SELECT {a}, a.cluster_id AS cluster_a, b.cluster_id AS cluster_b,
                sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2) + pow(a.z - b.z, 2)) AS distance
            FROM centres a JOIN centres b
                ON {same(keys, "a", "b")} AND a.zone = b.zone AND a.cluster_id < b.cluster_id
            QUALIFY row_number() OVER (
                PARTITION BY {a}, a.cluster_id, b.cluster_id
                ORDER BY greatest(a.depth, b.depth), a.depth + b.depth, a.zone
            ) = 1
        ),
        measured AS (
            SELECT p.*, ca.deposit AS deposit_a, cb.deposit AS deposit_b
            FROM pairs p
            JOIN clusters ca ON {same(keys, "p", "ca")} AND ca.cluster_id = p.cluster_a
            JOIN clusters cb ON {same(keys, "p", "cb")} AND cb.cluster_id = p.cluster_b
        ),
        ends AS (
            SELECT {k}, cluster_a AS cluster_id, deposit_a AS deposit, deposit_b AS other_deposit,
                distance
            FROM measured
            UNION ALL
            SELECT {k}, cluster_b, deposit_b, deposit_a, distance FROM measured
        ),
        nearest AS (
            SELECT {k}, cluster_id, deposit,
                min(distance) AS nearest_any,
                min(distance) FILTER (WHERE deposit = other_deposit) AS nearest_same
            FROM ends GROUP BY ALL
        ),
        location_totals AS (
            SELECT {k}, count(*) AS clusters FROM clusters GROUP BY ALL
        ),
        location_pairs AS (
            SELECT {k}, count(*) AS pairs, avg(distance) AS avg_distance,
                median(distance) AS median_distance
            FROM measured GROUP BY ALL
        ),
        location_nearest AS (
            SELECT {k}, avg(nearest_any) AS avg_nearest, median(nearest_any) AS median_nearest
            FROM nearest GROUP BY ALL
        ),
        deposit_totals AS (
            SELECT {k}, deposit, count(*) AS clusters FROM clusters GROUP BY ALL
        ),
        deposit_pairs AS (
            SELECT {k}, deposit_a AS deposit, count(*) AS pairs, avg(distance) AS avg_distance,
                median(distance) AS median_distance
            FROM measured WHERE deposit_a = deposit_b GROUP BY ALL
        ),
        deposit_nearest AS (
            SELECT {k}, deposit, avg(nearest_same) AS avg_nearest,
                median(nearest_same) AS median_nearest
            FROM nearest WHERE nearest_same IS NOT NULL GROUP BY ALL
        )
        SELECT {columns(keys, "d")}, d.deposit,
            t.clusters AS location_clusters,
            coalesce(lp.pairs, 0) AS location_cluster_pairs,
            round(lp.avg_distance, 1) AS location_avg_cluster_distance_m,
            round(lp.median_distance, 1) AS location_median_cluster_distance_m,
            round(ln.avg_nearest, 1) AS location_avg_nearest_cluster_m,
            round(ln.median_nearest, 1) AS location_median_nearest_cluster_m,
            d.clusters AS deposit_clusters,
            coalesce(dp.pairs, 0) AS deposit_cluster_pairs,
            round(dp.avg_distance, 1) AS deposit_avg_cluster_distance_m,
            round(dp.median_distance, 1) AS deposit_median_cluster_distance_m,
            round(dn.avg_nearest, 1) AS deposit_avg_nearest_cluster_m,
            round(dn.median_nearest, 1) AS deposit_median_nearest_cluster_m
        FROM deposit_totals d
        JOIN location_totals t ON {same(keys, "d", "t")}
        LEFT JOIN location_pairs lp ON {same(keys, "d", "lp")}
        LEFT JOIN location_nearest ln ON {same(keys, "d", "ln")}
        LEFT JOIN deposit_pairs dp ON {same([*keys, "deposit"], "d", "dp")}
        LEFT JOIN deposit_nearest dn ON {same([*keys, "deposit"], "d", "dn")}
    """


@job("cluster_distances", "Distances between clusters, any and of one deposit, per gravity well")
def cluster_distances(con) -> str:
    return cluster_distances_sql(LOCATION)


@job("cluster_distances_by_region", "cluster_distances, split by region within a gravity well")
def cluster_distances_by_region(con) -> str:
    return cluster_distances_sql(LOCATION_BY_REGION)
