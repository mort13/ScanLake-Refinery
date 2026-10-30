from . import job
from ._shared import (
    LOCATION,
    LOCATION_BY_REGION,
    ROCK_MATERIALS,
    columns,
    quality_band_pcts,
    quality_band_sums,
    same,
    scan_gaps,
)


def deposit_materials_sql(keys: list[str]) -> str:
    """One row per location, deposit and material; deposit columns repeat for each material.
    A deposit whose rocks have no composition gets one row with material NULL. Quality is given
    per quality band twice: as the share of the material's SCU and of its composition entries."""
    k = ", ".join(keys)
    return f"""
        WITH
        rocks AS (
            -- A cluster gap: the previous scan of the session was another cluster of this deposit
            -- at this location, i.e. the time from one cluster's last rock to the next one's first.
            SELECT *,
                capture_id IN (SELECT capture_id FROM compositions) AS has_composition,
                coalesce(counted AND same_location AND new_cluster, false) AS cluster_gap
            FROM ({scan_gaps([*keys, "deposit"])})
        ),
        locations AS (
            SELECT {k}, count(*) AS location_scans FROM rocks GROUP BY ALL
        ),
        deposits AS (
            SELECT {k}, deposit,
                count(*) AS deposit_scans,
                count(*) FILTER (has_composition) AS composition_scans,
                count(DISTINCT cluster_id) AS deposit_clusters,
                avg(mass) AS deposit_avg_mass,
                sum(mass) AS deposit_total_mass,
                max("timestamp") AS deposit_last_scan_at,
                count(*) FILTER (cluster_gap) AS deposit_cluster_gaps,
                avg(gap_seconds) FILTER (cluster_gap) AS deposit_avg_cluster_gap,
                median(gap_seconds) FILTER (cluster_gap) AS deposit_median_cluster_gap
            FROM rocks GROUP BY ALL
        ),
        materials AS (
            SELECT {k}, deposit, material,
                count(DISTINCT capture_id) AS material_scans,
                sum(amount) AS amount_sum,
                sum(material_mass) AS total_material_mass,
                sum(material_volume) AS total_scu,
                count(DISTINCT capture_id) FILTER (material_volume IS NOT NULL) AS scu_scans,
                {quality_band_sums()}
            FROM ({ROCK_MATERIALS}) GROUP BY ALL
        )
        SELECT
            {columns(keys, "d")}, d.deposit, m.material,
            l.location_scans,
            d.deposit_scans,
            round(100.0 * d.deposit_scans / l.location_scans, 2) AS deposit_share_pct,
            d.deposit_clusters,
            round(d.deposit_avg_mass, 2) AS deposit_avg_mass,
            d.deposit_total_mass,
            d.deposit_last_scan_at,
            d.deposit_cluster_gaps,
            round(d.deposit_avg_cluster_gap, 2) AS deposit_avg_cluster_gap_seconds,
            round(d.deposit_median_cluster_gap, 2) AS deposit_median_cluster_gap_seconds,
            d.composition_scans,
            m.material_scans,
            round(100.0 * m.material_scans / nullif(d.composition_scans, 0), 2) AS presence_pct,
            round(m.amount_sum / nullif(d.composition_scans, 0), 2) AS avg_share_pct,
            round(m.amount_sum / m.material_scans, 2) AS avg_share_when_present_pct,
            round(m.total_material_mass, 2) AS total_material_mass,
            round(m.total_scu, 2) AS total_scu,
            round(m.total_scu / nullif(m.scu_scans, 0), 2) AS avg_scu_when_present,
            {quality_band_pcts("m")}
        FROM deposits d
        JOIN locations l ON {same(keys, "d", "l")}
        LEFT JOIN materials m ON {same([*keys, "deposit"], "d", "m")}
    """


@job("deposit_materials", "Deposit shares and material amount and quality per gravity well")
def deposit_materials(con) -> str:
    return deposit_materials_sql(LOCATION)


@job("deposit_materials_by_region", "deposit_materials, split by region within a gravity well")
def deposit_materials_by_region(con) -> str:
    return deposit_materials_sql(LOCATION_BY_REGION)
