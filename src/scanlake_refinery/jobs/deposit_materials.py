from . import job
from ._shared import LOCATION, LOCATION_BY_REGION, ROCK_MATERIALS, columns, same


def deposit_materials_sql(keys: list[str]) -> str:
    """One row per location, deposit and material; deposit columns repeat for each material.
    A deposit whose rocks have no composition gets one row with material NULL."""
    k = ", ".join(keys)
    return f"""
        WITH
        rocks AS (
            SELECT *, capture_id IN (SELECT capture_id FROM compositions) AS has_composition
            FROM scans
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
                max("timestamp") AS deposit_last_scan_at
            FROM rocks GROUP BY ALL
        ),
        materials AS (
            SELECT {k}, deposit, material,
                count(DISTINCT capture_id) AS material_scans,
                sum(amount) AS amount_sum,
                sum(material_mass) AS total_material_mass,
                avg(quality) AS quality_avg,
                sum(quality * material_mass)
                    / nullif(sum(material_mass) FILTER (quality IS NOT NULL), 0)
                    AS quality_weighted_avg,
                min(quality) AS quality_min,
                quantile_disc(quality, 0.25) AS quality_p25,
                quantile_disc(quality, 0.5) AS quality_median,
                quantile_disc(quality, 0.75) AS quality_p75,
                max(quality) AS quality_max
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
            d.composition_scans,
            m.material_scans,
            round(100.0 * m.material_scans / nullif(d.composition_scans, 0), 2) AS presence_pct,
            round(m.amount_sum / nullif(d.composition_scans, 0), 2) AS avg_share_pct,
            round(m.amount_sum / m.material_scans, 2) AS avg_share_when_present_pct,
            round(m.total_material_mass, 2) AS total_material_mass,
            round(m.quality_avg, 2) AS quality_avg,
            round(m.quality_weighted_avg, 2) AS quality_weighted_avg,
            m.quality_min, m.quality_p25, m.quality_median, m.quality_p75, m.quality_max
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
