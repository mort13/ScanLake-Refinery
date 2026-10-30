from . import job
from ._shared import (
    LOCATION,
    LOCATION_BY_REGION,
    ROCK_MATERIALS,
    columns,
    quality_band_pcts,
    quality_band_sums,
    same,
)


def material_entries_sql(keys: list[str]) -> str:
    """One row per location, deposit, material and entry: a mineral listed twice in a rock (a small
    high-quality fraction, then the bulk) gets a row for its 1st and for its 2nd entry."""
    k = ", ".join(keys)
    return f"""
        WITH
        entries AS ({ROCK_MATERIALS}),
        deposits AS (
            SELECT {k}, deposit, count(DISTINCT capture_id) AS composition_scans
            FROM entries GROUP BY ALL
        ),
        material_entries AS (
            SELECT {k}, deposit, material, entry,
                count(*) AS entry_scans,
                sum(amount) AS amount_sum,
                sum(material_volume) AS total_scu,
                count(material_volume) AS scu_scans,
                {quality_band_sums()}
            FROM entries GROUP BY ALL
        )
        SELECT
            {columns(keys, "e")}, e.deposit, e.material, e.entry,
            d.composition_scans,
            e.entry_scans,
            round(100.0 * e.entry_scans / d.composition_scans, 2) AS presence_pct,
            round(e.amount_sum / d.composition_scans, 2) AS avg_share_pct,
            round(e.amount_sum / e.entry_scans, 2) AS avg_share_when_present_pct,
            round(e.total_scu, 2) AS total_scu,
            round(e.total_scu / nullif(e.scu_scans, 0), 2) AS avg_scu_when_present,
            {quality_band_pcts("e")}
        FROM material_entries e
        JOIN deposits d ON {same([*keys, "deposit"], "e", "d")}
    """


@job("material_entries", "Share, SCU and quality bands per material entry (1st, 2nd, ...)")
def material_entries(con) -> str:
    return material_entries_sql(LOCATION)


@job("material_entries_by_region", "material_entries, split by region within a gravity well")
def material_entries_by_region(con) -> str:
    return material_entries_sql(LOCATION_BY_REGION)
