"""Turn the raw per-session parquet files into four clean tables: scans, compositions,
confidences and positions.

Rebuilt from scratch on every run. Handles the schema drift in the bucket:
  - files from 2026-03-28..04-13 carry the *_conf columns inline in scans and store mass and
    resistance as int64; their compositions lack min_confidence;
  - the confidences files from that period hold only (capture_id, min_confidence), an artefact of
    the Store's migrate_confidences.py, and are excluded;
  - `timestamp` is an ISO string, with or without milliseconds / "Z", and becomes TIMESTAMPTZ (UTC);
  - from format v2, `mass` is INT32 or, in batches holding a deposit under 1 SCU (whose mass has
    decimals, e.g. 12.34), DOUBLE; it is read as DOUBLE.
Rows are deduplicated per capture: the most recently uploaded file wins.

`positions` (format v3, optional in an upload) holds where the ship was when a scan was taken,
one row per Zone line of the game's debug text. Positions of captures missing from `scans` are
dropped, and `is_local` marks each capture's local zone (asteroid field, planet or station):
the zone just above the first SolarSystem/Root line, as ScanLake Desktop picks it.

Names are normalised after the rebuild: deposit and mineral names are capitalised word by word
("iron" -> "Iron"), known variants are mapped through the *_ALIASES tables, and Capture's "none"
placeholder for an unset deposit, region or place becomes NULL.
"""

from __future__ import annotations

import logging
import re
import string
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

RAW_FILE_RE = re.compile(
    r"^(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/[^/]+/[^/]+/"
    r"(?P<kind>scans|compositions|confidences|positions)_batch(?P<batch>\d+)\.parquet$"
)

CONF_COLUMNS = ["deposit_conf", "mass_conf", "resistance_conf", "instability_conf", "volume_conf"]

SCAN_COLUMNS = [
    ("capture_id", "VARCHAR"),
    ("cluster_id", "VARCHAR"),
    ("session_id", "VARCHAR"),
    ("user_id", "VARCHAR"),
    ("user_name", "VARCHAR"),
    ("org", "VARCHAR"),
    ("timestamp", "TIMESTAMPTZ"),
    ("system", "VARCHAR"),
    ("gravity_well", "VARCHAR"),
    ("region", "VARCHAR"),
    ("place", "VARCHAR"),
    ("deposit", "VARCHAR"),
    ("mass", "DOUBLE"),
    ("resistance", "INTEGER"),
    ("instability", "DOUBLE"),
    ("volume", "DOUBLE"),
]
COMPOSITION_COLUMNS = [
    ("capture_id", "VARCHAR"),
    ("mat_index", "INTEGER"),
    ("type", "VARCHAR"),
    ("amount", "DOUBLE"),
    ("quality", "INTEGER"),
    ("material_volume", "DOUBLE"),
    ("min_confidence", "DOUBLE"),
]
CONFIDENCE_COLUMNS = [("capture_id", "VARCHAR")] + [(c, "DOUBLE") for c in CONF_COLUMNS]
POSITION_COLUMNS = [
    ("capture_id", "VARCHAR"),
    ("zone_index", "INTEGER"),
    ("zone", "VARCHAR"),
    ("pos_x", "DOUBLE"),
    ("pos_y", "DOUBLE"),
    ("pos_z", "DOUBLE"),
    ("cam_dir_x", "DOUBLE"),
    ("cam_dir_y", "DOUBLE"),
    ("cam_dir_z", "DOUBLE"),
    ("confidence", "DOUBLE"),
]
# Zone lines that frame the whole solar system; the local zone is the line just above them.
SYSTEM_ZONES = ("SolarSystem", "Root")
LINEAGE_COLUMNS = [
    ("source_key", "VARCHAR"),
    ("upload_date", "DATE"),
    ("batch", "INTEGER"),
    ("uploaded_at", "TIMESTAMPTZ"),
]
# Trimmed, blank becomes NULL. Zone names are the game's identifiers and are not capitalised.
NAME_COLUMNS = {"system", "gravity_well", "region", "place", "deposit", "type", "zone"}

# Spelling fixes, keyed by lower-cased name and applied after the rebuild. Deposit and mineral
# names are capitalised first, so only real variants and typos need an entry. Capture itself uses
# both "Aluminum" (deposits) and "Aluminium" (minerals); the Refinery settles on one spelling.
DEPOSIT_ALIASES: dict[str, str] = {"aluminium": "Aluminum"}
MINERAL_ALIASES: dict[str, str] = {"aluminium": "Aluminum", "corunum": "Corundum"}
GRAVITY_WELL_ALIASES: dict[str, str] = {"glacium ring": "Glaciem Ring"}
REGION_ALIASES: dict[str, str] = {"people's service station alpha": "PSS Alpha"}
# Columns where Capture writes "none" for "not set".
UNSET_IS_NONE = [("scans", "deposit"), ("scans", "region"), ("scans", "place")]

LATEST_FIRST = "uploaded_at DESC NULLS LAST, upload_date DESC, batch DESC, source_key DESC"


@dataclass(frozen=True)
class RawFile:
    path: str
    key: str
    kind: str
    upload_date: date
    batch: int


def discover(raw_dir: Path) -> list[RawFile]:
    files = []
    for p in sorted(raw_dir.rglob("*.parquet")):
        key = p.relative_to(raw_dir).as_posix()
        m = RAW_FILE_RE.match(key)
        if not m:
            log.debug("ignoring %s", key)
            continue
        files.append(
            RawFile(
                path=p.resolve().as_posix(),
                key=key,
                kind=m["kind"],
                upload_date=date(int(m["y"]), int(m["m"]), int(m["d"])),
                batch=int(m["batch"]),
            )
        )
    return files


def consolidate(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> dict:
    files = discover(raw_dir)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE raw_files "
        "(path VARCHAR, key VARCHAR, kind VARCHAR, upload_date DATE, batch INTEGER, "
        "uploaded_at TIMESTAMPTZ)"
    )
    if files:
        con.executemany(
            "INSERT INTO raw_files VALUES (?, ?, ?, ?, ?, NULL)",
            [[f.path, f.key, f.kind, f.upload_date, f.batch] for f in files],
        )
    con.execute(
        "UPDATE raw_files SET uploaded_at = TRY_CAST(s.uploaded AS TIMESTAMPTZ) "
        "FROM sync_state s WHERE s.key = raw_files.key"
    )
    # Respect deletions in the bucket: the mirror keeps the file, consolidation drops it.
    deleted = {
        row[0]
        for row in con.execute(
            "SELECT key FROM raw_files WHERE key IN "
            "(SELECT key FROM sync_state WHERE deleted_remote)"
        ).fetchall()
    }
    files = [f for f in files if f.key not in deleted]
    con.execute(
        "DELETE FROM raw_files WHERE key IN (SELECT key FROM sync_state WHERE deleted_remote)"
    )

    schemas = _schemas(con, [f.path for f in files])
    by_kind = {
        kind: [f for f in files if f.kind == kind]
        for kind in ("scans", "compositions", "confidences", "positions")
    }
    conf_files = [f for f in by_kind["confidences"] if _has_any(schemas[f.path], CONF_COLUMNS)]
    legacy_conf = [f for f in by_kind["confidences"] if f not in conf_files]
    inline_conf = [f for f in by_kind["scans"] if _has_any(schemas[f.path], CONF_COLUMNS)]

    con.execute("BEGIN")
    try:
        con.execute(
            "CREATE OR REPLACE TABLE scans AS "
            + _latest_per_capture(by_kind["scans"], SCAN_COLUMNS, schemas)
            + " ORDER BY capture_id"
        )
        con.execute(
            "CREATE OR REPLACE TABLE compositions AS "
            + _latest_file_per_capture(
                by_kind["compositions"], COMPOSITION_COLUMNS, schemas, "mat_index"
            )
            + " ORDER BY capture_id, mat_index"
        )
        con.execute(
            "CREATE OR REPLACE TABLE confidences AS "
            + _latest_per_capture(conf_files + inline_conf, CONFIDENCE_COLUMNS, schemas)
            + " ORDER BY capture_id"
        )
        con.execute(
            "CREATE OR REPLACE TABLE positions AS "
            + _positions(by_kind["positions"], schemas)
            + " ORDER BY capture_id, zone_index"
        )
        _normalise_names(con)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise

    summary = {
        "raw_files": len(files),
        "excluded_deleted_remote": len(deleted),
        "excluded_legacy_confidences": len(legacy_conf),
        "inline_confidence_files": len(inline_conf),
        "scans": _count(con, "scans"),
        "compositions": _count(con, "compositions"),
        "confidences": _count(con, "confidences"),
        "positions": _count(con, "positions"),
        "captures_with_position": con.execute(
            "SELECT count(DISTINCT capture_id) FROM positions"
        ).fetchone()[0],
        "clusters": con.execute("SELECT count(DISTINCT cluster_id) FROM scans").fetchone()[0],
    }
    log.info(
        "consolidate: %d files -> %d scans, %d compositions, %d confidences, %d clusters, "
        "%d positions of %d scans",
        summary["raw_files"],
        summary["scans"],
        summary["compositions"],
        summary["confidences"],
        summary["clusters"],
        summary["positions"],
        summary["captures_with_position"],
    )
    return summary


def _schemas(con: duckdb.DuckDBPyConnection, paths: list[str]) -> dict[str, set[str]]:
    schemas: dict[str, set[str]] = {p: set() for p in paths}
    if not paths:
        return schemas
    rows = con.execute(f"SELECT file_name, name FROM parquet_schema({_sql_list(paths)})").fetchall()
    for file_name, name in rows:
        schemas.setdefault(file_name, set()).add(name)
    return schemas


def _has_any(columns: set[str], wanted: list[str]) -> bool:
    return any(c in columns for c in wanted)


def _source(files: list[RawFile], columns: list[tuple[str, str]], schemas: dict) -> str:
    """SELECT the canonical columns plus lineage from `files`; NULL for columns a file set lacks."""
    available = set().union(*(schemas[f.path] for f in files))
    exprs = []
    for name, typ in columns:
        if name not in available:
            exprs.append(f"CAST(NULL AS {typ}) AS {_q(name)}")
        elif name in NAME_COLUMNS:
            exprs.append(f"NULLIF(trim(CAST(r.{_q(name)} AS VARCHAR)), '') AS {_q(name)}")
        else:
            exprs.append(f"TRY_CAST(r.{_q(name)} AS {typ}) AS {_q(name)}")
    exprs += ["f.key AS source_key", "f.upload_date", "f.batch", "f.uploaded_at"]
    return (
        f"SELECT {', '.join(exprs)} "
        f"FROM read_parquet({_sql_list([f.path for f in files])}, union_by_name = true, "
        f"filename = true) r JOIN raw_files f ON f.path = r.filename"
    )


def _empty(columns: list[tuple[str, str]]) -> str:
    cols = ", ".join(f"CAST(NULL AS {t}) AS {_q(n)}" for n, t in columns + LINEAGE_COLUMNS)
    return f"SELECT {cols} WHERE false"


def _latest_per_capture(files: list[RawFile], columns: list, schemas: dict) -> str:
    if not files:
        return _empty(columns)
    return (
        f"SELECT * FROM ({_source(files, columns, schemas)}) WHERE capture_id IS NOT NULL "
        f"QUALIFY row_number() OVER (PARTITION BY capture_id ORDER BY {LATEST_FIRST}) = 1"
    )


def _latest_file_per_capture(files: list[RawFile], columns: list, schemas: dict, item: str) -> str:
    """All rows of a capture come from the most recently uploaded file that contains it; one
    row per (capture_id, `item`)."""
    if not files:
        return _empty(columns)
    ranked = (
        f"SELECT *, dense_rank() OVER (PARTITION BY capture_id ORDER BY {LATEST_FIRST}) "
        f"AS file_rank "
        f"FROM ({_source(files, columns, schemas)}) WHERE capture_id IS NOT NULL"
    )
    return (
        f"SELECT * EXCLUDE (file_rank) FROM ({ranked}) WHERE file_rank = 1 "
        f"QUALIFY row_number() OVER (PARTITION BY capture_id, {_q(item)} ORDER BY source_key) = 1"
    )


def _positions(files: list[RawFile], schemas: dict) -> str:
    """The newest position of every scan in `scans`, with `is_local` on its local zone: the last
    line above the first SolarSystem/Root line, or the top line when that line is one of them
    (the rule of ScanLake Desktop's Position.local_zone)."""
    latest = _latest_file_per_capture(files, POSITION_COLUMNS, schemas, "zone_index")
    is_system = " OR ".join(f"starts_with(zone, '{z}')" for z in SYSTEM_ZONES)
    marked = (
        f"SELECT *, min(CASE WHEN {is_system} THEN zone_index END) "
        f"OVER (PARTITION BY capture_id) AS first_system "
        f"FROM ({latest}) WHERE zone_index IS NOT NULL "
        f"AND capture_id IN (SELECT capture_id FROM scans)"
    )
    local = (
        "coalesce(max(CASE WHEN first_system IS NULL OR zone_index < first_system "
        "THEN zone_index END) OVER (PARTITION BY capture_id), "
        "min(zone_index) OVER (PARTITION BY capture_id))"
    )
    columns = ", ".join(_q(name) for name, _ in POSITION_COLUMNS + LINEAGE_COLUMNS)
    return f"SELECT {columns}, zone_index = {local} AS is_local FROM ({marked})"


def _normalise_names(con: duckdb.DuckDBPyConnection) -> None:
    for table, column in UNSET_IS_NONE:
        con.execute(f"UPDATE {table} SET {_q(column)} = NULL WHERE lower({_q(column)}) = 'none'")
    _apply_aliases(con, "scans", "deposit", _capitalised(con, "scans", "deposit"), DEPOSIT_ALIASES)
    _apply_aliases(
        con, "compositions", "type", _capitalised(con, "compositions", "type"), MINERAL_ALIASES
    )
    _apply_aliases(con, "scans", "gravity_well", GRAVITY_WELL_ALIASES)
    _apply_aliases(con, "scans", "region", REGION_ALIASES)


def _capitalised(con: duckdb.DuckDBPyConnection, table: str, column: str) -> dict[str, str]:
    """Map every lower-cased value of `column` to its word-wise capitalised form."""
    names = con.execute(
        f"SELECT DISTINCT lower({_q(column)}) FROM {table} WHERE {_q(column)} IS NOT NULL"
    ).fetchall()
    return {name: string.capwords(name) for (name,) in names}


def _apply_aliases(
    con: duckdb.DuckDBPyConnection, table: str, column: str, *alias_maps: dict[str, str]
) -> None:
    """Rename case-insensitively; later maps win over earlier ones."""
    aliases: dict[str, str] = {}
    for m in alias_maps:
        aliases.update({alias.lower(): canonical for alias, canonical in m.items()})
    if not aliases:
        return
    con.executemany(
        f"UPDATE {table} SET {_q(column)} = ? WHERE lower({_q(column)}) = ? AND {_q(column)} <> ?",
        [[canonical, alias, canonical] for alias, canonical in aliases.items()],
    )


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"
