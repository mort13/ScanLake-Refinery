"""Export consolidated tables and results to parquet and upload changed files via the Gateway.

Layout in out/ and in the derived bucket:
    consolidated/{scans,compositions,confidences}.parquet
    results/<job>.parquet
    manifest.json        (written last, so readers never see a manifest ahead of its files)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path

import duckdb

from . import __version__
from .gateway import GatewayClient
from .state import utcnow

log = logging.getLogger(__name__)

CONSOLIDATED = {
    "scans": "capture_id",
    "compositions": "capture_id, mat_index",
    "confidences": "capture_id",
}
MANIFEST_KEY = "manifest.json"


def export(con: duckdb.DuckDBPyConnection, out_dir: Path, run_id: str) -> list[dict]:
    """Write every output file and the manifest into `out_dir`; return the manifest's file list."""
    outputs: list[tuple[str, str]] = [
        (f"consolidated/{t}.parquet", f"SELECT * FROM {t} ORDER BY {order}")
        for t, order in CONSOLIDATED.items()
    ]
    results = con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' "
        "AND table_name LIKE 'results\\_%' ESCAPE '\\' ORDER BY table_name"
    ).fetchall()
    outputs += [
        (f"results/{t.removeprefix('results_')}.parquet", f"SELECT * FROM {t} ORDER BY ALL")
        for (t,) in results
    ]

    files = []
    # Single-threaded writes keep the parquet bytes deterministic: unchanged data, same hash.
    con.execute("SET threads = 1")
    try:
        for key, query in outputs:
            path = out_dir / key
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".part")
            con.execute(f"COPY ({query}) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
            os.replace(tmp, path)
            rows = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
            files.append(
                {"key": key, "rows": rows, "bytes": path.stat().st_size, "sha256": _sha256(path)}
            )
    finally:
        con.execute("RESET threads")

    manifest = {
        "run_id": run_id,
        "generated_at": utcnow().isoformat(),
        "refinery_version": __version__,
        "revision": _revision(),
        "files": files,
    }
    (out_dir / MANIFEST_KEY).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    log.info("export: %d files written to %s", len(files), out_dir)
    return files


def upload(con: duckdb.DuckDBPyConnection, client: GatewayClient, out_dir: Path) -> dict:
    """Upload files whose sha256 differs from the last publish, then the manifest."""
    manifest = json.loads((out_dir / MANIFEST_KEY).read_text(encoding="utf-8"))
    published = dict(con.execute("SELECT key, sha256 FROM published").fetchall())
    uploaded, unchanged = [], 0
    for f in manifest["files"]:
        if published.get(f["key"]) == f["sha256"]:
            unchanged += 1
            continue
        _put(con, client, out_dir, f["key"], "application/vnd.apache.parquet")
        uploaded.append(f["key"])
    _put(con, client, out_dir, MANIFEST_KEY, "application/json")
    log.info("publish: %d uploaded, %d unchanged, manifest updated", len(uploaded), unchanged)
    return {"uploaded": uploaded, "unchanged": unchanged}


def _put(
    con: duckdb.DuckDBPyConnection,
    client: GatewayClient,
    out_dir: Path,
    key: str,
    content_type: str,
) -> None:
    data = (out_dir / key).read_bytes()
    client.upload(key, data, content_type)
    con.execute(
        "INSERT OR REPLACE INTO published (key, sha256, size, published_at) VALUES (?, ?, ?, ?)",
        [key, hashlib.sha256(data).hexdigest(), len(data), utcnow()],
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _revision() -> str | None:
    """Git revision of the deployed checkout, written to REVISION by the post-receive hook."""
    if rev := os.environ.get("REFINERY_REVISION"):
        return rev
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "REVISION"
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8").strip() or None
    return None
