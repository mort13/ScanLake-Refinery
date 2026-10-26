"""The DuckDB warehouse: sync bookkeeping, run history, consolidated tables and results."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb

# Append-only: each entry migrates the schema from version i to i + 1.
MIGRATIONS: list[str] = [
    """
    CREATE TABLE sync_state (
        key            VARCHAR PRIMARY KEY,
        size           BIGINT NOT NULL,
        etag           VARCHAR,
        uploaded       VARCHAR NOT NULL,
        fingerprint    VARCHAR NOT NULL,
        downloaded_at  TIMESTAMPTZ NOT NULL,
        deleted_remote BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE TABLE runs (
        run_id      VARCHAR PRIMARY KEY,
        command     VARCHAR NOT NULL,
        started_at  TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status      VARCHAR NOT NULL,
        details     VARCHAR,
        error       VARCHAR
    );
    CREATE TABLE published (
        key          VARCHAR PRIMARY KEY,
        sha256       VARCHAR NOT NULL,
        size         BIGINT NOT NULL,
        published_at TIMESTAMPTZ NOT NULL
    );
    """,
]


def connect(path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET TimeZone = 'UTC'")
    if not read_only:
        migrate(con)
    return con


def migrate(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
    row = con.execute("SELECT max(version) FROM schema_version").fetchone()
    current = row[0] if row and row[0] is not None else 0
    for version in range(current, len(MIGRATIONS)):
        con.execute("BEGIN")
        con.execute(MIGRATIONS[version])
        con.execute("INSERT INTO schema_version VALUES (?)", [version + 1])
        con.execute("COMMIT")


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


@contextmanager
def record_run(con: duckdb.DuckDBPyConnection, command: str) -> Iterator[dict]:
    """Record a run in `runs`; the caller fills the yielded dict with details."""
    run_id = new_run_id()
    details: dict = {"run_id": run_id}
    con.execute(
        "INSERT INTO runs (run_id, command, started_at, status) VALUES (?, ?, ?, 'running')",
        [run_id, command, utcnow()],
    )
    try:
        yield details
    except BaseException as e:
        con.execute(
            "UPDATE runs SET finished_at = ?, status = 'failed', details = ?, error = ? "
            "WHERE run_id = ?",
            [utcnow(), json.dumps(details, default=str), f"{type(e).__name__}: {e}", run_id],
        )
        raise
    con.execute(
        "UPDATE runs SET finished_at = ?, status = 'ok', details = ? WHERE run_id = ?",
        [utcnow(), json.dumps(details, default=str), run_id],
    )
