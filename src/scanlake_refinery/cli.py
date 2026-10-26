"""Command line entry point: `refinery <command>`."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

import duckdb

from . import __version__, jobs, publish, state
from .config import ConfigError, Settings, load_dotenv, load_settings
from .consolidate import consolidate
from .gateway import GatewayClient, GatewayError
from .lock import LockedError, RunLock
from .sync import sync

log = logging.getLogger("scanlake_refinery")

EX_TEMPFAIL = 75
TS_FORMAT = "%Y-%m-%d %H:%M:%SZ"


def make_client(settings: Settings) -> GatewayClient:
    return GatewayClient(
        settings.gateway_url, settings.require_api_key(), timeout=settings.http_timeout
    )


def cmd_sync(args: argparse.Namespace, settings: Settings) -> int:
    def step(con: duckdb.DuckDBPyConnection, run_id: str) -> dict:
        with make_client(settings) as client:
            return sync(con, client, settings.raw_dir, dry_run=args.dry_run)

    return _run(settings, "sync", step)


def cmd_consolidate(args: argparse.Namespace, settings: Settings) -> int:
    raw_dir = Path(args.raw).resolve() if args.raw else settings.raw_dir
    return _run(settings, "consolidate", lambda con, run_id: consolidate(con, raw_dir))


def cmd_compute(args: argparse.Namespace, settings: Settings) -> int:
    return _run(settings, "compute", lambda con, run_id: jobs.compute(con, args.job or None))


def cmd_publish(args: argparse.Namespace, settings: Settings) -> int:
    def step(con: duckdb.DuckDBPyConnection, run_id: str) -> dict:
        details: dict = {"files": len(publish.export(con, settings.out_dir, run_id))}
        if settings.publish and not args.no_upload:
            with make_client(settings) as client:
                details.update(publish.upload(con, client, settings.out_dir))
        return details

    return _run(settings, "publish", step)


def cmd_run(args: argparse.Namespace, settings: Settings) -> int:
    do_publish = settings.publish and not args.no_publish

    def step(con: duckdb.DuckDBPyConnection, run_id: str) -> dict:
        with make_client(settings) as client:
            details = {"sync": sync(con, client, settings.raw_dir)}
            details["consolidate"] = consolidate(con, settings.raw_dir)
            details["compute"] = jobs.compute(con)
            details["export"] = len(publish.export(con, settings.out_dir, run_id))
            if do_publish:
                details["publish"] = publish.upload(con, client, settings.out_dir)
            return details

    return _run(settings, "run", step)


def cmd_status(args: argparse.Namespace, settings: Settings) -> int:
    if not settings.warehouse_path.exists():
        print(f"No warehouse yet at {settings.warehouse_path}")
        return 0
    try:
        con = state.connect(settings.warehouse_path, read_only=True)
    except duckdb.IOException:
        print("A refinery run is in progress (warehouse is locked); try again shortly.")
        return EX_TEMPFAIL
    with con:
        print(f"Warehouse: {settings.warehouse_path}")
        # Timestamps are formatted in SQL: fetching TIMESTAMPTZ into Python would require pytz.
        tracked, gone, last = con.execute(
            "SELECT count(*), count(*) FILTER (deleted_remote), "
            f"strftime(max(downloaded_at), '{TS_FORMAT}') FROM sync_state"
        ).fetchone()
        print(f"Raw files: {tracked} tracked, {gone} deleted in bucket, last download {last}")
        tables = con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'main' "
            "AND (table_name IN ('scans', 'compositions', 'confidences') "
            "OR table_name LIKE 'results\\_%' ESCAPE '\\') ORDER BY table_name"
        ).fetchall()
        for name, rows in tables:
            print(f"  {name:<32} ~{rows} rows")
        print("Recent runs:")
        for run_id, command, status, took_ms, error in con.execute(
            "SELECT run_id, command, status, date_diff('millisecond', started_at, finished_at), "
            "error FROM runs ORDER BY started_at DESC LIMIT ?",
            [args.limit],
        ).fetchall():
            took = f"{took_ms / 1000:.1f}s" if took_ms is not None else "-"
            line = f"  {run_id}  {command:<12} {status:<8} {took:>7}"
            print(line + (f"  {error}" if error else ""))
    return 0


def _run(
    settings: Settings, command: str, step: Callable[[duckdb.DuckDBPyConnection, str], dict]
) -> int:
    with RunLock(settings.lock_path):
        con = state.connect(settings.warehouse_path)
        try:
            with state.record_run(con, command) as details:
                details.update(step(con, details["run_id"]))
        finally:
            con.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="refinery",
        description="Sync ScanLake raw data via the Gateway, consolidate it, compute results "
        "and publish them to the derived bucket.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--env-file", help="load settings from this file (default: ./.env)")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("sync", help="mirror new and changed raw files from the Gateway")
    p.add_argument("--dry-run", action="store_true", help="only report what would be downloaded")
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser("consolidate", help="rebuild scans, compositions and confidences")
    p.add_argument("--raw", help="read raw files from this directory instead of the mirror")
    p.set_defaults(func=cmd_consolidate)

    p = sub.add_parser("compute", help="run result jobs")
    p.add_argument("--job", action="append", help="run only this job (repeatable)")
    p.set_defaults(func=cmd_compute)

    p = sub.add_parser("publish", help="export parquet files and upload changed ones")
    p.add_argument("--no-upload", action="store_true", help="export only")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("run", help="sync, consolidate, compute and publish")
    p.add_argument("--no-publish", action="store_true", help="skip the upload step")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("status", help="show recent runs and table sizes")
    p.add_argument("--limit", type=int, default=10, help="number of runs to show")
    p.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.env_file and not Path(args.env_file).is_file():
        log.error("env file not found: %s", args.env_file)
        return 2
    load_dotenv(Path(args.env_file) if args.env_file else Path.cwd() / ".env")
    try:
        return args.func(args, load_settings())
    except LockedError as e:
        log.error("%s", e)
        return EX_TEMPFAIL
    except (ConfigError, GatewayError) as e:
        log.error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
