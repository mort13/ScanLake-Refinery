"""Incremental mirror of the raw bucket (via the Gateway) into data/raw.

Local files are never deleted: an object that disappears from the bucket is only marked
`deleted_remote`, so the mirror doubles as an archive of everything ever uploaded.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .gateway import GatewayClient, RemoteObject
from .state import utcnow

log = logging.getLogger(__name__)

# YYYY/MM/DD/<userId>/<sessionId>/<file>.parquet, as written by ScanLake Store. Segments may not
# start with a dot, which rules out "." and ".." path traversal into the local mirror.
_SEGMENT = r"[A-Za-z0-9_-][A-Za-z0-9_.-]*"
RAW_KEY_RE = re.compile(rf"^\d{{4}}/\d{{2}}/\d{{2}}/{_SEGMENT}/{_SEGMENT}/{_SEGMENT}\.parquet$")


@dataclass
class SyncPlan:
    new: list[RemoteObject] = field(default_factory=list)
    changed: list[RemoteObject] = field(default_factory=list)
    unchanged: int = 0
    vanished: list[str] = field(default_factory=list)
    skipped: int = 0

    @property
    def to_download(self) -> list[RemoteObject]:
        return self.new + self.changed


def plan_sync(
    remote: list[RemoteObject], known: dict[str, str], raw_dir: Path, already_vanished: set[str]
) -> SyncPlan:
    plan = SyncPlan()
    seen: set[str] = set()
    for obj in remote:
        if not RAW_KEY_RE.match(obj.key):
            plan.skipped += 1
            continue
        seen.add(obj.key)
        previous = known.get(obj.key)
        if previous is None:
            plan.new.append(obj)
        elif previous != obj.fingerprint or not (raw_dir / obj.key).is_file():
            plan.changed.append(obj)
        else:
            plan.unchanged += 1
    plan.vanished = sorted(set(known) - seen - already_vanished)
    return plan


def sync(
    con: duckdb.DuckDBPyConnection, client: GatewayClient, raw_dir: Path, *, dry_run: bool = False
) -> dict:
    remote = list(client.list_objects("files"))
    rows = con.execute("SELECT key, fingerprint, deleted_remote FROM sync_state").fetchall()
    known = {key: fp for key, fp, _ in rows}
    already_vanished = {key for key, _, deleted in rows if deleted}
    plan = plan_sync(remote, known, raw_dir, already_vanished)

    summary = {
        "listed": len(remote),
        "skipped": plan.skipped,
        "new": len(plan.new),
        "changed": len(plan.changed),
        "unchanged": plan.unchanged,
        "vanished": len(plan.vanished),
        "downloaded_bytes": 0,
        "dry_run": dry_run,
    }
    log.info(
        "sync: %d listed, %d new, %d changed, %d unchanged, %d vanished, %d skipped",
        summary["listed"],
        summary["new"],
        summary["changed"],
        summary["unchanged"],
        summary["vanished"],
        summary["skipped"],
    )
    if dry_run:
        for obj in plan.to_download:
            log.info("would download %s (%d bytes)", obj.key, obj.size)
        return summary

    root = raw_dir.resolve()
    for obj in plan.to_download:
        dest = (root / obj.key).resolve()
        if not dest.is_relative_to(root):
            raise ValueError(f"refusing to write outside the mirror: {obj.key}")
        summary["downloaded_bytes"] += client.download(obj.key, dest)
        con.execute(
            "INSERT OR REPLACE INTO sync_state "
            "(key, size, etag, uploaded, fingerprint, downloaded_at, deleted_remote) "
            "VALUES (?, ?, ?, ?, ?, ?, FALSE)",
            [obj.key, obj.size, obj.etag, obj.uploaded, obj.fingerprint, utcnow()],
        )
        log.debug("downloaded %s", obj.key)

    # Objects that reappear unchanged after being marked vanished.
    reappeared = [o.key for o in remote if o.key in already_vanished and o.key in known]
    if reappeared:
        con.executemany(
            "UPDATE sync_state SET deleted_remote = FALSE WHERE key = ?", [[k] for k in reappeared]
        )
    if plan.vanished:
        con.executemany(
            "UPDATE sync_state SET deleted_remote = TRUE WHERE key = ?",
            [[k] for k in plan.vanished],
        )
        log.warning("%d objects vanished from the bucket (kept locally)", len(plan.vanished))
    return summary
