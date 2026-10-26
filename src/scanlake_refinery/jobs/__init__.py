"""Result jobs: each job is a SQL query over the consolidated tables, stored as `results_<name>`.

Add a job by dropping a module into this package that decorates a function with `@job(...)`.
The function receives the connection and returns the SELECT statement for its result table.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
import re
from collections.abc import Callable
from dataclasses import dataclass

import duckdb

log = logging.getLogger(__name__)

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Job:
    name: str
    description: str
    query: Callable[[duckdb.DuckDBPyConnection], str]

    @property
    def table(self) -> str:
        return f"results_{self.name}"


REGISTRY: dict[str, Job] = {}


def job(name: str, description: str) -> Callable:
    if not _NAME_RE.match(name):
        raise ValueError(f"invalid job name: {name!r}")

    def register(fn: Callable[[duckdb.DuckDBPyConnection], str]) -> Callable:
        if name in REGISTRY:
            raise ValueError(f"duplicate job name: {name!r}")
        REGISTRY[name] = Job(name, description, fn)
        return fn

    return register


def load_all() -> dict[str, Job]:
    for module in pkgutil.iter_modules(__path__):
        importlib.import_module(f"{__name__}.{module.name}")
    return REGISTRY


def compute(con: duckdb.DuckDBPyConnection, only: list[str] | None = None) -> dict:
    jobs = load_all()
    selected = [jobs[n] for n in only] if only else list(jobs.values())
    summary: dict[str, int] = {}
    for j in selected:
        con.execute(f"CREATE OR REPLACE TABLE {j.table} AS {j.query(con)}")
        summary[j.name] = con.execute(f"SELECT count(*) FROM {j.table}").fetchone()[0]
        log.info("compute: %s -> %d rows", j.table, summary[j.name])
    # Drop result tables of jobs that no longer exist, so they stop being published.
    if not only:
        for (table,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'results\\_%' ESCAPE '\\'"
        ).fetchall():
            if table.removeprefix("results_") not in jobs:
                con.execute(f"DROP TABLE {table}")
                log.info("compute: dropped stale %s", table)
    return summary
