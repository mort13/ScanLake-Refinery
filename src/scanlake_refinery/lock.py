"""Single-run lock, so a manual run can't collide with the systemd timer.

Uses an OS-level lock that the kernel drops when the process exits, so a crash never leaves a
stale lock behind.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import IO


class LockedError(RuntimeError):
    pass


class RunLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: IO[str] | None = None

    def __enter__(self) -> RunLock:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self._path, "a+")  # held open for the lock's lifetime
        try:
            _lock(fh)
        except OSError as e:
            fh.close()
            raise LockedError(f"another refinery run holds {self._path}") from e
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()))
        fh.flush()
        self._fh = fh
        return self

    def __exit__(self, *exc: object) -> None:
        if self._fh is not None:
            _unlock(self._fh)
            self._fh.close()
            self._fh = None


if os.name == "nt":
    import msvcrt

    def _lock(fh: IO[str]) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(fh: IO[str]) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock(fh: IO[str]) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fh: IO[str]) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
