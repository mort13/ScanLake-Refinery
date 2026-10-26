"""Settings from environment variables (systemd EnvironmentFile on the server, .env locally)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_GATEWAY_URL = "https://gateway.scanlake.rocks"


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    gateway_url: str
    gateway_api_key: str | None
    data_dir: Path
    publish: bool
    http_timeout: float = 60.0

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def out_dir(self) -> Path:
        return self.data_dir / "out"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / ".refinery.lock"

    def require_api_key(self) -> str:
        if not self.gateway_api_key:
            raise ConfigError("GATEWAY_API_KEY is not set")
        return self.gateway_api_key


def _parse_bool(value: str) -> bool:
    v = value.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off", ""}:
        return False
    raise ConfigError(f"not a boolean: {value!r}")


def load_dotenv(path: Path) -> None:
    """Load KEY=VALUE lines from `path` into os.environ without overriding existing variables."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    timeout = env.get("REFINERY_HTTP_TIMEOUT", "60")
    try:
        http_timeout = float(timeout)
    except ValueError as e:
        raise ConfigError(f"REFINERY_HTTP_TIMEOUT is not a number: {timeout!r}") from e
    return Settings(
        gateway_url=env.get("GATEWAY_URL", DEFAULT_GATEWAY_URL).rstrip("/"),
        gateway_api_key=env.get("GATEWAY_API_KEY") or None,
        data_dir=Path(env.get("REFINERY_DATA_DIR", "./data")).resolve(),
        publish=_parse_bool(env.get("REFINERY_PUBLISH", "true")),
        http_timeout=http_timeout,
    )
