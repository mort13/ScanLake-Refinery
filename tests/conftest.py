from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import httpx
import pytest

from scanlake_refinery import state
from scanlake_refinery.config import Settings
from scanlake_refinery.gateway import GatewayClient

API_KEY = "sk_" + "a" * 64
USER = "bcc6dfdf-d483-43c1-a6f9-c3fd2ebb1db7"

LEGACY = f"2026/03/28/{USER}/11111111-1111-4111-8111-111111111111"
CURRENT = f"2026/06/24/{USER}/22222222-2222-4222-8222-222222222222"

# Files from before the April 2026 schema migration: *_conf inline in scans, BIGINT mass and
# resistance, timestamps without milliseconds or zone, compositions without min_confidence and a
# confidences file holding only (capture_id, min_confidence).
LEGACY_FILES = {
    f"{LEGACY}/scans_batch001.parquet": """
        SELECT * FROM (VALUES
            ('s1', 'u1', 'Mort13', 'RR', '2026-03-28T11:18:29', 'a1', 'c1', 'Stanton', 'Daymar',
             NULL, NULL, 'copper ', 9000::BIGINT, 0::BIGINT, 1.5, 10.0, 0.9, 0.8, 0.7, 0.6, 0.5),
            ('s1', 'u1', 'Mort13', 'RR', '2026-03-28T11:19:01', 'a2', 'c1', 'Stanton', 'Daymar',
             NULL, NULL, 'Quantanium', 12000::BIGINT, 78::BIGINT, 40.2, 20.0,
             0.9, 0.9, 0.9, 0.9, 0.9)
        ) t(session_id, user_id, user_name, org, "timestamp", capture_id, cluster_id, system,
            gravity_well, region, place, deposit, mass, resistance, instability, volume,
            deposit_conf, mass_conf, resistance_conf, instability_conf, volume_conf)
    """,
    f"{LEGACY}/compositions_batch001.parquet": """
        SELECT * FROM (VALUES
            ('a1', 0::INTEGER, 'Copper', 40.0, 500::INTEGER, 4.0),
            ('a1', 1::INTEGER, 'Inert Materials', 60.0, 0::INTEGER, 6.0),
            ('a2', 0::INTEGER, 'Quantanium', 20.0, 700::INTEGER, 4.0),
            ('a2', 1::INTEGER, 'Inert Materials', 80.0, 0::INTEGER, 16.0)
        ) t(capture_id, mat_index, type, amount, quality, material_volume)
    """,
    f"{LEGACY}/confidences_batch001.parquet": """
        SELECT * FROM (VALUES ('a1', 1.0), ('a1', 1.0), ('a2', 1.0), ('a2', 1.0))
            t(capture_id, min_confidence)
    """,
}

CURRENT_FILES = {
    f"{CURRENT}/scans_batch001.parquet": """
        SELECT * FROM (VALUES
            ('s2', 'u1', 'Mort13', 'RR', '2026-06-24T09:04:12.345Z', 'b1', 'c2', 'Nyx',
             'Keeger Ring', 'PSS Theta', 'none', 'Taranite', 1000::INTEGER, 44::INTEGER, 12.0, 5.0)
        ) t(session_id, user_id, user_name, org, "timestamp", capture_id, cluster_id, system,
            gravity_well, region, place, deposit, mass, resistance, instability, volume)
    """,
    f"{CURRENT}/compositions_batch001.parquet": """
        SELECT * FROM (VALUES
            ('b1', 0::INTEGER, 'Taranite', 30.0, 600::INTEGER, 1.5, 1.0),
            ('b1', 1::INTEGER, 'Inert Materials', 70.0, 0::INTEGER, 3.5, 1.0)
        ) t(capture_id, mat_index, type, amount, quality, material_volume, min_confidence)
    """,
    f"{CURRENT}/confidences_batch001.parquet": """
        SELECT * FROM (VALUES ('b1', 1.0, 1.0, 1.0, 1.0, 1.0))
            t(capture_id, deposit_conf, mass_conf, resistance_conf, instability_conf, volume_conf)
    """,
}

RAW_FILES = {**LEGACY_FILES, **CURRENT_FILES}
NON_DATA_FILES = {
    "manifest.json": b"{}",
    "public/models/word_model2k.onnx.data": b"\x00" * 16,
}


def parquet_bytes(sql: str, tmp_dir: Path) -> bytes:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    path = tmp_dir / (hashlib.sha256(sql.encode()).hexdigest()[:16] + ".parquet")
    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT parquet)")
    con.close()
    return path.read_bytes()


def write_raw(raw_dir: Path, files: dict[str, str], tmp_dir: Path) -> None:
    for key, sql in files.items():
        dest = raw_dir / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(parquet_bytes(sql, tmp_dir))


class FakeGateway:
    """In-memory stand-in for ScanLake Gateway, served through httpx.MockTransport."""

    def __init__(self) -> None:
        self.raw: dict[str, tuple[bytes, str]] = {}
        self.derived: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        self.fail_next: list[int] = []
        self.page_size = 1000

    def put_raw(self, key: str, data: bytes, uploaded: str = "2026-06-24T09:05:00.000Z") -> None:
        self.raw[key] = (data, uploaded)

    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        self.requests.append((method, path))
        if request.headers.get("X-API-Key") != API_KEY:
            return httpx.Response(401, json={"error": "Invalid API key"})
        if self.fail_next:
            return httpx.Response(
                self.fail_next.pop(0), headers={"Retry-After": "1"}, json={"error": "busy"}
            )

        if method == "GET" and path in ("/api/files", "/api/derived"):
            return self._list(request, path)
        if method == "GET" and path.startswith("/api/files/"):
            item = self.raw.get(path.removeprefix("/api/files/"))
            if item is None:
                return httpx.Response(404, json={"error": "File not found"})
            return httpx.Response(200, content=item[0])
        if method == "PUT" and path.startswith("/api/derived/"):
            key = path.removeprefix("/api/derived/")
            body = request.read()
            if request.headers.get("X-Content-SHA256") != hashlib.sha256(body).hexdigest():
                return httpx.Response(400, json={"error": "sha256 mismatch"})
            self.derived[key] = body
            return httpx.Response(200, json={"key": key, "size": len(body), "etag": _etag(body)})
        return httpx.Response(404, json={"error": "Not found"})

    def _list(self, request: httpx.Request, path: str) -> httpx.Response:
        if path == "/api/files":
            items = {k: (len(d), u, _etag(d)) for k, (d, u) in self.raw.items()}
        else:
            items = {
                k: (len(d), "2026-10-26T00:00:00.000Z", _etag(d)) for k, d in self.derived.items()
            }
        prefix = request.url.params.get("prefix", "")
        keys = sorted(k for k in items if k.startswith(prefix))
        start = int(request.url.params.get("cursor", "0"))
        limit = min(int(request.url.params.get("limit", "100")), self.page_size)
        page = keys[start : start + limit]
        body: dict = {
            "objects": [
                {"key": k, "size": items[k][0], "uploaded": items[k][1], "etag": items[k][2]}
                for k in page
            ],
            "truncated": start + limit < len(keys),
        }
        if body["truncated"]:
            body["cursor"] = str(start + limit)
        return httpx.Response(200, content=json.dumps(body).encode())

    def downloads(self) -> list[str]:
        return [p for m, p in self.requests if m == "GET" and p.startswith("/api/files/")]

    def uploads(self) -> list[str]:
        return [p.removeprefix("/api/derived/") for m, p in self.requests if m == "PUT"]


def _etag(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        gateway_url="https://gateway.test",
        gateway_api_key=API_KEY,
        data_dir=tmp_path / "data",
        publish=True,
    )


@pytest.fixture
def con(settings: Settings):
    c = state.connect(settings.warehouse_path)
    yield c
    c.close()


@pytest.fixture
def gateway(tmp_path: Path) -> FakeGateway:
    fake = FakeGateway()
    for key, sql in RAW_FILES.items():
        fake.put_raw(key, parquet_bytes(sql, tmp_path / "fixtures"))
    for key, data in NON_DATA_FILES.items():
        fake.put_raw(key, data)
    return fake


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(gateway: FakeGateway, sleeps: list[float]):
    c = GatewayClient(
        "https://gateway.test",
        API_KEY,
        transport=httpx.MockTransport(gateway.handler),
        sleep=sleeps.append,
    )
    yield c
    c.close()
