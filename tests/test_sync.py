import pytest
from conftest import LEGACY, RAW_FILES

from scanlake_refinery.gateway import GatewayError
from scanlake_refinery.sync import RAW_KEY_RE, sync


def test_initial_sync_mirrors_raw_parquet_only(con, client, gateway, settings):
    summary = sync(con, client, settings.raw_dir)

    assert summary["new"] == len(RAW_FILES)
    assert summary["skipped"] == 2  # manifest.json and public/ model weights
    for key in RAW_FILES:
        assert (settings.raw_dir / key).read_bytes() == gateway.raw[key][0]
    assert not (settings.raw_dir / "manifest.json").exists()
    assert not (settings.raw_dir / "public").exists()
    assert con.execute("SELECT count(*) FROM sync_state").fetchone()[0] == len(RAW_FILES)


def test_second_sync_downloads_nothing(con, client, gateway, settings):
    sync(con, client, settings.raw_dir)
    gateway.requests.clear()

    summary = sync(con, client, settings.raw_dir)

    assert (summary["new"], summary["changed"], summary["unchanged"]) == (0, 0, len(RAW_FILES))
    assert gateway.downloads() == []


def test_reuploaded_object_is_downloaded_again(con, client, gateway, settings):
    sync(con, client, settings.raw_dir)
    key = f"{LEGACY}/scans_batch001.parquet"
    gateway.put_raw(key, b"new bytes", uploaded="2026-10-01T00:00:00.000Z")

    summary = sync(con, client, settings.raw_dir)

    assert summary["changed"] == 1
    assert (settings.raw_dir / key).read_bytes() == b"new bytes"


def test_missing_local_file_is_restored(con, client, settings):
    sync(con, client, settings.raw_dir)
    key = next(iter(RAW_FILES))
    (settings.raw_dir / key).unlink()

    assert sync(con, client, settings.raw_dir)["changed"] == 1
    assert (settings.raw_dir / key).is_file()


def test_vanished_object_is_kept_and_flagged(con, client, gateway, settings):
    sync(con, client, settings.raw_dir)
    key = f"{LEGACY}/compositions_batch001.parquet"
    data = gateway.raw.pop(key)

    assert sync(con, client, settings.raw_dir)["vanished"] == 1
    assert (settings.raw_dir / key).is_file()
    flag = "SELECT deleted_remote FROM sync_state WHERE key = ?"
    assert con.execute(flag, [key]).fetchone()[0] is True
    # Reported once, not on every run.
    assert sync(con, client, settings.raw_dir)["vanished"] == 0

    gateway.raw[key] = data
    sync(con, client, settings.raw_dir)
    assert con.execute(flag, [key]).fetchone()[0] is False


def test_dry_run_changes_nothing(con, client, gateway, settings):
    summary = sync(con, client, settings.raw_dir, dry_run=True)

    assert summary["new"] == len(RAW_FILES)
    assert gateway.downloads() == []
    assert not settings.raw_dir.exists()
    assert con.execute("SELECT count(*) FROM sync_state").fetchone()[0] == 0


def test_listing_follows_cursor_pages(con, client, gateway, settings):
    gateway.page_size = 2

    assert sync(con, client, settings.raw_dir)["listed"] == len(RAW_FILES) + 2


def test_retries_rate_limits_with_retry_after(con, client, gateway, settings, sleeps):
    gateway.fail_next = [429, 503]

    assert sync(con, client, settings.raw_dir)["new"] == len(RAW_FILES)
    assert sleeps == [1.0, 1.0]


def test_gives_up_after_max_retries(con, client, gateway, settings):
    gateway.fail_next = [503] * 10

    with pytest.raises(GatewayError, match="HTTP 503"):
        sync(con, client, settings.raw_dir)


@pytest.mark.parametrize(
    "key",
    [
        "2026/01/01/u/s/scans_batch001.parquet",
        "2026/01/01/u-1/s_2/compositions_batch010.parquet",
    ],
)
def test_raw_key_pattern_accepts_store_layout(key):
    assert RAW_KEY_RE.match(key)


@pytest.mark.parametrize(
    "key",
    [
        "2026/01/01/../s/x.parquet",
        "2026/01/01/u/../x.parquet",
        "2026/01/01/u/s/.hidden.parquet",
        "2026/01/01/u/s/x.json",
        "public/models/x.parquet",
        "manifest.json",
    ],
)
def test_raw_key_pattern_rejects_other_keys(key):
    assert not RAW_KEY_RE.match(key)
