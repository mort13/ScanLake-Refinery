# ScanLake Refinery

The processing worker of ScanLake. It runs on a private Proxmox server and, once an hour:

1. **syncs** new and changed raw parquet files from the `scanlake-data` bucket,
2. **consolidates** them into clean `scans`, `compositions` and `confidences` tables,
3. **computes** result tables (`results_<job>`),
4. **publishes** consolidated data and results to the `scanlake-derived` bucket.

All traffic goes through **ScanLake Gateway** (`https://gateway.scanlake.rocks`). The server holds
no R2 credentials, only one Gateway API key. It makes outbound HTTPS calls and nothing else; SSH
is LAN-only and key-only and is used for administration and deployment.

```
[Capture] → Store Worker → R2 scanlake-data ──┐
                                               │ bindings
[Dashboard] → Pages proxy (GET only) ──▶ ScanLake Gateway ◀── R2 scanlake-derived
                                               ▲
                     outbound HTTPS, X-API-Key │ GET /api/files…   PUT /api/derived/…
                                               │
             Proxmox LXC "scanlake-refinery" (LAN only, sshd key-only)
             systemd timer → refinery run: sync → consolidate → compute → publish
Workstation ── ssh / git push (LAN) ──▶ LXC
```

## Pipeline

| Step | What it does |
|---|---|
| `sync` | Lists `GET /api/files` (cursor paging) and downloads only keys matching `YYYY/MM/DD/<user>/<session>/<file>.parquet` that are new or whose `etag/size/uploaded` changed. Skips `public/` and `manifest.json`. Never deletes local files: objects that vanish from the bucket are flagged `deleted_remote` and kept as an archive. |
| `consolidate` | Rebuilds the three tables from the whole mirror (seconds at the current size). It unions schemas by name, casts `mass`/`resistance` to INTEGER and `timestamp` to TIMESTAMPTZ (UTC), and moves the pre-April-2026 inline `*_conf` columns into `confidences`. It drops the legacy `(capture_id, min_confidence)` confidences files and files deleted in the bucket. When a capture appears more than once, the newest upload wins. Every row carries `source_key`, `upload_date`, `batch` and `uploaded_at`. Names are then normalised: deposit and mineral names are capitalised (`iron` → `Iron`), variants go through the `*_ALIASES` tables, and Capture's `none` for an unset deposit, region or place becomes NULL. |
| `compute` | Runs every job in `src/scanlake_refinery/jobs/` into `results_<name>`. |
| `publish` | Exports `out/consolidated/*.parquet`, `out/results/*.parquet`, `out/monthly/**/*.parquet` (ZSTD) and `out/manifest.json`, and deletes files in `out/` that are no longer produced. It uploads only files whose sha256 changed, and the manifest last. |

Data directory (`REFINERY_DATA_DIR`, on the server `/var/lib/scanlake-refinery`):

```
raw/               mirror of scanlake-data
warehouse.duckdb   sync_state, runs, published, scans, compositions, confidences, results_*
out/               files as published (consolidated/, results/, manifest.json)
```

Derived bucket layout, readable through `GET /api/derived/<key>`:

```
consolidated/scans.parquet  consolidated/compositions.parquet  consolidated/confidences.parquet
results/<job>.parquet
monthly/YYYY/MM/<userId>/{scans,compositions,confidences}.parquet
manifest.json      run_id, generated_at, revision, and rows/bytes/sha256 per file
```

`monthly/` holds the consolidated rows of every closed month (every month before the current UTC
month), one file per user and table. Rows are split by the month folder and user folder of their
raw file (`source_key`), so the files mirror the raw layout without the day and session levels.
They are rebuilt on every run and uploaded only when they change, so a month's files appear on
the first run after it ends. The raw bucket is left as it is.

### Results

`results/<job>.parquet` in the derived bucket, `results_<job>` in the warehouse. A location is
`system` + `gravity_well` (planet, moon, ring or belt); the `_by_region` variants add `region`
(NULL = not recorded). Timestamps are when a scan was saved in Capture, so pace is measured
between saves of one session; gaps over 10 minutes count as breaks and are left out.

| Job | One row per | Columns |
|---|---|---|
| `deposit_materials`, `deposit_materials_by_region` | location × deposit × material | `location_scans`; `deposit_scans`, `deposit_share_pct` (of the location's rocks), `deposit_clusters`, `deposit_avg_mass`, `deposit_total_mass`, `deposit_last_scan_at`, `deposit_cluster_gaps`, `deposit_avg_cluster_gap_seconds`, `deposit_median_cluster_gap_seconds` (time from the last rock of one cluster to the first rock of the next, counted only when both clusters are this deposit at this location and follow each other in a session); `composition_scans` (deposit rocks with a composition), `material_scans`, `presence_pct`, `avg_share_pct` (mean % of the rock, absent = 0; sums to 100 per deposit), `avg_share_when_present_pct`, `total_material_mass`, `total_scu` and `avg_scu_when_present` (from `material_volume`, SCU per rock that contains the material); `quality_0_100_scu_pct`, `quality_101_200_scu_pct`, … `quality_901_1000_scu_pct` (share of the material's SCU in each quality band) and the same ten bands as `…_count_pct` (share of its composition entries); each set sums to 100 |
| `location_stats`, `location_stats_by_region` | location | `scans`, `clusters`, `sessions`, `users`, `deposits`, `total_mass`, `avg_mass`, `first_scan_at`, `last_scan_at`, `gaps_counted`, `avg_gap_seconds`, `median_gap_seconds` |
| `user_stats` | user name (case-insensitive) | `user_ids`, `sessions`, `scans`, `clusters`, `total_mass`, `biggest_scan_mass`, `most_scans_in_session`, `best_session_avg_gap_seconds` (sessions with ≥ 30 scans), `avg_gap_seconds`, `first_scan_at`, `last_scan_at` |
| `highscores` | category × rank | `user_name`, `value`, `unit`, `session_id`, `capture_id`, `achieved_at`, `detail`; categories `fastest_session_avg_scan_time` (≥ 30 scans, lower wins), `biggest_scan`, `most_scans_in_session`, `most_scans_total`; top 10, each user's best entry only, ties go to the earlier one |
| `scan_counts` | day × location × deposit | `rocks`, `clusters`, `sessions`, `total_mass` |

A rock can list the same mineral twice; shares, SCU and presence add those entries up, quality
bands place each entry (and its SCU) by its own quality. The quality bands are NULL for
`Inert Materials`, and entries with a quality outside 0–1000 are left out of them.
Example:

```sql
SELECT deposit, deposit_share_pct, material, avg_share_pct,
       quality_401_500_scu_pct, quality_401_500_count_pct
FROM 'results/deposit_materials.parquet'
WHERE gravity_well = 'Bloom'
ORDER BY deposit_share_pct DESC, avg_share_pct DESC;
```

### Adding a result job

Drop a module into `src/scanlake_refinery/jobs/`:

```python
from . import job

@job("deposit_masses", "Mass distribution per deposit")
def deposit_masses(con) -> str:
    return "SELECT deposit, count(*) AS rocks, avg(mass) AS avg_mass FROM scans GROUP BY ALL"
```

It is picked up automatically, stored as `results_deposit_masses` and published as
`results/deposit_masses.parquet`. Shared SQL (scan gaps, rock materials, user grouping, the pause
threshold) is in `jobs/_shared.py`. Spelling fixes go into `DEPOSIT_ALIASES`, `MINERAL_ALIASES`,
`GRAVITY_WELL_ALIASES` and `REGION_ALIASES` in `consolidate.py`.

## Local development (Windows)

```bash
py -m pip install --user uv
py -m uv sync
cp .env.example .env
```

`.env` is git-ignored. Then use `py -m uv run refinery ...`, or `.venv\Scripts\refinery ...`:

```bash
py -m uv run pytest
py -m uv run ruff check .
py -m uv run refinery consolidate --raw "../ScanLake Analyse/scanlake-data"
py -m uv run refinery compute
py -m uv run refinery publish --no-upload
py -m uv run refinery status
```

With a Gateway key in `.env`, `refinery sync --dry-run` shows what a sync would download.

| Variable | Default | |
|---|---|---|
| `GATEWAY_URL` | `https://gateway.scanlake.rocks` | |
| `GATEWAY_API_KEY` | none | needed by `sync`, `run` and uploads |
| `REFINERY_DATA_DIR` | `./data` | |
| `REFINERY_PUBLISH` | `true` | `false` keeps results local |
| `REFINERY_HTTP_TIMEOUT` | `60` | seconds |

## Server setup (one time)

### 1. Gateway: derived bucket and API key

The Gateway needs the `scanlake-derived` bucket binding, the `PUT /api/derived/<key>` route
and key scopes (see the ScanLake Gateway README). Create the Refinery key there:

```bash
node scripts/create-api-key.mjs --owner scanlake-refinery --scopes derived:write
```

Run the printed `wrangler kv key put` command and keep the plaintext key for step 4. The key
has `maxUploadBytes: 0`, so it can't upload raw data through the Store Worker, which shares the
key store.

### 2. Container (on the Proxmox host, as root)

Copy `deploy/pve-create-lxc.sh` and your `~/.ssh/id_ed25519.pub` to the host, then:

```bash
./pve-create-lxc.sh --vmid 120 --ip 192.168.178.50/24 --gw 192.168.178.1 --pubkey /root/id_ed25519.pub
```

This creates an unprivileged Debian 13 container with `nesting=1` and `onboot=1`, and only your
key for root. Add `--firewall` for a guest firewall that admits only SSH and ping from the LAN
(read the note it prints about the datacenter-level switch).

### 3. Bootstrap (from the workstation, in this repo)

```bash
scp -r deploy root@192.168.178.50:/root/refinery-deploy
ssh -t root@192.168.178.50 bash /root/refinery-deploy/bootstrap-lxc.sh --admin mo
```

The bootstrap sets up:

- packages, unattended-upgrades and a pinned `uv`;
- users `mo` (sudo, SSH) and `scanlake` (system user that runs the job, no shell);
- `/opt/scanlake-refinery` (code), `/var/lib/scanlake-refinery` (data) and
  `/etc/scanlake-refinery/refinery.env` (secrets, root:scanlake 0640);
- a bare deploy repo with a push-to-deploy hook;
- the systemd units;
- sshd: no root login, no passwords, `AllowUsers mo@192.168.178.*`.

After that, root can only get in via `pct enter <vmid>` on the host.

### 4. Deploy and enable

Add to `~/.ssh/config`:

```
Host scanlake-refinery
    HostName 192.168.178.50
    User mo
    IdentityFile ~/.ssh/id_ed25519
```

Then add the remote and deploy:

```bash
git remote add deploy scanlake-refinery:/srv/git/scanlake-refinery.git
git push deploy main
ssh -t scanlake-refinery sudoedit /etc/scanlake-refinery/refinery.env
ssh -t scanlake-refinery sudo /opt/scanlake-refinery/deploy/install-units.sh
```

In `refinery.env`, set `GATEWAY_API_KEY`. The last command enables the hourly timer.

Every later `git push deploy main` checks out the code, runs `uv sync --frozen` against the
system Python, and writes `REVISION`. If a systemd unit changed, the push output says to rerun
`install-units.sh`.

## Operations

```bash
ssh scanlake-refinery
systemctl list-timers scanlake-refinery.timer
journalctl -u scanlake-refinery -n 100
sudo systemctl start scanlake-refinery.service
sudo -u scanlake REFINERY_DATA_DIR=/var/lib/scanlake-refinery /opt/scanlake-refinery/.venv/bin/refinery status
```

The first command shows the next and last run. `journalctl` shows the logs of the last runs.
`systemctl start` runs the pipeline now, and `refinery status` lists runs and table sizes.

- **Rotate the key:** create a new key with the Gateway script and put it into `refinery.env`.
  Then set `"active": false` on the old KV entry.
- **Backups:** `raw/` is a full archive of the bucket, so include the container in a vzdump job
  (Datacenter > Backup).
- **Failed runs** show up as a failed unit (`systemctl --failed`) and as `failed` in
  `refinery status`. Notifications (e.g. `OnFailure=` or a healthcheck ping) aren't set up yet.
- **`status=226/NAMESPACE`:** the sandboxing in the unit needs `nesting=1` on the container.

## Security model

- **No inbound exposure.** The container only opens outbound HTTPS to the Gateway. There is no
  port forward on the router.
- **SSH** is key-only, for one admin user, from `192.168.178.*` only. Root login is off.
- **The Gateway key** can read raw files and write only to the derived bucket (`derived:write`).
  It can't delete anything, and it can't write to `scanlake-data`. Browsers can't use the write
  route, because CORS allows only GET.
- **The job** runs as `scanlake` under a hardened systemd unit. The filesystem is read-only
  except its data dir, there are no capabilities, and only IP and Unix sockets are allowed.
