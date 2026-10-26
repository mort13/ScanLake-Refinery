# Task: create the LXC container for ScanLake Refinery

Please create one new container on this Proxmox host. Nothing else on the host should change.

## What it is for
A small Python batch job (DuckDB) that runs hourly. It only makes outbound HTTPS calls to
`https://gateway.scanlake.rocks`. Nothing needs to be reachable from outside the LAN, and no
router port forwards are needed. Everything inside the container (users, packages, sshd hardening,
systemd units) is set up later from the workstation by a bootstrap script over SSH, so don't install
or configure anything inside it.

## Container spec
- **Template:** the newest `debian-13-standard_*_amd64.tar.zst` (download it with `pveam` if missing)
- **Unprivileged:** yes (`--unprivileged 1`)
- **Features:** `nesting=1`. The job's systemd unit uses sandboxing that needs it.
- **Hostname:** `scanlake-refinery`
- **CPU / RAM / swap:** 2 cores, 2048 MB, 512 MB
- **Root disk:** 16 GB on the default VM storage (e.g. `local-lvm`)
- **Network:** `eth0` on the LAN bridge (e.g. `vmbr0`), `firewall=1`.
  - Use a static IPv4 in `192.168.178.0/24` that is free and outside the FritzBox DHCP range (by default `.20`–`.200`, e.g. `.210`).
  - The gateway is `192.168.178.1`.
  - If you use DHCP instead, report the address and recommend a DHCP reservation.
- **Start at boot:** yes (`--onboot 1`)
- **Description:** `ScanLake Refinery: syncs raw scans via ScanLake Gateway, publishes results`
- **Root SSH key:** inject exactly this public key with `--ssh-public-keys`. Password login is not needed.
  ```
  ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEaSVY1eCRt1GlGi9b8d6/WsZ9TEtKS4vAt2IZPJt9X9 nutzer@DESKTOP-1GE742U
  ```

## Optional: guest firewall
Only do this if the datacenter-level firewall is already enabled here. Don't turn on the
datacenter firewall just for this. If it is enabled, create `/etc/pve/firewall/<vmid>.fw`:
```
[OPTIONS]
enable: 1
policy_in: DROP
policy_out: ACCEPT

[RULES]
IN SSH(ACCEPT) -source 192.168.178.0/24 -log nolog
IN Ping(ACCEPT) -source 192.168.178.0/24 -log nolog
```

## Then
1. Start the container.
2. Check it has network access: `pct exec <vmid> -- apt-get update` succeeds.
3. Check that sshd is running in it and that root's `authorized_keys` contains the key above.

## Report back
- the VMID
- the IP address
- whether the guest firewall was enabled
- anything that deviated from this spec
