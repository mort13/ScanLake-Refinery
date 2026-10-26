#!/usr/bin/env bash
# Install or update the systemd units and, once the code is deployed, enable the hourly timer.
# Run as root; bootstrap-lxc.sh calls it, and post-receive asks for it when a unit changes.
set -euo pipefail

APP_DIR="/opt/scanlake-refinery"
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[[ $EUID -eq 0 ]] || { echo "error: run as root (sudo $0)" >&2; exit 1; }

install -m 0644 "$HERE/systemd/scanlake-refinery.service" "$HERE/systemd/scanlake-refinery.timer" \
    /etc/systemd/system/
systemctl daemon-reload

if [[ -x $APP_DIR/.venv/bin/refinery ]]; then
    systemctl enable --now scanlake-refinery.timer
    systemctl list-timers scanlake-refinery.timer --no-pager
else
    echo "Units installed. The timer gets enabled when this script runs again after the first"
    echo "deployment (git push deploy main)."
fi
