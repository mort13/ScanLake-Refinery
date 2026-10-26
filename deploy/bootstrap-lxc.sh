#!/usr/bin/env bash
# One-time setup inside the ScanLake Refinery container; safe to run again. Run as root:
#
#   scp -r deploy root@<container-ip>:/root/refinery-deploy
#   ssh -t root@<container-ip> bash /root/refinery-deploy/bootstrap-lxc.sh --admin <your-user>
#
# Afterwards root can no longer log in over SSH; use <your-user> (key only, LAN only) or
# `pct enter <vmid>` on the Proxmox host.
set -euo pipefail

ADMIN=""
LAN_PATTERN="192.168.178.*"
UV_VERSION="0.13.5"

SERVICE_USER="scanlake"
APP_DIR="/opt/scanlake-refinery"
DATA_DIR="/var/lib/scanlake-refinery"
CONF_DIR="/etc/scanlake-refinery"
GIT_REPO="/srv/git/scanlake-refinery.git"
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

usage() {
    cat <<EOF
Usage: $0 --admin USER [--lan PATTERN]

  --admin USER     login user for SSH, sudo and deployments
  --lan PATTERN    sshd address pattern allowed to log in, default: $LAN_PATTERN
EOF
    exit 2
}

die() { echo "error: $*" >&2; exit 1; }
step() { printf '\n==> %s\n' "$*"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --admin) ADMIN="$2"; shift 2 ;;
        --lan) LAN_PATTERN="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown option: $1" >&2; usage ;;
    esac
done

[[ $EUID -eq 0 ]] || die "run as root"
[[ $ADMIN =~ ^[a-z_][a-z0-9_-]*$ ]] || usage
[[ $LAN_PATTERN =~ ^[0-9a-fA-F.:*?/]+$ ]] || die "unexpected --lan pattern: $LAN_PATTERN"

step "Packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q --no-install-recommends \
    ca-certificates curl git sudo openssh-server python3 python3-venv unattended-upgrades
cat > /etc/apt/apt.conf.d/20auto-upgrades <<EOF
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF

step "uv $UV_VERSION"
if [[ $(uv --version 2>/dev/null | awk '{print $2}') != "$UV_VERSION" ]]; then
    triple="$(uname -m)-unknown-linux-gnu"
    url="https://github.com/astral-sh/uv/releases/download/$UV_VERSION/uv-$triple.tar.gz"
    tmp=$(mktemp -d)
    curl -fsSL -o "$tmp/uv-$triple.tar.gz" "$url"
    curl -fsSL -o "$tmp/uv-$triple.tar.gz.sha256" "$url.sha256"
    (cd "$tmp" && sha256sum -c "uv-$triple.tar.gz.sha256")
    tar -xzf "$tmp/uv-$triple.tar.gz" -C "$tmp"
    install -m 0755 "$tmp/uv-$triple/uv" "$tmp/uv-$triple/uvx" /usr/local/bin/
    rm -rf "$tmp"
fi
uv --version

step "Users"
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --user-group --home-dir "$DATA_DIR" --no-create-home \
        --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if ! id "$ADMIN" &>/dev/null; then
    useradd --create-home --shell /bin/bash "$ADMIN"
fi
usermod -aG sudo "$ADMIN"
install -d -m 0700 -o "$ADMIN" -g "$ADMIN" "/home/$ADMIN/.ssh"
if [[ ! -s /home/$ADMIN/.ssh/authorized_keys ]]; then
    [[ -s /root/.ssh/authorized_keys ]] \
        || die "no SSH key for $ADMIN and none in /root/.ssh/authorized_keys to copy"
    install -m 0600 -o "$ADMIN" -g "$ADMIN" /root/.ssh/authorized_keys \
        "/home/$ADMIN/.ssh/authorized_keys"
fi
if passwd -S "$ADMIN" | awk '{exit !($2 == "L" || $2 == "NP")}'; then
    if [[ -t 0 ]]; then
        echo "Set a password for $ADMIN (used for sudo only; SSH stays key-only):"
        passwd "$ADMIN"
    else
        echo "warning: $ADMIN has no password, so sudo won't work. Set one with 'passwd $ADMIN'" \
            "(pct enter on the Proxmox host, or rerun this script with ssh -t)." >&2
    fi
fi

step "Directories"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$DATA_DIR"
install -d -m 0750 -o root -g "$SERVICE_USER" "$CONF_DIR"
if [[ ! -f $CONF_DIR/refinery.env ]]; then
    install -m 0640 -o root -g "$SERVICE_USER" "$HERE/refinery.env.example" "$CONF_DIR/refinery.env"
fi
install -d -m 0755 -o "$ADMIN" -g "$ADMIN" "$APP_DIR"

step "Deploy repository $GIT_REPO"
install -d -m 0755 /srv/git
if [[ ! -d $GIT_REPO ]]; then
    install -d -o "$ADMIN" -g "$ADMIN" "$GIT_REPO"
    sudo -u "$ADMIN" git init --quiet --bare --initial-branch=main "$GIT_REPO"
fi
install -m 0755 -o "$ADMIN" -g "$ADMIN" "$HERE/git-hooks/post-receive" "$GIT_REPO/hooks/post-receive"

step "systemd units"
bash "$HERE/install-units.sh"

step "sshd: key-only, $ADMIN from $LAN_PATTERN"
sed -e "s|__ADMIN__|$ADMIN|" -e "s|__LAN_PATTERN__|$LAN_PATTERN|" \
    "$HERE/sshd_config.d/10-scanlake.conf" > /etc/ssh/sshd_config.d/10-scanlake.conf
chmod 0644 /etc/ssh/sshd_config.d/10-scanlake.conf
sshd -t
systemctl reload ssh.service 2>/dev/null || systemctl restart ssh.service

ADDR=$(hostname -I | awk '{print $1}')
cat <<EOF

Done. Next, on the workstation:

  1. Add to ~/.ssh/config:
       Host scanlake-refinery
           HostName $ADDR
           User $ADMIN
           IdentityFile ~/.ssh/id_ed25519
  2. In the ScanLake Refinery repo:
       git remote add deploy scanlake-refinery:$GIT_REPO
       git push deploy main
  3. ssh scanlake-refinery, put the Gateway key into $CONF_DIR/refinery.env
     (sudoedit $CONF_DIR/refinery.env), then enable the hourly timer:
       sudo $APP_DIR/deploy/install-units.sh
EOF
