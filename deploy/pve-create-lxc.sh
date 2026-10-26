#!/usr/bin/env bash
# Create the unprivileged Debian 13 container for ScanLake Refinery. Run as root on the Proxmox host:
#
#   ./pve-create-lxc.sh --vmid 120 --ip 192.168.178.50/24 --gw 192.168.178.1 --pubkey /root/mo.pub
#
# --pubkey is the workstation's ~/.ssh/id_ed25519.pub (copy it to the host first). Add --firewall
# to also write a Proxmox guest firewall that only admits SSH and ping from the LAN.
set -euo pipefail

VMID=""
CT_HOSTNAME="scanlake-refinery"
IP=""
GW=""
PUBKEY=""
BRIDGE="vmbr0"
STORAGE="local-lvm"
TEMPLATE_STORAGE="local"
DISK_GB=16
MEMORY_MB=2048
CORES=2
LAN_CIDR="192.168.178.0/24"
FIREWALL=0

usage() {
    cat <<EOF
Usage: $0 --vmid ID --ip CIDR|dhcp [--gw IP] --pubkey FILE [options]

  --vmid ID            container id (must be unused)
  --ip CIDR|dhcp       e.g. 192.168.178.50/24, or dhcp (then reserve the address in the router)
  --gw IP              default gateway, required with a static --ip
  --pubkey FILE        SSH public key allowed to log in
  --hostname NAME      default: $CT_HOSTNAME
  --bridge NAME        default: $BRIDGE
  --storage NAME       rootfs storage, default: $STORAGE
  --disk GB            default: $DISK_GB
  --memory MB          default: $MEMORY_MB
  --cores N            default: $CORES
  --lan CIDR           LAN allowed by --firewall, default: $LAN_CIDR
  --firewall           write /etc/pve/firewall/<vmid>.fw (see the note printed at the end)
EOF
    exit 2
}

die() { echo "error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vmid) VMID="$2"; shift 2 ;;
        --ip) IP="$2"; shift 2 ;;
        --gw) GW="$2"; shift 2 ;;
        --pubkey) PUBKEY="$2"; shift 2 ;;
        --hostname) CT_HOSTNAME="$2"; shift 2 ;;
        --bridge) BRIDGE="$2"; shift 2 ;;
        --storage) STORAGE="$2"; shift 2 ;;
        --disk) DISK_GB="$2"; shift 2 ;;
        --memory) MEMORY_MB="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --lan) LAN_CIDR="$2"; shift 2 ;;
        --firewall) FIREWALL=1; shift ;;
        -h|--help) usage ;;
        *) echo "unknown option: $1" >&2; usage ;;
    esac
done

[[ $EUID -eq 0 ]] || die "run as root"
command -v pct >/dev/null || die "pct not found; run this on the Proxmox host"
[[ -n $VMID && -n $IP && -n $PUBKEY ]] || usage
[[ -f $PUBKEY ]] || die "public key not found: $PUBKEY"
[[ $IP == dhcp || -n $GW ]] || die "--gw is required with a static --ip"
if pct status "$VMID" &>/dev/null; then die "container $VMID already exists"; fi

echo "==> Looking up the Debian 13 template"
pveam update >/dev/null
TEMPLATE=$(pveam available --section system \
    | awk '$2 ~ /^debian-13-standard_.*_amd64\.tar\.zst$/ {print $2}' | sort -V | tail -n 1)
[[ -n $TEMPLATE ]] || die "no debian-13-standard template offered by pveam"
if ! pveam list "$TEMPLATE_STORAGE" | grep -qF "$TEMPLATE"; then
    pveam download "$TEMPLATE_STORAGE" "$TEMPLATE"
fi

if [[ $IP == dhcp ]]; then
    NET0="name=eth0,bridge=$BRIDGE,ip=dhcp,firewall=1"
else
    NET0="name=eth0,bridge=$BRIDGE,ip=$IP,gw=$GW,firewall=1"
fi

echo "==> Creating container $VMID ($CT_HOSTNAME) from $TEMPLATE"
pct create "$VMID" "$TEMPLATE_STORAGE:vztmpl/$TEMPLATE" \
    --hostname "$CT_HOSTNAME" \
    --description "ScanLake Refinery: syncs raw scans via ScanLake Gateway, publishes results" \
    --unprivileged 1 \
    --features nesting=1 \
    --cores "$CORES" \
    --memory "$MEMORY_MB" \
    --swap 512 \
    --rootfs "$STORAGE:$DISK_GB" \
    --net0 "$NET0" \
    --onboot 1 \
    --ssh-public-keys "$PUBKEY"

if [[ $FIREWALL -eq 1 ]]; then
    echo "==> Writing guest firewall /etc/pve/firewall/$VMID.fw"
    cat > "/etc/pve/firewall/$VMID.fw" <<EOF
[OPTIONS]
enable: 1
policy_in: DROP
policy_out: ACCEPT

[RULES]
IN SSH(ACCEPT) -source $LAN_CIDR -log nolog
IN Ping(ACCEPT) -source $LAN_CIDR -log nolog
EOF
fi

pct start "$VMID"
sleep 3
ADDR=$(pct exec "$VMID" -- hostname -I 2>/dev/null | awk '{print $1}') || true

cat <<EOF

Container $VMID is running${ADDR:+ at $ADDR}. Next, from the workstation (in the repo folder):

  scp -r deploy root@${ADDR:-<container-ip>}:/root/refinery-deploy
  ssh -t root@${ADDR:-<container-ip>} bash /root/refinery-deploy/bootstrap-lxc.sh --admin <your-user>

EOF
if [[ $FIREWALL -eq 1 ]]; then
    cat <<EOF
Firewall note: guest rules only apply while the firewall is also enabled at datacenter level
(Datacenter > Firewall > Options). Before enabling it there, make sure the host's own rules
still admit the web UI (8006) and SSH from your LAN, or you can lock yourself out of Proxmox.
EOF
fi
