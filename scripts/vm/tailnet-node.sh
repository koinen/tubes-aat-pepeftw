#!/usr/bin/env bash
# Puts this VM on the team tailnet, then runs k3s-server.sh or k3s-agent.sh on its tailnet ip.
# Run as root inside the VM. Laptops with Vagrant use tailnet-join.sh, which calls this script.
set -euo pipefail

usage() {
  cat >&2 << 'EOF'
usage, as root inside the VM:
  TS_AUTHKEY=<key> bash tailnet-node.sh vpn
  TS_AUTHKEY=<key> [K3S_TOKEN=<token>] bash tailnet-node.sh server
  TS_AUTHKEY=<key> K3S_TOKEN=<token> bash tailnet-node.sh agent <node-a tailnet ip>
TS_AUTHKEY can be left out once the VM is already on the tailnet.
EOF
  exit 1
}

ROLE="${1:-}"
SERVER_IP="${2:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"

case "$ROLE" in
  vpn | server | agent) ;;
  *) usage ;;
esac

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

if [ "$ROLE" = "agent" ]; then
  if ! [[ "$SERVER_IP" =~ ^100\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    echo "the second argument must be node-a's tailnet ip (100.x.y.z), check that the token was not passed in its place" >&2
    exit 1
  fi
  if [ -z "${K3S_TOKEN:-}" ]; then
    echo "set K3S_TOKEN to the token from node-a" >&2
    exit 1
  fi
fi

find_script() {
  local dir
  for dir in "$HERE" /home/bnpb /tmp; do
    if [ -f "${dir}/$1" ]; then
      echo "${dir}/$1"
      return
    fi
  done
  echo "missing $1, copy scripts/vm/$1 next to this script" >&2
  exit 1
}

if ! command -v tailscale > /dev/null; then
  command -v curl > /dev/null || { apt-get update -qq && apt-get install -y -qq curl; }
  curl -fsSL https://tailscale.com/install.sh | sh
fi
systemctl enable --now tailscaled > /dev/null

if ! tailscale ip -4 > /dev/null 2>&1; then
  if [ -z "${TS_AUTHKEY:-}" ]; then
    echo "this VM is not on the tailnet yet, set TS_AUTHKEY" >&2
    exit 1
  fi
  tailscale up --auth-key="$TS_AUTHKEY" --hostname="$(hostname)" --accept-dns=false
fi

TS_IP=""
for _ in $(seq 1 30); do
  TS_IP="$(tailscale ip -4 2> /dev/null | head -n 1 || true)"
  [ -n "$TS_IP" ] && break
  sleep 2
done
if [ -z "$TS_IP" ]; then
  echo "tailscale is up but has no ipv4 address, check tailscale status" >&2
  exit 1
fi
echo "$(hostname) tailnet ip ${TS_IP}"

case "$ROLE" in
  vpn)
    echo "on the tailnet, k3s not touched"
    ;;
  server)
    SERVER_SCRIPT="$(find_script k3s-server.sh)"
    if [ -n "${K3S_TOKEN:-}" ]; then
      export K3S_TOKEN
    else
      unset K3S_TOKEN
    fi
    bash "$SERVER_SCRIPT" "$TS_IP"
    echo
    echo "agents join over the tailnet with:"
    echo "  K3S_TOKEN=\$(sudo cat /var/lib/rancher/k3s/server/node-token) on node-a, then"
    echo "  TS_AUTHKEY=<key> K3S_TOKEN=<token> bash scripts/vm/tailnet-join.sh <node> agent ${TS_IP}"
    ;;
  agent)
    AGENT_SCRIPT="$(find_script k3s-agent.sh)"
    tailscale ping -c 3 "$SERVER_IP" || true
    if ! curl -sk --max-time 10 -o /dev/null "https://${SERVER_IP}:6443/ping"; then
      echo "cannot reach https://${SERVER_IP}:6443, check that node-a runs k3s on its tailnet ip and shows in tailscale status" >&2
      exit 1
    fi
    echo "node-a api reachable, joining. If it stays on 'Starting k3s-agent', run journalctl -u k3s-agent -f in another shell"
    bash "$AGENT_SCRIPT" "$SERVER_IP" "$K3S_TOKEN" "$TS_IP"
    ;;
esac
