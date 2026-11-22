#!/usr/bin/env bash
# Runs tailnet-node.sh inside a Vagrant VM from the laptop (Git Bash on Windows, or any shell with vagrant).
set -euo pipefail

NODE="${1:-}"
ROLE="${2:-}"
SERVER_IP="${3:-}"

usage() {
  cat >&2 << 'EOF'
usage, from any directory:
  TS_AUTHKEY=<key> bash scripts/vm/tailnet-join.sh <node> vpn
  TS_AUTHKEY=<key> [K3S_TOKEN=<token>] bash scripts/vm/tailnet-join.sh node-a server
  TS_AUTHKEY=<key> K3S_TOKEN=<token> bash scripts/vm/tailnet-join.sh <node> agent <node-a tailnet ip>
EOF
  exit 1
}

[ -n "$NODE" ] || usage
case "$ROLE" in
  vpn | server) ;;
  agent) [ -n "$SERVER_IP" ] || usage ;;
  *) usage ;;
esac

cd "$(dirname "$0")"

# MSYS_NO_PATHCONV stops Git Bash from rewriting /tmp/... into a Windows path.
for f in tailnet-node.sh k3s-server.sh k3s-agent.sh; do
  MSYS_NO_PATHCONV=1 vagrant upload "$f" "/tmp/$f" "$NODE" > /dev/null
done

vagrant ssh "$NODE" -c "sudo TS_AUTHKEY=$(printf %q "${TS_AUTHKEY:-}") K3S_TOKEN=$(printf %q "${K3S_TOKEN:-}") bash /tmp/tailnet-node.sh $(printf %q "$ROLE") $(printf %q "$SERVER_IP")"
