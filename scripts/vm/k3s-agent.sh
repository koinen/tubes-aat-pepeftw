#!/usr/bin/env bash
set -euo pipefail

K3S_VERSION="${K3S_VERSION:-v1.37.2+k3s1}"
SERVER_IP="${1:?usage: sudo bash k3s-agent.sh <server-ip> <token> [node-ip]}"
TOKEN="${2:?usage: sudo bash k3s-agent.sh <server-ip> <token> [node-ip]}"
NODE_IP="${3:-$(ip -4 route get "$SERVER_IP" | awk '{for (i = 1; i <= NF; i++) if ($i == "src") print $(i + 1)}')}"

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

NODE_IFACE="$(ip -4 -o addr show | awk -v ip="$NODE_IP" '{split($4, a, "/"); if (a[1] == ip) print $2}')"
if [ -z "$NODE_IFACE" ]; then
  echo "no interface holds ${NODE_IP}, pass the node ip as an argument" >&2
  exit 1
fi

swapoff -a
sed -i '/\sswap\s/s/^/#/' /etc/fstab
command -v curl > /dev/null || apt-get install -y curl

curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" K3S_URL="https://${SERVER_IP}:6443" K3S_TOKEN="$TOKEN" sh -s - agent \
  --node-ip "$NODE_IP" \
  --flannel-iface "$NODE_IFACE"

systemctl is-active k3s-agent
echo "joined ${SERVER_IP} as $(hostname) with ip ${NODE_IP}, check with kubectl get nodes on the laptop"
