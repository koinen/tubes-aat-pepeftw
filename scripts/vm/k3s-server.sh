#!/usr/bin/env bash
set -euo pipefail

K3S_VERSION="${K3S_VERSION:-v1.37.2+k3s1}"
NODE_IP="${1:-$(ip -4 route get 1.1.1.1 | awk '{for (i = 1; i <= NF; i++) if ($i == "src") print $(i + 1)}')}"

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root, for example: sudo bash k3s-server.sh [node-ip]" >&2
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

curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" sh -s - server \
  --node-ip "$NODE_IP" \
  --tls-san "$NODE_IP" \
  --flannel-iface "$NODE_IFACE" \
  --disable traefik \
  --disable servicelb \
  --write-kubeconfig-mode 644

until k3s kubectl get node "$(hostname)" 2> /dev/null | grep -q " Ready"; do sleep 3; done
k3s kubectl get nodes -o wide

echo
echo "run this on every other VM:"
echo "  sudo bash k3s-agent.sh ${NODE_IP} $(cat /var/lib/rancher/k3s/server/node-token)"
echo
echo "then on the laptop:"
echo "  bash scripts/vm/fetch-kubeconfig.sh ${NODE_IP}"
