#!/usr/bin/env bash
set -euo pipefail

ROLE="$1"
SERVER_IP="$2"
TOKEN="$3"
PUBKEY="$4"

if ! id bnpb > /dev/null 2>&1; then
  useradd -m -s /bin/bash -G sudo bnpb
fi
echo "bnpb ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/bnpb
install -d -m 700 -o bnpb -g bnpb /home/bnpb/.ssh
if [ -n "$PUBKEY" ] && ! grep -qF "$PUBKEY" /home/bnpb/.ssh/authorized_keys 2> /dev/null; then
  echo "$PUBKEY" >> /home/bnpb/.ssh/authorized_keys
fi
touch /home/bnpb/.ssh/authorized_keys
chown bnpb:bnpb /home/bnpb/.ssh/authorized_keys
chmod 600 /home/bnpb/.ssh/authorized_keys
install -m 755 -o bnpb -g bnpb /tmp/k3s-server.sh /tmp/k3s-agent.sh /home/bnpb/

NODE_IP=""
for _ in $(seq 1 30); do
  NODE_IP="$(ip -4 -o addr show scope global | awk '{split($4, a, "/"); if (a[1] !~ /^10\.0\.2\./) {print a[1]; exit}}')"
  [ -n "$NODE_IP" ] && break
  sleep 2
done
if [ -z "$NODE_IP" ]; then
  echo "no bridged ip found, check the Wi-Fi adapter chosen for public_network" >&2
  exit 1
fi
echo "$(hostname) bridged ip ${NODE_IP}"

if [ "$ROLE" = "server" ]; then
  if [ -z "$TOKEN" ]; then
    echo "set K3S_TOKEN before vagrant up node-a" >&2
    exit 1
  fi
  K3S_TOKEN="$TOKEN" bash /home/bnpb/k3s-server.sh "$NODE_IP"
  exit 0
fi

if [ -z "$SERVER_IP" ] || [ -z "$TOKEN" ]; then
  echo "VM ready but not joined, run: SERVER_IP=<node-a ip> K3S_TOKEN=<token> vagrant provision $(hostname)"
  exit 0
fi
bash /home/bnpb/k3s-agent.sh "$SERVER_IP" "$TOKEN" "$NODE_IP"
