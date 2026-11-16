#!/usr/bin/env bash
set -euo pipefail

SERVER_IP="${1:?usage: fetch-kubeconfig.sh <node-a-ip> [ssh-user]}"
SSH_USER="${2:-${VM_USER:-bnpb}}"
OUT="${HOME}/.kube/bnpb-vm.yaml"

mkdir -p "${HOME}/.kube"
ssh "${SSH_USER}@${SERVER_IP}" "cat /etc/rancher/k3s/k3s.yaml" \
  | sed -e "s#https://127.0.0.1:6443#https://${SERVER_IP}:6443#" -e "s#: default\$#: bnpb-vm#" > "$OUT"

KUBECONFIG="$OUT" kubectl get nodes -o wide
echo
echo "use the VM cluster in this shell with:"
echo "  export KUBECONFIG=${OUT}"
