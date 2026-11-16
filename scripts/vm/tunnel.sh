#!/usr/bin/env bash
set -euo pipefail

SERVER_IP="${1:?usage: tunnel.sh <node-a-ip> [ssh-user]}"
SSH_USER="${2:-${VM_USER:-bnpb}}"

forwards=()
for port in 8080 8081 8082 8083 8084 8085 8086; do
  forwards+=(-L "${port}:${SERVER_IP}:$((port + 22000))")
done

echo "localhost:8080 to 8086 now reach NodePorts 30080 to 30086 on ${SERVER_IP}, press Ctrl+C to stop"
exec ssh -N -o ServerAliveInterval=15 -o ExitOnForwardFailure=yes "${forwards[@]}" "${SSH_USER}@${SERVER_IP}"
