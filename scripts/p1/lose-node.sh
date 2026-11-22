#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

NODE="${1:?usage: lose-node.sh <kind worker> [--keep-disk]}"
WIPE=true
[ "${2:-}" = "--keep-disk" ] && WIPE=false

if [ "$WIPE" = true ]; then
  for dir in $(kubectl get pv -o jsonpath='{range .items[*]}{.spec.hostPath.path} {.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}{"\n"}{end}' | awk -v node="$NODE" '$2 == node {print $1}'); do
    echo "wiping $dir on $NODE"
    docker exec "$NODE" sh -c "rm -rf '$dir'/* '$dir'/.[!.]* 2>/dev/null || true"
  done
fi

docker stop "$NODE" > /dev/null
echo "$NODE stopped at $(date +%H:%M:%S), disk wiped: $WIPE"
