#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

DEAD="${1:?usage: replace-node.sh <dead k8s node>}"

pvcs="$(kubectl get pv -o jsonpath='{range .items[*]}{.spec.claimRef.namespace}/{.spec.claimRef.name} {.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}{"\n"}{end}' \
  | awk -v ns="$NS" -v node="$DEAD" '$2 == node && index($1, ns "/") == 1 {sub(ns "/", "", $1); print $1}')"

echo "removing node object $DEAD so its pods can move"
kubectl delete node "$DEAD" --wait=false 2>/dev/null || echo "node $DEAD already gone"

for pvc in $pvcs; do
  pod="${pvc#data-}"
  echo "releasing $pvc, the disk of $pod that lived on $DEAD"
  kubectl -n "$NS" delete pvc "$pvc" --wait=false
  kubectl -n "$NS" delete pod "$pod" --force --grace-period=0 2>/dev/null || true
done

for pvc in $pvcs; do
  pod="${pvc#data-}"
  until kubectl -n "$NS" get pod "$pod" > /dev/null 2>&1; do sleep 2; done
  kubectl -n "$NS" wait --for=condition=Ready "pod/$pod" --timeout=300s
  echo "$pod now on $(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName}') with an empty disk"
done

bash "$(dirname "$0")/decommission-dead.sh"
bash "$(dirname "$0")/recovery-status.sh"
