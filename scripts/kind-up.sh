#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
CLUSTER="${KIND_CLUSTER:-bnpb-dev}"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"
then
  echo "kind cluster $CLUSTER already exists"
else
  kind create cluster --name "$CLUSTER" --config deploy/kind/cluster.yaml
fi

kubectl config use-context "kind-$CLUSTER"
kubectl get nodes -o wide
