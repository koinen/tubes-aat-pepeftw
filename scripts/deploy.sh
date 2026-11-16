#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

NAMESPACE="sistem-informasi-kebencanaan-bnpb"
OVERLAY="dev"
KIND_CLUSTER="bnpb-dev"
REGISTRY="bnpb/"
TAG=""
ONLY=""
FRESH=false
HEALTH=false
BUILD=true
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"

usage() {
  cat <<EOF
usage: scripts/deploy.sh [options]
  --overlay <name>     kustomize overlay under deploy/overlays (default dev)
  --registry <prefix>  image prefix such as ghcr.io/owner/ (default bnpb/, implies --no-kind when changed)
  --no-kind            push images instead of kind load
  --tag <tag>          reuse an existing image tag
  --no-build           skip docker build (requires --tag)
  --only <service>     rebuild one service and roll it out
  --fresh              delete the namespace and all its data first
  --health             run the hurl health check on a non-kind cluster through the tunnel
EOF
}

while [ $# -gt 0 ]
do
  case "$1" in
    --overlay) OVERLAY="$2"
      shift 2 ;;
    --registry) REGISTRY="$2"
      KIND_CLUSTER=""
      shift 2 ;;
    --no-kind) KIND_CLUSTER=""
      shift ;;
    --tag) TAG="$2"
      shift 2 ;;
    --no-build) BUILD=false
      shift ;;
    --only) ONLY="$2"
      shift 2 ;;
    --fresh) FRESH=true
      shift ;;
    --health) HEALTH=true
      shift ;;
    -h|--help) usage
      exit 0 ;;
    *) usage >&2
      exit 1 ;;
  esac
done

. scripts/deploy/secrets.sh
. scripts/deploy/images.sh
. scripts/deploy/stages.sh

if [ -z "$TAG" ]
then
  if [ "$BUILD" = false ]
  then
    echo "--no-build requires --tag" >&2
    exit 1
  fi
  TAG="$(image_tag)"
fi

kubectl get nodes > /dev/null
load_env

if [ -n "$ONLY" ]
then
  build_images "$ONLY"
  publish_images "$ONLY"
  kubectl set image -n "$NAMESPACE" "deployment/${ONLY}" "${ONLY}=$(image_name "$ONLY")"
  wait_rollouts deployment "$ONLY"
  exit 0
fi

if [ "$FRESH" = true ]
then
  kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true
fi

if [ "$BUILD" = true ]
then
  build_images $SERVICES
  publish_images $SERVICES
fi

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
apply_secrets
deploy_all
health_check
echo "deployed tag $TAG to namespace $NAMESPACE"
