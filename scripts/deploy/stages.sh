render() {
  kubectl kustomize "deploy/overlays/${OVERLAY}" \
    | sed -e "s#image: bnpb/\([a-z-]*\):TAG#image: ${REGISTRY}\1:${TAG}#"
}

apply_stage() {
  echo "== stage $1"
  render | kubectl apply -n "$NAMESPACE" -l "bnpb.io/stage=$1" -f -
}

recreate_jobs() {
  kubectl delete job -n "$NAMESPACE" -l "bnpb.io/stage=$1" --ignore-not-found --wait=true
  apply_stage "$1"
}

wait_jobs() {
  local job
  for job in "$@"
  do
    kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$job" --timeout="${WAIT_TIMEOUT}"
  done
}

wait_rollouts() {
  local kind="$1"
  shift
  local name
  for name in "$@"
  do
    kubectl rollout status -n "$NAMESPACE" "${kind}/${name}" --timeout="${WAIT_TIMEOUT}"
  done
}

wait_pods_running() {
  kubectl wait -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Running pod -l "app=$1" --timeout="${WAIT_TIMEOUT}"
}

deploy_all() {
  apply_stage infra
  wait_pods_running crdb
  wait_rollouts statefulset kafka
  wait_rollouts deployment auth-store client-api-store

  recreate_jobs init
  wait_jobs crdb-init kafka-topics
  wait_rollouts statefulset crdb

  recreate_jobs migrate
  wait_jobs aggregator-migrate

  apply_stage apps
  wait_rollouts deployment aggregator auth client-api notifier dashboard-updater bmkg-mock pvmbg-mock
}

health_check() {
  if [ -z "$KIND_CLUSTER" ]
  then
    echo "health check via hurl runs only on kind, skipping"
    return 0
  fi
  hurl --test --retry 20 --retry-interval 3000 scripts/demo/00-health.hurl
}
