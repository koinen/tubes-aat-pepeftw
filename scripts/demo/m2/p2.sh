#!/usr/bin/env bash
# M2 P2 demo: three Aggregators, one leader, fencing. Runbook: scripts/demo/m2/P2-P4-RUNBOOK.md
#
# Usage: scripts/demo/m2/p2.sh <command> [args]
#
# Read-only:
#   preflight            context, nodes, aggregator pods (node placement), readiness, lease, fence epoch
#   leader               lease holder, node, epoch (leaseTransitions) and the epoch stored in leader_fence
#   workers              per pod: poller_started / leader_workers_ready lines (only the leader polls)
#   relay [since]        per pod: kafka_published lines since <since> (default 10m); only the leader sends
#   duplicates           rows vs distinct source_ref_id per source (target: equal)
#   fence-log [since]    per pod: "stale leader epoch" rejections since <since> (default 10m)
#
# Cluster-changing (CONFIRM=yes; each writes a log under evidence/m2-p2/):
#   kill-leader          delete the leader pod (graceful), time until another pod holds the lease
#   kill-leader --hard   force-delete (grace 0, the lease is NOT released), same timing
#   clock-skew <name>    run Tester Agent practice scenario <name> and compare leader/epoch/rows before and after
#
# Env: KIND=1 (local kind cluster with PostgreSQL), NS, CONFIRM, SINCE.
set -euo pipefail
cd "$(dirname "$0")/../../.."
. scripts/p1/lib.sh

# KIND=1: local kind cluster (PostgreSQL in deploy/aggregator-db) instead of the VM cluster (CockroachDB).
if [ "${KIND:-}" = "1" ]; then
  CRDB_POD="kind-aggregator-db"
  crdb_sql() { kubectl -n "$NS" exec -i deploy/aggregator-db -- sh -c 'psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -f -' <<< "$1"; }
  crdb_sql_tsv() { kubectl -n "$NS" exec -i deploy/aggregator-db -- sh -c 'psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -tA -F , -f -' <<< "$1" | tr ',' '\t'; }
fi

POD_RE='^aggregator-[0-9a-f]{9,10}-[a-z0-9]{5}$'
EVIDENCE_DIR="evidence/m2-p2"

die() { echo "p2.sh: $*" >&2; exit 1; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

need_crdb() { [ -n "${CRDB_POD:-}" ] || die "no ready CockroachDB pod found: wrong kubectl context? (export KUBECONFIG=~/.kube/bnpb-vm.yaml)"; }
need_confirm() { [ "${CONFIRM:-}" = "yes" ] || die "$1 changes the cluster ($(kubectl config current-context)); rerun with CONFIRM=yes"; }
agg_pods() { kubectl -n "$NS" get pods -o name | sed 's#^pod/##' | grep -E "$POD_RE" || true; }
lease_holder() { kubectl -n "$NS" get lease aggregator-leader -o jsonpath='{.spec.holderIdentity}'; }
lease_epoch() { kubectl -n "$NS" get lease aggregator-leader -o jsonpath='{.spec.leaseTransitions}'; }
pod_node() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'; }
fence_epoch() { crdb_sql_tsv "SELECT epoch FROM leader_fence LIMIT 1" | head -1; }

snapshot() {
  echo "leader=$(lease_holder) node=$(pod_node "$(lease_holder)" 2>/dev/null || echo ?) lease_epoch=$(lease_epoch) fence_epoch=$(fence_epoch)"
  crdb_sql_tsv "SELECT source, count(*) FROM hazard_events GROUP BY source ORDER BY source" | tr '\t' '=' | paste -sd' ' -
}

cmd_preflight() {
  need_crdb
  echo "# preflight $(now)"
  echo "context: $(kubectl config current-context)"
  kubectl get nodes -o wide
  echo; echo "## aggregator pods (one per node expected)"
  kubectl -n "$NS" get pods -o wide | grep -E '^NAME|^aggregator-[0-9a-f]'
  echo; echo "## service endpoints"
  kubectl -n "$NS" get endpoints aggregator
  echo; echo "## readinessProbe"
  kubectl -n "$NS" get deploy aggregator -o jsonpath='{.spec.template.spec.containers[0].readinessProbe}{"\n"}'
  echo; echo "## lease and fence"
  snapshot
}

cmd_leader() {
  need_crdb
  local h; h="$(lease_holder)"
  echo "leader      : $h"
  echo "node        : $(pod_node "$h")"
  echo "lease epoch : $(lease_epoch)  (leaseTransitions)"
  echo "fence epoch : $(fence_epoch)  (leader_fence, the highest epoch the store has accepted)"
}

cmd_workers() {
  local p
  for p in $(agg_pods); do
    echo "$p: poller_started=$(kubectl -n "$NS" logs "$p" | grep -c poller_started || true) leader_workers_ready=$(kubectl -n "$NS" logs "$p" | grep -c leader_workers_ready || true)"
  done
  echo "leader: $(lease_holder)"
}

cmd_relay() {
  local since="${1:-${SINCE:-10m}}" p
  echo "kafka_published since $since"
  for p in $(agg_pods); do
    echo "$p: $(kubectl -n "$NS" logs "$p" --since="$since" | grep -c kafka_published || true)"
  done
  echo "leader: $(lease_holder)"
}

cmd_duplicates() {
  need_crdb
  crdb_sql "SELECT source, count(*) AS rows, count(DISTINCT source_ref_id) AS distinct_refs, count(*) - count(DISTINCT source_ref_id) AS duplicates FROM hazard_events GROUP BY source ORDER BY source"
}

cmd_fence_log() {
  local since="${1:-${SINCE:-10m}}" p
  echo "'stale leader epoch' rejections since $since"
  for p in $(agg_pods); do
    echo "$p: $(kubectl -n "$NS" logs "$p" --since="$since" | grep -c 'stale leader epoch' || true)"
  done
}

start_log() {
  mkdir -p "$EVIDENCE_DIR"
  LOG="$EVIDENCE_DIR/$1-$(date -u +%Y%m%dT%H%M%SZ).log"
  exec > >(tee "$LOG") 2>&1
}

cmd_kill_leader() {
  need_crdb; need_confirm kill-leader
  start_log kill-leader
  local hard=false; [ "${1:-}" = "--hard" ] && hard=true
  local old epoch0 t0 new secs
  old="$(lease_holder)"; epoch0="$(lease_epoch)"
  echo "== before ($(now)) =="; snapshot
  t0=$(date +%s)
  if $hard; then
    echo "force-delete $old (grace 0, the lease is not released)"
    kubectl -n "$NS" delete pod "$old" --grace-period=0 --force --wait=false
  else
    echo "delete $old (graceful, the lease is released)"
    kubectl -n "$NS" delete pod "$old" --wait=false
  fi
  new=""; secs=-1
  for _ in $(seq 1 120); do
    sleep 1
    new="$(lease_holder 2>/dev/null || true)"
    if [ -n "$new" ] && [ "$new" != "$old" ]; then secs=$(( $(date +%s) - t0 )); break; fi
  done
  echo "== after ($(now)) =="; snapshot
  echo "--- SUMMARY ---"
  echo "old leader      : $old"
  echo "new leader      : ${new:-none}"
  echo "failover time   : ${secs}s (target <= 60)"
  echo "lease epoch     : $epoch0 -> $(lease_epoch)"
  cmd_duplicates
  echo "log: $LOG"
}

cmd_clock_skew() {
  need_crdb; need_confirm clock-skew
  local name="${1:-}"
  [ -n "$name" ] || die "usage: p2.sh clock-skew <scenario>   (list: kubectl exec -n tester-agent daemonset/tester-agent -- /app list)"
  start_log clock-skew
  echo "== before ($(now)) =="; snapshot
  kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$name"
  echo "== after ($(now)) =="; snapshot
  cmd_fence_log 5m
  cmd_duplicates
  echo "log: $LOG"
}

[ $# -ge 1 ] || { sed -n '2,24p' "$0"; exit 1; }
c="$1"; shift
case "$c" in
  preflight) cmd_preflight ;;
  leader) cmd_leader ;;
  workers) cmd_workers ;;
  relay) cmd_relay "$@" ;;
  duplicates) cmd_duplicates ;;
  fence-log) cmd_fence_log "$@" ;;
  kill-leader) cmd_kill_leader "$@" ;;
  clock-skew) cmd_clock_skew "$@" ;;
  *) die "unknown command: $c" ;;
esac