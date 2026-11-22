#!/usr/bin/env bash
# M2 P4 demo: Satellite feed -> WILDFIRE. Runbook: scripts/demo/m2/P2-P4-RUNBOOK.md
#
# Usage: scripts/demo/m2/p4.sh <command> [args]
#
# Read-only:
#   status               feed offset, detections, WILDFIRE count, duplicates, leader pod
#   watch [seconds]      status every 5 s (default 60 s): shows the feed being processed live
#   metrics              satellite metrics from the leader pod (lag, late detections, latency histogram)
#   gaps [since]         "satellite_offset_gap" lines per aggregator pod since <since> (default 10m; target 0)
#
# Cluster-changing (CONFIRM=yes; the log goes to evidence/m2-p4/):
#   crash                force-delete the leader pod while the feed runs; time until the offset advances again;
#                        compare offset, detections, fires, duplicates before and after
#
# Env: KIND=1 (local kind cluster with PostgreSQL), NS, CONFIRM, METRICS_PORT (default 8080), METRICS_PATH (default /metrics), SETTLE (seconds, default 60).
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
EVIDENCE_DIR="evidence/m2-p4"
METRICS_PORT="${METRICS_PORT:-8080}"
METRICS_PATH="${METRICS_PATH:-/metrics}"
SETTLE="${SETTLE:-60}"

die() { echo "p4.sh: $*" >&2; exit 1; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
need_crdb() { [ -n "${CRDB_POD:-}" ] || die "no ready CockroachDB pod found: wrong kubectl context? (export KUBECONFIG=~/.kube/bnpb-vm.yaml)"; }
need_confirm() { [ "${CONFIRM:-}" = "yes" ] || die "$1 changes the cluster ($(kubectl config current-context)); rerun with CONFIRM=yes"; }
agg_pods() { kubectl -n "$NS" get pods -o name | sed 's#^pod/##' | grep -E "$POD_RE" || true; }
lease_holder() { kubectl -n "$NS" get lease aggregator-leader -o jsonpath='{.spec.holderIdentity}'; }
q() { crdb_sql_tsv "$1" | head -1; }

# offset detections fires duplicates
snap() {
  echo "$(q "SELECT last_offset FROM satellite_feed_state") $(q "SELECT count(*) FROM satellite_detection") $(q "SELECT count(*) FROM hazard_events WHERE source='SATELIT'") $(q "SELECT count(*) - count(DISTINCT source_ref_id) FROM hazard_events WHERE source='SATELIT'")"
}

cmd_status() {
  need_crdb
  local o d f u
  read -r o d f u <<< "$(snap)"
  echo "$(now) leader=$(lease_holder) offset=$o detections=$d fires=$f duplicates=$u"
}

cmd_watch() {
  local secs="${1:-60}" end=$(( $(date +%s) + ${1:-60} ))
  while [ "$(date +%s)" -lt "$end" ]; do cmd_status; sleep 5; done
}

cmd_metrics() {
  local h; h="$(lease_holder)"
  echo "leader pod: $h (http://:$METRICS_PORT$METRICS_PATH via the API server proxy)"
  kubectl get --raw "/api/v1/namespaces/$NS/pods/$h:$METRICS_PORT/proxy$METRICS_PATH" \
    | grep -E '^bnpb_satellite_' || die "no bnpb_satellite_* metrics: set METRICS_PORT / METRICS_PATH to where the aggregator exposes them"
}

cmd_gaps() {
  local since="${1:-10m}" p
  for p in $(agg_pods); do
    echo "$p: $(kubectl -n "$NS" logs "$p" --since="$since" | grep -c satellite_offset_gap || true)"
  done
}

cmd_crash() {
  need_crdb; need_confirm crash
  mkdir -p "$EVIDENCE_DIR"
  local log="$EVIDENCE_DIR/crash-$(date -u +%Y%m%dT%H%M%SZ).log"
  exec > >(tee "$log") 2>&1
  local old o0 d0 f0 u0 o1 d1 f1 u1 ok rec t0 o gaps
  old="$(lease_holder)"
  read -r o0 d0 f0 u0 <<< "$(snap)"
  echo "leader before : $old"
  echo "BEFORE ($(now)): offset=$o0 detections=$d0 fires=$f0 duplicates=$u0"
  t0=$(date +%s)
  kubectl -n "$NS" delete pod "$old" --grace-period=0 --force --wait=false
  echo "CRASH: $old force-deleted (grace 0, the lease is not released)"
  ok="$(q "SELECT last_offset FROM satellite_feed_state")"
  rec=-1
  for _ in $(seq 1 120); do
    sleep 1
    o="$(q "SELECT last_offset FROM satellite_feed_state" 2>/dev/null || echo 0)"
    if [ "$o" -gt "$ok" ] 2>/dev/null; then rec=$(( $(date +%s) - t0 )); break; fi
  done
  echo "FEED PROCESSING RESUMED after ${rec}s"
  echo "leader after  : $(lease_holder)"
  echo "settling ${SETTLE}s ..."
  sleep "$SETTLE"
  read -r o1 d1 f1 u1 <<< "$(snap)"
  echo "AFTER ($(now)): offset=$o1 detections=$d1 fires=$f1 duplicates=$u1"
  gaps=0
  for p in $(agg_pods); do gaps=$(( gaps + $(kubectl -n "$NS" logs "$p" --since=10m | grep -c satellite_offset_gap || true) )); done
  echo "--- SUMMARY ---"
  echo "recovery time : ${rec}s (target <= 60)"
  echo "offset        : $o0 -> $o1 (must advance)"
  echo "offset gaps   : $gaps (target 0)"
  echo "duplicates    : $u1 (target 0)"
  echo "fires         : $f0 -> $f1 (must not drop)"
  echo "log: $log"
}

[ $# -ge 1 ] || { sed -n '2,20p' "$0"; exit 1; }
c="$1"; shift
case "$c" in
  status) cmd_status ;;
  watch) cmd_watch "$@" ;;
  metrics) cmd_metrics ;;
  gaps) cmd_gaps "$@" ;;
  crash) cmd_crash ;;
  *) die "unknown command: $c" ;;
esac