#!/usr/bin/env bash
# M2 P3 demo (C5): parked PVMBG messages are replayed after the schema change is declared.
#
#   1. The mock switches to breaking schema versions 3, 4 and 5; the reports are parked.
#   2. The rules go into the aggregator ConfigMap and the aggregator rolls out ("sistem disesuaikan").
#   3. POST replay, poll the job until done.
#   4. Checks: no PVMBG schema rows left parked, one hazard per source_ref_id, one outbox row per
#      hazard, and a second replay selects 0.
#
# Usage: scripts/demo/m2/p3-replay.sh [--kill-leader]
#   --kill-leader  delete the leader pod as soon as the job is running; the next leader resumes it.
# Env: NS, SECONDS_PER_VERSION (default 40; the mock writes a report every 5 s), OPS_TOKEN,
#      AUDIT_TOKEN, PVMBG_ADMIN_KEY (default: read from .env).
set -euo pipefail
cd "$(dirname "$0")/../../.."
. scripts/p1/lib.sh

KILL_LEADER=false
[ "${1:-}" = "--kill-leader" ] && KILL_LEADER=true
SECONDS_PER_VERSION="${SECONDS_PER_VERSION:-40}"
RULES="rename:alert_status=alert_level,numeric_string:eruption_count_24h,optional:ash_column_height_m"
AGG_PORT=28085
MOCK_PORT=28082

env_value() { grep "^$1=" .env | cut -d= -f2- | tr -d '\r'; }
OPS_TOKEN="${OPS_TOKEN:-$(env_value OPS_TOKEN)}"
AUDIT_TOKEN="${AUDIT_TOKEN:-$(env_value AUDIT_TOKEN)}"
PVMBG_ADMIN_KEY="${PVMBG_ADMIN_KEY:-$(env_value PVMBG_ADMIN_KEY)}"

EVIDENCE_DIR="evidence/m2-p3"
mkdir -p "$EVIDENCE_DIR"
LOG="$EVIDENCE_DIR/p3-replay-$(date -u +%Y%m%dT%H%M%SZ)$($KILL_LEADER && echo -kill-leader || true).log"
exec > >(tee "$LOG") 2>&1

step() { printf '\n== %s ==\n' "$*"; }

FORWARDS=()
cleanup() { for pid in "${FORWARDS[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

leader_pod() {
  kubectl -n "$NS" get lease aggregator-leader -o jsonpath='{.spec.holderIdentity}'
}

# Forward to a pod that is not the leader, so deleting the leader does not cut the connection.
forward_aggregator() {
  local leader pod
  leader="$(leader_pod)"
  pod="$(kubectl -n "$NS" get pods -l app=aggregator --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v "^${leader}$" | head -1)"
  kubectl -n "$NS" port-forward "pod/$pod" "$AGG_PORT:8080" > /dev/null &
  FORWARDS+=("$!")
  echo "aggregator API through $pod (leader: $leader)"
}

forward_mock() {
  kubectl -n "$NS" port-forward svc/pvmbg-mock "$MOCK_PORT:8080" > /dev/null &
  FORWARDS+=("$!")
}

wait_port() {
  for _ in $(seq 1 30); do
    curl -s -o /dev/null "http://localhost:$1/health" && return 0
    sleep 1
  done
  echo "port $1 did not come up" >&2
  return 1
}

agg() { curl -s -H "Authorization: Bearer $OPS_TOKEN" "$@"; }

schema_version() {
  curl -s -X POST -H "X-Admin-Key: $PVMBG_ADMIN_KEY" -H 'Content-Type: application/json' \
    -d "{\"version\":$1}" "http://localhost:$MOCK_PORT/admin/schema-version"
  echo
}

parked_schema_rows() {
  crdb_sql_tsv "SELECT count(*) FROM unprocessed_messages WHERE source = 'PVMBG' AND status = 'parked' AND reason LIKE 'schema_incompatible:%'"
}

# Polls a job until it is done or failed and prints it.
wait_job() {
  local job="$1" body status
  for _ in $(seq 1 120); do
    body="$(agg "http://localhost:$AGG_PORT/internal/unprocessed/replay/$job")"
    status="$(echo "$body" | jq -r .status)"
    if [ "$status" = done ] || [ "$status" = failed ]; then
      echo "$body" | jq .
      return 0
    fi
    sleep 1
  done
  echo "job $job did not finish: $body" >&2
  return 1
}

step "0. Current state"
forward_mock
forward_aggregator
wait_port "$MOCK_PORT"
wait_port "$AGG_PORT"
echo "PVMBG_ADAPTATIONS now: '$(kubectl -n "$NS" get cm aggregator-config -o jsonpath='{.data.PVMBG_ADAPTATIONS}')'"
echo "parked PVMBG schema rows: $(parked_schema_rows)"

step "1. Breaking schema versions 3, 4, 5 (${SECONDS_PER_VERSION}s each)"
for v in 3 4 5; do
  schema_version "$v"
  sleep "$SECONDS_PER_VERSION"
done
schema_version 2
echo "parked PVMBG schema rows: $(parked_schema_rows)"
curl -s -H "Authorization: Bearer $AUDIT_TOKEN" \
  "http://localhost:$AGG_PORT/internal/audit/unprocessed?source=PVMBG&limit=3" | jq '.items[] | {message_id, source_ref_id, status, reason}'

step "2. Declare the rules and roll out the aggregator"
kubectl -n "$NS" patch cm aggregator-config --type merge -p "{\"data\":{\"PVMBG_ADAPTATIONS\":\"$RULES\"}}"
kubectl -n "$NS" rollout restart deploy/aggregator
kubectl -n "$NS" rollout status deploy/aggregator --timeout=300s
cleanup
FORWARDS=()
forward_mock
forward_aggregator
wait_port "$AGG_PORT"

step "3. Replay"
accepted="$(agg -X POST "http://localhost:$AGG_PORT/internal/unprocessed/replay?source=PVMBG")"
echo "$accepted" | jq .
JOB="$(echo "$accepted" | jq -r .job_id)"

if $KILL_LEADER; then
  for _ in $(seq 1 30); do
    [ "$(agg "http://localhost:$AGG_PORT/internal/unprocessed/replay/$JOB" | jq -r .status)" = running ] && break
    sleep 0.2
  done
  before="$(agg "http://localhost:$AGG_PORT/internal/unprocessed/replay/$JOB")"
  echo "job before the kill: $(echo "$before" | jq -c '{status, selected}')"
  if [ "$(echo "$before" | jq -r .status)" != running ]; then
    echo "NOTE: the job was not running yet or had already finished; the kill does not land mid-job"
  fi
  old="$(leader_pod)"
  kubectl -n "$NS" delete pod "$old" --wait=false
  echo "deleted leader $old"
fi
wait_job "$JOB" | tee "$EVIDENCE_DIR/job.json"
$KILL_LEADER && echo "leader now: $(leader_pod)"
jq -e '.status == "done" and .selected == .recovered + .still_parked + .handed_to_retry + .not_replayable' \
  "$EVIDENCE_DIR/job.json" > /dev/null && echo "PASS: done, counters add up" || echo "FAIL: counters do not add up"

step "4. Checks"
echo "parked PVMBG schema rows: $(parked_schema_rows)"
crdb_sql "SELECT status, count(*) FROM unprocessed_messages WHERE source = 'PVMBG' GROUP BY status ORDER BY status"
crdb_sql "SELECT count(*) AS duplicated_source_ref_ids FROM (SELECT source_ref_id FROM hazard_events WHERE source = 'PVMBG' GROUP BY source_ref_id HAVING count(*) > 1)"
crdb_sql "SELECT count(*) AS hazards_with_more_than_one_created_event FROM (SELECT hazard_id FROM outbox WHERE event_type = 'HAZARD_CREATED' AND hazard_id IN (SELECT hazard_id FROM hazard_events WHERE source = 'PVMBG') GROUP BY hazard_id HAVING count(*) > 1)"
crdb_sql "SELECT count(*) AS outbox_rows_of_this_job FROM outbox WHERE correlation_id = '$JOB'"
crdb_sql "SELECT h.source_ref_id, h.occurred_at, h.ingested_at, u.received_at FROM unprocessed_messages u JOIN hazard_events h ON h.source = 'PVMBG' AND h.source_ref_id = u.source_ref_id WHERE u.status = 'recovered' ORDER BY u.received_at LIMIT 5"

step "5. A second replay selects 0"
JOB2="$(agg -X POST "http://localhost:$AGG_PORT/internal/unprocessed/replay?source=PVMBG" | jq -r .job_id)"
wait_job "$JOB2" | tee "$EVIDENCE_DIR/job-second.json"
jq -e '.status == "done" and .recovered == 0' "$EVIDENCE_DIR/job-second.json" > /dev/null \
  && echo "PASS: nothing left to recover" || echo "FAIL: the second replay recovered rows"

echo
echo "evidence: $LOG"
