#!/usr/bin/env bash
# M2 P3 K3 demo (C4): poison input at every entry point is parked, never crashes or blocks a service.
#
#   1. PVMBG mock POST /admin/corrupt serves one truncated, deep, invalid_utf8 and oversized (50 MB)
#      response each. The poller parks each whole body as PVMBG and keeps polling.
#   2. Bad field-report bodies get 413/400 from client-api (NodePort 30080), and nothing is produced.
#   3. Raw bytes are produced straight to Kafka: four bad values to hazard-events, one to
#      field-reports, one to unprocessed itself. A 50 MB record is refused by the broker at produce time.
#   4. notifier, audit-log and dashboard-updater forward each bad hazard-events value to topic
#      unprocessed and commit; the aggregator parks it as INTERNAL, one row per record although three
#      consumer groups forwarded it. SQL and GET /internal/unprocessed?source=... show the rows.
#
# Usage: scripts/demo/m2/p3-poison.sh
# Env: NS, BASE_URL (client-api, default http://<node-ip>:30080), AUTH_URL (default
#      http://<node-ip>:30084), LAPANGAN_SECRET, OPS_TOKEN, PVMBG_ADMIN_KEY (default: read from .env),
#      KCAT_IMAGE (default edenhill/kcat:1.7.1; sends raw bytes, which kafka-console-producer cannot).
set -euo pipefail
cd "$(dirname "$0")/../../.."
. scripts/p1/lib.sh

KCAT_IMAGE="${KCAT_IMAGE:-edenhill/kcat:1.7.1}"
AGG_PORT=28086
MOCK_PORT=28083
GROUPS_HAZARD=(notifier audit-log dashboard-updater)

env_value() { grep "^$1=" .env | cut -d= -f2- | tr -d '\r'; }
LAPANGAN_SECRET="${LAPANGAN_SECRET:-$(env_value LAPANGAN_SECRET)}"
OPS_TOKEN="${OPS_TOKEN:-$(env_value OPS_TOKEN)}"
PVMBG_ADMIN_KEY="${PVMBG_ADMIN_KEY:-$(env_value PVMBG_ADMIN_KEY)}"
NODE_IP="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
BASE_URL="${BASE_URL:-http://$NODE_IP:30080}"
AUTH_URL="${AUTH_URL:-http://$NODE_IP:30084}"

RUN="poison-$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="evidence/m2-p3"
mkdir -p "$EVIDENCE_DIR"
LOG="$EVIDENCE_DIR/p3-poison-$RUN.log"
exec > >(tee "$LOG") 2>&1

step() { printf '\n== %s ==\n' "$*"; }

KCAT_POD="kcat-$(date +%s)"
FORWARDS=()
cleanup() {
  for pid in "${FORWARDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  kubectl -n "$NS" delete pod "$KCAT_POD" --wait=false > /dev/null 2>&1 || true
}
trap cleanup EXIT

kafka_pod() {
  kubectl -n "$NS" get pods -l app=kafka --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -1
}
kafka() { local tool="$1"; shift; kubectl -n "$NS" exec "$(kafka_pod)" -- "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }

ensure_topic() { # ensure_topic <topic> <extra kafka-topics.sh args...>, like deploy/base/kafka/scripts.yaml
  local topic="$1"; shift
  if ! kafka kafka-topics.sh --describe --topic "$topic" 2>/dev/null | grep -q "Topic: $topic"; then
    echo "topic $topic is missing; creating it like deploy/base/kafka/scripts.yaml"
    kafka kafka-topics.sh --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 3 \
      --config retention.ms=604800000 --config min.insync.replicas=2 "$@"
  fi
}

# Total lag of a consumer group on a topic, or "none" before the group has committed anything.
group_lag() {
  kafka kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null \
    | awk -v t="$2" '$2 == t && $6 ~ /^[0-9]+$/ {lag += $6; n++} END {print (n ? lag : "none")}'
}

wait_lag_zero() {
  local lag
  for _ in $(seq 1 60); do
    lag="$(group_lag "$1" "$2")"
    [ "$lag" = 0 ] && break
    sleep 2
  done
  echo "  $1 on $2: lag $lag"
}

# produce <topic>: sends stdin, byte for byte, as one record.
produce() { kubectl -n "$NS" exec -i "$KCAT_POD" -- kcat -b kafka:9092 -t "$1" -P; }

wait_port() {
  for _ in $(seq 1 30); do
    curl -s -o /dev/null "http://localhost:$1/health" && return 0
    sleep 1
  done
  echo "port $1 did not come up" >&2
  return 1
}

# rows_since <source> <code>: unprocessed rows of this run.
rows_since() {
  crdb_sql_tsv "SELECT count(*) FROM unprocessed_messages WHERE source = '$1' AND reason LIKE '$2:%' AND received_at >= '$START'"
}

step "0. Setup"
START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "run $RUN, start $START, client-api $BASE_URL"
ensure_topic field-reports
ensure_topic unprocessed --config max.message.bytes=2097152
kafka kafka-topics.sh --describe --topic unprocessed
kubectl -n "$NS" port-forward svc/pvmbg-mock "$MOCK_PORT:8080" > /dev/null &
FORWARDS+=("$!")
kubectl -n "$NS" port-forward svc/aggregator "$AGG_PORT:8080" > /dev/null &
FORWARDS+=("$!")
kubectl -n "$NS" run "$KCAT_POD" --image="$KCAT_IMAGE" --restart=Never --command -- sleep 900 > /dev/null
kubectl -n "$NS" wait --for=condition=Ready "pod/$KCAT_POD" --timeout=120s
wait_port "$MOCK_PORT"
wait_port "$AGG_PORT"

step "1. PVMBG mock serves corrupted responses (POST /admin/corrupt)"
for pair in truncated:malformed_json deep:too_deep invalid_utf8:invalid_utf8 oversized:too_large; do
  kind="${pair%%:*}" code="${pair##*:}"
  curl -s -X POST -H "X-Admin-Key: $PVMBG_ADMIN_KEY" -H 'Content-Type: application/json' \
    -d "{\"kind\":\"$kind\",\"count\":1}" "http://localhost:$MOCK_PORT/admin/corrupt"
  echo
  n=0
  for _ in $(seq 1 30); do
    n="$(rows_since PVMBG "$code")"
    [ "$n" -ge 1 ] && break
    sleep 2
  done
  echo "  $kind -> PVMBG rows with $code: $n"
done
echo "-- the poller kept going: PVMBG status"
curl -s "http://localhost:$AGG_PORT/internal/sources/status" -H "X-Internal-Key: $(env_value INTERNAL_API_KEY)" \
  | jq -c '.data.PVMBG // .' || true

step "2. Bad field-report bodies (client-api rejects them; nothing is produced)"
TOKEN="$(curl -s -X POST "$AUTH_URL/token" -H 'Content-Type: application/json' \
  -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"field-team\",\"client_secret\":\"$LAPANGAN_SECRET\"}" | jq -r .access_token)"
[ -n "$TOKEN" ] && [ "$TOKEN" != null ] || { echo "no field-team token; check LAPANGAN_SECRET" >&2; exit 1; }
before="$(kafka kafka-get-offsets.sh --topic field-reports | awk -F: '{s += $3} END {print s}')"
GOOD='"latitude":-7.54,"longitude":110.44,"reported_at":"2026-11-18T03:00:00Z"'
post_report() { # post_report <label> <file with the body>
  local out
  out="$(curl -s -o /tmp/p3-poison-body.$$ -w '%{http_code}' -X POST "$BASE_URL/field-reports" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $RUN-$1" --data-binary "@$2" || true)"
  printf '  %-14s -> %s %s\n' "$1" "$out" "$(jq -c '.error // .' /tmp/p3-poison-body.$$ 2>/dev/null | cut -c1-120)"
}
B=/tmp/p3-poison-req.$$
{ printf '{%s,"description":"' "$GOOD"; head -c 70000 /dev/zero | tr '\0' a; printf '"}'; } > "$B"; post_report too-large "$B"
printf '{%s,"description":"Sl\xffeman"}' "$GOOD" > "$B"; post_report invalid-utf8 "$B"
{ printf '{%s,"description":"x","n":' "$GOOD"; head -c 40 /dev/zero | tr '\0' '['; head -c 40 /dev/zero | tr '\0' ']'; printf '}'; } > "$B"; post_report too-deep "$B"
printf '{%s,"description":"terpoton' "$GOOD" > "$B"; post_report truncated "$B"
printf '{%s,"description":"x","extra":1}' "$GOOD" > "$B"; post_report unknown-field "$B"
after="$(kafka kafka-get-offsets.sh --topic field-reports | awk -F: '{s += $3} END {print s}')"
echo "  field-reports end offsets: $before before, $after after (nothing produced)"

step "3. Raw bytes produced straight to Kafka"
printf '{"event_id":"%s-1","hazard_id":"hz' "$RUN" | produce hazard-events && echo "  hazard-events: truncated JSON"
printf '{"event_id":"%s-2","hazard_id":"hz-2","payload":{"area_name":"Sl\xffeman"}}' "$RUN" | produce hazard-events && echo "  hazard-events: invalid UTF-8"
{ printf '{"event_id":"%s-3","hazard_id":"hz-3","x":' "$RUN"; head -c 40 /dev/zero | tr '\0' '['; head -c 40 /dev/zero | tr '\0' ']'; printf '}'; } | produce hazard-events && echo "  hazard-events: 40 levels deep"
printf '{"event_id":"%s-4"}' "$RUN" | produce hazard-events && echo "  hazard-events: no hazard_id"
printf '%s: bukan laporan' "$RUN" | produce field-reports && echo "  field-reports: not JSON"
printf '%s: dikirim langsung ke unprocessed' "$RUN" | produce unprocessed && echo "  unprocessed: no headers"
echo "  hazard-events: 50 MB record (the broker's message.max.bytes is ~1 MiB):"
kubectl -n "$NS" exec "$(kafka_pod)" -- sh -c "head -c 52428800 /dev/zero | tr '\\0' x | \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hazard-events \
  --command-property max.request.size=60000000 --command-property buffer.memory=120000000 \
  --command-property batch.size=60000000 2>&1" | grep -m1 -E 'RecordTooLarge|MESSAGE_TOO_LARGE' || echo "  (no producer error seen)"

step "4. Consumers forward, commit, and the aggregator parks once per record"
for g in "${GROUPS_HAZARD[@]}"; do wait_lag_zero "$g" hazard-events; done
wait_lag_zero aggregator-unprocessed unprocessed
wait_lag_zero aggregator-field-reports field-reports
for d in "${GROUPS_HAZARD[@]}"; do
  echo "-- $d"
  kubectl -n "$NS" logs "deploy/$d" --since-time="$START" 2>/dev/null | grep '"message_invalid"' | jq -c '{topic, partition, offset, reason}' || true
done
echo "-- records on topic unprocessed (all three consumers forward each bad value):"
kafka kafka-get-offsets.sh --topic unprocessed
crdb_sql "SELECT source, endpoint, status, length(payload) AS bytes, reason FROM unprocessed_messages WHERE received_at >= '$START' ORDER BY source, received_at"
crdb_sql "SELECT endpoint, count(*) AS rows FROM unprocessed_messages WHERE source = 'INTERNAL' AND received_at >= '$START' GROUP BY endpoint ORDER BY endpoint"

step "5. GET /internal/unprocessed?source=... (the ops list; client-api /unprocessed forwards to it)"
for source in PVMBG TIM_LAPANGAN INTERNAL; do
  echo "-- source=$source"
  curl -s -H "Authorization: Bearer $OPS_TOKEN" "http://localhost:$AGG_PORT/internal/unprocessed?source=$source&limit=10&since=$START" \
    | jq -c '.items[] | {source, status, reason: (.reason | .[0:90])}'
done

step "6. Every service is still up"
kubectl -n "$NS" get pods -l 'app in (aggregator,notifier,audit-log,dashboard-updater,client-api,pvmbg-mock)'

echo
echo "evidence: $LOG"
rm -f /tmp/p3-poison-body.$$ "$B"
