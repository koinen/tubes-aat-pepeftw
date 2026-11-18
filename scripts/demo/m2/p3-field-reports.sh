#!/usr/bin/env bash
# M2 P3 K6 demo (C7): a field report that got 202 is never lost and is stored exactly once.
#
#   1. The aggregator is scaled to 0: 202 does not wait for it, only for Kafka (acks=all).
#   2. About 30 reports go through client-api's NodePort. Mid-run one client-api pod and one Kafka
#      broker pod are deleted. Every 5th key is sent again, and one key is reused with another body.
#      429, 503 and dropped connections are retried with the same Idempotency-Key after Retry-After.
#   3. The aggregator scales back up and the consumer group's lag goes to 0.
#   4. SQL: every key that got 202 is in field_reports exactly once, and the reused key's second
#      body is parked as TIM_LAPANGAN idempotency_conflict.
#
# Usage: scripts/demo/m2/p3-field-reports.sh
# Env: NS, REPORTS (default 30), BASE_URL (client-api, default http://<node-ip>:30080),
#      AUTH_URL (default http://<node-ip>:30084), LAPANGAN_SECRET (default: read from .env),
#      AGG_REPLICAS (default 3).
set -euo pipefail
cd "$(dirname "$0")/../../.."
. scripts/p1/lib.sh

REPORTS="${REPORTS:-30}"
AGG_REPLICAS="${AGG_REPLICAS:-3}"
TOPIC=field-reports
GROUP=aggregator-field-reports

env_value() { grep "^$1=" .env | cut -d= -f2- | tr -d '\r'; }
LAPANGAN_SECRET="${LAPANGAN_SECRET:-$(env_value LAPANGAN_SECRET)}"
NODE_IP="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
BASE_URL="${BASE_URL:-http://$NODE_IP:30080}"
AUTH_URL="${AUTH_URL:-http://$NODE_IP:30084}"

RUN="demo-$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="evidence/m2-p3"
mkdir -p "$EVIDENCE_DIR"
LOG="$EVIDENCE_DIR/p3-field-reports-$RUN.log"
SENDS="$EVIDENCE_DIR/p3-field-reports-$RUN-sends.tsv"
exec > >(tee "$LOG") 2>&1

step() { printf '\n== %s ==\n' "$*"; }

restore_aggregator() { kubectl -n "$NS" scale deploy/aggregator --replicas="$AGG_REPLICAS" > /dev/null 2>&1 || true; }
trap restore_aggregator EXIT

kafka_pod() {
  kubectl -n "$NS" get pods -l app=kafka --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -1
}
kafka() { local tool="$1"; shift; kubectl -n "$NS" exec "$(kafka_pod)" -- "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }

# Total lag of the consumer group, or "none" before the group has committed anything.
group_lag() {
  kafka kafka-consumer-groups.sh --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t && $6 ~ /^[0-9]+$/ {lag += $6; n++} END {print (n ? lag : "none")}'
}

# Access tokens live 60 s; refresh_token gets a new one when the current one is 45 s old.
TOKEN=""
TOKEN_AT=0
refresh_token() {
  if [ -z "$TOKEN" ] || [ $(( $(date +%s) - TOKEN_AT )) -ge 45 ]; then
    TOKEN="$(curl -s -X POST "$AUTH_URL/token" -H 'Content-Type: application/json' \
      -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"field-team\",\"client_secret\":\"$LAPANGAN_SECRET\"}" | jq -r .access_token)"
    TOKEN_AT="$(date +%s)"
  fi
}

# body <n> <description>: the same arguments always give the same body, so a resend is a duplicate.
RUN_EPOCH="$(date +%s)"
body() {
  jq -nc --arg d "$2" --argjson n "$1" --argjson t "$RUN_EPOCH" \
    '{latitude: (-7.54 - $n / 1000), longitude: 110.44, reported_at: ($t + $n | todate), description: $d}'
}

# send <key> <body>: retries the same key until 202, and records every attempt.
ACCEPTED=()
send() {
  local key="$1" payload="$2" attempt=0 out status latency wait
  while :; do
    attempt=$((attempt + 1))
    refresh_token
    out="$(curl -s -o /tmp/p3-fr-body.$$ -D /tmp/p3-fr-head.$$ -w '%{http_code} %{time_total}' --max-time 10 \
      -X POST "$BASE_URL/field-reports" -H "Authorization: Bearer $TOKEN" \
      -H 'Content-Type: application/json' -H "Idempotency-Key: $key" -d "$payload" || true)"
    status="${out%% *}"
    latency="${out##* }"
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date -u +%H:%M:%S)" "$key" "$attempt" "$status" "$latency" >> "$SENDS"
    case "$status" in
      202)
        echo "$key -> 202 $(jq -r .report_id /tmp/p3-fr-body.$$) (${latency}s, attempt $attempt)"
        ACCEPTED+=("$key")
        return 0 ;;
      429|503)
        wait="$(grep -i '^retry-after:' /tmp/p3-fr-head.$$ | tr -dc '0-9')"
        echo "$key -> $status $(jq -r .error.code /tmp/p3-fr-body.$$ 2>/dev/null), resend after ${wait:-5}s"
        sleep "${wait:-5}" ;;
      401)
        echo "$key -> 401, new token"
        TOKEN="" ;;
      *)
        echo "$key -> ${status:-000} (connection dropped?), resend after 1s"
        sleep 1 ;;
    esac
    [ "$attempt" -lt 60 ] || { echo "$key: gave up after $attempt attempts" >&2; return 1; }
  done
}

step "0. Setup"
echo "client-api $BASE_URL, auth $AUTH_URL, run $RUN"
if ! kafka kafka-topics.sh --describe --topic "$TOPIC" 2>/dev/null | grep -q "Topic: $TOPIC"; then
  echo "topic $TOPIC is missing; creating it like deploy/base/kafka/scripts.yaml"
  kafka kafka-topics.sh --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 3 \
    --config retention.ms=604800000 --config min.insync.replicas=2
fi
kafka kafka-topics.sh --describe --topic "$TOPIC"
echo "RATE_LIMIT_MAX: $(kubectl -n "$NS" get cm client-api-config -o jsonpath='{.data.RATE_LIMIT_MAX}') per $(kubectl -n "$NS" get cm client-api-config -o jsonpath='{.data.RATE_LIMIT_WINDOW_SECONDS}')s (429 is honoured, not counted as an error)"
printf 'time\tkey\tattempt\tstatus\tlatency_s\n' > "$SENDS"
refresh_token
[ "$TOKEN" != null ] && [ -n "$TOKEN" ] || { echo "no field-team token; check LAPANGAN_SECRET" >&2; exit 1; }

step "1. Scale the aggregator to 0"
kubectl -n "$NS" scale deploy/aggregator --replicas=0
kubectl -n "$NS" wait --for=delete pod -l app=aggregator --timeout=120s || true
kubectl -n "$NS" get pods -l app=aggregator

step "2. Send $REPORTS reports; delete a client-api pod and a Kafka broker mid-run"
for i in $(seq 1 "$REPORTS"); do
  key="$RUN-$(printf '%02d' "$i")"
  send "$key" "$(body "$i" "Laporan $i: abu vulkanik di sektor $((i % 4))")"
  if [ "$i" -eq $((REPORTS / 3)) ]; then
    pod="$(kubectl -n "$NS" get pods -l app=client-api -o jsonpath='{.items[0].metadata.name}')"
    kubectl -n "$NS" delete pod "$pod" --wait=false
    echo ">>> deleted client-api pod $pod"
  fi
  if [ "$i" -eq $((2 * REPORTS / 3)) ]; then
    broker="$(kubectl -n "$NS" get pods -l app=kafka -o jsonpath='{.items[-1:].metadata.name}')"
    kubectl -n "$NS" delete pod "$broker" --wait=false
    echo ">>> deleted Kafka broker $broker"
  fi
done

step "2b. Resend every 5th key (same body), and reuse one key with another body"
for i in $(seq 5 5 "$REPORTS"); do
  key="$RUN-$(printf '%02d' "$i")"
  send "$key" "$(body "$i" "Laporan $i: abu vulkanik di sektor $((i % 4))")"
done
CONFLICT_KEY="$RUN-01"
send "$CONFLICT_KEY" "$(body 1 "Laporan 1 DIUBAH: lahar dingin")"

echo
echo "202 latency while the aggregator was at 0 (seconds):"
awk -F'\t' '$4 == 202 {print $5}' "$SENDS" | sort -n \
  | awk '{a[NR] = $1} END {printf "  n=%d  p50=%s  p95=%s  max=%s\n", NR, a[int(NR * 0.5 + 0.5)], a[int(NR * 0.95 + 0.5)], a[NR]}'
echo "attempts by status:"
awk -F'\t' 'NR > 1 {c[$4]++} END {for (s in c) printf "  %s: %d\n", s, c[s]}' "$SENDS"
echo "consumer group lag while the aggregator is down: $(group_lag)"

step "3. Scale the aggregator back to $AGG_REPLICAS and wait for lag 0"
kubectl -n "$NS" scale deploy/aggregator --replicas="$AGG_REPLICAS"
kubectl -n "$NS" rollout status deploy/aggregator --timeout=300s
for _ in $(seq 1 60); do
  lag="$(group_lag)"
  [ "$lag" = 0 ] && break
  sleep 2
done
echo "lag: $lag"
kafka kafka-consumer-groups.sh --describe --group "$GROUP" || true

step "4. Checks"
crdb_sql "SELECT count(*) AS rows, count(DISTINCT idempotency_key) AS distinct_keys FROM field_reports WHERE idempotency_key LIKE '$RUN-%'"
DISTINCT_ACCEPTED="$(printf '%s\n' "${ACCEPTED[@]}" | sort -u | wc -l | tr -d ' ')"
STORED="$(crdb_sql_tsv "SELECT count(*) FROM field_reports WHERE idempotency_key LIKE '$RUN-%'")"
DUPLICATED="$(crdb_sql_tsv "SELECT count(*) FROM (SELECT idempotency_key FROM field_reports WHERE idempotency_key LIKE '$RUN-%' GROUP BY subject, idempotency_key HAVING count(*) > 1)")"
MISSING=0
for key in $(printf '%s\n' "${ACCEPTED[@]}" | sort -u); do
  n="$(crdb_sql_tsv "SELECT count(*) FROM field_reports WHERE subject = 'field-team' AND idempotency_key = '$key'")"
  [ "$n" = 1 ] || { echo "key $key: $n rows"; MISSING=$((MISSING + 1)); }
done
echo "202 responses: ${#ACCEPTED[@]}, distinct keys with 202: $DISTINCT_ACCEPTED, rows stored: $STORED, keys stored twice: $DUPLICATED"
if [ "$MISSING" = 0 ] && [ "$STORED" = "$DISTINCT_ACCEPTED" ] && [ "$DUPLICATED" = 0 ]; then
  echo "PASS: every key that got 202 is stored exactly once"
else
  echo "FAIL: $MISSING keys are not stored exactly once"
fi
crdb_sql "SELECT idempotency_key, description, kafka_partition, kafka_offset FROM field_reports WHERE idempotency_key = '$CONFLICT_KEY'"
crdb_sql "SELECT source, endpoint, source_ref_id, status, reason FROM unprocessed_messages WHERE source = 'TIM_LAPANGAN' AND received_at > now() - INTERVAL '1 hour' ORDER BY received_at"

echo
echo "evidence: $LOG and $SENDS"
rm -f /tmp/p3-fr-body.$$ /tmp/p3-fr-head.$$
