#!/usr/bin/env bash
# M2 P5 (C12) measurement and fault helpers for the VM cluster. Runbook: scripts/demo/m2/P5-RUNBOOK.md
#
# Usage: scripts/demo/m2/p5.sh <command> [args]
#
# Read-only:
#   preflight                 context, nodes, pod health, image digests, sensor profile, endpoints
#   mark "<text>"             append a UTC line to the run timeline
#   offsets <start|end>       record per-partition end offsets of field-team-receipts
#   clocks <T0|T1>            record each node's time-sync status (kubectl debug node)
#   sample <seconds>          K4: poll /sources/status and the COASTAL list every 2 s (status, time_total)
#   sensor-rate <since>       K4: Sensor Pantai HTTP calls per minute + breaker changes from aggregator logs
#   collect <T0> <T1>         K1/K2/K8: audit cohort + receipts between offsets -> score
#   tester-logs               K6: save the Tester Agent action log
#   k6 <k3|k5> [k6 args]      K3/K5 load (grafana/k6 in docker, host network)
#
# Cluster-changing (team runs these, each one is written to the timeline):
#   cleanup-sensor-history    restart the sensor, wipe SENSOR_PANTAI rows, restart the notifier (CONFIRM=yes)
#   fault-sensor <on|off>     NetworkPolicy drops aggregator -> sensor traffic (calls hit the 2 s timeout)
#   pvmbg-outage <on|off> [hang|error]
#   config <KEY> <VALUE>      patch client-api-config and restart client-api (RATE_LIMIT_MAX, MAX_CONCURRENT_REQUESTS)
#   config-restore            RATE_LIMIT_MAX=10, MAX_CONCURRENT_REQUESTS=200
#
# Env: NS, OUT (default docs/results/p5), PREFIX (default practice-), NODE (NodePort host; default the
#      kubeconfig server host), AUDIT_TOKEN / MEDIA_SECRET / LAPANGAN_SECRET / BNPB_SECRET / PVMBG_ADMIN_KEY
#      (default: Secret bnpb-secrets, then .env).
set -euo pipefail
cd "$(dirname "$0")/../../.."
. scripts/p1/lib.sh

OUT="${OUT:-docs/results/p5}"
PREFIX="${PREFIX:-practice-}"
RECEIPTS_TOPIC="field-team-receipts"
KAFKA_POD="${KAFKA_POD:-kafka-0}"
KAFKA_BIN=/opt/kafka/bin
PY="python3 -I scripts/demo/m2/p5_slo.py"
K6_IMAGE="grafana/k6:0.54.0"
mkdir -p "$OUT"

now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }
f() { echo "$OUT/$PREFIX$1"; }
die() { echo "p5.sh: $*" >&2; exit 1; }

secret() {
  local v
  v="$(kubectl -n "$NS" get secret bnpb-secrets -o "jsonpath={.data.$1}" 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [ -z "$v" ] && [ -f .env ]; then v="$(grep "^$1=" .env | cut -d= -f2- | tr -d '\r' || true)"; fi
  printf '%s' "$v"
}

node_host() {
  if [ -n "${NODE:-}" ]; then echo "$NODE"; return; fi
  kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' | sed -E 's#^https?://##; s#:[0-9]+$##'
}

mark() { echo "$(now)  $*" | tee -a "$(f timeline.txt)"; }

need_confirm() {
  local ctx; ctx="$(kubectl config current-context)"
  case "$ctx" in kind-*) die "$1: current context is $ctx, not the VM cluster (export KUBECONFIG=~/.kube/bnpb-vm.yaml)" ;; esac
  [ "${CONFIRM:-}" = "yes" ] || die "$1 changes cluster $ctx; rerun with CONFIRM=yes"
}

cmd_preflight() {
  local o; o="$(f preflight.txt)"
  {
    echo "# preflight $(now)"
    echo "context: $(kubectl config current-context)"
    echo "nodehost: $(node_host)"
    kubectl get nodes -o wide
    echo; echo "## pods not Running/ready"
    kubectl -n "$NS" get pods --no-headers | awk '$3 != "Running" && $3 != "Completed" || $2 ~ /^0\//' || true
    echo; echo "## placement"
    kubectl -n "$NS" get pods -o wide -L role --no-headers | awk '{print $1, $3, $7, $NF}'
    echo; echo "## sensor profile"
    kubectl -n "$NS" get deploy sensor-pantai -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' | grep -E 'PANTAI_(PROFILE|SPEED|SEED)' || true
    echo; echo "## notifier webhook + client-api limits"
    kubectl -n "$NS" get cm notifier-config -o jsonpath='{.data.FIELD_TEAM_WEBHOOK_URL}{"\n"}'
    kubectl -n "$NS" get cm client-api-config -o jsonpath='RATE_LIMIT_MAX={.data.RATE_LIMIT_MAX} MAX_CONCURRENT_REQUESTS={.data.MAX_CONCURRENT_REQUESTS}{"\n"}'
    echo; echo "## tester agent"
    kubectl -n tester-agent get pods -o wide --no-headers 2>&1 || true
    echo; echo "## receipts topic"
    kubectl -n "$NS" exec "$KAFKA_POD" -- "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic "$RECEIPTS_TOPIC" 2>&1 | head -5
  } 2>&1 | tee "$o"
  kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .status.containerStatuses[*]}{.imageID}{" "}{end}{"\n"}{end}' > "$(f digests.txt)"
  echo "digests -> $(f digests.txt)"
  local host token code
  host="$(node_host)"; token="$(secret AUDIT_TOKEN)"
  code="$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 -H "Authorization: Bearer $token" \
    "http://$host:30085/internal/audit/events?source=SENSOR_PANTAI&limit=1" || true)"
  echo "audit API via http://$host:30085 -> $code" | tee -a "$o"
  code="$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 "http://$host:30080/health" || true)"
  echo "client-api via http://$host:30080/health -> $code" | tee -a "$o"
}

cmd_offsets() {
  local which="${1:-}"; [ "$which" = start ] || [ "$which" = end ] || die "offsets <start|end>"
  local o; o="$(f "offsets-$which.txt")"
  {
    echo "# $RECEIPTS_TOPIC end offsets at $(now)"
    kubectl -n "$NS" exec "$KAFKA_POD" -- "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server localhost:9092 \
      --topic "$RECEIPTS_TOPIC" --time -1
  } | tee "$o"
  mark "receipt offsets ($which) recorded"
}

# offsets file lines look like "field-team-receipts:<partition>:<offset>"
read_offsets() { grep "^$RECEIPTS_TOPIC:" "$1" | awk -F: '{print $2, $3}'; }

cmd_clocks() {
  local label="${1:-}"; [ -n "$label" ] || die "clocks <T0|T1>"
  local o; o="$(f "clocks-$label.txt")"
  : > "$o"
  echo "# node time sync at $(now) (local), label $label" | tee -a "$o"
  for n in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    local pod
    pod="$(kubectl debug "node/$n" --image=busybox:1.36 --profile=sysadmin -- \
      chroot /host sh -c 'date -u +%Y-%m-%dT%H:%M:%S.%NZ; timedatectl timesync-status 2>&1 || timedatectl 2>&1; chronyc tracking 2>/dev/null || true' 2>&1 \
      | sed -nE 's/.*Creating debugging pod ([^ ]+) .*/\1/p')"
    if [ -z "$pod" ]; then echo "== $n: kubectl debug failed" | tee -a "$o"; continue; fi
    kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" --timeout=60s >/dev/null 2>&1 || true
    { echo "== $n"; kubectl logs "$pod" 2>&1; } | tee -a "$o"
    kubectl delete pod "$pod" --wait=false >/dev/null 2>&1 || true
  done
  mark "clocks ($label) recorded"
}

cmd_sample() {
  local secs="${1:-600}" host
  host="$(node_host)"
  mark "K4 sampler start (${secs}s)"
  BNPB_SECRET="$(secret BNPB_SECRET)" $PY sample --base "http://$host:30080" --auth "http://$host:30084" \
    --duration "$secs" | tee -a "$(f c4-fast.txt)"
  mark "K4 sampler end"
}

cmd_sensor_rate() {
  local since="${1:-}"; [ -n "$since" ] || die "sensor-rate <since RFC3339>"
  local o; o="$(f c4-rate.txt)"
  {
    echo "# Sensor Pantai calls since $since, collected $(now)"
    echo "# pods: $(kubectl -n "$NS" get pods -l app=aggregator -o jsonpath='{.items[*].metadata.name}')"
    kubectl -n "$NS" logs -l app=aggregator --since-time="$since" --tail=-1 --prefix --max-log-requests=10 \
      | $PY sensor-rate
  } | tee "$o"
}

cmd_collect() {
  local t0="${1:-}" t1="${2:-}"; [ -n "$t0" ] && [ -n "$t1" ] || die "collect <T0> <T1> (RFC 3339 UTC)"
  [ -f "$(f offsets-start.txt)" ] && [ -f "$(f offsets-end.txt)" ] || die "record offsets start/end first"
  local host token since raw
  host="$(node_host)"; token="$(secret AUDIT_TOKEN)"
  since="$(date -u -d "$t0 - 5 minutes" +%Y-%m-%dT%H:%M:%SZ)"
  raw="$OUT/raw"; mkdir -p "$raw"

  AUDIT_TOKEN="$token" $PY fetch-audit --base "http://$host:30085" --since "$since" \
    --out "$raw/${PREFIX}audit-events.jsonl" --pages-out "$raw/${PREFIX}audit-pages.json"
  AUDIT_TOKEN="$token" $PY fetch-unprocessed --base "http://$host:30085" --out "$(f c2-rejected.json)"

  # Parked SENSOR_PANTAI payloads, to count criticals that never became a HazardEvent.
  # Each row comes back hex-encoded so TSV quoting cannot touch the JSON.
  crdb_sql_tsv "SELECT encode(json_build_object('message_id', message_id, 'reason', reason,
      'received_at', received_at::STRING, 'payload_hex', encode(payload, 'hex'))::STRING::BYTES, 'hex')
    FROM unprocessed_messages WHERE source = 'SENSOR_PANTAI' AND received_at >= '$since'" \
    | python3 -I -c 'import sys
for line in sys.stdin:
    line = line.strip().strip("\"")
    if line:
        print(bytes.fromhex(line).decode("utf-8"))' > "$raw/${PREFIX}parked.jsonl"

  : > "$raw/${PREFIX}receipts.jsonl"
  local p start end n
  while read -r p end; do
    start="$(read_offsets "$(f offsets-start.txt)" | awk -v p="$p" '$1 == p {print $2}')"
    start="${start:-0}"; n=$((end - start))
    echo "receipts partition $p: offsets $start..$end ($n records)"
    [ "$n" -gt 0 ] || continue
    kubectl -n "$NS" exec "$KAFKA_POD" -- "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server localhost:9092 \
      --topic "$RECEIPTS_TOPIC" --partition "$p" --offset "$start" --max-messages "$n" --timeout-ms 30000 \
      2>/dev/null >> "$raw/${PREFIX}receipts.jsonl" || true
  done < <(read_offsets "$(f offsets-end.txt)")
  echo "receipts read: $(grep -c '^{' "$raw/${PREFIX}receipts.jsonl" || true)"

  set +e
  $PY score --events "$raw/${PREFIX}audit-events.jsonl" --receipts "$raw/${PREFIX}receipts.jsonl" \
    --pages "$raw/${PREFIX}audit-pages.json" --parked "$raw/${PREFIX}parked.jsonl" \
    --t0 "$t0" --t1 "$t1" --evidence-dir "$OUT" --prefix "$PREFIX" \
    --json-out "$(f c8-slo.json)" | tee "$(f c1-critical.txt)"
  set -e
  mark "collect done for window $t0 .. $t1"
}

cmd_tester_logs() {
  kubectl -n tester-agent logs -l app=tester-agent --tail=-1 --prefix --max-log-requests=10 > "$(f c6-tester-agent.log)"
  echo "-> $(f c6-tester-agent.log) ($(wc -l < "$(f c6-tester-agent.log)") lines)"
}

cmd_cleanup_sensor_history() {
  need_confirm cleanup-sensor-history
  mark "cleanup: deleting sensor-pantai pod (sensor time restarts at zero)"
  kubectl -n "$NS" delete pod -l app=sensor-pantai --wait=true
  kubectl -n "$NS" rollout status deploy/sensor-pantai --timeout=120s
  local started
  started="$(kubectl -n "$NS" get pods -l app=sensor-pantai -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}')"
  mark "S = sensor container started at $started"
  # Wipe only after the restart: otherwise the poller re-reads the old run and the new criticals dedupe away.
  mark "cleanup: deleting SENSOR_PANTAI rows"
  crdb_sql "DELETE FROM outbox WHERE hazard_id IN (SELECT hazard_id FROM hazard_events WHERE source = 'SENSOR_PANTAI');
    DELETE FROM hazard_events WHERE source = 'SENSOR_PANTAI';
    DELETE FROM unprocessed_messages WHERE source = 'SENSOR_PANTAI';
    DELETE FROM rejected_records WHERE source = 'SENSOR_PANTAI';
    DELETE FROM poll_cursor WHERE source = 'SENSOR_PANTAI';
    DELETE FROM source_status WHERE source = 'SENSOR_PANTAI';"
  mark "cleanup: deleting notifier pod (empties its emptyDir seen-store)"
  kubectl -n "$NS" delete pod -l app=notifier --wait=true
  kubectl -n "$NS" rollout status deploy/notifier --timeout=120s
  mark "cleanup done; next: offsets start"
  echo "T0 should be S + 25 min: $(date -u -d "$started + 25 minutes" +%Y-%m-%dT%H:%M:%SZ)"
  echo "practice criticals expected near S + 30 min and S + 90 min"
}

cmd_fault_sensor() {
  need_confirm fault-sensor
  case "${1:-}" in
    on)
      kubectl -n "$NS" patch networkpolicy sensor-pantai-aggregator-only --type=json \
        -p '[{"op":"replace","path":"/spec/ingress","value":[]}]'
      mark "FAULT sensor: aggregator -> sensor traffic dropped (NetworkPolicy ingress [])" ;;
    off)
      kubectl -n "$NS" patch networkpolicy sensor-pantai-aggregator-only --type=json \
        -p '[{"op":"replace","path":"/spec/ingress","value":[{"from":[{"podSelector":{"matchLabels":{"app":"aggregator"}}}],"ports":[{"port":8080}]}]}]'
      mark "FAULT sensor cleared: NetworkPolicy restored" ;;
    *) die "fault-sensor <on|off>" ;;
  esac
}

cmd_pvmbg_outage() {
  need_confirm pvmbg-outage
  local state="${1:-}" mode="${2:-hang}" enabled
  case "$state" in on) enabled=true ;; off) enabled=false ;; *) die "pvmbg-outage <on|off> [hang|error]" ;; esac
  curl -sS --max-time 10 -X POST "http://$(node_host):30082/admin/outage" -H "X-Admin-Key: $(secret PVMBG_ADMIN_KEY)" \
    -H 'Content-Type: application/json' -d "{\"enabled\": $enabled, \"mode\": \"$mode\"}"; echo
  mark "FAULT pvmbg outage enabled=$enabled mode=$mode"
}

cmd_config() {
  need_confirm config
  local key="${1:-}" value="${2:-}"; [ -n "$key" ] && [ -n "$value" ] || die "config <KEY> <VALUE>"
  kubectl -n "$NS" patch configmap client-api-config --type=merge -p "{\"data\":{\"$key\":\"$value\"}}"
  kubectl -n "$NS" rollout restart deploy/client-api
  kubectl -n "$NS" rollout status deploy/client-api --timeout=180s
  mark "config client-api $key=$value (client-api restarted)"
}

cmd_config_restore() {
  need_confirm config-restore
  kubectl -n "$NS" patch configmap client-api-config --type=merge \
    -p '{"data":{"RATE_LIMIT_MAX":"10","MAX_CONCURRENT_REQUESTS":"200"}}'
  kubectl -n "$NS" rollout restart deploy/client-api
  kubectl -n "$NS" rollout status deploy/client-api --timeout=180s
  mark "config client-api restored (RATE_LIMIT_MAX=10, MAX_CONCURRENT_REQUESTS=200)"
}

cmd_k6() {
  local mode="${1:-}"; shift || true
  [ "$mode" = k3 ] || [ "$mode" = k5 ] || die "k6 <k3|k5> [extra k6 args]"
  local host o rc=0; host="$(node_host)"; o="$(f "k6-$mode-$(date -u +%H%M%S).txt")"
  mark "k6 $mode start ($*)"
  docker run --rm --network host -v "$PWD/scripts/demo/m2:/scripts:ro" \
    -e MODE="$mode" -e API="http://$host:30080" -e AUTH="http://$host:30084" \
    -e MEDIA_SECRET="$(secret MEDIA_SECRET)" -e LAPANGAN_SECRET="$(secret LAPANGAN_SECRET)" \
    -e BNPB_SECRET="$(secret BNPB_SECRET)" \
    -e DURATION="${DURATION:-5m}" -e MEDIA_VUS="${MEDIA_VUS:-400}" -e FIELD_VUS="${FIELD_VUS:-50}" -e READ_VUS="${READ_VUS:-20}" \
    "$K6_IMAGE" run "$@" /scripts/p5-k6.js 2>&1 | tee "$o" || rc=$?
  mark "k6 $mode end (exit $rc) -> $o"
  return "$rc"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  preflight) cmd_preflight ;;
  mark) mark "$*" ;;
  offsets) cmd_offsets "$@" ;;
  clocks) cmd_clocks "$@" ;;
  sample) cmd_sample "$@" ;;
  sensor-rate) cmd_sensor_rate "$@" ;;
  collect) cmd_collect "$@" ;;
  tester-logs) cmd_tester_logs ;;
  cleanup-sensor-history) cmd_cleanup_sensor_history ;;
  fault-sensor) cmd_fault_sensor "$@" ;;
  pvmbg-outage) cmd_pvmbg_outage "$@" ;;
  config) cmd_config "$@" ;;
  config-restore) cmd_config_restore ;;
  k6) cmd_k6 "$@" ;;
  *) sed -n '2,30p' "$0"; exit 2 ;;
esac
