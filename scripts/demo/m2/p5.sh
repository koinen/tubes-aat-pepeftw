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
#   k6 <k3|k5> [k6 args]      K3/K5 load: a k6 pod calling the Services (K6_WHERE=local: docker on this
#                             host via the NodePorts; K6_NODE=<node> pins the pod)
#
# Cluster-changing (team runs these, each one is written to the timeline):
#   cleanup-sensor-history    stop aggregator + sensor, wipe SENSOR_PANTAI rows, start the aggregator, restart the
#                             notifier; the sensor stays stopped (CONFIRM=yes)
#   gameday-start [minutes]   S = T0: start the sensor at PANTAI_SPEED (default 6), mark T0, run inject-auto
#                             (INJECT=hand: no injector, a teammate injects)
#   fault-sensor <on|off>     NetworkPolicy drops aggregator -> sensor traffic (calls hit the 2 s timeout)
#   pvmbg-outage <on|off> [hang|error]
#   config <KEY> <VALUE>      patch client-api-config and restart client-api (RATE_LIMIT_MAX, MAX_CONCURRENT_REQUESTS)
#   config-restore            RATE_LIMIT_MAX=10, MAX_CONCURRENT_REQUESTS=200
#   inject-auto [minutes]     hidden random fault plan run in the background (default 30 min, at least 20;
#                             the C12 injector; KILL_VM, VBOX). gameday-start calls it.
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
    echo; echo "## iptables on each node (the Tester Agent's partition needs it on the host)"
    for n in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
      echo "$n: $(node_exec "$n" 'command -v iptables || echo MISSING' | tail -1)"
    done
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

# node_exec <node> <sh script>: runs the script in the node's root filesystem through a debug pod.
node_exec() {
  local pod
  pod="$(kubectl debug "node/$1" --image=busybox:1.36 --profile=sysadmin -- chroot /host sh -c "$2" 2>&1 \
    | sed -nE 's/.*Creating debugging pod ([^ ]+) .*/\1/p')"
  if [ -z "$pod" ]; then echo "kubectl debug failed"; return 1; fi
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" --timeout=60s >/dev/null 2>&1 || true
  kubectl logs "$pod" 2>&1
  kubectl delete pod "$pod" --wait=false >/dev/null 2>&1 || true
}

cmd_clocks() {
  local label="${1:-}"; [ -n "$label" ] || die "clocks <T0|T1>"
  local o; o="$(f "clocks-$label.txt")"
  : > "$o"
  echo "# node time sync at $(now) (local), label $label" | tee -a "$o"
  for n in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    { echo "== $n"; node_exec "$n" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ; timedatectl timesync-status 2>&1 || timedatectl 2>&1; chronyc tracking 2>/dev/null || true'; } | tee -a "$o"
  done
  mark "clocks ($label) recorded"
}

cmd_sample() {
  local secs="${1:-600}" host o
  host="$(node_host)"; o="$(f c4-fast.txt)"
  # One sampler per file: two at once interleave their rows (practice run, 2026-11-22).
  [ ! -s "$o" ] || [ "${APPEND:-}" = 1 ] || die "sample: $o already has data; move it away or set APPEND=1"
  mark "K4 sampler start (${secs}s)"
  BNPB_SECRET="$(secret BNPB_SECRET)" $PY sample --base "http://$host:30080" --auth "http://$host:30084" \
    --duration "$secs" | tee -a "$o"
  mark "K4 sampler end"
}

# Reads each aggregator pod's log on its own, so one pod on an unreachable node costs only its lines.
cmd_sensor_rate() {
  local since="${1:-}"; [ -n "$since" ] || die "sensor-rate <since RFC3339>"
  local o logs pod n; o="$(f c4-rate.txt)"; logs="$(mktemp)"
  {
    echo "# Sensor Pantai calls since $since, collected $(now)"
    for pod in $(kubectl -n "$NS" get pods -l app=aggregator -o jsonpath='{.items[*].metadata.name}'); do
      if timeout 120 kubectl -n "$NS" logs "$pod" --since-time="$since" --tail=-1 > "$logs.pod" 2> /dev/null; then
        cat "$logs.pod" >> "$logs"
        echo "# $pod: $(grep -c '"target":"SENSOR_PANTAI"' "$logs.pod" || true) SENSOR_PANTAI lines"
      else
        echo "# $pod: log could not be read; not counted"
      fi
    done
    rm -f "$logs.pod"
    $PY sensor-rate < "$logs"
  } | tee "$o"
  rm -f "$logs"
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

# Stops the sensor and wipes its history. The aggregator is scaled to 0 first: a poll cycle that is still
# running would otherwise save the old run's cursor after the wipe, and the new run would be read from
# the middle (practice run, 2026-11-22: the cycle began at obs-002195 and skipped every critical).
# The sensor stays at 0 replicas until gameday-start, so nothing is read before T0.
cmd_cleanup_sensor_history() {
  need_confirm cleanup-sensor-history
  local replicas
  replicas="$(kubectl -n "$NS" get deploy aggregator -o jsonpath='{.spec.replicas}')"
  [ "${replicas:-0}" -gt 0 ] || replicas=3
  mark "cleanup: scaling aggregator ($replicas replicas) and sensor-pantai to 0"
  kubectl -n "$NS" scale deploy/aggregator deploy/sensor-pantai --replicas=0
  kubectl -n "$NS" wait --for=delete pod -l app=aggregator --timeout=180s || true
  kubectl -n "$NS" wait --for=delete pod -l app=sensor-pantai --timeout=180s || true
  [ -z "$(kubectl -n "$NS" get pods -l app=aggregator -o name)" ] || die "cleanup: aggregator pods still exist; rerun"
  mark "cleanup: deleting SENSOR_PANTAI rows"
  crdb_sql "DELETE FROM outbox WHERE hazard_id IN (SELECT hazard_id FROM hazard_events WHERE source = 'SENSOR_PANTAI');
    DELETE FROM hazard_events WHERE source = 'SENSOR_PANTAI';
    DELETE FROM unprocessed_messages WHERE source = 'SENSOR_PANTAI';
    DELETE FROM poll_cursor WHERE source = 'SENSOR_PANTAI';
    DELETE FROM source_status WHERE source = 'SENSOR_PANTAI';"
  crdb_sql "SELECT (SELECT count(*) FROM hazard_events WHERE source = 'SENSOR_PANTAI') AS sensor_rows,
    (SELECT count(*) FROM poll_cursor WHERE source = 'SENSOR_PANTAI') AS cursors"
  mark "cleanup: scaling aggregator back to $replicas"
  kubectl -n "$NS" scale deploy/aggregator --replicas="$replicas"
  kubectl -n "$NS" rollout status deploy/aggregator --timeout=300s
  mark "cleanup: deleting notifier pod (empties its emptyDir seen-store)"
  kubectl -n "$NS" delete pod -l app=notifier --wait=true
  kubectl -n "$NS" rollout status deploy/notifier --timeout=120s
  mark "cleanup done (sensor stays at 0 until gameday-start); next: offsets start, clocks T0"
}

# gameday-start [minutes]: S = T0. Starts the sensor at PANTAI_SPEED (default 6, so the practice
# criticals at sim 1800/5400/9000 s land at T0+5, +15 and +25 min), marks T0 and starts the injector.
cmd_gameday_start() {
  need_confirm gameday-start
  local win="${1:-30}" speed="${PANTAI_SPEED:-6}" started c
  kubectl -n "$NS" set env deploy/sensor-pantai -c sensor PANTAI_SPEED="$speed"
  kubectl -n "$NS" scale deploy/sensor-pantai --replicas=1
  kubectl -n "$NS" rollout status deploy/sensor-pantai --timeout=180s
  started="$(kubectl -n "$NS" get pods -l app=sensor-pantai -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}')"
  mark "S = sensor container started at $started (PANTAI_SPEED=$speed)"
  for c in 1800 5400 9000; do
    echo "practice critical (sim t=${c}s) expected near $(date -u -d "$started + $((c / speed)) seconds" +%H:%M:%SZ)"
  done
  mark "T0 game day start (window $win min)"
  if [ "${INJECT:-auto}" = hand ]; then
    mark "injector: a teammate injects by hand"
  else
    cmd_inject_auto "$win"
  fi
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
  local state="${1:-}" mode="${2:-hang}" enabled code
  case "$state" in on) enabled=true ;; off) enabled=false ;; *) die "pvmbg-outage <on|off> [hang|error]" ;; esac
  code="$(curl -sS -o /dev/stderr -w '%{http_code}' --max-time 10 -X POST "http://$(node_host):30082/admin/outage" \
    -H "X-Admin-Key: $(secret PVMBG_ADMIN_KEY)" -H 'Content-Type: application/json' \
    -d "{\"enabled\": $enabled, \"mode\": \"$mode\"}" || true)"
  echo >&2
  if [ "$code" != 200 ]; then
    mark "FAILED pvmbg outage enabled=$enabled mode=$mode (HTTP ${code:-none}); outage state unknown, check the mock"
    return 1
  fi
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
  # One load at a time: K5 run during the K3 run made both results unreadable (practice run, 2026-11-22).
  local running
  running="$(kubectl -n "$NS" get pods -l app=p5-k6 --no-headers 2> /dev/null | awk '$3 == "Running" || $3 == "Pending" {print $1}')"
  [ -z "$running" ] || die "k6: another k6 pod is still running ($running); wait for it to end"
  ! docker ps --format '{{.Image}}' 2> /dev/null | grep -qx "$K6_IMAGE" || die "k6: a local k6 container is still running"
  local o rc=0; o="$(f "k6-$mode-$(date -u +%H%M%S).txt")"
  mark "k6 $mode start (${K6_WHERE:-cluster}${K6_NODE:+ on $K6_NODE}; $*)"
  if [ "${K6_WHERE:-cluster}" = local ]; then
    local host; host="$(node_host)"
    docker run --rm --network host -v "$PWD/scripts/demo/m2:/scripts:ro" \
      -e MODE="$mode" -e API="http://$host:30080" -e AUTH="http://$host:30084" \
      -e MEDIA_SECRET="$(secret MEDIA_SECRET)" -e LAPANGAN_SECRET="$(secret LAPANGAN_SECRET)" \
      -e BNPB_SECRET="$(secret BNPB_SECRET)" \
      -e DURATION="${DURATION:-5m}" -e MEDIA_VUS="${MEDIA_VUS:-400}" -e FIELD_VUS="${FIELD_VUS:-50}" -e READ_VUS="${READ_VUS:-20}" \
      "$K6_IMAGE" run "$@" /scripts/p5-k6.js 2>&1 | tee "$o" || rc=$?
  else
    k6_in_cluster "$mode" "$o" "$@" || rc=$?
  fi
  mark "k6 $mode end (exit $rc) -> $o"
  return "$rc"
}

# duration_s: DURATION (default 5m; s, m or h) in seconds.
duration_s() {
  local d="${DURATION:-5m}"
  case "$d" in
    *[0-9]s) [[ "${d%s}" =~ ^[0-9]+$ ]] && echo "${d%s}" ;;
    *[0-9]m) [[ "${d%m}" =~ ^[0-9]+$ ]] && echo $(( ${d%m} * 60 )) ;;
    *[0-9]h) [[ "${d%h}" =~ ^[0-9]+$ ]] && echo $(( ${d%h} * 3600 )) ;;
    *) return 1 ;;
  esac
}

# k6_in_cluster <mode> <out file> [k6 args]: runs k6 as a pod and calls the client-api and auth Services
# directly, so the laptop's (often relayed) tailnet link to the cluster is not part of the latency.
# K6_NODE pins the pod to one node; by default the scheduler picks a node with room.
k6_in_cluster() {
  local mode="$1" o="$2" pod phase rc args
  shift 2
  duration_s > /dev/null || die "k6: DURATION must look like 90s, 5m or 1h (got ${DURATION:-5m})"
  pod="p5-k6-$mode-$(date -u +%H%M%S)"
  args="$(printf '%s\n' run "$@" /scripts/p5-k6.js | jq -R . | jq -cs .)"
  kubectl -n "$NS" create configmap p5-k6-script --from-file=p5-k6.js=scripts/demo/m2/p5-k6.js \
    --dry-run=client -o yaml | kubectl apply -f - > /dev/null
  kubectl -n "$NS" apply -f - > /dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: {app: p5-k6}
spec:
  restartPolicy: Never
  ${K6_NODE:+nodeSelector: {kubernetes.io/hostname: $K6_NODE\}}
  containers:
    - name: k6
      image: $K6_IMAGE
      args: $args
      env:
        - {name: MODE, value: "$mode"}
        - {name: API, value: "http://client-api:8080"}
        - {name: AUTH, value: "http://auth:8080"}
        - {name: DURATION, value: "${DURATION:-5m}"}
        - {name: MEDIA_VUS, value: "${MEDIA_VUS:-400}"}
        - {name: FIELD_VUS, value: "${FIELD_VUS:-50}"}
        - {name: READ_VUS, value: "${READ_VUS:-20}"}
        - {name: MEDIA_SECRET, valueFrom: {secretKeyRef: {name: bnpb-secrets, key: MEDIA_SECRET}}}
        - {name: LAPANGAN_SECRET, valueFrom: {secretKeyRef: {name: bnpb-secrets, key: LAPANGAN_SECRET}}}
        - {name: BNPB_SECRET, valueFrom: {secretKeyRef: {name: bnpb-secrets, key: BNPB_SECRET}}}
      resources:
        requests: {cpu: 500m, memory: 256Mi}
      volumeMounts:
        - {name: script, mountPath: /scripts, readOnly: true}
  volumes:
    - name: script
      configMap: {name: p5-k6-script}
YAML
  echo "k6 pod $pod: waiting for it to start"
  for _ in $(seq 1 90); do
    phase="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}' 2> /dev/null || true)"
    [ -n "$phase" ] && [ "$phase" != Pending ] && break
    sleep 2
  done
  if [ -z "$phase" ] || [ "$phase" = Pending ]; then
    { echo "k6 pod $pod did not start in 3 min (phase ${phase:-missing})"; kubectl -n "$NS" describe pod "$pod" 2>&1 | tail -15; } | tee "$o"
    kubectl -n "$NS" delete pod "$pod" --wait=false > /dev/null 2>&1 || true
    return 1
  fi
  echo "k6 pod $pod on $(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName}'), phase $phase"
  # Live view only: a kubelet tunnel drop can end the follow early, so the file is written from the
  # full log once the pod has finished.
  kubectl -n "$NS" logs -f "$pod" 2>&1 || true
  # The run lasts DURATION plus setup and teardown; give up 5 min after that.
  local deadline
  deadline=$(( $(date -u +%s) + $(duration_s) + 300 ))
  until phase="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}' 2> /dev/null)" \
      && { [ "$phase" = Succeeded ] || [ "$phase" = Failed ]; }; do
    if [ "$(date -u +%s)" -ge "$deadline" ]; then
      echo "k6 pod $pod has not ended (phase ${phase:-unknown}); stopping it" >&2
      kubectl -n "$NS" logs "$pod" > "$o" 2>&1 || echo "could not read the k6 log of $pod" >> "$o"
      kubectl -n "$NS" delete pod "$pod" --wait=false > /dev/null 2>&1 || true
      return 1
    fi
    sleep 5
  done
  kubectl -n "$NS" logs "$pod" > "$o" 2>&1 || echo "could not read the k6 log of $pod" >> "$o"
  rc="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}')"
  kubectl -n "$NS" delete pod "$pod" --wait=false > /dev/null
  echo "k6 output -> $o"
  return "${rc:-1}"
}

# ---- automatic injector (stands in for the teammate injector of C12) ----
VBOX="${VBOX:-/mnt/c/Program Files/Oracle/VirtualBox/VBoxManage.exe}"
KILL_VM="${KILL_VM:-node-e}"

# Writes a random plan, "<seconds after T0> <action>" per line. At least one fault is active from
# T0+3 to T0+10 min (C12); every fault is cleared before T1, except the VM, which starts after T1.
inject_plan() {
  local win=$(( $1 * 60 )) menu anchor others name t d
  menu="pvmbg sensor partition kill-leader"
  [ -x "$VBOX" ] && menu="$menu vm"
  # Anchor: a fault that lasts, starting T0+1..3 min and running past T0+10.
  anchor="$(printf '%s\n' pvmbg sensor $([ -x "$VBOX" ] && echo vm) | shuf -n 1)"
  t=$(shuf -i 60-180 -n 1)
  case "$anchor" in
    vm) echo "$t vm-off"; echo "$((win + 60)) vm-on" ;;
    *) echo "$t $anchor-on"; echo "$(( 600 + $(shuf -i 0-180 -n 1) )) $anchor-off" ;;
  esac
  # One or two more faults later in the window, cleared at least 1 min before T1.
  others="$(printf '%s\n' $menu | grep -vx "$anchor" | shuf -n "$(shuf -i 1-2 -n 1)")"
  for name in $others; do
    t=$(shuf -i 240-$(( win - 360 )) -n 1)
    case "$name" in
      partition|kill-leader) echo "$t $name" ;;
      vm) echo "$t vm-off"; echo "$((win + 60)) vm-on" ;;
      *) d=$(shuf -i 120-300 -n 1); echo "$t $name-on"; echo "$(( t + d )) $name-off" ;;
    esac
  done
}

inject_action() {
  local agent
  case "$1" in
    pvmbg-on) cmd_pvmbg_outage on hang ;;
    pvmbg-off) cmd_pvmbg_outage off ;;
    sensor-on) cmd_fault_sensor on ;;
    sensor-off) cmd_fault_sensor off ;;
    partition|kill-leader)
      # A tester-agent pod on node-a, so a dead worker node cannot take the action with it.
      agent="$(kubectl -n tester-agent get pods --field-selector spec.nodeName=node-a -o name 2> /dev/null | head -1 || true)"
      if [ -z "$agent" ]; then mark "FAILED tester-agent $1: no tester-agent pod on node-a"; return 1; fi
      mark "FAULT tester-agent $1 (via $agent)"
      kubectl -n tester-agent exec "${agent#pod/}" -- /app run practice "$1" 2>&1 | sed 's/^/  [agent] /' & ;;
    vm-off) mark "FAULT power off VM $KILL_VM"; "$VBOX" controlvm "$KILL_VM" poweroff ;;
    vm-on) mark "CLEAR start VM $KILL_VM (after T1)"; "$VBOX" startvm "$KILL_VM" --type headless ;;
    *) mark "injector: unknown action $1" ;;
  esac
}

# inject-run <plan> <t0 epoch>: the background worker. INJECT_DRY_RUN=1 only logs the actions;
# INJECT_SPEED=<n> runs the plan n times faster (testing).
cmd_inject_run() {
  local plan="$1" t0="$2" off act at speed="${INJECT_SPEED:-1}"
  while read -r off act; do
    at=$(( t0 + off / speed ))
    while [ "$(date -u +%s)" -lt "$at" ]; do sleep 1; done
    if [ "${INJECT_DRY_RUN:-}" = 1 ]; then mark "DRY-RUN T0+${off}s $act"; else inject_action "$act" || mark "injector: $act failed"; fi
  done < <(sort -n "$plan")
  wait
  mark "injector done; plan revealed below"
  cat "$plan"
}

# inject-auto [window minutes]: run at T0. Draws a hidden random plan, records only its sha256 in the
# timeline, and runs it in the background. The plan and log are revealed at the end of the window.
cmd_inject_auto() {
  need_confirm inject-auto
  local win="${1:-30}" plan log t0 sum
  # The anchor fault can end at T0+13 min and later faults need room before T1.
  case "$win" in ''|*[!0-9]*) die "inject-auto: window must be whole minutes" ;; esac
  [ "$win" -ge 20 ] || die "inject-auto: window must be at least 20 min"
  plan="$(f inject-plan.txt)"; log="$(f injector.log)"
  [ ! -s "$plan" ] || [ "${INJECT_DRY_RUN:-}" = 1 ] || die "inject-auto: $plan exists (one game day per prefix)"
  t0="$(date -u +%s)"
  ( umask 077; inject_plan "$win" > "$plan" )
  sum="$(sha256sum "$plan" | cut -c1-16)"
  mark "injector started: T0=$(date -u -d "@$t0" +%Y-%m-%dT%H:%M:%SZ), window ${win} min, plan sha256 $sum (hidden until the end)"
  setsid nohup "$PWD/scripts/demo/m2/p5.sh" inject-run "$plan" "$t0" > "$log" 2>&1 < /dev/null &
  echo "injector running in the background (pid $!). Do not read $plan or $log before T1."
}

cmd="${1:-}"; shift || true
case "$cmd" in
  inject-auto) cmd_inject_auto "$@" ;;
  inject-run) cmd_inject_run "$@" ;;
  preflight) cmd_preflight ;;
  mark) mark "$*" ;;
  offsets) cmd_offsets "$@" ;;
  clocks) cmd_clocks "$@" ;;
  sample) cmd_sample "$@" ;;
  sensor-rate) cmd_sensor_rate "$@" ;;
  collect) cmd_collect "$@" ;;
  tester-logs) cmd_tester_logs ;;
  cleanup-sensor-history) cmd_cleanup_sensor_history ;;
  gameday-start) cmd_gameday_start "$@" ;;
  fault-sensor) cmd_fault_sensor "$@" ;;
  pvmbg-outage) cmd_pvmbg_outage "$@" ;;
  config) cmd_config "$@" ;;
  config-restore) cmd_config_restore ;;
  k6) cmd_k6 "$@" ;;
  *) sed -n '2,32p' "$0"; exit 2 ;;
esac
