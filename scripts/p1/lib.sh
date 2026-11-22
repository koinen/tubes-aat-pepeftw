#!/usr/bin/env bash
export MSYS_NO_PATHCONV=1
NS="${NS:-sistem-informasi-kebencanaan-bnpb}"
DB_NAME="${DB_NAME:-aggregator}"

# ready_crdb_pod [skip]: the first Ready crdb pod, other than <skip>.
ready_crdb_pod() {
  kubectl -n "$NS" get pods -l app=crdb \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].ready}{"\n"}{end}' \
    | awk -v skip="${1:-}" '$2 == "true" && $1 != skip {print $1; exit}'
}

CRDB_POD="${CRDB_POD:-$(ready_crdb_pod)}"

crdb_cli() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach "$@" --insecure --host=crdb-public:26257
}

# One try by default. The M2 demo scripts set CRDB_RETRIES higher: on the laptop cluster a crdb pod
# restart or a short quorum loss would otherwise end a run on the first failed query.
CRDB_RETRIES="${CRDB_RETRIES:-1}"
CRDB_TIMEOUT="${CRDB_TIMEOUT:-60}"

# A failure worth retrying: a hung try (killed by timeout), or a connection, quorum or kubelet-tunnel error.
CRDB_TRANSIENT='cannot dial|connection refused|connection reset|unavailable|deadline|timeout|timed out|EOF|error dialing backend|proxy error|unable to upgrade|container not found|ambiguous|restart transaction|no such host'

# crdb_run <format> <sql>: with one try, runs through $CRDB_POD. With retries, every call picks a pod
# that is Ready now, and each retry of a transient failure waits 10 s and picks a Ready pod other than
# the one that failed. Any other error (bad SQL, missing table) fails at once. A try that hangs is
# killed after $CRDB_TIMEOUT s and retried, so give retrying callers idempotent statements only.
crdb_run() {
  local try pod="$CRDB_POD" out err rc
  if [ "${CRDB_RETRIES:-1}" -gt 1 ]; then
    pod="$(ready_crdb_pod || true)"
    pod="${pod:-$CRDB_POD}"
  fi
  err="$(mktemp)"
  for try in $(seq 1 "${CRDB_RETRIES:-1}"); do
    rc=0
    out="$(timeout "$CRDB_TIMEOUT" kubectl -n "$NS" exec "$pod" -- \
        /cockroach/cockroach sql --insecure -d "$DB_NAME" --format="$1" -e "$2" 2> "$err")" || rc=$?
    if [ "$rc" = 0 ]; then
      [ ! -s "$err" ] || cat "$err" >&2
      rm -f "$err"
      printf '%s\n' "$out"
      return 0
    fi
    [ "$rc" = 124 ] || grep -qiE "$CRDB_TRANSIENT" "$err" || break
    if [ "$try" -lt "${CRDB_RETRIES:-1}" ]; then
      echo "  (crdb via $pod failed, try $try/$CRDB_RETRIES: $(tail -1 "$err"); retrying in 10 s)" >&2
      sleep 10
      pod="$(ready_crdb_pod "$pod" || true)"
      pod="${pod:-$CRDB_POD}"
    fi
  done
  cat "$err" >&2
  rm -f "$err"
  return 1
}

crdb_sql() { crdb_run table "$1"; }

crdb_sql_tsv() { crdb_run tsv "$1" | tail -n +2; }

# wait_crdb_stable [checks]: blocks until every crdb pod is Ready and no range is unavailable,
# <checks> times in a row 10 s apart (default 3). Gives up after 10 min.
wait_crdb_stable() {
  local need="${1:-3}" ok=0 total ready unavailable
  echo "waiting for CockroachDB: all pods Ready and no unavailable ranges, $need checks in a row"
  for _ in $(seq 1 60); do
    total="$(kubectl -n "$NS" get pods -l app=crdb --no-headers 2> /dev/null | wc -l)"
    ready="$(kubectl -n "$NS" get pods -l app=crdb \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2> /dev/null | grep -c true || true)"
    unavailable="$(CRDB_POD="$(ready_crdb_pod || true)" CRDB_RETRIES=1 CRDB_TIMEOUT=20 crdb_sql_tsv "SELECT COALESCE(sum((metrics->>'ranges.unavailable')::FLOAT)::INT, -1) FROM crdb_internal.kv_store_status" 2> /dev/null || echo x)"
    if [ "$total" -gt 0 ] && [ "$ready" = "$total" ] && [ "$unavailable" = 0 ]; then
      ok=$((ok + 1))
    else
      ok=0
    fi
    echo "  $(date -u +%T) crdb Ready $ready/$total, unavailable ranges ${unavailable:-?} (-1: metric missing), good checks $ok/$need"
    [ "$ok" -ge "$need" ] && return 0
    sleep 10
  done
  echo "CockroachDB did not settle in 10 min. Check kubectl get pods -l app=crdb and the node links." >&2
  return 1
}
