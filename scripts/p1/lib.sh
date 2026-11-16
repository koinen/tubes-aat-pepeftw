#!/usr/bin/env bash
export MSYS_NO_PATHCONV=1
NS="${NS:-sistem-informasi-kebencanaan-bnpb}"
DB_NAME="${DB_NAME:-aggregator}"

ready_crdb_pod() {
  kubectl -n "$NS" get pods -l app=crdb \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].ready}{"\n"}{end}' \
    | awk '$2 == "true" {print $1; exit}'
}

CRDB_POD="${CRDB_POD:-$(ready_crdb_pod)}"

crdb_cli() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach "$@" --insecure --host=crdb-public:26257
}

crdb_sql() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach sql --insecure -d "$DB_NAME" --format=table -e "$1"
}

crdb_sql_tsv() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach sql --insecure -d "$DB_NAME" --format=tsv -e "$1" | tail -n +2
}
