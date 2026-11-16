#!/usr/bin/env bash
export MSYS_NO_PATHCONV=1
NS="${NS:-sistem-informasi-kebencanaan-bnpb}"
DB_NAME="${DB_NAME:-aggregator}"
CRDB_POD="${CRDB_POD:-crdb-0}"

crdb_sql() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach sql --insecure -d "$DB_NAME" --format=table -e "$1"
}

crdb_sql_tsv() {
  kubectl -n "$NS" exec -i "$CRDB_POD" -- /cockroach/cockroach sql --insecure -d "$DB_NAME" --format=tsv -e "$1" | tail -n +2
}
