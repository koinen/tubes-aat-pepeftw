#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

FILE="${1:?usage: load-catalog.sh <catalog.csv> [workers]}"
WORKERS="${2:-16}"

kubectl -n "$NS" exec -i deploy/aggregator -- ./aggregator load-catalog --workers "$WORKERS" < "$FILE" \
  | grep -E '"msg":"(catalog_progress|catalog_done|catalog_row_failed|load_catalog_failed)"'
