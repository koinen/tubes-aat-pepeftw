#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

ENDPOINT="${1:?usage: audit.sh <events|checksum|location|unprocessed> [source or hazard_id]}"
ARG="${2:-}"
BASE="${AGGREGATOR_URL:-http://localhost:8085}"
TOKEN="${AUDIT_TOKEN:-$(grep '^AUDIT_TOKEN=' .env | cut -d= -f2- | tr -d '\r')}"

case "$ENDPOINT" in
  checksum) QUERY="source=${ARG:-BMKG}" ;;
  location) QUERY="hazard_id=${ARG:?hazard_id required}" ;;
  events|unprocessed) QUERY="limit=5${ARG:+&source=$ARG}" ;;
  *) echo "unknown endpoint $ENDPOINT" >&2
    exit 1 ;;
esac

curl -s -w '\nstatus %{http_code} in %{time_total}s\n' -H "Authorization: Bearer ${TOKEN}" "${BASE}/internal/audit/${ENDPOINT}?${QUERY}"
