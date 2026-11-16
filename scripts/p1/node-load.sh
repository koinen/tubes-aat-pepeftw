#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

SAMPLES="${1:-15}"
INTERVAL="${2:-2}"

query="
SELECT substring(g.locality FROM 'node=(.*)'),
       round((s.metrics->>'rebalancing.writespersecond')::FLOAT),
       (SELECT count(*) FROM [SHOW RANGES FROM TABLE hazard_events WITH DETAILS] AS r WHERE r.lease_holder = s.node_id)
FROM crdb_internal.kv_store_status AS s
JOIN crdb_internal.gossip_nodes AS g ON g.node_id = s.node_id
ORDER BY 1"

printf '%-8s %-26s %14s %22s\n' "sample" "k8s_node" "writes_per_sec" "hazard_leaseholders"
for i in $(seq 1 "$SAMPLES"); do
  if rows="$(crdb_sql_tsv "$query" 2>/dev/null)" && [ -n "$rows" ]
  then
    while IFS=$'\t' read -r node wps lh; do
      printf '%-8s %-26s %14s %22s\n' "$i" "$node" "$wps" "$lh"
    done <<< "$rows"
  else
    printf '%-8s %s\n' "$i" "sample skipped, store too busy to answer"
  fi
  sleep "$INTERVAL"
done
