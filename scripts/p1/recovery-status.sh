#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

WAIT=false
[ "${1:-}" = "--wait" ] && WAIT=true

status() {
  crdb_sql "
SELECT g.node_id AS crdb_node,
       substring(g.locality FROM 'node=(.*)') AS k8s_node,
       g.address,
       l.membership,
       g.is_live
FROM crdb_internal.gossip_nodes AS g
JOIN crdb_internal.gossip_liveness AS l USING (node_id)
ORDER BY g.node_id"
  crdb_sql "
SELECT sum((metrics->>'ranges.underreplicated')::FLOAT)::INT AS underreplicated_ranges,
       sum((metrics->>'ranges.unavailable')::FLOAT)::INT AS unavailable_ranges
FROM crdb_internal.kv_store_status"
}

if [ "$WAIT" = false ]; then
  status
  exit 0
fi

start=$(date +%s)
until [ "$(crdb_sql_tsv "SELECT sum((metrics->>'ranges.underreplicated')::FLOAT)::INT + sum((metrics->>'ranges.unavailable')::FLOAT)::INT FROM crdb_internal.kv_store_status" 2>/dev/null || echo x)" = "0" ]; do
  echo "$(date +%H:%M:%S) still recovering"
  sleep 10
done
echo "fully replicated again after $(( $(date +%s) - start ))s"
status
