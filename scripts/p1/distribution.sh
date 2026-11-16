#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

TABLE="${1:-hazard_events}"

echo "== rows in ${TABLE}"
crdb_sql "SELECT count(*) AS rows FROM ${TABLE}"

echo "== ranges, replicas and leaseholders of ${TABLE} per node"
crdb_sql "
WITH r AS (
  SELECT range_id, lease_holder, replicas, range_size
  FROM [SHOW RANGES FROM TABLE ${TABLE} WITH DETAILS]
), t AS (
  SELECT count(*) AS ranges, sum(range_size) AS bytes FROM r
)
SELECT substring(g.locality FROM 'node=(.*)') AS k8s_node,
       g.node_id AS crdb_node,
       count(r.range_id) FILTER (WHERE g.node_id = ANY (r.replicas)) AS replicas,
       count(r.range_id) FILTER (WHERE r.lease_holder = g.node_id) AS leaseholders,
       (SELECT ranges FROM t) AS total_ranges,
       round(100.0 * COALESCE(sum(r.range_size) FILTER (WHERE g.node_id = ANY (r.replicas)), 0) / NULLIF((SELECT bytes FROM t), 0), 1) AS pct_of_table_bytes
FROM crdb_internal.gossip_nodes AS g
CROSS JOIN r
GROUP BY g.node_id, g.locality
ORDER BY k8s_node"

echo "== store usage per node"
crdb_sql "
SELECT substring(g.locality FROM 'node=(.*)') AS k8s_node,
       s.node_id AS crdb_node,
       round((s.metrics->>'livebytes')::FLOAT / 1048576, 1) AS live_mib,
       (s.metrics->>'replicas')::FLOAT::INT AS replicas_all_tables,
       (s.metrics->>'replicas.leaseholders')::FLOAT::INT AS leaseholders_all_tables
FROM crdb_internal.kv_store_status AS s
JOIN crdb_internal.gossip_nodes AS g ON g.node_id = s.node_id
ORDER BY k8s_node"
