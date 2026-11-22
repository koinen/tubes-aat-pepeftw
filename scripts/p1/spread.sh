#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

PREFIX="${1:?usage: spread.sh <source_ref_id prefix, for example catalog-spike>}"

ranges=""
for d in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do
  [ -n "$ranges" ] && ranges="${ranges} UNION ALL "
  ranges="${ranges}SELECT '${d}' AS digit, range_id, lease_holder_locality, replicas FROM [SHOW RANGE FROM TABLE hazard_events FOR ROW ('${d}0000000-0000-0000-0000-000000000001'::UUID)]"
done

echo "== rows with source_ref_id prefix ${PREFIX} per leaseholder node"
crdb_sql "
WITH r AS (${ranges}),
h AS (
  SELECT substring(hazard_id::STRING, 1, 1) AS digit, count(*) AS rows
  FROM hazard_events WHERE source_ref_id LIKE '${PREFIX}%' GROUP BY 1
)
SELECT substring(r.lease_holder_locality FROM 'node=(.*)') AS leaseholder_node,
       count(DISTINCT r.range_id) AS ranges_hit,
       sum(h.rows) AS rows_written,
       round(100.0 * sum(h.rows) / (SELECT sum(rows) FROM h), 1) AS pct_of_writes
FROM h JOIN r USING (digit)
GROUP BY 1 ORDER BY 1"
