#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/lib.sh"

dead_ids="$(crdb_sql_tsv "SELECT node_id FROM crdb_internal.gossip_nodes AS g JOIN crdb_internal.gossip_liveness AS l USING (node_id) WHERE NOT g.is_live AND l.membership = 'active'" | tr '\n' ' ')"
if [ -z "${dead_ids// /}" ]; then
  echo "no dead cockroach nodes to decommission"
  exit 0
fi
echo "decommissioning dead cockroach nodes: $dead_ids"
crdb_cli node decommission $dead_ids --wait=none
