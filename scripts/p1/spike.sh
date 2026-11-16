#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

COUNT="${1:-5000}"
PLACE="${2:-Selat Sunda}"
SEED="${SEED:-$(date +%s)}"
FILE="$(mktemp)"
trap 'rm -f "$FILE"' EXIT

go run ./tools/catalog-gen -spike "$COUNT" -spike-place "$PLACE" -seed "$SEED" > "$FILE"
echo "spike of ${COUNT} events in ${PLACE}, sampling node load while loading"

bash scripts/p1/node-load.sh 20 2 &
sampler=$!
bash scripts/p1/load-catalog.sh "$FILE"
wait "$sampler"
