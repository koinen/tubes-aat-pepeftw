#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1
mkdir -p evidence

echo "Load-test settings: rate limit raised, PVMBG delay fixed at 3 s"
RATE_LIMIT_MAX=1000000 docker compose up -d client-api
PVMBG_DELAY_MIN=3000ms PVMBG_DELAY_MAX=3000ms docker compose up -d pvmbg-mock
hurl --test --variables-file .env scripts/demo/00-health.hurl

status=0
echo "2.1 BMKG-only latency while PVMBG is slow"
docker compose --profile loadtest run --rm k6 run --summary-export=/evidence/p2-isolation.json /scripts/k6-isolation.js || status=1

echo "2.2 Sustained load: 50 users for 60 s"
docker compose --profile loadtest run --rm k6 run --summary-export=/evidence/p2-load.json /scripts/k6-load.js || status=1

echo "Restoring the normal settings from .env"
docker compose up -d client-api pvmbg-mock

exit $status
