#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."

echo "Before the schema change:"
docker compose ps aggregator pvmbg-mock --format "table {{.Service}}\t{{.Status}}"

hurl --test --variables-file .env --report-html evidence/p1 scripts/demo/p1-schema.hurl

echo "After the schema change (uptime keeps growing, nothing restarted):"
docker compose ps aggregator pvmbg-mock --format "table {{.Service}}\t{{.Status}}"
