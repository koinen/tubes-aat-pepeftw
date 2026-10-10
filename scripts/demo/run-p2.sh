#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."

echo "Before the outage:"
docker compose ps aggregator client-api pvmbg-mock --format "table {{.Service}}\t{{.Status}}"

hurl --test --variables-file .env --report-html evidence/p2 scripts/demo/p2-outage.hurl scripts/demo/p2-ratelimit.hurl

echo "After recovery (uptime keeps growing, nothing restarted):"
docker compose ps aggregator client-api pvmbg-mock --format "table {{.Service}}\t{{.Status}}"
