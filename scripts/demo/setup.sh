#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."

[ -f .env ] || cp .env.example .env
sh scripts/gen-auth-keys.sh

if [ "${1:-}" = "--fresh" ]; then
	docker compose down -v
fi

docker compose up -d --build
hurl --test --variables-file .env scripts/demo/00-health.hurl
