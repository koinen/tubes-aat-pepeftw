#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/../.."

hurl --test --variables-file .env --report-html evidence/p3 scripts/demo/p3-auth.hurl

echo "3.6 Secrets in git history (.env or key files ever committed):"
leaked=$(git log --all --name-only --format= | grep -E '(^|/)\.env$|\.pem$' | sort -u || true)
if [ -z "$leaked" ]; then
	echo "PASS: none found"
else
	echo "FAIL: $leaked"
	exit 1
fi
