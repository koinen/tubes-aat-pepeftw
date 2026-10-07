#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."
mkdir -p secrets

if [ -f secrets/auth_priv.pem ] && [ -f secrets/auth_pub.pem ]; then
	echo "auth keys already exist in secrets/, keeping them"
	exit 0
fi

command -v openssl >/dev/null 2>&1 || { echo "openssl is required (Git Bash, macOS and Linux include it)" >&2; exit 1; }

openssl genpkey -algorithm ed25519 -out secrets/auth_priv.pem
openssl pkey -in secrets/auth_priv.pem -pubout -out secrets/auth_pub.pem
chmod 600 secrets/auth_priv.pem
echo "auth keys created in secrets/"
