SECRET_KEYS="DB_PASSWORD INTERNAL_API_KEY BMKG_API_KEY PVMBG_TOKEN_SECRET PVMBG_TOKEN PVMBG_ADMIN_KEY REDIS_PASSWORD MEDIA_SECRET LAPANGAN_SECRET BNPB_SECRET"

load_env() {
  if [ ! -f .env ]
  then
    cp .env.example .env
    echo ".env created from .env.example"
  fi
  set -a
  . <(tr -d '\r' < .env)
  set +a
}

apply_secrets() {
  local args=()
  local key
  for key in $SECRET_KEYS
  do
    if [ -z "${!key:-}" ]
    then
      echo "missing secret $key in .env" >&2
      return 1
    fi
    args+=("--from-literal=${key}=${!key}")
  done

  kubectl create secret generic bnpb-secrets -n "$NAMESPACE" "${args[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -

  sh scripts/gen-auth-keys.sh
  kubectl create secret generic auth-keys -n "$NAMESPACE" \
    --from-file=auth_priv.pem=secrets/auth_priv.pem \
    --from-file=auth_pub.pem=secrets/auth_pub.pem \
    --dry-run=client -o yaml | kubectl apply -f -
}
