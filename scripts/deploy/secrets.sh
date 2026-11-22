SECRET_KEYS="DB_PASSWORD INTERNAL_API_KEY BMKG_API_KEY PVMBG_TOKEN_SECRET PVMBG_TOKEN SATELLITE_TOKEN PVMBG_ADMIN_KEY REDIS_PASSWORD MEDIA_SECRET LAPANGAN_SECRET BNPB_SECRET"
OPTIONAL_SECRET_KEYS="AUDIT_TOKEN OPS_TOKEN"

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
  for key in $OPTIONAL_SECRET_KEYS
  do
    if [ -n "${!key:-}" ]
    then
      args+=("--from-literal=${key}=${!key}")
    fi
  done

  kubectl create secret generic bnpb-secrets -n "$NAMESPACE" "${args[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -

  sh scripts/gen-auth-keys.sh
  kubectl create secret generic auth-keys -n "$NAMESPACE" \
    --from-file=auth_priv.pem=secrets/auth_priv.pem \
    --from-file=auth_pub.pem=secrets/auth_pub.pem \
    --dry-run=client -o yaml | kubectl apply -f -

  # Name and key are fixed by the assistant's Sensor Pantai manifest. We choose the value.
  kubectl create secret generic pantai-token -n "$NAMESPACE" \
    --from-literal=token="${PANTAI_AUTH_TOKEN:-pantai-dev-token}" \
    --dry-run=client -o yaml | kubectl apply -f -

  # The key file the assistant hands out opens the exam profile (overlay vm-exam).
  if [ -f secrets/pantai.key ]
  then
    kubectl create secret generic pantai-key -n "$NAMESPACE" \
      --from-file=key=secrets/pantai.key \
      --dry-run=client -o yaml | kubectl apply -f -
  elif [[ "${OVERLAY:-}" == *exam ]]
  then
    echo "overlay $OVERLAY needs secrets/pantai.key from the assistant" >&2
    return 1
  fi

  if [ -n "${GHCR_USER:-}" ] && [ -n "${GHCR_TOKEN:-}" ]
  then
    kubectl create secret docker-registry ghcr-pull -n "$NAMESPACE" \
      --docker-server=ghcr.io --docker-username="$GHCR_USER" --docker-password="$GHCR_TOKEN" \
      --dry-run=client -o yaml | kubectl apply -f -
  fi
}
