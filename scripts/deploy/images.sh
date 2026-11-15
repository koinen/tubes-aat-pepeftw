SERVICES="aggregator auth client-api notifier audit-log dashboard-updater bmkg-mock pvmbg-mock"

image_tag() {
  local tag
  tag="$(git rev-parse --short HEAD)"
  if [ -n "$(git status --porcelain)" ]
  then
    tag="${tag}-$(date +%Y%m%d%H%M%S)"
  fi
  echo "$tag"
}

image_name() {
  echo "${REGISTRY}${1}:${TAG}"
}

build_images() {
  local svc
  for svc in "$@"
  do
    echo "building $(image_name "$svc")"
    docker build -q -f "cmd/${svc}/Dockerfile" -t "$(image_name "$svc")" . > /dev/null
  done
}

publish_images() {
  local svc
  for svc in "$@"
  do
    if [ -n "$KIND_CLUSTER" ]
    then
      kind load docker-image --name "$KIND_CLUSTER" "$(image_name "$svc")" > /dev/null
    else
      docker push "$(image_name "$svc")" > /dev/null
    fi
    echo "published $(image_name "$svc")"
  done
}
