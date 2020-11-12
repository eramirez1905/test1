#!/usr/bin/env bash

set -euo pipefail

BASE_DIR=$(cd "$(dirname "$0")" && pwd)

usage() {
cat << EOF
  usage: $0 options
  OPTIONS:
    -e Environment (production-dwh|staging)
EOF
exit 1;
}

while getopts "e:" OPTION; do
  case ${OPTION} in
    e) ENVIRONMENT=${OPTARG};;
    *) usage;;
  esac
done

if [[ -z "${ENVIRONMENT:-}" ]]; then
  echo "$0 parameter missing"
  usage
fi

KUBECONFIG="$HOME/.kube_tool/${ENVIRONMENT}"
KUBECTL="kubectl --kubeconfig=${KUBECONFIG}"

${KUBECTL} apply -f "$BASE_DIR/kubernetes/job.yaml"

sleep 1

pod=$(${KUBECTL} get pods --selector=job-name=debian --output=jsonpath='{.items[0].metadata.name}')
echo "To connect to the pod, please run:"
echo "${KUBECTL} exec -ti $pod bash"
echo
echo "To delete the pod, please run:"
echo "${KUBECTL} delete job debian"
