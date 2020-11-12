#!/usr/bin/env bash

set -euo pipefail

usage() {
cat << EOF
  usage: $0 options
  OPTIONS:
    -p YAML files relative path in /opt/airflow
EOF
exit 1;
}

while getopts "p:" OPTION; do
  case ${OPTION} in
    p) YAML_FILES_RELATIVE_PATH=${OPTARG};;
    *) usage;;
  esac
done

if [[ -z "${YAML_FILES_RELATIVE_PATH:-}" ]]; then
  echo "YAML files relative path parameter missing"
  usage
fi

echo 'python -m yamllint -c ${AIRFLOW_HOME}/.yamllint.yml ${AIRFLOW_HOME}/${YAML_FILES_RELATIVE_PATH}' | docker-compose run --rm  --no-deps \
  -e AIRFLOW__CORE__SQL_ALCHEMY_CONN="" \
  -e AIRFLOW_BUSINESS_UNIT="log" \
  webserver ""
