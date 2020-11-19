#!/usr/bin/env bash

set -euo pipefail

usage() {
cat << EOF
  usage: $0 options
  OPTIONS:
    -b Business Unit
EOF
exit 1;
}

while getopts "b:" OPTION; do
  case ${OPTION} in
    b) AIRFLOW_BUSINESS_UNIT=${OPTARG};;
    *) usage;;
  esac
done

if [[ -z "${AIRFLOW_BUSINESS_UNIT:-}" ]]; then
  echo "Business Unit parameter missing"
  usage
fi

if [[ "${AIRFLOW_BUSINESS_UNIT}" == "datahub" ]]; then
  AIRFLOW_BUSINESS_UNIT="_skeleton"
  export AIRFLOW_BUSINESS_UNIT
fi

docker-compose up -d mysql
docker-compose run -e AIRFLOW_BUSINESS_UNIT --rm webserver initdb

if [[ "${AIRFLOW_BUSINESS_UNIT}" == "datahub" ]]; then
  echo 'PYTHONPATH="${AIRFLOW_HOME}/dags/$AIRFLOW_BUSINESS_UNIT" python -m unittest discover -v -t ${AIRFLOW_HOME}/src -s ${AIRFLOW_HOME}/src/datahub/tests' | docker-compose run --rm \
  -e AIRFLOW_BUSINESS_UNIT webserver ""
else
  echo 'python -m unittest discover -v -t ${AIRFLOW_HOME}/dags/$AIRFLOW_BUSINESS_UNIT -s ${AIRFLOW_HOME}/dags/$AIRFLOW_BUSINESS_UNIT/tests' | docker-compose run --rm \
    -e AIRFLOW_BUSINESS_UNIT \
    -e AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT="fake" \
    webserver ""
fi
