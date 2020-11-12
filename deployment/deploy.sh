#!/usr/bin/env bash

set -euo pipefail

BRANCH=$1

if [[ -z ${HUBOT_API_KEY_PRODUCTION} ]]; then
  echo "HUBOT_API_KEY_PRODUCTION environment variable is missing."
  exit 1
fi

if [[ -z ${BRANCH} ]]; then
  echo "BRANCH parameter is missing."
  exit 1
fi

curl https://bot-eu.datahub.deliveryhero.com/hubot/deploy \
  --fail \
  -X POST -H "Content-Type: application/json" \
  -H "x-api-key: $HUBOT_API_KEY_PRODUCTION" \
  -d "
{
  \"user\": {
    \"id\": \"logot\",
    \"name\": \"logot\",
    \"real_name\": \"logot\",
    \"email_address\": \"tech.logisticsdata@deliveryhero.com\"
  },
  \"message\": {
    \"id\": null,
    \"room\": null
  },
  \"branch\": \"$BRANCH\",
  \"application\": \"airflow\",
  \"environment\": \"production-dwh\",
  \"region\": \"eu\"
}"
