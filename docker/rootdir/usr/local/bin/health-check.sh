#!/usr/bin/env bash

set -e

hostname=$1

if [[ -z ${hostname} ]]; then
  echo "hostname parameter is missing. Please provide it as parameter. e.g. localhost:8080"
  exit 1
fi

for type in metadatabase scheduler
do
  response=$(curl -Ss --connect-timeout 2 --max-time 2 http://${hostname}/health)
  status=$(echo ${response} | jq -r ".$type.status")

  if [[ "$status" = "unhealthy" ]]; then
    echo "$type $status"
    exit 1
  fi
done
