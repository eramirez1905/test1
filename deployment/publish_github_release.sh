#!/usr/bin/env bash

set -xe

# Get tag and commit locally and on Travis
GIT_TAG=${TRAVIS_BRANCH:=$(git tag -l --contains HEAD)}

echo "=== Pushing GitHub release ==="

# Set application version
: ${APP_VERSION:=${GIT_TAG}}

REPOSITORY_REGISTRY="683110685365.dkr.ecr.eu-west-1.amazonaws.com"

# Create version file
echo ${APP_VERSION} > version

# Publish the release notes
docker pull ${REPOSITORY_REGISTRY}/release-notes:latest
docker run \
  -v "$(pwd)":/usr/src/app \
  -e GITHUB_TOKEN="${GITHUB_TOKEN}" \
  -e RELEASE_NOTES_NOTIFY="${RELEASE_NOTES_NOTIFY}" \
  "${REPOSITORY_REGISTRY}/release-notes"

# Reset version file
cat /dev/null > version
