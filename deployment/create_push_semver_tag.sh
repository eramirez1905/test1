#!/bin/bash

# Inspired by https://stackoverflow.com/questions/3760086/automatic-tagging-of-releases
git fetch --tags

PREFIX='v'
TRAVIS_REPO_SLUG=${1:-${TRAVIS_REPO_SLUG}}
GIT_COMMIT=${2:-$(git rev-parse HEAD 2> /dev/null)}

# get highest tag number
VERSION=`git describe --abbrev=0 --tags --match "${PREFIX}*"`

#replace . with space so can split into an array
VERSION_BITS=(${VERSION//./ })

# get number parts and increase last one by 1
MAJOR=$(date +'%y')
MINOR=$(date +'%W')
PATCH=0

if [[ "${VERSION_BITS[0]}.${VERSION_BITS[1]}" = "${PREFIX}${MAJOR}.${MINOR}" ]]; then
    PATCH=$((VERSION_BITS[2] + 1))
fi

# create new tag
NEW_TAG="${PREFIX}$MAJOR.$MINOR.$PATCH"

echo "Updating $VERSION to $NEW_TAG"

# get current hash and see if it already has a tag
NEEDS_TAG=`git describe --contains ${GIT_COMMIT} 2> /dev/null`

# only tag if no tag already (would be better if the git describe command above could have a silent option)
if [[ -z "$NEEDS_TAG" ]]; then
    echo "Tagged with $NEW_TAG"
    git tag ${NEW_TAG}
    curl -X POST -H "Content-Type:application/json" -H "Authorization: token ${GITHUB_TOKEN}" \
        "https://api.github.com/repos/${TRAVIS_REPO_SLUG}/git/refs" \
        -d "{\"ref\": \"refs/tags/${NEW_TAG}\",\"sha\": \"${GIT_COMMIT}\"}"

else
    echo "Already a tag on this commit"
fi
