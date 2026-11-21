#!/bin/bash

set -e

BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
if [ -z "$BRANCH_NAME" ]
then
    echo "This is on a merge branch. Will not continue"
    exit 0
fi

source $(dirname "$0")/version.sh

# Trigger read version
read_version
echo "Build version: $BUILD_VERSION"

git config --local user.name "github-actions[bot]"
git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
git tag -f "v$BUILD_VERSION" "$(git rev-parse HEAD)"
git push origin "v$BUILD_VERSION"
