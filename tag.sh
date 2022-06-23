#!/bin/bash

set -e

# Read version from python code
read_version() {
   BUILD_VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}' aperturedb/__init__.py | tr -d '"')
}

# Trigger read version
read_version
echo "Build version: $BUILD_VERSION"

git config --local user.name "github-actions[bot]"
git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
git tag "v$BUILD_VERSION" $TAG_BASE
git push origin "v$BUILD_VERSION"
