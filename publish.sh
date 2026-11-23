#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
source version.sh
read_version
repository=registry-1.docker.io/aperturedata/aperturedb-python
tag="v$BUILD_VERSION"
wheel="aperturedb-${BUILD_VERSION}-py3-none-any.whl"
artifact_type=application/vnd.aperturedata.python-wheel.v1
revision=$(git rev-parse HEAD)

# A retry must promote the same bytes that infra may already have published.
tags=$(oras repo tags "$repository")
if grep -Fxq "$tag" <<< "$tags"; then
    manifest=$(oras manifest fetch "$repository:$tag")
    if ! jq -e --arg type "$artifact_type" --arg revision "$revision" --arg wheel "$wheel" \
        '.artifactType == $type and .annotations["org.opencontainers.image.revision"] == $revision
         and (.layers | length == 1) and .layers[0].mediaType == "application/zip"
         and .layers[0].annotations["org.opencontainers.image.title"] == $wheel' <<< "$manifest" >/dev/null; then
        echo "$repository:$tag already exists with a different artifact or source revision" >&2
        exit 1
    fi
    echo "Reusing $repository:$tag"
    exit 0
fi

dist=$(mktemp -d)
trap 'rm -rf "$dist"' EXIT
# Build the tagged sources in a clean tree so stale build/lib files on a
# reused runner cannot enter the release wheel.
mkdir "$dist/source"
git archive "$revision" | tar -x -C "$dist/source"
python3 -m pip wheel --no-deps --wheel-dir "$dist" "$dist/source"
cd "$dist"
oras push "$repository:$tag" --artifact-type "$artifact_type" \
    --annotation "org.opencontainers.image.revision=$revision" \
    --annotation "org.opencontainers.image.version=$BUILD_VERSION" \
    "$wheel:application/zip"
