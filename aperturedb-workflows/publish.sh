#!/usr/bin/env bash
# Publish the aperturedb-workflows wheel as aperturedata/aperturedb-workflows:<version>,
# using the SDK version in aperturedb/__init__.py. A published version is reused
# only when its wheel was built from the same aperturedb-workflows sources.
# Changing them needs a version bump.
set -euo pipefail

cd "$(dirname "$0")/.."
source version.sh
read_version
repository=registry-1.docker.io/aperturedata/aperturedb-workflows
target="$repository:$BUILD_VERSION"
wheel="aperturedb_workflows-${BUILD_VERSION}-py3-none-any.whl"
artifact_type=application/vnd.aperturedata.python-wheel.v1
revision=$(git rev-parse HEAD)

package_version=$(awk '$1=="__version__" && $2=="=" {print $3}' \
    aperturedb-workflows/aperturedb_workflows/__init__.py | tr -d '"')
if [[ $package_version != "$BUILD_VERSION" ]]; then
    echo "aperturedb-workflows version $package_version must match the SDK version $BUILD_VERSION" >&2
    exit 1
fi

# Docker Hub cannot list tags in a repository before the first push. ORAS
# 1.3.0 reports a missing manifest with this reference-specific diagnostic.
missing="Error response from registry: failed to fetch the content of \"$target\": $target: not found"
if manifest=$(oras manifest fetch "$target" 2>&1); then
    if ! jq -e --arg type "$artifact_type" --arg version "$BUILD_VERSION" --arg wheel "$wheel" \
        '.artifactType == $type and .annotations["org.opencontainers.image.version"] == $version
         and (.annotations["org.opencontainers.image.revision"] | test("^[0-9a-f]{40}$"))
         and (.layers | length == 1) and .layers[0].mediaType == "application/zip"
         and .layers[0].annotations["org.opencontainers.image.title"] == $wheel' <<< "$manifest" >/dev/null; then
        echo "Unexpected wheel artifact at $target" >&2
        exit 1
    fi
    built_from=$(jq -r '.annotations["org.opencontainers.image.revision"]' <<< "$manifest")
    if ! git diff --quiet "$built_from" "$revision" -- aperturedb-workflows; then
        echo "aperturedb-workflows changed since $built_from; bump the version to publish it" >&2
        exit 1
    fi
    echo "Reusing $target built from $built_from"
    exit 0
elif [[ $manifest != "$missing" ]]; then
    printf 'Could not check %s: %s\n' "$target" "$manifest" >&2
    exit 1
fi

dist=$(mktemp -d)
trap 'rm -rf "$dist"' EXIT
# Build the committed sources so stale files on a reused runner stay out.
mkdir "$dist/source"
git archive "$revision" aperturedb-workflows | tar -x -C "$dist/source"
python3 -m pip wheel --no-deps --wheel-dir "$dist" "$dist/source/aperturedb-workflows"
cd "$dist"
oras push "$target" --artifact-type "$artifact_type" \
    --annotation "org.opencontainers.image.revision=$revision" \
    --annotation "org.opencontainers.image.version=$BUILD_VERSION" \
    "$wheel:application/zip"
