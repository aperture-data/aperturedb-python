#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
source version.sh
read_version
repository=registry-1.docker.io/aperturedata/aperturedb-python
tag="$BUILD_VERSION"
wheel="aperturedb-${BUILD_VERSION}-py3-none-any.whl"
artifact_type=application/vnd.aperturedata.python-wheel.v1
revision=$(git rev-parse HEAD)

# A retry must promote the same bytes that infra may already have published.
tags=$(oras repo tags "$repository")
if grep -Fxq "$tag" <<< "$tags"; then
    manifest=$(oras manifest fetch "$repository:$tag")
    if ! jq -e --arg type "$artifact_type" --arg version "$tag" --arg wheel "$wheel" \
        '.artifactType == $type and .annotations["org.opencontainers.image.version"] == $version
         and (.annotations["org.opencontainers.image.revision"] | test("^[0-9a-f]{40}$"))
         and (.layers | length == 1) and .layers[0].mediaType == "application/zip"
         and .layers[0].annotations["org.opencontainers.image.title"] == $wheel
         and (.layers[0].digest | test("^sha256:[0-9a-f]{64}$"))' <<< "$manifest" >/dev/null; then
        echo "Unexpected wheel artifact at $repository:$tag" >&2
        exit 1
    fi
    built_from=$(jq -r '.annotations["org.opencontainers.image.revision"]' <<< "$manifest")
    if [[ $built_from != "$revision" ]]; then
        # An image-tag correction may reissue a release after its wheel is on PyPI.
        if ! git merge-base --is-ancestor "$built_from" "$revision" ||
           ! git diff --quiet "$built_from" "$revision" -- . ':(top,exclude,literal)README.md' \
               ':(top,exclude,literal).github/workflows/main.yml' ':(top,exclude,literal)ci.sh' ':(top,exclude,literal)publish.sh'; then
            echo "Wheel source $built_from is unrelated or package inputs changed" >&2
            exit 1
        fi

        # The README is wheel metadata. Its only allowed change is dropping the v
        # from version-tag examples. Leave the published wheel's metadata intact.
        if ! git diff --quiet "$built_from" "$revision" -- README.md; then
            readme=$(git show "$built_from:README.md" | sed -E 's/`v([0-9]+\.[0-9]+\.[0-9]+)`/`\1`/g' | git hash-object --stdin)
            if [[ $readme != "$(git rev-parse "$revision:README.md")" ]]; then
                echo "README changed beyond the image tag format" >&2
                exit 1
            fi
        fi

        digest=$(jq -r '.layers[0].digest' <<< "$manifest")
        if ! curl --fail --silent --show-error --max-time 30 "https://pypi.org/pypi/aperturedb/$tag/json" |
             jq -e --arg wheel "$wheel" --arg hash "${digest#sha256:}" \
                 '[.urls[] | select(.filename == $wheel) | .digests.sha256] == [$hash]' >/dev/null; then
            echo "Published PyPI wheel $wheel must match OCI digest $digest" >&2
            exit 1
        fi
    fi
    echo "Reusing $repository:$tag built from $built_from"
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
