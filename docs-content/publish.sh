#!/usr/bin/env bash
# Upload a complete content generation to docs over its internal rsync service.
set -euo pipefail

: "${DOCS_PROVIDER:?DOCS_PROVIDER must be set}"
target=${DOCS_RSYNC_URL-rsync://docs-connections:873/content}
while [[ "$target" == */ ]]; do
    target=${target%/}
done
command=(rsync -rlt --mkpath --timeout=120 --contimeout=10)
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
touch "$temporary/.ready"

while true; do
    read -r generation < /proc/sys/kernel/random/uuid
    generation=${generation//-/}
    destination="$target/$DOCS_PROVIDER/$generation"

    # The receiver ignores this generation until the separate marker arrives.
    if "${command[@]}" /content/ "$destination/data/" &&
        "${command[@]}" "$temporary/.ready" "$destination/"; then
        printf 'Uploaded %s %s\n' "$DOCS_PROVIDER" "$generation"
        break
    else
        status=$?
        printf 'Upload failed (rsync exit %s); retrying in 10 seconds\n' "$status" >&2
        sleep 10
    fi
done
