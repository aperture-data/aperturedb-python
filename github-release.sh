#!/bin/bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
source "${script_dir}/version.sh"
read_version

python3 "${script_dir}/scripts/github_release.py" \
    --version "$BUILD_VERSION" --commit "$(git rev-parse HEAD)"
