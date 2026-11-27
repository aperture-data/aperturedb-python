#!/usr/bin/env bash
# Build the SDK and aperturedb-workflows wheels from this checkout, install
# them together in a clean Python 3.10 container and test the workflows wheel.
set -euo pipefail
cd "$(dirname "$0")/.."
docker run --rm -e PIP_DISABLE_PIP_VERSION_CHECK=1 -v "$PWD:/src:ro" python:3.10-slim bash -euo pipefail -c '
    mkdir /build
    cp -r /src/aperturedb /src/pyproject.toml /src/README.md /src/LICENSE /src/aperturedb-workflows /build/
    pip wheel --quiet --no-deps --wheel-dir /dist /build /build/aperturedb-workflows
    pip install --quiet --root-user-action=ignore /dist/*.whl pytest
    cd /tmp
    pytest -q /build/aperturedb-workflows/test
'
