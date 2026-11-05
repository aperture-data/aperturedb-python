#!/bin/bash
set -e

# The database stack comes from the published Compose artifact; see
# test/stack.sh. The ports match the earlier devcontainer stack.
ADB_TCP_PORT=${ADB_TCP_PORT:-55556} HTTP_PORT=${HTTP_PORT:-8087} \
    HTTPS_PORT=${HTTPS_PORT:-8443} test/stack.sh up aperturedb-python-dev
docker build -t aperturedata/aperturedb-notebook:dependencies docker/dependencies
