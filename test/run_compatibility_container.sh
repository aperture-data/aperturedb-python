#!/bin/bash
# Required modern SDK validation, using the test image already built by ci.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "${SCRIPT_DIR}"
: "${RUNNER_NAME:?Set a unique runner name}"
: "${SDK_COMPAT_COMPOSE:?Set the modern server Compose artifact}"
SDK_COMPAT_APERTUREDB_IMAGE=${SDK_COMPAT_APERTUREDB_IMAGE:-aperturedata/aperturedb@sha256:2f348a70535aefba49ad105e6241c97f4c97ca86e121340f98907aa6a32e4107}
[[ "$SDK_COMPAT_APERTUREDB_IMAGE" =~ ^aperturedata/aperturedb@sha256:[0-9a-f]{64}$ ]]
REPOSITORY=${1:-aperturedata/aperturedb-python-tests:latest}
stack="${RUNNER_NAME}_sdk_compat"

teardown() {
    PUBLISH_PORTS=false ADB_COMPOSE="$SDK_COMPAT_COMPOSE" APERTUREDB_IMAGE="$SDK_COMPAT_APERTUREDB_IMAGE" LNZ_FORCE_SSL=true ./stack.sh down "$stack" || true
}
trap teardown EXIT

PUBLISH_PORTS=false ADB_COMPOSE="$SDK_COMPAT_COMPOSE" APERTUREDB_IMAGE="$SDK_COMPAT_APERTUREDB_IMAGE" LNZ_FORCE_SSL=true ./stack.sh up "$stack"
python3 "${SCRIPT_DIR}/record_modern_stack.py" --project "$stack" \
    --output "${SCRIPT_DIR}/../qualification/sdk-modern-stack.json"
python3 "${SCRIPT_DIR}/require_modern_stack.py" "${SCRIPT_DIR}/../qualification/sdk-modern-stack.json"
mkdir -p output/client/sdk-compat

# These exact host names are covered by the stack's generated CA certificate.
# The Python runner rejects missing/plaintext/unverified/legacy endpoint configs.
endpoints='[{"host":"lenz","port":55551,"hello_version":2,"use_ssl":true,"ca_cert":"/ca/ca.crt","verify_hostname":true},{"host":"http-proxy","port":8443,"rest":true,"hello_version":2,"use_ssl":true,"ca_cert":"/ca/ca.crt","verify_hostname":true}]'
docker run --rm \
    --network="${stack}_default" \
    -v "$(pwd)/aperturedb/${stack}/ca:/ca:ro" \
    -v "$(pwd)/output/client/sdk-compat:/compat-output" \
    -v "$(pwd)/pytest.ini:/aperturedata/test/pytest.ini:ro" \
    -e ADB_COMPAT_ENDPOINTS="$endpoints" \
    -e ADB_COMPAT_USERNAME=admin \
    -e ADB_COMPAT_PASSWORD=admin \
    -e ADB_COMPAT_JUNIT=/compat-output/junit.xml \
    -e SKIP_SLOW_TESTS=true \
    -e PROJECT=aperturedata \
    -e KAGGLE_username=ci \
    -e KAGGLE_key=dummy \
    --workdir /aperturedata/test \
    --entrypoint python3 \
    "$REPOSITORY" run_compatibility.py
