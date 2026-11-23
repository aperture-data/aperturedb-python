#!/bin/bash
# Required modern SDK validation, using the test image already built by ci.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "${SCRIPT_DIR}"
: "${RUNNER_NAME:?Set a unique runner name}"
REPOSITORY=${1:-aperturedata/aperturedb-python-tests:latest}
stack="${RUNNER_NAME}_sdk_compat"

qualification="$SCRIPT_DIR/../qualification"
mkdir -p "$qualification/sdk-compat" "$SCRIPT_DIR/output"

# Resolve the image supplied by this Compose artifact before starting any services.
selected=$(env -u APERTUREDB_IMAGE ./stack.sh image "$stack")
if [[ ! "$selected" =~ ^(docker\.io/)?aperturedata/aperturedb(:[a-zA-Z0-9_.-]+)?@(sha256:[0-9a-f]{64})$ ]]; then
    echo "Compose must select an official ApertureDB image by digest." >&2
    exit 1
fi
database_image="aperturedata/aperturedb@${BASH_REMATCH[3]}"
printf '%s\n' "$database_image" > "$qualification/sdk-selected-image"

teardown() {
    PUBLISH_PORTS=false APERTUREDB_IMAGE="$database_image" LNZ_FORCE_SSL=true ./stack.sh down "$stack" || true
}
trap teardown EXIT

PUBLISH_PORTS=false APERTUREDB_IMAGE="$database_image" LNZ_FORCE_SSL=true ./stack.sh up "$stack"
python3 "${SCRIPT_DIR}/record_modern_stack.py" --project "$stack" \
    --output "$qualification/sdk-modern-stack.json"
python3 "${SCRIPT_DIR}/require_modern_stack.py" "$qualification/sdk-modern-stack.json" --expected-image "$database_image"

# These exact host names are covered by the stack's generated CA certificate.
# The Python runner rejects missing/plaintext/unverified/legacy endpoint configs.
endpoints='[{"host":"lenz","port":55551,"hello_version":2,"use_ssl":true,"ca_cert":"/ca/ca.crt","verify_hostname":true},{"host":"http-proxy","port":8443,"rest":true,"hello_version":2,"use_ssl":true,"ca_cert":"/ca/ca.crt","verify_hostname":true}]'
docker run --rm \
    --network="${stack}_default" \
    -v "$(pwd)/aperturedb/${stack}/ca:/ca:ro" \
    -v "$SCRIPT_DIR/output:/aperturedata/test/output" \
    -v "$qualification/sdk-compat:/compat-output" \
    -v "$(pwd)/pytest.ini:/aperturedata/test/pytest.ini:ro" \
    -e ADB_COMPAT_ENDPOINTS="$endpoints" \
    -e ADB_COMPAT_USERNAME=admin \
    -e ADB_COMPAT_PASSWORD=admin \
    -e ADB_COMPAT_JUNIT=/compat-output/junit.xml \
    -e PROJECT=aperturedata \
    -e KAGGLE_username=ci \
    -e KAGGLE_key=dummy \
    --workdir /aperturedata/test \
    --entrypoint python3 \
    "$REPOSITORY" run_compatibility.py
