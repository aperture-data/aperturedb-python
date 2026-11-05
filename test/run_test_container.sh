#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "${SCRIPT_DIR}"

function check_containers_networks(){
    echo "Running containers and networks cleanup"
    docker ps
    echo "Existing networks"
    docker network ls
}

function get_sudo() {
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        echo "sudo"
    else
        echo ""
    fi
}

# Each protocol runs against its own stack from the Compose artifact named by
# ADB_COMPOSE; see stack.sh. The stacks publish no host ports: test
# containers join the <stack>_default network.
function stacks() {
    if [ "$TEST_PROTOCOL" == "http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
        echo "${RUNNER_NAME}_http"
    fi
    if [ "$TEST_PROTOCOL" == "non_http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
        echo "${RUNNER_NAME}_non_http"
    fi
}

function teardown() {
    echo "Tearing down containers and networks..."
    for stack in $(stacks); do
        PUBLISH_PORTS=false ./stack.sh down "$stack" || true
    done
}
trap teardown EXIT

TESTING_LOG_PATH="/aperturedb/test/server_logs"

# Check if TEST_PROTOCOL is set, otherwise default to both
TEST_PROTOCOL=${TEST_PROTOCOL:-"both"}

$(get_sudo) rm -rf output
mkdir -m 777 output

for stack in $(stacks); do
    PUBLISH_PORTS=false ./stack.sh up "$stack"
done

# Server logs, and this runner state, are archived with failed test logs.
for stack in $(stacks); do
    mkdir -p "aperturedb/${stack}/data/aperturedb_logs/runner_state"
    check_containers_networks > "aperturedb/${stack}/data/aperturedb_logs/runner_state/runner_state.log"
done
check_containers_networks

REPOSITORY="aperturedata/aperturedb-python-tests"
if ! [ -z ${1+x} ]
then
     REPOSITORY="$1"
fi

# Wait for the stack(s) to be ready instead of blindly sleeping. Each lenz
# instance exposes a health port (58085) that becomes reachable once the
# service is up; the HTTP proxy is ready once port 8081 answers.
wait_for_stack() {
    local tag=$1
    local network=${tag}_default
    local timeout=60
    local elapsed=0
    echo "Waiting for stack ${tag} to become ready (timeout ${timeout}s)..."
    while [ $elapsed -lt $timeout ]; do
        local lenz_ready=0
        local proxy_ready=0

        if docker run --rm --network=${network} curlimages/curl:latest \
                nc -z -w 2 lenz 58085 >/dev/null 2>&1; then
            lenz_ready=1
        fi

        if [[ "$tag" == *"_http" ]]; then
            if docker run --rm --network=${network} curlimages/curl:latest \
                    -sS -o /dev/null -m 2 http://http-proxy:8081/ >/dev/null 2>&1; then
                proxy_ready=1
            fi

            if [ "$lenz_ready" -eq 1 ] && [ "$proxy_ready" -eq 1 ]; then
                echo "Stack ${tag} is ready after ${elapsed}s"
                return 0
            fi
        else
            if [ "$lenz_ready" -eq 1 ]; then
                echo "Stack ${tag} is ready after ${elapsed}s"
                return 0
            fi
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "WARNING: stack ${tag} did not report ready within ${timeout}s; proceeding anyway"
    return 0
}

if [ "$TEST_PROTOCOL" == "http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    wait_for_stack "${RUNNER_NAME}_http"
fi
if [ "$TEST_PROTOCOL" == "non_http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    wait_for_stack "${RUNNER_NAME}_non_http"
fi

pid1=0
pid2=0

if [ "$TEST_PROTOCOL" == "http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    echo "running tests on docker image $REPOSITORY against ${RUNNER_NAME}_http"
    docker run --rm \
        -v $(pwd)/output:/aperturedata/test/output \
        -v "$(pwd)/aperturedb/${RUNNER_NAME}_http/ca:/ca" \
        --network=${RUNNER_NAME}_http_default \
        -v "$(pwd)/aperturedb/${RUNNER_NAME}_http/data/aperturedb_logs":"${TESTING_LOG_PATH}" \
        -v $(pwd)/run_test.sh:/aperturedata/test/run_test.sh \
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
        -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
        -e APERTUREDB_LOG_PATH="${TESTING_LOG_PATH}" \
        -e FILTER="http" \
        -e SKIP_SLOW_TESTS="${SKIP_SLOW_TESTS:-false}" \
        $REPOSITORY &
    pid1=$!
fi

if [ "$TEST_PROTOCOL" == "non_http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    echo "running tests on docker image $REPOSITORY against ${RUNNER_NAME}_non_http"
    docker run --rm \
        -v $(pwd)/output:/aperturedata/test/output \
        -v "$(pwd)/aperturedb/${RUNNER_NAME}_non_http/ca:/ca" \
        --network=${RUNNER_NAME}_non_http_default \
        -v "$(pwd)/aperturedb/${RUNNER_NAME}_non_http/data/aperturedb_logs":"${TESTING_LOG_PATH}" \
        -v $(pwd)/run_test.sh:/aperturedata/test/run_test.sh \
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
        -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
        -e APERTUREDB_LOG_PATH="${TESTING_LOG_PATH}" \
        -e FILTER="not http" \
        -e SKIP_SLOW_TESTS="${SKIP_SLOW_TESTS:-false}" \
        $REPOSITORY &
    pid2=$!
fi

exit_code1=0
exit_code2=0

if [ "$pid1" != "0" ]; then
    wait $pid1
    exit_code1=$?
fi

if [ "$pid2" != "0" ]; then
    wait $pid2
    exit_code2=$?
fi

if [ $exit_code1 -ne 0 ]; then
    echo "Tests failed for HTTP"
    exit $exit_code1
fi
if [ $exit_code2 -ne 0 ]; then
    echo "Tests failed for NON_HTTP"
    exit $exit_code2
fi

echo "Tests completed"
echo " --- Runner name: ${RUNNER_NAME} ---"
check_containers_networks
