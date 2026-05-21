#!/bin/bash

set -e

function check_containers_networks(){
    echo "Running containers and networks cleanup"
    docker ps
    echo "Existing networks"
    docker network ls
}

function run_aperturedb_instance(){
    set -e
    TAG=$1
    #Ensure clean environment (as much as possible)
    RUNNER_NAME=$TAG docker compose -f docker-compose.yml down --remove-orphans
    docker network rm ${TAG}_host_default || true

    # ensure latest db
    docker compose pull

    rm -rf output
    mkdir -m 777 output

    docker network create ${TAG}_host_default
    GATEWAY=$(docker network inspect ${TAG}_host_default | jq -r .[0].IPAM.Config[0].Gateway)
    GATEWAY=$GATEWAY RUNNER_NAME=$TAG docker compose -f docker-compose.yml up -d
    if [ "$TAG" == "${RUNNER_NAME}_http" ]; then
        PORT=$(RUNNER_NAME=$TAG docker compose -f docker-compose.yml port nginx 80 | cut -d: -f2)
    else
        PORT=$(RUNNER_NAME=$TAG docker compose -f docker-compose.yml port lenz 55551 | cut -d: -f2)
    fi
    echo "$GATEWAY:$PORT"
}

IP_REGEX='[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}'

# Check if TEST_PROTOCOL is set, otherwise default to both
TEST_PROTOCOL=${TEST_PROTOCOL:-"both"}

if [ "$TEST_PROTOCOL" == "http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    GATEWAY_HTTP=$(run_aperturedb_instance "${RUNNER_NAME}_http" | grep $IP_REGEX )
fi

if [ "$TEST_PROTOCOL" == "non_http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    GATEWAY_NON_HTTP=$(run_aperturedb_instance "${RUNNER_NAME}_non_http" | grep $IP_REGEX )
fi

# The LOG_PATH and RUNNER_INFO_PATH are set to the current working directory
LOG_PATH="$(pwd)/aperturedb/logs"
TESTING_LOG_PATH="/aperturedb/test/server_logs"
RUNNER_INFO_PATH="$(pwd)/aperturedb/logs/runner_state"

check_containers_networks | tee "$RUNNER_INFO_PATH"/runner_state.log

REPOSITORY="aperturedata/aperturedb-python-tests"
if ! [ -z ${1+x} ]
then
     REPOSITORY="$1"
fi

# Wait for the stack(s) to be ready instead of blindly sleeping. Each lenz
# instance exposes a health port (58085) that becomes reachable once the
# service is up; nginx is ready as soon as port 80 accepts connections.
wait_for_stack() {
    local tag=$1
    local network=${tag}_default
    local timeout=60
    local elapsed=0
    echo "Waiting for stack ${tag} to become ready (timeout ${timeout}s)..."
    while [ $elapsed -lt $timeout ]; do
        local is_ready=0
        if docker run --rm --network=${network} curlimages/curl:latest \
                -sS -o /dev/null -m 2 http://lenz:58085/ >/dev/null 2>&1; then
            if [[ "$tag" == *"_http" ]]; then
                if docker run --rm --network=${network} curlimages/curl:latest \
                        -sS -o /dev/null -m 2 http://nginx:80/ >/dev/null 2>&1; then
                    is_ready=1
                fi
            else
                is_ready=1
            fi
        fi
        
        if [ "$is_ready" -eq 1 ]; then
            echo "Stack ${tag} is ready after ${elapsed}s"
            return 0
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
    echo "running tests on docker image $REPOSITORY with $GATEWAY_HTTP"
    docker run \
        -v $(pwd)/output:/aperturedata/test/output \
        -v $(pwd)/${RUNNER_NAME}_http_ca:/ca \
        --network=${RUNNER_NAME}_http_default \
        -v "$LOG_PATH":"${TESTING_LOG_PATH}" \
        -v $(pwd)/run_test.sh:/aperturedata/test/run_test.sh \
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
        -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
        -e APERTUREDB_LOG_PATH="${TESTING_LOG_PATH}" \
        -e GATEWAY="nginx" \
        -e FILTER="http" \
        $REPOSITORY &
    pid1=$!
fi

if [ "$TEST_PROTOCOL" == "non_http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    echo "running tests on docker image $REPOSITORY with $GATEWAY_NON_HTTP"
    docker run \
        -v $(pwd)/output:/aperturedata/test/output \
        -v $(pwd)/${RUNNER_NAME}_non_http_ca:/ca \
        --network=${RUNNER_NAME}_non_http_default \
        -v "$LOG_PATH":"${TESTING_LOG_PATH}" \
        -v $(pwd)/run_test.sh:/aperturedata/test/run_test.sh \
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
        -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
        -e APERTUREDB_LOG_PATH="${TESTING_LOG_PATH}" \
        -e GATEWAY="lenz" \
        -e FILTER="not http" \
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
