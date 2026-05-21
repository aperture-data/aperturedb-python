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
    echo "$GATEWAY"
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

sleep 20 # wait for the containers to be up and running

pid1=0
pid2=0

if [ "$TEST_PROTOCOL" == "http" ] || [ "$TEST_PROTOCOL" == "both" ]; then
    echo "running tests on docker image $REPOSITORY with $GATEWAY_HTTP"
    docker run \
        -v $(pwd)/output:/aperturedata/test/output \
        -v $(pwd)/${RUNNER_NAME}_http_ca:/ca \
        --network=${RUNNER_NAME}_http_default \
        -v "$LOG_PATH":"${TESTING_LOG_PATH}" \
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
