#!/bin/bash

set -u
set -e

#Ensure clean environment (as much as possible)
docker compose -f docker-compose.yml down --remove-orphans
docker network rm ${RUNNER_NAME}_default || true

# ensure latest db
docker compose pull

rm -rf output
mkdir -m 777 output

docker network create ${RUNNER_NAME}_default
GATEWAY=$(docker network inspect ${RUNNER_NAME}_default | jq -r .[0].IPAM.Config[0].Gateway)
echo "Gateway: $GATEWAY"
export GATEWAY
docker compose -f docker-compose.yml up -d

LOG_PATH="$(pwd)/aperturedb/logs"
TESTING_LOG_PATH="/aperturedb/test/server_logs"

REPOSITORY="aperturedata/aperturedb-python-tests"
if ! [ -z ${1+x} ]
then
     REPOSITORY="$1"
fi
echo "running tests on docker image $REPOSITORY"

docker run \
    --network ${RUNNER_NAME}_default \
    -v $(pwd)/output:/aperturedata/test/output \
    -v "$LOG_PATH":"${TESTING_LOG_PATH}" \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
    -e APERTUREDB_LOG_PATH="${TESTING_LOG_PATH}" \
    -e GATEWAY=${GATEWAY} \
    $REPOSITORY

echo "Tests completed successfully"
