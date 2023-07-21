#!/bin/bash

set -u
set -e

docker compose down --remove-orphans
rm -rf aperturedb/db
rm -rf output
mkdir -m 777 output
docker compose up -d

LOG_PATH="$(pwd)/apreturedb/logs"
TESTING_LOG_PATH="/aperturedb/test/server_logs"

REPOSITORY="aperturedata/aperturedb-python-tests"
if ! [ -z ${1+x} ]
then
     REPOSITORY="$1"
fi
echo "running tests on docker image $REPOSITORY"

docker run \
    --network test_default \
    -v $(pwd)/output:/aperturedata/test/output \
    -v "$LOG_PATH":"${TESTING_LOG_PATH}" \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
    -e APERTUREDB_LOG_PATH=="${TESTING_LOG_PATH}" \
    $REPOSITORY
