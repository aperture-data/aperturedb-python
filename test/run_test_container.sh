#!/bin/bash

set -u
set -e

docker compose down
rm -rf aperturedb/db
rm -rf output
mkdir output
docker compose up -d

REPOSITORY="aperturedata/aperturedb-python-tests"
if ! [ -z ${1+x} ]
then
     REPOSITORY="$1"
fi
echo "running tests on docker image $REPOSITORY"

docker run \
    --network test_default \
    -v ./output:/aperturedata/test/output \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e GCP_SERVICE_ACCOUNT_KEY="$GCP_SERVICE_ACCOUNT_KEY" \
    $REPOSITORY
