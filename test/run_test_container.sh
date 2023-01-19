#!/bin/bash

set -u
set -e


rm -rf aperturedb/db
docker-compose down && docker-compose up -d

docker run \
    --link test_aperturedb_1:aperturedb \
    --network test_default \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e GCP_SERVICE_ACCOUNT_KEY=$GCP_SERVICE_ACCOUNT_KEY \
    aperturedata/aperturedb-python-tests