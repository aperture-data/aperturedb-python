#!/bin/bash

set -u
set -e

mkdir -p output
rm -rf output/*
mkdir -p input/blobs

echo "Downloading images..."
python3 download_images.py          # Test ImageDownloader
echo "Done downloading images."

echo "Generating input files..."
python3 generateInput.py
echo "Done generating input files."

echo "Running tests..."
CREDENTIALS_FILE='/tmp/key.json'
echo $GCP_SERVICE_ACCOUNT_KEY > $CREDENTIALS_FILE
export GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS_FILE
# capture errors
set +e
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_*.py -v
RESULT=$?

if [[ $RESULT != 0 ]]; then
	echo "Test failed; outputting db log:"
	if [[ "${APERTUREDB_LOG_PATH}" != "" ]]; then
		cat -n "${APERTUREDB_LOG_PATH}"/aperturedb.INFO

		BUCKET=python-ci-runs
		NOW=$(date -Iseconds)
		ARCHIVE_NAME=logs.tar.gz

		tar czf ${ARCHIVE_NAME} ${APERTUREDB_LOG_PATH}
		aws s3 cp ${ARCHIVE_NAME} s3://${BUCKET}/aperturedb-${NOW}.tgz

	else
		echo "Unable to output log, APERTUREDB_LOG_PATH not set."
	fi
	exit 1
else
	echo "Generating coverage..."
	coverage html -i --directory=output
fi

