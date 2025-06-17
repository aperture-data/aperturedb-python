#!/bin/bash

set -u
set -e
set -o pipefail

mkdir -p output
rm -rf output/*
mkdir -p input/blobs

echo "Downloading images..."
python3 download_images.py
RESULT=$?
if [[ $RESULT != 0 ]]; then
	echo "Download failed."
	exit 1
fi
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
CLIENT_PATH="${APERTUREDB_LOG_PATH}/../client/${FILTER}"
CLIENT_PATH=${CLIENT_PATH// /_}
mkdir -p ${CLIENT_PATH}
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest -m "$FILTER" test_*.py -v | tee ${CLIENT_PATH}/test.log
RESULT=$?
cp error*.log -v ${CLIENT_PATH}

if [[ $RESULT != 0 ]]; then
	echo "Test failed; outputting db log:"
	if [[ "${APERTUREDB_LOG_PATH}" != "" ]]; then

		BUCKET=python-ci-runs
		NOW=$(date -Iseconds)
		ARCHIVE_NAME=logs.tar.gz
		DESTINATION="s3://${BUCKET}/aperturedb-${NOW}-${FILTER}.tgz"
		tar czf ${ARCHIVE_NAME} ${APERTUREDB_LOG_PATH}/..
		aws s3 cp ${ARCHIVE_NAME} $DESTINATION
		echo "Log output to $DESTINATION"
	else
		echo "Unable to output log, APERTUREDB_LOG_PATH not set."
	fi
	exit 1
else
	echo "Generating coverage..."
	coverage html -i --directory=output
	python adb_timing_tests.py
fi

