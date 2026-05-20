#!/bin/bash

set -u
set -e
set -o pipefail

: "${GCP_SERVICE_ACCOUNT_KEY:=""}"
: "${APERTUREDB_LOG_PATH:=""}"
: "${FILTER:=""}"

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
if [ -n "$GCP_SERVICE_ACCOUNT_KEY" ]; then
	CREDENTIALS_FILE=$(mktemp)
	trap 'rm -f "$CREDENTIALS_FILE"' EXIT
	printf "%s\n" "$GCP_SERVICE_ACCOUNT_KEY" > "$CREDENTIALS_FILE"
	export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
fi
# capture errors
set +e

SAFE_FILTER=$(printf "%s" "$FILTER" | tr -c 'a-zA-Z0-9_-' '_')
if [ -n "$APERTUREDB_LOG_PATH" ]; then
	CLIENT_PATH="${APERTUREDB_LOG_PATH}/../client/${SAFE_FILTER}"
else
	CLIENT_PATH="output/client/${SAFE_FILTER}"
fi
mkdir -p "$CLIENT_PATH"

if [ -n "$FILTER" ]; then
	PYTEST_ARGS=("-m" "$FILTER")
else
	PYTEST_ARGS=()
fi

PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy python3 -m pytest --cov=aperturedb "${PYTEST_ARGS[@]}" test_*.py -v | tee "${CLIENT_PATH}/test.log"
RESULT=$?
cp error*.log -v "$CLIENT_PATH" || true

if [[ $RESULT != 0 ]]; then
	echo "Test failed; outputting db log:"
	if [[ "${APERTUREDB_LOG_PATH}" != "" ]]; then

		BUCKET=python-ci-runs
		NOW=$(date -Iseconds)
		ARCHIVE_NAME=logs.tar.gz
		DESTINATION="s3://${BUCKET}/aperturedb-${NOW}-${SAFE_FILTER}.tgz"
		tar czf "${ARCHIVE_NAME}" "${APERTUREDB_LOG_PATH}/.."
		docker run --rm -v "$(pwd)":/workspace -w /workspace -e AWS_ACCESS_KEY_ID -e AWS_DEFAULT_REGION -e AWS_SECRET_ACCESS_KEY amazon/aws-cli s3 cp "${ARCHIVE_NAME}" "$DESTINATION"
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

