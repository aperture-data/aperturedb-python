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
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_*.py -v -k "test_updateif_fails"
echo "Generating coverage..."
coverage html -i --directory=output
