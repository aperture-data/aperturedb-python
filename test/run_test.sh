#!/bin/bash

set -u
set -e

rm -rf output
mkdir output
mkdir -p input/blobs

echo "Downloading images..."
python3 download_images.py          # Test ImageDownloader
echo "Done downloading images."

echo "Generating input files..."
python3 generateInput.py
echo "Done generating input files."

echo "Running tests..."
KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_*.py -v
