#!/bin/bash

set -u
set -e

pip3 install --upgrade pip
(cd .. && pip3 install --upgrade .)

sudo rm -rf aperturedb/db
rm -rf output
mkdir output
mkdir -p input/blobs

docker-compose down && docker-compose up -d

echo "Downloading images..."
python3 download_images.py          # Test ImageDownloader
echo "Done downloading images."

echo "Generating input files..."
python3 generateInput.py
echo "Done generating input files."

echo "Running tests..."
python3 -m unittest discover --pattern=test_*.py -v
