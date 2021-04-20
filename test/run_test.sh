#!/bin/bash

set -u

DOCKERIMGNAME=aperturedb-py-test
docker stop $DOCKERIMGNAME && docker rm $DOCKERIMGNAME

sudo rm -rf aperturedb/db
rm -rf output
mkdir output

docker run         \
    --privileged   \
    -p 55555:55555 \
    -p 80:80       \
    -t -d          \
    --name $DOCKERIMGNAME \
    --mount src=/home/remis/ad/py-aperturedb/test/aperturedb,target="/aperturedb/",type=bind \
    aperturedata/aperturedb:v0.5.2-webui

echo "Generating input files..."
python3 generateInput.py
echo "Done generating input files."

echo "Running tests..."
python3 -m unittest discover --pattern=test_*.py -v

# Comment the following lines to check on the WebUI after tests are done.
docker stop $DOCKERIMGNAME && docker rm $DOCKERIMGNAME
sudo rm -rf aperturedb/db
