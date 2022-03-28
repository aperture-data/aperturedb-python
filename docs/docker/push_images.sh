#!/bin/bash

# This script pushes the docker images to dockerhub

VERSION=$1

echo "Pushing Docker images for ApertureDB python docs version:" $VERSION

docker push aperturedata/aperturedb-python-docs:v$VERSION
