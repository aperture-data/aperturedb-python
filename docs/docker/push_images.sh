#!/bin/bash

# This script pushes the docker images to dockerhub

VERSION=$1
if [ -z "$VERSION" ]
    then
    VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}'  ../../aperturedb/__init__.py | tr -d '"')
fi

echo "Pushing Docker images for ApertureDB python docs version:" $VERSION

docker push aperturedata/aperturedb-python-docs:v$VERSION
