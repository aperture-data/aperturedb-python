#!/bin/bash

set -e

VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}' ../aperturedb/__init__.py | tr -d '"')

DEPENDECIES_IMAGE="aperturedata/aperturedb-notebook:dependencies"

echo "Building dependencies"
docker pull $DEPENDECIES_IMAGE
( cd dependencies && \
  docker build --cache-from $DEPENDECIES_IMAGE -t $DEPENDECIES_IMAGE . )

echo "Building Jupyter Notebook docker image..."
( cd notebook && \
  docker build -t aperturedata/aperturedb-notebook:v$VERSION . )

docker push $DEPENDECIES_IMAGE
docker push aperturedata/aperturedb-notebook:v$VERSION
