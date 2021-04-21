#!/bin/bash

VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}' ../aperturedb/__init__.py | tr -d '"')

echo "Building Jupyter Notebook docker image..."
( cd notebook && \
  docker build -t aperturedata/aperturedb-notebook:v$VERSION . )

docker push aperturedata/aperturedb-notebook:v$VERSION
