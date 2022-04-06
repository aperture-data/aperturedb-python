#!/bin/bash

set -e

VERSION=$1
if [ -z "$VERSION" ]
    then
    VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}'  ../../aperturedb/__init__.py | tr -d '"')
fi


mkdir -p build/docs
cp -r ../../{setup.py,README.md,aperturedb} build
cp -r ../{*.*,Makefile,_static} build/docs

docker build -t aperturedata/aperturedb-python-docs:v$VERSION .

rm -rf html