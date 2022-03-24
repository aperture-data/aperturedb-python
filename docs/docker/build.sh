#!/bin/bash

set -e

VERSION=$1

mkdir -p build/docs
cp ../../{setup.py,README.md} build
cp  ../{*.*,Makefile} build/docs
cp -r ../../aperturedb build

docker build -t aperturedata/aperturedb-python-docs:v$VERSION .

rm -rf html