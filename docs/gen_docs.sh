#!/bin/bash

set -e

#Install the dependencies
python3 -m venv env
. env/bin/activate
pip install -r requirements-documentation.txt

#Install aperturedb into site packages.
cd ..
python setup.py install
cd -

cd _static
dot -Tsvg parallelizer.dot > parallelizer.svg
cd -

#Convert examples to docs.
rm -rf examples
mkdir examples
cd examples
find ../../examples/ -name *.ipynb | xargs -i cp {} .
jupyter nbconvert --to rst *.ipynb
cd -

#Generate the docs from the docstrings.
make clean && make html

