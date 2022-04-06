#!/bin/bash

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

#Generate the docs from the docstrings.
make clean && make html

