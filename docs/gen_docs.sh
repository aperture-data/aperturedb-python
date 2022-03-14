#!/bin/bash

cd ..
#Install the dependencies
pip install -r requirements.txt

#Install aperturedb into site packages.
python setup.py install
cd -

#Generate the docs from the docstrings.
sphinx-apidoc -f -o . ../aperturedb
make clean && make html