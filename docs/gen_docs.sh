#!/bin/bash

#Install the dependencies
pip install -r requirements-documentation.txt

#Install aperturedb into site packages.
cd ..
python setup.py install
cd -

#Generate the docs from the docstrings.
make clean && make html

