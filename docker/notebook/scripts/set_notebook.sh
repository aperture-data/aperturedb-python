#!/bin/bash

set -e

cd /aperturedata
pip install -e .


pip uninstall python_opencv
cd /build_cv/python_loader
pip uninstall --yes opencv_python
pip install -e .



