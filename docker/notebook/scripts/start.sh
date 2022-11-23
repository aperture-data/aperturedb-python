#!/bin/bash

mkdir /notebooks
cd /notebooks
jupyter-lab --port=8888 --no-browser --allow-root --ip=0.0.0.0
