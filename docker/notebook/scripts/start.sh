#!/bin/bash

mkdir /notebooks
cd /notebooks
jupyter notebook --port=8888 --no-browser --allow-root --ip=0.0.0.0
