#!/bin/bash

# Configure the Juypter Notebook password
jupyter lab --generate-config

PASS_HASH=$(python3 -c "from jupyter_server.auth import passwd; print(passwd('${PASSWORD:-test}'))")
echo "c.NotebookApp.password='${PASS_HASH}' ">> /root/.jupyter/jupyter_lab_config.py

mkdir /notebooks
cd /notebooks
jupyter-lab --port=8888 --no-browser --allow-root --ip=0.0.0.0
