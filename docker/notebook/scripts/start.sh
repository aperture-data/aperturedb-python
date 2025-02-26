#!/bin/bash

# Configure the Juypter Notebook password
jupyter lab --generate-config

echo "c.ServerApp.base_url='${BASE_URL:-/}'">> /root/.jupyter/jupyter_lab_config.py

PASS_HASH=$(python3 -c "from jupyter_server.auth import passwd; print(passwd('${PASSWORD:-test}'))")
echo "c.NotebookApp.password='${PASS_HASH}'">> /root/.jupyter/jupyter_lab_config.py

NOTEBOOK_DIR=${notebook_dir:-/notebooks}
mkdir -p ${NOTEBOOK_DIR}
echo "c.NotebookApp.notebook_dir='${NOTEBOOK_DIR}'">> /root/.jupyter/jupyter_lab_config.py

cd ${HOME}
jupyter-lab --port=8888 --no-browser --allow-root --ip=0.0.0.0
