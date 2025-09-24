#!/bin/bash

# apt update && apt install -y python3-pip
# python3 -m venv /opt/venv
# source /opt/venv/bin/activate
pip install -e .

adb config create tcp --host=${DB_HOST} --port=${DB_PORT} --ca-cert=${CA_CERT} --no-interactive
adb config create http --host=nginx --port=443 --use-rest --ca-cert=${CA_CERT} --no-interactive

adb --install-completion
