#!/usr/bin/env bash

# This comes from mounted directory
USER_REQ_FILE="/app/workspace/requirements.txt"
VENV_PATH=/opt/venv

python3 -m venv $VENV_PATH
source $VENV_PATH/bin/activate

# Installing jupyter
pip install jupyterlab

# Installing edk requirements
pip install -r /app/edk-requirements.txt

pip install -e /app/earth-data-kit # Installing earth_data_kit in editable mode

pip install -r "$USER_REQ_FILE" # Installing users requirements

echo "EDK is ready to be used"

# Tailing so container keeps running
tail -f /dev/null
