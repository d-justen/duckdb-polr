#!/bin/bash

VENV_PATH="$PWD/venv"

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python Virtual Environment"
  python3 -m venv $VENV_PATH
  source "$VENV_PATH/bin/activate"
  pip install pip --upgrade > /dev/null
  pip -q install -r requirements.txt
  echo "$HOSTNAME"
fi
