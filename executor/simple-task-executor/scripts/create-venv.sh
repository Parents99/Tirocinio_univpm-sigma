#!/bin/bash
pip install virtualenv
virtualenv -p ${1:-python3} $PARTNER_VOLUME_PATH/venvs/${2:-venv}
source $PARTNER_VOLUME_PATH/venvs/${2:-venv}/bin/activate
pip install -r base-requirements.txt
pip install -r ${3:-requirements.txt}
deactivate
mv ${3:-requirements.txt} $PARTNER_VOLUME_PATH/venvs/${2:-venv}/
