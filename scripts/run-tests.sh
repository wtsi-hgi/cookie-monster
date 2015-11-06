#!/bin/bash
./scripts/pip-install-requirements.sh
pip install -q nose
nosetests -v