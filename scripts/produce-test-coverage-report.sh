#!/bin/bash
./scripts/pip-install-requirements.sh
pip install -q nose
nosetests --with-coverage --cover-package=cookiemonster --cover-html