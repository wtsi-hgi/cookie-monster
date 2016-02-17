#!/bin/bash
pip install -q -r requirements.txt
pip install -q -r test_requirements.txt

nosetests -v --with-coverage --cover-package=cookiemonster --cover-html --cover-inclusive