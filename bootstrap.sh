#!/bin/sh
set -e

export FLASK_APP=./main/trips.py
pipenv run flask run -h 0.0.0.0 -p 8000