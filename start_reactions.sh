#!/bin/bash

PYTHONPATH=$PYTHONPATH:.

pipenv run python3 ./src/index.py -c crawler_reactions -s facebook
