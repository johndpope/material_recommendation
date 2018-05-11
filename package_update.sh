#!/bin/sh

thrift -r --out . --gen py material_recommendation_service.thrift
python setup.py sdist upload -r cy
