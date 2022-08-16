#!/bin/sh
set -e

apk add --update \
  python2 \
  python2-dev
wget -O - https://bootstrap.pypa.io/pip/2.7/get-pip.py | python
pip install \
  cffi \
  cryptography
