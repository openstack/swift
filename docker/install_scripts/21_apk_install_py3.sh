#!/bin/sh
set -e

apk add --update \
  python3 \
  python3-dev \
  py3-pip \
  py3-cffi \
  py3-cryptography

if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi

