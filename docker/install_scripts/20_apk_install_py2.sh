#!/bin/sh
set -e

apk add --update \
  python \
  python-dev \
  py-pip \
  py-cffi \
  py-cryptography
