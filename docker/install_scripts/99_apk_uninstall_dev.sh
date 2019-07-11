#!/bin/sh
set -e

cd /
rm -rf /build

apk del gnupg
apk del git
apk del openssl-dev
apk del sqlite-dev
apk del zlib-dev
apk del g++
apk del libffi-dev
apk del libxslt-dev
apk del libxml2-dev
apk del python-dev
rm -rf /var/cache/apk/*
