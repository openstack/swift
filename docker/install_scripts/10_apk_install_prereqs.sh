#!/bin/sh
set -e

echo "@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories
apk add --update \
  linux-headers \
  liberasurecode@testing \
  liberasurecode-dev@testing \
  gnupg \
  git \
  curl \
  rsync \
  memcached \
  openssl \
  openssl-dev \
  sqlite \
  sqlite-libs \
  sqlite-dev \
  xfsprogs \
  zlib-dev \
  g++ \
  libffi \
  libffi-dev \
  libxslt \
  libxslt-dev \
  libxml2 \
  libxml2-dev \
