# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all build-snappy build-leveldb build-losf install install-snappy install-leveldb

all: build-snappy build-leveldb build-losf

install: install-snappy install-leveldb

build-losf:
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc -I ../../swift/obj fmgr.proto --go_out=plugins=grpc:proto
	go get
	go build -o ../../bin/swift-rpc-losf

# assuming installation will be taken by setup.py when swift-rpc-losf is
# included bin/ directory
# install-losf:

build-snappy:
	git submodule update --init snappy
	sed -i 's/\(BUILD_SHARED_LIBS "Build.*\) OFF/\1 ON/' snappy/CMakeLists.txt
	mkdir -p snappy/build
	cmake -S snappy -B snappy/build
	$(MAKE) -C snappy/build all

install-snappy:
	$(MAKE) -C snappy/build install

build-leveldb:
	git submodule update --init leveldb
	# add set(BUILD_SHARED_LIBS ON) to leveldb/CMakeLists.txt (only if it doesn't exist)
	grep -q -x -F 'set(BUILD_SHARED_LIBS ON)' leveldb/CMakeLists.txt || echo 'set(BUILD_SHARED_LIBS ON)' >> leveldb/CMakeLists.txt
	mkdir -p leveldb/build
	cmake -S leveldb -B leveldb/build
	$(MAKE) -C leveldb/build all
	# TODO: Make sure if it should run after leveldb install
	CGO_CFLAGS=/usr/local/include CGO_LDFLAGS="-L/usr/local/lib" go get github.com/jmhodges/levigo

install-leveldb:
	$(MAKE) -C leveldb/build install
