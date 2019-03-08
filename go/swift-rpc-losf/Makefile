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

.PHONY: all build-snappy install-snappy build-leveldb install-leveldb install-go-leveldb build-losf
all: build-snappy install-snappy build-leveldb install-leveldb install-go-leveldb build-losf

build-losf:
	# this installs the protoc-gen-go in $HOME/go/bin, and requires the PATH be set accordingly
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc -I ../../swift/obj fmgr.proto --go_out=proto
	go get
	go build -o ../../bin/swift-rpc-losf

# TODO: installation will be taken by setup.py when swift-rpc-losf is
# included bin/ directory

build-snappy:
	git submodule update --init snappy
	sed -i 's/\(BUILD_SHARED_LIBS "Build.*\) OFF/\1 ON/' snappy/CMakeLists.txt
	mkdir -p snappy/build
	cmake -S snappy -B snappy/build
	$(MAKE) -C snappy/build all

install-snappy:
	sudo $(MAKE) -C snappy/build install

build-leveldb:
	git submodule update --init leveldb
	mkdir -p leveldb/build
	cmake -DBUILD_SHARED_LIBS=ON -S leveldb -B leveldb/build
	cmake --build leveldb/build

install-leveldb:
	sudo $(MAKE) -C leveldb/build install

install-go-leveldb:
	CGO_CFLAGS=/usr/local/include CGO_LDFLAGS="-L/usr/local/lib -Wl,-rpath=/usr/local/lib" go get github.com/jmhodges/levigo
