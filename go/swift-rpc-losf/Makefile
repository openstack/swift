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

.PHONY: all build

all: build

build:
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc -I ../../swift/obj fmgr.proto --go_out=plugins=grpc:proto
	go get
	go build -o ../../swift-rpc-losf
