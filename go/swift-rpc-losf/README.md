This is the RPC server part of the "LOSF" (Lots Of Small Files) work.

Setup
=====
You will need a working golang environment, gcc and tools (build-essential), and cmake >= 3.9 (ubuntu 16.04 has a version that is too old, get it from cmake.org)

Get protobuf from https://github.com/google/protobuf/releases and make "protoc" available in your $PATH
```
go get -u github.com/golang/protobuf/protoc-gen-go
```

Get snappy, the compression library used by leveldb
```
git clone https://github.com/google/snappy.git
cd snappy
sed -i 's/\(BUILD_SHARED_LIBS "Build.*\) OFF/\1 ON/' CMakeLists.txt
mkdir build
cd build
cmake ..
make
make install (sudo)
```

Get leveldb. It seems to have switched to cmake (march 2018)
```
git clone https://github.com/google/leveldb.git
cd leveldb
add set(BUILD_SHARED_LIBS ON) to CMakeLists.txt
mkdir build
cd build
cmake ..
make
make install (sudo)
```

You should now have a libleveldb.so with snappy support available. (may require setting LD_LIBRARY_PATH depending on your system's configuration)

Get the levigo package (golang wrapper for leveldb)
```
CGO_CFLAGS=/usr/local/include CGO_LDFLAGS="-L/usr/local/lib" go get github.com/jmhodges/levigo
```

Make a link for the swift RPC golang code to your go environment, for example :

`ln -s /path/to/swift/go/swift-rpc-losf $GOPATH/src/github.com/openstack/`

Generate code from the proto file (This is optional, the generated files are included in this branch)
Then build the binary
```
cd $GOPATH/src/github.com/openstack
protoc -I /path/to/swift/swift/obj fmgr.proto --go_out=plugins=grpc:proto
go get
go build
```

Usage ****OUTDATED*****
=====
Currently it does not read the ring, you need to start one process per disk and policy on your object-server.
For example : swift-rpc-losf -diskPath=/srv/node/sda -policyIdx=0 -waitForMount=false

Note that a new database is marked dirty, because there may already be data on disk. (original db may have been removed or corrupted)
