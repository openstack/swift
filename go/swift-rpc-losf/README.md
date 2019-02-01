This is the RPC server part of the "LOSF" (Lots Of Small Files) work.

Setup
=====
You will need a working golang environment, gcc and tools (build-essential), and cmake >= 3.9 (ubuntu 16.04 has a version that is too old, get it from cmake.org)

GO>=1.11 is required to support go modules (https://github.com/golang/go/wiki/Modules).

Run `make` command in this directory
```
make
sudo make install
```

Usage ****OUTDATED*****
=====
Currently it does not read the ring, you need to start one process per disk and policy on your object-server.
For example : swift-rpc-losf -diskPath=/srv/node/sda -policyIdx=0 -waitForMount=false

Note that a new database is marked dirty, because there may already be data on disk. (original db may have been removed or corrupted)
