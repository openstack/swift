#!/bin/sh

# Get liberasurecode
cd $BUILD_DIR
git clone https://github.com/openstack/liberasurecode.git
cd liberasurecode/
./autogen.sh
./configure
make
#make test
make install
# cp -r /usr/local/lib /usr/lib/python3.6/site-packages/ && \
cd $BUILD_DIR
rm -rf $BUILD_DIR/liberasurecode
