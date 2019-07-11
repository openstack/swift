#!/bin/sh
set -e

echo
echo
echo
echo "building python and pip"

# export PATH=$PATH:/usr/include

mkdir $BUILD_DIR/python27
mkdir $BUILD_DIR/python36

echo
echo
echo
echo "building python 2.7.15"

cd $BUILD_DIR/python27
wget https://www.python.org/ftp/python/2.7.15/Python-2.7.15.tgz
tar -zxvf Python-2.7.15.tgz
cd Python-2.7.15
./configure --enable-optimizations
make
make DESTDIR=/opt/python27 install

echo
echo
echo
echo "building python 3.6.5"

cd $BUILD_DIR/python36
wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz
tar -zxvf Python-3.6.5.tgz
cd Python-3.6.5
./configure --enable-optimizations
make
make DESTDIR=/opt/python36 install

export PATH=$PATH:/opt/python27/usr/local/bin:/opt/python36/usr/local/bin
echo "export PATH=$PATH:/opt/python27/usr/local/bin:/opt/python36/usr/local/bin" >> /etc/profile

echo
echo
echo
echo "building pip"
wget https://bootstrap.pypa.io/get-pip.py
python ./get-pip.py

echo
echo
echo
echo "deleting python internal test dirs"
for f in `cat /opt/swift/docker/install_scripts/python_test_dirs` ; do rm -rf $f; done

rm -rf $BUILD_DIR/python27
rm -rf $BUILD_DIR/python36
