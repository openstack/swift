#!/bin/sh
set -e

echo "+ + + + + + + + + + upgrading pip" && \
pip install -U pip && \
cd /opt/swift && \
pip install -r requirements.txt

#echo "+ + + + + + + + + + installing pastedeploy" && \
#pip install pastedeploy && \
#echo "+ + + + + + + + + + installing eventlet" && \
#pip install eventlet && \
#echo "+ + + + + + + + + + installing greenlet" && \
#pip install greenlet && \
#echo "+ + + + + + + + + + installing netifaces" && \
#pip install netifaces && \
#echo "+ + + + + + + + + + installing setuptools" && \
#pip install setuptools && \
#echo "+ + + + + + + + + + installing requests" && \
#pip install requests && \
#echo "+ + + + + + + + + + installing six" && \
#pip install six && \
#echo "+ + + + + + + + + + installing cryptography" && \
#pip install cryptography && \
#echo "+ + + + + + + + + + installing dnspython" && \
#pip install dnspython
#echo "+ + + + + + + + + + installing xattr" && \
#pip install xattr
#echo "+ + + + + + + + + + installing pyeclib" && \
#pip install pyeclib
#echo "+ + + + + + + + + + installing lxml" && \
#pip install lxml
