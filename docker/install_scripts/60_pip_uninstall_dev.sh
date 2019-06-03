#!/bin/sh
set -e

echo "- - - - - - - - uninstalling simplejson"
pip uninstall --yes simplejson
echo "- - - - - - - - uninstalling pyopenssl"
pip uninstall --yes pyopenssl
echo "- - - - - - - - deleting python3-dev residue (config-3.6m-x86_64-linux-gnu)"
rm -rf /opt/usr/local/lib/python3.6/config-3.6m-x86_64-linux-gnu/
