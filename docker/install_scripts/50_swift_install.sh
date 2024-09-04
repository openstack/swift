#!/bin/sh
set -e

pip install -U pip
cd /opt/swift
pip install -r requirements.txt
pip install -e .

cp doc/saio/bin/* $HOME/bin
chmod +x $HOME/bin/*
sed -i "s/bash/sh/g" $HOME/bin/*
sed -i "s/sudo //g" $HOME/bin/*
mkdir /root/tmp
echo "export PATH=${PATH}:$HOME/bin" >> $HOME/.shrc
echo "export PYTHON_EGG_CACHE=/root/tmp" >> $HOME/.shrc
echo "export ENV=$HOME/.shrc" >> $HOME/.profile
chmod +x $HOME/.shrc
chmod +x $HOME/.profile
