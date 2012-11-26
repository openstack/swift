#!/bin/bash

SRC_DIR=$(dirname $0)

cd ${SRC_DIR}/test/functional
nosetests --exe $@
func1=$?
cd -

cd ${SRC_DIR}/test/functionalnosetests
nosetests --exe $@
func2=$?
cd -

exit $((func1 + func2))
