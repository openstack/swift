#!/bin/bash
# Copyright (c) 2014 OpenStack Foundation
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

cd $(readlink -f $(dirname $0))

. ./swift3.config

CONF_DIR=$(readlink -f ./conf)

rm -rf $TEST_DIR
mkdir -p ${TEST_DIR}/etc ${TEST_DIR}/log
mkdir -p ${TEST_DIR}/sda ${TEST_DIR}/sdb ${TEST_DIR}/sdc
mkdir -p ${TEST_DIR}/certs ${TEST_DIR}/private

# create config files
if [ "$AUTH" == 'keystone' ]; then
    MIDDLEWARE="swift3 s3token keystoneauth"
elif [ "$AUTH" == 'tempauth' ]; then
    MIDDLEWARE="swift3 tempauth"
else
    echo "unknown auth: $AUTH"
    exit 1
fi

for server in keystone swift proxy-server object-server container-server account-server; do
    sed -e "s#%MIDDLEWARE%#${MIDDLEWARE}#g" \
	-e "s#%S3ACL%#${S3ACL}#g" \
	-e "s#%DNS_BUCKET_NAMES%#${DNS_BUCKET_NAMES}#g" \
	-e "s#%CHECK_BUCKET_OWNER%#${CHECK_BUCKET_OWNER}#g" \
	-e "s#%USER%#`whoami`#g" \
	-e "s#%TEST_DIR%#${TEST_DIR}#g" \
	-e "s#%CONF_DIR%#${CONF_DIR}#g" \
	-e "s#%MIN_SEGMENT_SIZE%#${MIN_SEGMENT_SIZE}#g" \
	conf/${server}.conf.in \
	> conf/${server}.conf
done

# setup keystone
if [ "$AUTH" == 'keystone' ]; then
    . ./setup_keystone
fi

sed \
-e "s#%ADMIN_ACCESS_KEY%#${ADMIN_ACCESS_KEY:-test:tester}#g" \
-e "s#%ADMIN_SECRET_KEY%#${ADMIN_SECRET_KEY:-testing}#g" \
-e "s#%TESTER_ACCESS_KEY%#${TESTER_ACCESS_KEY:-test:tester2}#g" \
-e "s#%TESTER_SECRET_KEY%#${TESTER_SECRET_KEY:-testing2}#g" \
conf/ceph-s3.conf.in > conf/ceph-s3.conf

# build ring
cd ${TEST_DIR}/etc/

swift-ring-builder object.builder create 0 3 0
swift-ring-builder container.builder create 0 3 0
swift-ring-builder account.builder create 0 3 0

swift-ring-builder object.builder add r1z0-127.0.0.1:6000/sda 1
swift-ring-builder object.builder add r1z1-127.0.0.1:6000/sdb 1
swift-ring-builder object.builder add r1z2-127.0.0.1:6000/sdc 1
swift-ring-builder container.builder add r1z0-127.0.0.1:6001/sda 1
swift-ring-builder container.builder add r1z1-127.0.0.1:6001/sdb 1
swift-ring-builder container.builder add r1z2-127.0.0.1:6001/sdc 1
swift-ring-builder account.builder add r1z0-127.0.0.1:6002/sda 1
swift-ring-builder account.builder add r1z1-127.0.0.1:6002/sdb 1
swift-ring-builder account.builder add r1z2-127.0.0.1:6002/sdc 1

swift-ring-builder object.builder rebalance
swift-ring-builder container.builder rebalance
swift-ring-builder account.builder rebalance

cd -

# start swift servers

_start()
{
    local name=$1; shift
    local log_file="${LOG_DEST:-${TEST_DIR}/log}/${name}.log"
    mkdir -p "$(dirname "${log_file}")"

    echo Start ${name}-server.
    "$@" > "${log_file}" 2>&1 &
    export ${name}_pid=$!

    local cnt
    for cnt in `seq 60`; do # wait at most 60 seconds
	if ! grep 'Started child' "${log_file}" > /dev/null ; then
	    return
	fi
	sleep 1
    done

    cat "${log_file}"
    echo "Cannot start ${name}-server."
    exit 1
}

_start account ./run_daemon.py account 6002 conf/account-server.conf -v
_start container ./run_daemon.py container 6001 conf/container-server.conf -v
_start object ./run_daemon.py object 6000 conf/object-server.conf -v

coverage erase
_start proxy coverage run --branch --include=../../*  --omit=./* \
    ./run_daemon.py proxy 8080 conf/proxy-server.conf -v

# run tests
if [ -z "$CEPH_TESTS" ]; then
    nosetests -v "$@"
    rvalue=$?

    # show report
    coverage report
    coverage html
else
    set -e
    pushd ${TEST_DIR}
    git clone https://github.com/swiftstack/s3compat.git
    popd
    pushd ${TEST_DIR}/s3compat
    git submodule update --init

    # ceph/s3-tests has some rather ancient requirements,
    # so drop into another virtualenv
    # TODO: this may no longer be necessary?
    VENV="$(mktemp -d)"
    virtualenv "$VENV"
    . "$VENV/bin/activate"
    pip install -r requirements.txt
    pip freeze

    S3TEST_CONF="${CONF_DIR}/ceph-s3.conf" ./bin/run_ceph_tests.py "$@" || true

    # show report
    ./bin/get_ceph_test_attributes.py
    ./bin/report.py --detailed output/ceph-s3.out.yaml \
        --known-failures "${CONF_DIR}/ceph-known-failures-${AUTH}.yaml" \
        --detailedformat console output/ceph-s3.out.xml | \
        tee "${LOG_DEST:-${TEST_DIR}/log}/ceph-s3-summary.log"

    # the report's exit code indicates NEW_FAILUREs / UNEXPECTED_PASSes
    rvalue=${PIPESTATUS[0]}

    cp output/ceph-s3.out.xml "${LOG_DEST:-${TEST_DIR}/log}/ceph-s3-details.xml"
    popd
    rm -rf "$VENV"
    set +e
fi

# cleanup
kill -HUP $proxy_pid $account_pid $container_pid $object_pid
if [ -n "$keystone_pid" ]; then
    kill -TERM $keystone_pid
fi

sleep 3

exit $rvalue
