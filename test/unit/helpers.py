# Copyright (c) 2010-2016 OpenStack Foundation
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

"""
Provides helper functions for unit tests.

This cannot be in test/unit/__init__.py because that module is imported by the
py34 unit test job and there are imports here that end up importing modules
that are not yet ported to py34, such wsgi.py which import mimetools.
"""
import os
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
import time


from eventlet import spawn, wsgi
import mock
from shutil import rmtree
import six.moves.cPickle as pickle

import swift
from swift.account import server as account_server
from swift.common import storage_policy
from swift.common.ring import RingData
from swift.common.storage_policy import StoragePolicy, ECStoragePolicy
from swift.common.middleware import listing_formats, proxy_logging
from swift.common import utils
from swift.common.utils import mkdirs, normalize_timestamp, NullLogger
from swift.common.wsgi import SwiftHttpProtocol
from swift.container import server as container_server
from swift.obj import server as object_server
from swift.proxy import server as proxy_server
import swift.proxy.controllers.obj

from test import listen_zero
from test.unit import write_fake_ring, DEFAULT_TEST_EC_TYPE, debug_logger, \
    connect_tcp, readuntil2crlfs


def setup_servers(the_object_server=object_server, extra_conf=None):
    """
    Setup proxy, account, container and object servers using a set of fake
    rings and policies.

    :param the_object_server: The object server module to use (optional,
                              defaults to swift.obj.server)
    :param extra_conf: A dict of config options that will update the basic
                       config passed to all server instances.
    :returns: A dict containing the following entries:
                  orig_POLICIES: the value of storage_policy.POLICIES prior to
                                 it being patched with fake policies
                  orig_SysLogHandler: the value of utils.SysLogHandler prior to
                                      it being patched
                  testdir: root directory used for test files
                  test_POLICIES: a StoragePolicyCollection of fake policies
                  test_servers: a tuple of test server instances
                  test_sockets: a tuple of sockets used by test servers
                  test_coros: a tuple of greenthreads in which test servers are
                              running
    """
    context = {
        "orig_POLICIES": storage_policy._POLICIES,
        "orig_SysLogHandler": utils.SysLogHandler}

    utils.HASH_PATH_SUFFIX = b'endcap'
    utils.SysLogHandler = mock.MagicMock()
    # Since we're starting up a lot here, we're going to test more than
    # just chunked puts; we're also going to test parts of
    # proxy_server.Application we couldn't get to easily otherwise.
    context["testdir"] = _testdir = \
        os.path.join(mkdtemp(), 'tmp_test_proxy_server_chunked')
    mkdirs(_testdir)
    rmtree(_testdir)
    for drive in ('sda1', 'sdb1', 'sdc1', 'sdd1', 'sde1',
                  'sdf1', 'sdg1', 'sdh1', 'sdi1', 'sdj1',
                  'sdk1', 'sdl1'):
        mkdirs(os.path.join(_testdir, drive, 'tmp'))
    conf = {'devices': _testdir, 'swift_dir': _testdir,
            'mount_check': 'false', 'allowed_headers':
            'content-encoding, x-object-manifest, content-disposition, foo',
            'allow_versions': 't'}
    if extra_conf:
        conf.update(extra_conf)
    prolis = listen_zero()
    acc1lis = listen_zero()
    acc2lis = listen_zero()
    con1lis = listen_zero()
    con2lis = listen_zero()
    obj1lis = listen_zero()
    obj2lis = listen_zero()
    obj3lis = listen_zero()
    obj4lis = listen_zero()
    obj5lis = listen_zero()
    obj6lis = listen_zero()
    objsocks = [obj1lis, obj2lis, obj3lis, obj4lis, obj5lis, obj6lis]
    context["test_sockets"] = \
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis, obj3lis,
         obj4lis, obj5lis, obj6lis)
    account_ring_path = os.path.join(_testdir, 'account.ring.gz')
    account_devs = [
        {'port': acc1lis.getsockname()[1]},
        {'port': acc2lis.getsockname()[1]},
    ]
    write_fake_ring(account_ring_path, *account_devs)
    container_ring_path = os.path.join(_testdir, 'container.ring.gz')
    container_devs = [
        {'port': con1lis.getsockname()[1]},
        {'port': con2lis.getsockname()[1]},
    ]
    write_fake_ring(container_ring_path, *container_devs)
    storage_policy._POLICIES = storage_policy.StoragePolicyCollection([
        StoragePolicy(0, 'zero', True),
        StoragePolicy(1, 'one', False),
        StoragePolicy(2, 'two', False),
        ECStoragePolicy(3, 'ec', ec_type=DEFAULT_TEST_EC_TYPE,
                        ec_ndata=2, ec_nparity=1, ec_segment_size=4096),
        ECStoragePolicy(4, 'ec-dup', ec_type=DEFAULT_TEST_EC_TYPE,
                        ec_ndata=2, ec_nparity=1, ec_segment_size=4096,
                        ec_duplication_factor=2)])
    obj_rings = {
        0: ('sda1', 'sdb1'),
        1: ('sdc1', 'sdd1'),
        2: ('sde1', 'sdf1'),
        # sdg1, sdh1, sdi1 taken by policy 3 (see below)
    }
    for policy_index, devices in obj_rings.items():
        policy = storage_policy.POLICIES[policy_index]
        obj_ring_path = os.path.join(_testdir, policy.ring_name + '.ring.gz')
        obj_devs = [
            {'port': objsock.getsockname()[1], 'device': dev}
            for objsock, dev in zip(objsocks, devices)]
        write_fake_ring(obj_ring_path, *obj_devs)

    # write_fake_ring can't handle a 3-element ring, and the EC policy needs
    # at least 6 devs to work with (ec_k=2, ec_m=1, duplication_factor=2),
    # so we do it manually
    devs = [{'id': 0, 'zone': 0, 'device': 'sdg1', 'ip': '127.0.0.1',
             'port': obj1lis.getsockname()[1]},
            {'id': 1, 'zone': 0, 'device': 'sdh1', 'ip': '127.0.0.1',
             'port': obj2lis.getsockname()[1]},
            {'id': 2, 'zone': 0, 'device': 'sdi1', 'ip': '127.0.0.1',
             'port': obj3lis.getsockname()[1]},
            {'id': 3, 'zone': 0, 'device': 'sdj1', 'ip': '127.0.0.1',
             'port': obj4lis.getsockname()[1]},
            {'id': 4, 'zone': 0, 'device': 'sdk1', 'ip': '127.0.0.1',
             'port': obj5lis.getsockname()[1]},
            {'id': 5, 'zone': 0, 'device': 'sdl1', 'ip': '127.0.0.1',
             'port': obj6lis.getsockname()[1]}]
    pol3_replica2part2dev_id = [[0, 1, 2, 0],
                                [1, 2, 0, 1],
                                [2, 0, 1, 2]]
    pol4_replica2part2dev_id = [[0, 1, 2, 3],
                                [1, 2, 3, 4],
                                [2, 3, 4, 5],
                                [3, 4, 5, 0],
                                [4, 5, 0, 1],
                                [5, 0, 1, 2]]
    obj3_ring_path = os.path.join(
        _testdir, storage_policy.POLICIES[3].ring_name + '.ring.gz')
    part_shift = 30
    with closing(GzipFile(obj3_ring_path, 'wb')) as fh:
        pickle.dump(RingData(pol3_replica2part2dev_id, devs, part_shift), fh)

    obj4_ring_path = os.path.join(
        _testdir, storage_policy.POLICIES[4].ring_name + '.ring.gz')
    part_shift = 30
    with closing(GzipFile(obj4_ring_path, 'wb')) as fh:
        pickle.dump(RingData(pol4_replica2part2dev_id, devs, part_shift), fh)

    prosrv = proxy_server.Application(conf, logger=debug_logger('proxy'))
    for policy in storage_policy.POLICIES:
        # make sure all the rings are loaded
        prosrv.get_object_ring(policy.idx)
    # don't lose this one!
    context["test_POLICIES"] = storage_policy._POLICIES
    acc1srv = account_server.AccountController(
        conf, logger=debug_logger('acct1'))
    acc2srv = account_server.AccountController(
        conf, logger=debug_logger('acct2'))
    con1srv = container_server.ContainerController(
        conf, logger=debug_logger('cont1'))
    con2srv = container_server.ContainerController(
        conf, logger=debug_logger('cont2'))
    obj1srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj1'))
    obj2srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj2'))
    obj3srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj3'))
    obj4srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj4'))
    obj5srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj5'))
    obj6srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj6'))
    context["test_servers"] = \
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv, obj2srv, obj3srv,
         obj4srv, obj5srv, obj6srv)
    nl = NullLogger()
    logging_prosv = proxy_logging.ProxyLoggingMiddleware(
        listing_formats.ListingFilter(prosrv), conf, logger=prosrv.logger)
    prospa = spawn(wsgi.server, prolis, logging_prosv, nl,
                   protocol=SwiftHttpProtocol)
    acc1spa = spawn(wsgi.server, acc1lis, acc1srv, nl,
                    protocol=SwiftHttpProtocol)
    acc2spa = spawn(wsgi.server, acc2lis, acc2srv, nl,
                    protocol=SwiftHttpProtocol)
    con1spa = spawn(wsgi.server, con1lis, con1srv, nl,
                    protocol=SwiftHttpProtocol)
    con2spa = spawn(wsgi.server, con2lis, con2srv, nl,
                    protocol=SwiftHttpProtocol)
    obj1spa = spawn(wsgi.server, obj1lis, obj1srv, nl,
                    protocol=SwiftHttpProtocol)
    obj2spa = spawn(wsgi.server, obj2lis, obj2srv, nl,
                    protocol=SwiftHttpProtocol)
    obj3spa = spawn(wsgi.server, obj3lis, obj3srv, nl,
                    protocol=SwiftHttpProtocol)
    obj4spa = spawn(wsgi.server, obj4lis, obj4srv, nl,
                    protocol=SwiftHttpProtocol)
    obj5spa = spawn(wsgi.server, obj5lis, obj5srv, nl,
                    protocol=SwiftHttpProtocol)
    obj6spa = spawn(wsgi.server, obj6lis, obj6srv, nl,
                    protocol=SwiftHttpProtocol)
    context["test_coros"] = \
        (prospa, acc1spa, acc2spa, con1spa, con2spa, obj1spa, obj2spa, obj3spa,
         obj4spa, obj5spa, obj6spa)
    # Create account
    ts = normalize_timestamp(time.time())
    partition, nodes = prosrv.account_ring.get_nodes('a')
    for node in nodes:
        conn = swift.proxy.controllers.obj.http_connect(node['ip'],
                                                        node['port'],
                                                        node['device'],
                                                        partition, 'PUT', '/a',
                                                        {'X-Timestamp': ts,
                                                         'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
    # Create another account
    # used for account-to-account tests
    ts = normalize_timestamp(time.time())
    partition, nodes = prosrv.account_ring.get_nodes('a1')
    for node in nodes:
        conn = swift.proxy.controllers.obj.http_connect(node['ip'],
                                                        node['port'],
                                                        node['device'],
                                                        partition, 'PUT',
                                                        '/a1',
                                                        {'X-Timestamp': ts,
                                                         'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
    # Create containers, 1 per test policy
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile('rwb')
    fd.write(b'PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
             b'Connection: close\r\nX-Auth-Token: t\r\n'
             b'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = b'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, "Expected '%s', encountered '%s'" % (
        exp, headers[:len(exp)])
    # Create container in other account
    # used for account-to-account tests
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile('rwb')
    fd.write(b'PUT /v1/a1/c1 HTTP/1.1\r\nHost: localhost\r\n'
             b'Connection: close\r\nX-Auth-Token: t\r\n'
             b'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = b'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, "Expected '%s', encountered '%s'" % (
        exp, headers[:len(exp)])

    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile('rwb')
    fd.write(
        b'PUT /v1/a/c1 HTTP/1.1\r\nHost: localhost\r\n'
        b'Connection: close\r\nX-Auth-Token: t\r\nX-Storage-Policy: one\r\n'
        b'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = b'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, \
        "Expected %r, encountered %r" % (exp, headers[:len(exp)])

    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile('rwb')
    fd.write(
        b'PUT /v1/a/c2 HTTP/1.1\r\nHost: localhost\r\n'
        b'Connection: close\r\nX-Auth-Token: t\r\nX-Storage-Policy: two\r\n'
        b'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = b'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, \
        "Expected '%s', encountered '%s'" % (exp, headers[:len(exp)])
    return context


def teardown_servers(context):
    for server in context["test_coros"]:
        server.kill()
    rmtree(os.path.dirname(context["testdir"]))
    utils.SysLogHandler = context["orig_SysLogHandler"]
    storage_policy._POLICIES = context["orig_POLICIES"]
