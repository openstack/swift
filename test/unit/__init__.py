# Copyright (c) 2010-2012 OpenStack Foundation
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

""" Swift tests """

import os
import copy
import logging
import logging.handlers
import sys
from contextlib import contextmanager, closing
from collections import defaultdict
from collections.abc import Iterable
import itertools
from numbers import Number
from tempfile import NamedTemporaryFile
import time
import eventlet
from eventlet import greenpool, debug as eventlet_debug
from eventlet.green import socket
from tempfile import mkdtemp, mkstemp, gettempdir
from shutil import rmtree
import signal
import json
import random
import errno
import xattr
from io import BytesIO
from uuid import uuid4
import pickle
from http.client import HTTPException

from swift.common import storage_policy, swob, utils, exceptions
from swift.common.memcached import MemcacheConnectionError
from swift.common.storage_policy import (StoragePolicy, ECStoragePolicy,
                                         VALID_EC_TYPES)
from swift.common.utils import Timestamp, md5, close_if_possible, checksum
from test import get_config
from test.debug_logger import FakeLogger
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.ring import Ring, RingData, RingBuilder
from swift.obj import server

import functools
from gzip import GzipFile
from unittest import mock as mocklib
import inspect
from unittest import SkipTest


# try not to import this module from swift
if not os.path.basename(sys.argv[0]).startswith('swift'):
    # never patch HASH_PATH_SUFFIX AGAIN!
    utils.HASH_PATH_SUFFIX = b'endcap'


EC_TYPE_PREFERENCE = [
    'liberasurecode_rs_vand',
    'jerasure_rs_vand',
]
for eclib_name in EC_TYPE_PREFERENCE:
    if eclib_name in VALID_EC_TYPES:
        break
else:
    raise SystemExit('ERROR: unable to find suitable PyECLib type'
                     ' (none of %r found in %r)' % (
                         EC_TYPE_PREFERENCE,
                         VALID_EC_TYPES,
                     ))
DEFAULT_TEST_EC_TYPE = eclib_name


def patch_policies(thing_or_policies=None, legacy_only=False,
                   with_ec_default=False, fake_ring_args=None):
    if isinstance(thing_or_policies, (
            Iterable, storage_policy.StoragePolicyCollection)):
        return PatchPolicies(thing_or_policies, fake_ring_args=fake_ring_args)

    if legacy_only:
        default_policies = [
            StoragePolicy(0, name='legacy', is_default=True),
        ]
        default_ring_args = [{}]
    elif with_ec_default:
        default_policies = [
            ECStoragePolicy(0, name='ec', is_default=True,
                            ec_type=DEFAULT_TEST_EC_TYPE, ec_ndata=10,
                            ec_nparity=4, ec_segment_size=4096),
            StoragePolicy(1, name='unu'),
        ]
        default_ring_args = [{'replicas': 14}, {}]
    else:
        default_policies = [
            StoragePolicy(0, name='nulo', is_default=True),
            StoragePolicy(1, name='unu'),
        ]
        default_ring_args = [{}, {}]

    fake_ring_args = fake_ring_args or default_ring_args
    decorator = PatchPolicies(default_policies, fake_ring_args=fake_ring_args)

    if not thing_or_policies:
        return decorator
    else:
        # it's a thing, we return the wrapped thing instead of the decorator
        return decorator(thing_or_policies)


class PatchPolicies(object):
    """
    Why not mock.patch?  In my case, when used as a decorator on the class it
    seemed to patch setUp at the wrong time (i.e. in setUp the global wasn't
    patched yet)
    """

    def __init__(self, policies, fake_ring_args=None):
        if isinstance(policies, storage_policy.StoragePolicyCollection):
            self.policies = policies
        else:
            self.policies = storage_policy.StoragePolicyCollection(policies)
        self.fake_ring_args = fake_ring_args or [None] * len(self.policies)

    def _setup_rings(self):
        """
        Our tests tend to use the policies rings like their own personal
        playground - which can be a problem in the particular case of a
        patched TestCase class where the FakeRing objects are scoped in the
        call to the patch_policies wrapper outside of the TestCase instance
        which can lead to some bled state.

        To help tests get better isolation without having to think about it,
        here we're capturing the args required to *build* a new FakeRing
        instances so we can ensure each test method gets a clean ring setup.

        The TestCase can always "tweak" these fresh rings in setUp - or if
        they'd prefer to get the same "reset" behavior with custom FakeRing's
        they can pass in their own fake_ring_args to patch_policies instead of
        setting the object_ring on the policy definitions.
        """
        for policy, fake_ring_arg in zip(self.policies, self.fake_ring_args):
            if fake_ring_arg is not None:
                policy.object_ring = FakeRing(**fake_ring_arg)

    def __call__(self, thing):
        if isinstance(thing, type):
            return self._patch_class(thing)
        else:
            return self._patch_method(thing)

    def _patch_class(self, cls):
        """
        Creating a new class that inherits from decorated class is the more
        common way I've seen class decorators done - but it seems to cause
        infinite recursion when super is called from inside methods in the
        decorated class.
        """

        orig_setUp = cls.setUp

        def unpatch_cleanup(cls_self):
            if cls_self._policies_patched:
                self.__exit__(None, None, None)
                cls_self._policies_patched = False

        def setUp(cls_self):
            if not getattr(cls_self, '_policies_patched', False):
                self.__enter__()
                cls_self._policies_patched = True
                cls_self.addCleanup(unpatch_cleanup, cls_self)
            orig_setUp(cls_self)

        cls.setUp = setUp

        return cls

    def _patch_method(self, f):
        @functools.wraps(f)
        def mywrapper(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return mywrapper

    def __enter__(self):
        self._orig_POLICIES = storage_policy._POLICIES
        storage_policy._POLICIES = self.policies
        try:
            self._setup_rings()
        except:  # noqa
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *args):
        storage_policy._POLICIES = self._orig_POLICIES


class FakeRing(Ring):

    def __init__(self, replicas=3, max_more_nodes=0, part_power=0,
                 base_port=1000, separate_replication=False,
                 next_part_power=None, reload_time=15):
        self.serialized_path = '/foo/bar/object.ring.gz'
        self._base_port = base_port
        self.max_more_nodes = max_more_nodes
        self._part_shift = 32 - part_power
        self._init_device_char()
        self.separate_replication = separate_replication
        # 9 total nodes (6 more past the initial 3) is the cap, no matter if
        # this is set higher, or R^2 for R replicas
        self.reload_time = reload_time
        self.set_replicas(replicas)
        self._next_part_power = next_part_power
        self._reload()

    def has_changed(self):
        """
        The real implementation uses getmtime on the serialized_path attribute,
        which doesn't exist on our fake and relies on the implementation of
        _reload which we override.  So ... just NOOPE.
        """
        return False

    def _reload(self):
        self._rtime = time.time()

    @property
    def device_char(self):
        return next(self._device_char_iter)

    def _init_device_char(self):
        self._device_char_iter = itertools.cycle(
            ['sd%s' % chr(ord('a') + x) for x in range(26)])

    def add_node(self, dev):
        # round trip through json to ensure unicode like real rings
        self._devs.append(json.loads(json.dumps(dev)))

    def set_replicas(self, replicas):
        self.replicas = replicas
        self._devs = []
        self._init_device_char()
        for x in range(self.replicas):
            ip = '10.0.0.%s' % x
            port = self._base_port + x
            if self.separate_replication:
                repl_ip = '10.0.1.%s' % x
                repl_port = port + 100
            else:
                repl_ip, repl_port = ip, port
            dev = {
                'ip': ip,
                'replication_ip': repl_ip,
                'port': port,
                'replication_port': repl_port,
                'device': self.device_char,
                'zone': x % 3,
                'region': x % 2,
                'id': x,
                'weight': 1,
            }
            self.add_node(dev)

    @property
    def replica_count(self):
        return self.replicas

    def _get_part_nodes(self, part):
        return [dict(node, index=i) for i, node in enumerate(list(self._devs))]

    def get_more_nodes(self, part):
        index_counter = itertools.count()
        for x in range(self.replicas, (self.replicas + self.max_more_nodes)):
            ip = '10.0.0.%s' % x
            port = self._base_port + x
            if self.separate_replication:
                repl_ip = '10.0.1.%s' % x
                repl_port = port + 100
            else:
                repl_ip, repl_port = ip, port
            yield {'ip': ip,
                   'replication_ip': repl_ip,
                   'port': port,
                   'replication_port': repl_port,
                   'device': 'sda',
                   'zone': x % 3,
                   'region': x % 2,
                   'id': x,
                   'handoff_index': next(index_counter)}


def write_fake_ring(path, *devs):
    """
    Pretty much just a two node, two replica, 2 part power ring...
    """
    dev1 = {'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
            'port': 6200}
    dev2 = {'id': 1, 'zone': 0, 'device': 'sdb1', 'ip': '127.0.0.1',
            'port': 6200}

    dev1_updates, dev2_updates = devs or ({}, {})

    dev1.update(dev1_updates)
    dev2.update(dev2_updates)

    replica2part2dev_id = [[0, 1, 0, 1], [1, 0, 1, 0]]
    devs = [dev1, dev2]
    part_shift = 30
    with closing(GzipFile(path, 'wb')) as f:
        pickle.dump(RingData(replica2part2dev_id, devs, part_shift), f)


def write_stub_builder(tmpdir, region=1, name=''):
    """
    Pretty much just a three node, three replica, 8 part power builder...

    :param tmpdir: a place to write the builder, be sure to clean it up!
    :param region: an integer, fills in region and ip
    :param name: the name of the builder (i.e. <name>.builder)
    """
    name = name or str(region)
    replicas = 3
    builder = RingBuilder(8, replicas, 1)
    for i in range(replicas):
        dev = {'weight': 100,
               'region': '%d' % region,
               'zone': '1',
               'ip': '10.0.0.%d' % region,
               'port': '3600',
               'device': 'sdb%d' % i}
        builder.add_dev(dev)
    builder.rebalance()
    builder_file = os.path.join(tmpdir, '%s.builder' % name)
    builder.save(builder_file)
    return builder, builder_file


class FabricatedRing(Ring):
    """
    When a FakeRing just won't do - you can fabricate one to meet
    your tests needs.
    """

    def __init__(self, replicas=6, devices=8, nodes=4, port=6200,
                 part_power=4):
        self.devices = devices
        self.nodes = nodes
        self.port = port
        self.replicas = replicas
        self._part_shift = 32 - part_power
        self._reload()

    def has_changed(self):
        return False

    def _reload(self, *args, **kwargs):
        self._rtime = time.time() * 2
        if hasattr(self, '_replica2part2dev_id'):
            return
        self._devs = [{
            'region': 1,
            'zone': 1,
            'weight': 1.0,
            'id': i,
            'device': 'sda%d' % i,
            'ip': '10.0.0.%d' % (i % self.nodes),
            'replication_ip': '10.0.0.%d' % (i % self.nodes),
            'port': self.port,
            'replication_port': self.port,
        } for i in range(self.devices)]

        self._replica2part2dev_id = [
            [None] * 2 ** self.part_power
            for i in range(self.replicas)
        ]
        dev_ids = itertools.cycle(range(self.devices))
        for p in range(2 ** self.part_power):
            for r in range(self.replicas):
                self._replica2part2dev_id[r][p] = next(dev_ids)
        self._update_bookkeeping()


def track(f):
    def wrapper(self, *a, **kw):
        self.calls.append(getattr(mocklib.call, f.__name__)(*a, **kw))
        return f(self, *a, **kw)
    return wrapper


class FakeMemcache(object):

    def __init__(self, error_on_set=None, error_on_get=None):
        self.store = {}
        self.times = {}
        self.calls = []
        self.error_on_incr = False
        self.error_on_get = error_on_get or []
        self.error_on_set = error_on_set or []
        self.init_incr_return_neg = False

    def clear_calls(self):
        del self.calls[:]

    @track
    def get(self, key, raise_on_error=False):
        if self.error_on_get and self.error_on_get.pop(0):
            if raise_on_error:
                raise MemcacheConnectionError()
        return self.store.get(key)

    @property
    def keys(self):
        return self.store.keys

    @track
    def set(self, key, value, serialize=True, time=0, raise_on_error=False):
        if self.error_on_set and self.error_on_set.pop(0):
            if raise_on_error:
                raise MemcacheConnectionError()
        if serialize:
            value = json.loads(json.dumps(value))
        else:
            assert isinstance(value, (str, bytes))
        self.store[key] = value
        self.times[key] = time
        return True

    @track
    def incr(self, key, delta=1, time=0):
        if self.error_on_incr:
            raise MemcacheConnectionError('Memcache restarting')
        if self.init_incr_return_neg:
            # simulate initial hit, force reset of memcache
            self.init_incr_return_neg = False
            return -10000000
        self.store[key] = int(self.store.setdefault(key, 0)) + delta
        if self.store[key] < 0:
            self.store[key] = 0
        return self.store[key]

    # tracked via incr()
    def decr(self, key, delta=1, time=0):
        return self.incr(key, delta=-delta, time=time)

    @track
    def delete(self, key):
        try:
            del self.store[key]
            del self.times[key]
        except Exception:
            pass
        return True

    def delete_all(self):
        self.store.clear()
        self.times.clear()


# This decorator only makes sense in the context of FakeMemcache;
# may as well clean it up now
del track


class FakeIterable(object):
    def __init__(self, values):
        self.next_call_count = 0
        self.close_call_count = 0
        self.values = iter(values)

    def __iter__(self):
        return self

    def __next__(self):
        self.next_call_count += 1
        return next(self.values)

    def close(self):
        self.close_call_count += 1


def readuntil2crlfs(fd):
    rv = b''
    lc = b''
    crlfs = 0
    while crlfs < 2:
        c = fd.read(1)
        if not c:
            raise ValueError("didn't get two CRLFs; just got %r" % rv)
        rv = rv + c
        if c == b'\r' and lc != b'\n':
            crlfs = 0
        if lc == b'\r' and c == b'\n':
            crlfs += 1
        lc = c
    return rv


def readlength(fd, size, timeout=1.0):
    buf = b''
    with eventlet.Timeout(timeout):
        while len(buf) < size:
            chunk = fd.read(min(64, size - len(buf)))
            buf += chunk
            if len(buf) >= size:
                break
    return buf


def connect_tcp(hostport):
    rv = socket.socket()
    rv.connect(hostport)
    return rv


@contextmanager
def tmpfile(content):
    with NamedTemporaryFile('w', delete=False) as f:
        file_name = f.name
        f.write(str(content))
    try:
        yield file_name
    finally:
        os.unlink(file_name)


@contextmanager
def temptree(files, contents=''):
    # generate enough contents to fill the files
    c = len(files)
    contents = (list(contents) + [''] * c)[:c]
    tempdir = mkdtemp()
    for path, content in zip(files, contents):
        if os.path.isabs(path):
            path = '.' + path
        new_path = os.path.join(tempdir, path)
        subdir = os.path.dirname(new_path)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        with open(new_path, 'w') as f:
            f.write(str(content))
    try:
        yield tempdir
    finally:
        rmtree(tempdir)


def with_tempdir(f):
    """
    Decorator to give a single test a tempdir as argument to test method.
    """
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        tempdir = mkdtemp()
        args = list(args)
        args.append(tempdir)
        try:
            return f(*args, **kwargs)
        finally:
            rmtree(tempdir)
    return wrapped


class NullLoggingHandler(logging.Handler):

    def emit(self, record):
        pass


class UnmockTimeModule(object):
    """
    Even if a test mocks time.time - you can restore unmolested behavior in a
    another module who imports time directly by monkey patching it's imported
    reference to the module with an instance of this class
    """

    _orig_time = time.time

    def __getattribute__(self, name):
        if name == 'time':
            return UnmockTimeModule._orig_time
        return getattr(time, name)


# logging.LogRecord.__init__ calls time.time
logging.time = UnmockTimeModule()


original_syslog_handler = logging.handlers.SysLogHandler


def fake_syslog_handler():
    for attr in dir(original_syslog_handler):
        if attr.startswith('LOG'):
            setattr(FakeLogger, attr,
                    copy.copy(getattr(logging.handlers.SysLogHandler, attr)))
    FakeLogger.priority_map = \
        copy.deepcopy(logging.handlers.SysLogHandler.priority_map)

    logging.handlers.SysLogHandler = FakeLogger


if utils.config_true_value(
        get_config('unit_test').get('fake_syslog', 'False')):
    fake_syslog_handler()


@contextmanager
def quiet_eventlet_exceptions():
    orig_state = greenpool.DEBUG
    eventlet_debug.hub_exceptions(False)
    try:
        yield
    finally:
        eventlet_debug.hub_exceptions(orig_state)


@contextmanager
def mock_check_drive(isdir=False, ismount=False):
    """
    All device/drive/mount checking should be done through the constraints
    module. If we keep the mocking consistently within that module, we can
    keep our tests robust to further rework on that interface.

    Replace the constraint modules underlying os calls with mocks.

    :param isdir: return value of constraints isdir calls, default False
    :param ismount: return value of constraints ismount calls, default False
    :returns: a dict of constraint module mocks
    """
    mock_base = 'swift.common.constraints.'
    with mocklib.patch(mock_base + 'isdir') as mock_isdir, \
            mocklib.patch(mock_base + 'utils.ismount') as mock_ismount:
        mock_isdir.return_value = isdir
        mock_ismount.return_value = ismount
        yield {
            'isdir': mock_isdir,
            'ismount': mock_ismount,
        }


@contextmanager
def mock(update):
    returns = []
    deletes = []
    for key, value in update.items():
        imports = key.split('.')
        attr = imports.pop(-1)
        module = __import__(imports[0], fromlist=imports[1:])
        for modname in imports[1:]:
            module = getattr(module, modname)
        if hasattr(module, attr):
            returns.append((module, attr, getattr(module, attr)))
        else:
            deletes.append((module, attr))
        setattr(module, attr, value)
    try:
        yield True
    finally:
        for module, attr, value in returns:
            setattr(module, attr, value)
        for module, attr in deletes:
            delattr(module, attr)


class FakeStatus(object):
    """
    This will work with our fake_http_connect, if you hand in one of these
    instead of a status int or status int tuple to the "codes" iter you can
    add some eventlet sleep to the expect and response stages of the
    connection.
    """

    def __init__(self, status, expect_sleep=None, response_sleep=None):
        """
        :param status: the response status int, or a tuple of
                       ([expect_status, ...], response_status)
        :param expect_sleep: float, time to eventlet sleep during expect, can
                             be a iter of floats
        :param response_sleep: float, time to eventlet sleep during response
        """
        # connect exception
        if inspect.isclass(status) and issubclass(status, Exception):
            raise status('FakeStatus Error')
        if isinstance(status, (Exception, eventlet.Timeout)):
            raise status
        if isinstance(status, tuple):
            self.expect_status = list(status[:-1])
            self.status = status[-1]
            self.explicit_expect_list = True
        else:
            self.expect_status, self.status = ([], status)
            self.explicit_expect_list = False
        if not self.expect_status:
            # when a swift backend service returns a status before reading
            # from the body (mostly an error response) eventlet.wsgi will
            # respond with that status line immediately instead of 100
            # Continue, even if the client sent the Expect 100 header.
            # BufferedHttp and the proxy both see these error statuses
            # when they call getexpect, so our FakeConn tries to act like
            # our backend services and return certain types of responses
            # as expect statuses just like a real backend server would do.
            if self.status in (507, 412, 409):
                self.expect_status = [status]
            else:
                self.expect_status = [100, 100]

        # setup sleep attributes
        if not isinstance(expect_sleep, (list, tuple)):
            expect_sleep = [expect_sleep] * len(self.expect_status)
        self.expect_sleep_list = list(expect_sleep)
        while len(self.expect_sleep_list) < len(self.expect_status):
            self.expect_sleep_list.append(None)
        self.response_sleep = response_sleep

    def __repr__(self):
        return '%s(%s, expect_status=%r, response_sleep=%s)' % (
            self.__class__.__name__, self.status,
            self.expect_status, self.response_sleep)

    def get_response_status(self):
        if self.response_sleep is not None:
            eventlet.sleep(self.response_sleep)
        if self.expect_status and self.explicit_expect_list:
            raise Exception('Test did not consume all fake '
                            'expect status: %r' % (self.expect_status,))
        if isinstance(self.status, (Exception, eventlet.Timeout)):
            raise self.status
        return self.status

    def get_expect_status(self):
        expect_sleep = self.expect_sleep_list.pop(0)
        if expect_sleep is not None:
            eventlet.sleep(expect_sleep)
        expect_status = self.expect_status.pop(0)
        if isinstance(expect_status, (Exception, eventlet.Timeout)):
            raise expect_status
        return expect_status


class SlowBody(object):
    """
    This will work with our fake_http_connect, if you hand in these
    instead of strings it will make reads take longer by the given
    amount.  It should be a little bit easier to extend than the
    current slow kwarg - which inserts whitespace in the response.
    Also it should be easy to detect if you have one of these (or a
    subclass) for the body inside of FakeConn if we wanted to do
    something smarter than just duck-type the str/buffer api
    enough to get by.
    """

    def __init__(self, body, slowness):
        self.body = body
        self.slowness = slowness

    def slowdown(self):
        eventlet.sleep(self.slowness)

    def __getitem__(self, s):
        return SlowBody(self.body[s], self.slowness)

    def __len__(self):
        return len(self.body)

    def __radd__(self, other):
        self.slowdown()
        return other + self.body


def fake_http_connect(*code_iter, **kwargs):

    class FakeConn(object):

        SLOW_READS = 4
        SLOW_WRITES = 4

        def __init__(self, status, etag=None, body=b'', timestamp=-1,
                     headers=None, expect_headers=None, connection_id=None,
                     give_send=None, give_expect=None):
            if not isinstance(status, FakeStatus):
                status = FakeStatus(status)
            self._status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
            self.sent = 0
            self.received = 0
            self.etag = etag
            self.body = body
            self._headers = headers or {}
            self.expect_headers = expect_headers or {}
            if timestamp == -1:
                # -1 is reserved to mean "magic default"
                if status.status != 404:
                    self.timestamp = '1'
                else:
                    self.timestamp = '0'
            else:
                # tests may specify int, string, Timestamp or None
                self.timestamp = timestamp
            self.connection_id = connection_id
            self.give_send = give_send
            self.give_expect = give_expect
            self.closed = False
            if 'slow' in kwargs and isinstance(kwargs['slow'], list):
                try:
                    self._next_sleep = kwargs['slow'].pop(0)
                except IndexError:
                    self._next_sleep = None

            # if we're going to be slow, we need a body to send slowly
            am_slow, _junk = self.get_slow()
            if am_slow and len(self.body) < self.SLOW_READS:
                self.body += b" " * (self.SLOW_READS - len(self.body))

            # be nice to trixy bits with node_iter's
            eventlet.sleep()

        def getresponse(self):
            exc = kwargs.get('raise_exc')
            if exc:
                if isinstance(exc, (Exception, eventlet.Timeout)):
                    raise exc
                raise Exception('test')
            if kwargs.get('raise_timeout_exc'):
                raise eventlet.Timeout()
            self.status = self._status.get_response_status()
            return self

        def getexpect(self):
            if self.give_expect:
                self.give_expect(self)
            expect_status = self._status.get_expect_status()
            headers = dict(self.expect_headers)
            if expect_status == 409:
                headers['X-Backend-Timestamp'] = self.timestamp
            response = FakeConn(expect_status,
                                timestamp=self.timestamp,
                                headers=headers)
            response.status = expect_status
            return response

        def getheaders(self):
            etag = self.etag
            if not etag:
                if isinstance(self.body, bytes):
                    etag = ('"' + md5(
                        self.body, usedforsecurity=False).hexdigest() + '"')
                else:
                    etag = '"68b329da9893e34099c7d8ad5cb9c940"'

            am_slow, _junk = self.get_slow()
            headers = HeaderKeyDict({
                'content-length': len(self.body),
                'content-type': 'x-application/test',
                'x-timestamp': self.timestamp,
                'x-backend-timestamp': self.timestamp,
                'last-modified': self.timestamp,
                'x-object-meta-test': 'testing',
                'x-delete-at': '9876543210',
                'etag': etag,
                'x-works': 'yes',
            })
            if self.status // 100 == 2:
                headers['x-account-container-count'] = \
                    kwargs.get('count', 12345)
            if not self.timestamp:
                # when timestamp is None, HeaderKeyDict raises KeyError
                headers.pop('x-timestamp', None)
            try:
                if next(container_ts_iter) is False:
                    headers['x-container-timestamp'] = '1'
            except StopIteration:
                pass
            headers.update(self._headers)
            return headers.items()

        def get_slow(self):
            if 'slow' in kwargs and isinstance(kwargs['slow'], list):
                if self._next_sleep is not None:
                    return True, self._next_sleep
                else:
                    return False, 0.01
            if kwargs.get('slow') and isinstance(kwargs['slow'], Number):
                return True, kwargs['slow']
            return bool(kwargs.get('slow')), 0.1

        def read(self, amt=None):
            am_slow, value = self.get_slow()
            if am_slow:
                if self.sent < self.SLOW_READS:
                    slowly_read_byte = self.body[self.sent:self.sent + 1]
                    self.sent += 1
                    eventlet.sleep(value)
                    return slowly_read_byte
            if amt is None:
                rv = self.body[self.sent:]
            else:
                rv = self.body[self.sent:self.sent + amt]
            self.sent += len(rv)
            return rv

        def send(self, data=None):
            if self.give_send:
                self.give_send(self, data)
            am_slow, value = self.get_slow()
            if am_slow:
                if self.received < self.SLOW_WRITES:
                    self.received += 1
                    eventlet.sleep(value)

        def getheader(self, name, default=None):
            return HeaderKeyDict(self.getheaders()).get(name, default)

        def nuke_from_orbit(self):
            # wrapped connections from buffered_http have this helper
            self.close()

        def close(self):
            self.closed = True

    # unless tests provide timestamps we use the "magic default"
    timestamps_iter = iter(kwargs.get('timestamps') or [-1] * len(code_iter))
    etag_iter = iter(kwargs.get('etags') or [None] * len(code_iter))
    if isinstance(kwargs.get('headers'), (list, tuple)):
        headers_iter = iter(kwargs['headers'])
    else:
        headers_iter = iter([kwargs.get('headers', {})] * len(code_iter))
    if isinstance(kwargs.get('expect_headers'), (list, tuple)):
        expect_headers_iter = iter(kwargs['expect_headers'])
    else:
        expect_headers_iter = iter([kwargs.get('expect_headers', {})] *
                                   len(code_iter))

    x = kwargs.get('missing_container', [False] * len(code_iter))
    if not isinstance(x, (tuple, list)):
        x = [x] * len(code_iter)
    container_ts_iter = iter(x)
    code_iter = iter(code_iter)
    conn_id_and_code_iter = enumerate(code_iter)
    static_body = kwargs.get('body', None)
    body_iter = kwargs.get('body_iter', None)
    if body_iter:
        body_iter = iter(body_iter)
    unexpected_requests = []

    def connect(*args, **ckwargs):
        if kwargs.get('slow_connect', False):
            eventlet.sleep(0.1)
        if 'give_content_type' in kwargs:
            if len(args) >= 7 and 'Content-Type' in args[6]:
                kwargs['give_content_type'](args[6]['Content-Type'])
            else:
                kwargs['give_content_type']('')
        try:
            i, status = next(conn_id_and_code_iter)
        except StopIteration:
            # the code under test may swallow the StopIteration, so by logging
            # unexpected requests here we allow the test framework to check for
            # them after the connect function has been used.
            unexpected_requests.append((args, ckwargs))
            raise

        if 'give_connect' in kwargs:
            give_conn_fn = kwargs['give_connect']

            argspec = inspect.getfullargspec(give_conn_fn)
            if argspec.varkw or 'connection_id' in argspec.args:
                ckwargs['connection_id'] = i
            give_conn_fn(*args, **ckwargs)
        etag = next(etag_iter)
        headers = next(headers_iter)
        expect_headers = next(expect_headers_iter)
        timestamp = next(timestamps_iter)

        if isinstance(status, int) and status <= 0:
            raise HTTPException()
        if body_iter is None:
            body = static_body or b''
        else:
            body = next(body_iter)
        conn = FakeConn(status, etag, body=body, timestamp=timestamp,
                        headers=headers, expect_headers=expect_headers,
                        connection_id=i, give_send=kwargs.get('give_send'),
                        give_expect=kwargs.get('give_expect'))
        if 'capture_connections' in kwargs:
            kwargs['capture_connections'].append(conn)
        return conn

    connect.unexpected_requests = unexpected_requests
    connect.code_iter = code_iter

    return connect


@contextmanager
def mocked_http_conn(*args, **kwargs):
    requests = []
    responses = []

    def capture_requests(ip, port, method, path, headers, qs, ssl):
        req = {
            'ip': ip,
            'port': port,
            'method': method,
            'path': path,
            'headers': headers,
            'qs': qs,
            'ssl': ssl,
        }
        requests.append(req)
    kwargs.setdefault('give_connect', capture_requests)
    kwargs['capture_connections'] = responses
    fake_conn = fake_http_connect(*args, **kwargs)
    fake_conn.requests = requests
    fake_conn.responses = responses
    with mocklib.patch('swift.common.bufferedhttp.http_connect_raw',
                       new=fake_conn):
        yield fake_conn
        left_over_status = list(fake_conn.code_iter)
        if left_over_status:
            raise AssertionError('left over status %r' % left_over_status)
        if fake_conn.unexpected_requests:
            raise AssertionError(
                '%d unexpected requests:\n%s' %
                (len(fake_conn.unexpected_requests),
                 '\n  '.join('%r' % (req,)
                             for req in fake_conn.unexpected_requests)))


def make_timestamp_iter(offset=0):
    return iter(Timestamp(t)
                for t in itertools.count(int(time.time()) + offset))


@contextmanager
def mock_timestamp_now(now=None, klass=Timestamp):
    if now is None:
        now = klass.now()
    with mocklib.patch('swift.common.utils.Timestamp.now',
                       classmethod(lambda c: now)):
        yield now


@contextmanager
def mock_timestamp_now_with_iter(ts_iter):
    with mocklib.patch('swift.common.utils.Timestamp.now',
                       side_effect=ts_iter):
        yield


class Timeout(object):
    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._exit)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

    def _exit(self, signum, frame):
        class TimeoutException(Exception):
            pass
        raise TimeoutException


def requires_o_tmpfile_support_in_tmp(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not utils.o_tmpfile_in_tmpdir_supported():
            raise SkipTest('Requires O_TMPFILE support in TMPDIR')
        return func(*args, **kwargs)
    return wrapper


def requires_crc32c(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            checksum.crc32c()
        except NotImplementedError as e:
            raise SkipTest(str(e))
        return func(*args, **kwargs)
    return wrapper


def requires_crc64nvme(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            checksum.crc64nvme()
        except NotImplementedError as e:
            raise SkipTest(str(e))
        return func(*args, **kwargs)
    return wrapper


class StubResponse(object):

    def __init__(self, status, body=b'', headers=None, frag_index=None,
                 slowdown=None, slowdown_after=0):
        self.status = status
        self.body = body
        self.readable = BytesIO(body)
        try:
            self._slowdown = iter(slowdown)
        except TypeError:
            self._slowdown = iter([slowdown])
        self.slowdown_after = slowdown_after
        self.headers = HeaderKeyDict(headers)
        if frag_index is not None:
            self.headers['X-Object-Sysmeta-Ec-Frag-Index'] = frag_index
        fake_reason = ('Fake', 'This response is a lie.')
        self.reason = swob.RESPONSE_REASONS.get(status, fake_reason)[0]
        self.bytes_read = 0

    def slowdown(self):
        if self.bytes_read < self.slowdown_after:
            return
        try:
            wait = next(self._slowdown)
        except StopIteration:
            wait = None
        if wait is not None:
            eventlet.sleep(wait)

    def nuke_from_orbit(self):
        if hasattr(self, 'swift_conn'):
            self.swift_conn.close()

    def getheader(self, header_name, default=None):
        return self.headers.get(header_name, default)

    def getheaders(self):
        if 'Content-Length' not in self.headers:
            self.headers['Content-Length'] = len(self.body)
        return self.headers.items()

    def read(self, amt=0):
        self.slowdown()
        res = self.readable.read(amt)
        self.bytes_read += len(res)
        return res

    def readline(self, size=-1):
        self.slowdown()
        res = self.readable.readline(size)
        self.bytes_read += len(res)
        return res

    def __repr__(self):
        info = ['Status: %s' % self.status]
        if self.headers:
            info.append('Headers: %r' % dict(self.headers))
        if self.body:
            info.append('Body: %r' % self.body)
        return '<StubResponse %s>' % ', '.join(info)


def encode_frag_archive_bodies(policy, body):
    """
    Given a stub body produce a list of complete frag_archive bodies as
    strings in frag_index order.

    :param policy: a StoragePolicy instance, with policy_type EC_POLICY
    :param body: a string, the body to encode into frag archives

    :returns: list of strings, the complete frag_archive bodies for the given
              plaintext
    """
    segment_size = policy.ec_segment_size
    # split up the body into buffers
    chunks = [body[x:x + segment_size]
              for x in range(0, len(body), segment_size)]
    # encode the buffers into fragment payloads
    fragment_payloads = []
    for chunk in chunks:
        fragments = policy.pyeclib_driver.encode(chunk) \
            * policy.ec_duplication_factor
        if not fragments:
            break
        fragment_payloads.append(fragments)

    # join up the fragment payloads per node
    ec_archive_bodies = [b''.join(frags)
                         for frags in zip(*fragment_payloads)]
    return ec_archive_bodies


def make_ec_object_stub(test_body, policy, timestamp):
    segment_size = policy.ec_segment_size
    test_body = test_body or (
        b'test' * segment_size)[:-random.randint(1, 1000)]
    timestamp = timestamp or utils.Timestamp.now()
    etag = md5(test_body, usedforsecurity=False).hexdigest()
    ec_archive_bodies = encode_frag_archive_bodies(policy, test_body)

    return {
        'body': test_body,
        'etag': etag,
        'frags': ec_archive_bodies,
        'timestamp': timestamp
    }


def fake_ec_node_response(node_frags, policy):
    """
    Given a list of entries for each node in ring order, where the entries
    are a dict (or list of dicts) which describes the fragment (or
    fragments) that are on the node; create a function suitable for use
    with capture_http_requests that will accept a req object and return a
    response that will suitably fake the behavior of an object server who
    had the given fragments on disk at the time.

    :param node_frags: a list. Each item in the list describes the
        fragments that are on a node; each item is a dict or list of dicts,
        each dict describing a single fragment; where the item is a list,
        repeated calls to get_response will return fragments in the order
        of the list; each dict has keys:
            - obj: an object stub, as generated by _make_ec_object_stub,
                that defines all of the fragments that compose an object
                at a specific timestamp.
            - frag: the index of a fragment to be selected from the object
                stub
            - durable (optional): True if the selected fragment is durable
    :param policy: storage policy to return
    """
    node_map = {}  # maps node ip and port to node index
    all_nodes = []
    call_count = {}  # maps node index to get_response call count for node

    def _build_node_map(req, policy):
        part = utils.split_path(req['path'], 5, 5, True)[1]
        all_nodes.extend(policy.object_ring.get_part_nodes(part))
        all_nodes.extend(policy.object_ring.get_more_nodes(part))
        for i, node in enumerate(all_nodes):
            node_map[(node['ip'], node['port'])] = i
            call_count[i] = 0

    # normalize node_frags to a list of fragments for each node even
    # if there's only one fragment in the dataset provided.
    for i, frags in enumerate(node_frags):
        if isinstance(frags, dict):
            node_frags[i] = [frags]

    def get_response(req):
        requested_policy = int(
            req['headers']['X-Backend-Storage-Policy-Index'])
        if int(policy) != requested_policy:
            AssertionError(
                "Requested polciy doesn't fit the fake response policy")
        if not node_map:
            _build_node_map(req, policy)

        try:
            node_index = node_map[(req['ip'], req['port'])]
        except KeyError:
            raise Exception("Couldn't find node %s:%s in %r" % (
                req['ip'], req['port'], all_nodes))
        try:
            frags = node_frags[node_index]
        except IndexError:
            raise Exception('Found node %r:%r at index %s - '
                            'but only got %s stub response nodes' % (
                                req['ip'], req['port'], node_index,
                                len(node_frags)))

        if not frags:
            return StubResponse(404)

        # determine response fragment (if any) for this call
        resp_frag = frags[call_count[node_index]]
        call_count[node_index] += 1
        frag_prefs = req['headers'].get('X-Backend-Fragment-Preferences')
        if not (frag_prefs or resp_frag.get('durable', True)):
            return StubResponse(404)

        # prepare durable timestamp and backend frags header for this node
        obj_stub = resp_frag['obj']
        ts2frags = defaultdict(list)
        durable_timestamp = None
        for frag in frags:
            ts_frag = frag['obj']['timestamp']
            if frag.get('durable', True):
                durable_timestamp = ts_frag.internal
            ts2frags[ts_frag].append(frag['frag'])

        try:
            body = obj_stub['frags'][resp_frag['frag']]
        except IndexError as err:
            raise Exception(
                'Frag index %s not defined: node index %s, frags %r\n%s' %
                (resp_frag['frag'], node_index, [f['frag'] for f in frags],
                 err))
        headers = {
            'X-Object-Sysmeta-Ec-Content-Length': len(obj_stub['body']),
            'X-Object-Sysmeta-Ec-Etag': obj_stub['etag'],
            'X-Object-Sysmeta-Ec-Frag-Index':
                policy.get_backend_index(resp_frag['frag']),
            'X-Backend-Timestamp': obj_stub['timestamp'].internal,
            'X-Timestamp': obj_stub['timestamp'].normal,
            'X-Backend-Data-Timestamp': obj_stub['timestamp'].internal,
            'X-Backend-Fragments':
                server._make_backend_fragments_header(ts2frags)
        }
        if durable_timestamp:
            headers['X-Backend-Durable-Timestamp'] = durable_timestamp

        return StubResponse(200, body, headers)

    return get_response


supports_xattr_cached_val = None


def xattr_supported_check():
    """
    This check simply sets more than 4k of metadata on a tempfile and
    returns True if it worked and False if not.

    We want to use *more* than 4k of metadata in this check because
    some filesystems (eg ext4) only allow one blocksize worth of
    metadata. The XFS filesystem doesn't have this limit, and so this
    check returns True when TMPDIR is XFS. This check will return
    False under ext4 (which supports xattrs <= 4k) and tmpfs (which
    doesn't support xattrs at all).

    """
    global supports_xattr_cached_val

    if supports_xattr_cached_val is not None:
        return supports_xattr_cached_val

    # assume the worst -- xattrs aren't supported
    supports_xattr_cached_val = False

    big_val = b'x' * (4096 + 1)  # more than 4k of metadata
    try:
        fd, tmppath = mkstemp()
        xattr.setxattr(fd, 'user.swift.testing_key', big_val)
    except IOError as e:
        if errno.errorcode.get(e.errno) in ('ENOSPC', 'ENOTSUP', 'EOPNOTSUPP',
                                            'ERANGE'):
            # filesystem does not support xattr of this size
            return False
        raise
    else:
        supports_xattr_cached_val = True
        return True
    finally:
        # clean up the tmpfile
        os.close(fd)
        os.unlink(tmppath)


def skip_if_no_xattrs():
    if not xattr_supported_check():
        raise SkipTest('Large xattrs not supported in `%s`. Skipping test' %
                       gettempdir())


def unlink_files(paths):
    for path in paths:
        try:
            os.unlink(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise


class FakeHTTPResponse(object):

    def __init__(self, resp):
        self.resp = resp

    @property
    def status(self):
        return self.resp.status_int

    @property
    def data(self):
        return self.resp.body


def attach_fake_replication_rpc(rpc, replicate_hook=None, errors=None):
    class FakeReplConnection(object):

        def __init__(self, node, partition, hash_, logger):
            self.logger = logger
            self.node = node
            self.partition = partition
            self.path = '/%s/%s/%s' % (node['device'], partition, hash_)
            self.host = node['replication_ip']

        def replicate(self, op, *sync_args):
            print('REPLICATE: %s, %s, %r' % (self.path, op, sync_args))
            resp = None
            if errors and op in errors and errors[op]:
                resp = errors[op].pop(0)
            if not resp:
                replicate_args = self.path.lstrip('/').split('/')
                args = [op] + copy.deepcopy(list(sync_args))
                with mock_check_drive(isdir=not rpc.mount_check,
                                      ismount=rpc.mount_check):
                    swob_response = rpc.dispatch(replicate_args, args)
                resp = FakeHTTPResponse(swob_response)
            if replicate_hook:
                replicate_hook(op, *sync_args)
            return resp

    return FakeReplConnection


def group_by_byte(contents):
    # This looks a little funny, but iterating through a byte string on py3
    # yields a sequence of ints, not a sequence of single-byte byte strings
    # as it did on py2.
    byte_iter = (contents[i:i + 1] for i in range(len(contents)))
    return [
        (char, sum(1 for _ in grp))
        for char, grp in itertools.groupby(byte_iter)]


def generate_db_path(tempdir, server_type):
    return os.path.join(
        tempdir, '%ss' % server_type, 'part', 'suffix', 'hash',
        '%s-%s.db' % (server_type, uuid4()))


class FakeSource(object):
    def __init__(self, chunks, headers=None, body=b''):
        self.chunks = list(chunks)
        self.headers = headers or {}
        self.status = 200
        self.swift_conn = None
        self.body = body

    def read(self, _read_size):
        if self.chunks:
            chunk = self.chunks.pop(0)
            if chunk is None:
                raise exceptions.ChunkReadTimeout()
            else:
                return chunk
        else:
            return self.body

    def getheader(self, header):
        # content-length for the whole object is generated dynamically
        # by summing non-None chunks
        if header.lower() == "content-length":
            if self.chunks:
                return str(sum(len(c) for c in self.chunks
                               if c is not None))
            return len(self.read(-1))
        return self.headers.get(header.lower())

    def getheaders(self):
        return [('content-length', self.getheader('content-length'))] + \
               [(k, v) for k, v in self.headers.items()]


class CaptureIterator(object):
    """
    Wraps an iterable, forwarding all calls to the wrapped iterable but
    capturing the calls via a callback.

    This class may be used to observe garbage collection, so tests should not
    have to hold a reference to instances of this class because that would
    prevent them being garbage collected. Calls are therefore captured via a
    callback rather than being stashed locally.

    :param wrapped: an iterable to wrap.
    :param call_capture_callback: a function that will be called to capture
        calls to this iterator.
    """
    def __init__(self, wrapped, call_capture_callback):
        self.call_capture_callback = call_capture_callback
        self.wrapped_iter = wrapped

    def _capture_call(self):
        # call home to capture the call
        self.call_capture_callback(inspect.stack()[1][3])

    def __iter__(self):
        return self

    def __next__(self):
        self._capture_call()
        return next(self.wrapped_iter)

    def __del__(self):
        self._capture_call()

    def close(self):
        self._capture_call()
        close_if_possible(self.wrapped_iter)


class CaptureIteratorFactory(object):
    """
    Create instances of ``CaptureIterator`` to wrap a given iterable, and
    provides a callback function for the ``CaptureIterator`` to capture its
    calls.

    :param wrapped: an iterable to wrap.
    """
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.instance_count = 0
        self.captured_calls = defaultdict(list)

    def log_call(self, instance_number, call):
        self.captured_calls[instance_number].append(call)

    def __call__(self, *args, **kwargs):
        # note: do not keep a reference to the CaptureIterator because that
        # would prevent it being garbage collected
        self.instance_count += 1
        return CaptureIterator(
            self.wrapped(*args, **kwargs),
            functools.partial(self.log_call, self.instance_count))


def get_node_error_stats(proxy_app, ring_node):
    node_key = proxy_app.error_limiter.node_key(ring_node)
    return proxy_app.error_limiter.stats.get(node_key) or {}


def node_error_count(proxy_app, ring_node):
    # Reach into the proxy's internals to get the error count for a
    # particular node
    return get_node_error_stats(proxy_app, ring_node).get('errors', 0)


def node_error_counts(proxy_app, ring_nodes):
    # Reach into the proxy's internals to get the error counts for a
    # list of nodes
    return sorted([get_node_error_stats(proxy_app, node).get('errors', 0)
                   for node in ring_nodes], reverse=True)


def node_last_error(proxy_app, ring_node):
    # Reach into the proxy's internals to get the last error for a
    # particular node
    return get_node_error_stats(proxy_app, ring_node).get('last_error')


def set_node_errors(proxy_app, ring_node, value, last_error):
    # Set the node's error count to value
    node_key = proxy_app.error_limiter.node_key(ring_node)
    stats = {'errors': value,
             'last_error': last_error}
    proxy_app.error_limiter.stats[node_key] = stats
