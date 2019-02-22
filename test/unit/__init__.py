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

from __future__ import print_function
import os
import copy
import logging
import logging.handlers
import sys
from contextlib import contextmanager, closing
from collections import defaultdict, Iterable
from hashlib import md5
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

import six.moves.cPickle as pickle
from six import BytesIO
from six.moves import range
from six.moves.http_client import HTTPException

from swift.common import storage_policy, swob, utils
from swift.common.storage_policy import (StoragePolicy, ECStoragePolicy,
                                         VALID_EC_TYPES)
from swift.common.utils import Timestamp, NOTICE
from test import get_config
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.ring import Ring, RingData, RingBuilder
from swift.obj import server

import functools
from gzip import GzipFile
import mock as mocklib
import inspect
import unittest
import unittest2


class SkipTest(unittest2.SkipTest, unittest.SkipTest):
    pass

EMPTY_ETAG = md5().hexdigest()

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
                self.__exit__()
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
            self.__exit__()
            raise

    def __exit__(self, *args):
        storage_policy._POLICIES = self._orig_POLICIES


class FakeRing(Ring):

    def __init__(self, replicas=3, max_more_nodes=0, part_power=0,
                 base_port=1000):
        self._base_port = base_port
        self.max_more_nodes = max_more_nodes
        self._part_shift = 32 - part_power
        self._init_device_char()
        # 9 total nodes (6 more past the initial 3) is the cap, no matter if
        # this is set higher, or R^2 for R replicas
        self.set_replicas(replicas)
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
            dev = {
                'ip': ip,
                'replication_ip': ip,
                'port': port,
                'replication_port': port,
                'device': self.device_char,
                'zone': x % 3,
                'region': x % 2,
                'id': x,
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
            yield {'ip': '10.0.0.%s' % x,
                   'replication_ip': '10.0.0.%s' % x,
                   'port': self._base_port + x,
                   'replication_port': self._base_port + x,
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


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def keys(self):
        return self.store.keys()

    def set(self, key, value, time=0):
        self.store[key] = value
        return True

    def incr(self, key, time=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


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


class WARN_DEPRECATED(Exception):
    def __init__(self, msg):
        self.msg = msg
        print(self.msg)


class FakeLogger(logging.Logger, object):
    # a thread safe fake logger

    def __init__(self, *args, **kwargs):
        self._clear()
        self.name = 'swift.unit.fake_logger'
        self.level = logging.NOTSET
        if 'facility' in kwargs:
            self.facility = kwargs['facility']
        self.statsd_client = None
        self.thread_locals = None
        self.parent = None

    store_in = {
        logging.ERROR: 'error',
        logging.WARNING: 'warning',
        logging.INFO: 'info',
        logging.DEBUG: 'debug',
        logging.CRITICAL: 'critical',
        NOTICE: 'notice',
    }

    def warn(self, *args, **kwargs):
        raise WARN_DEPRECATED("Deprecated Method warn use warning instead")

    def notice(self, msg, *args, **kwargs):
        """
        Convenience function for syslog priority LOG_NOTICE. The python
        logging lvl is set to 25, just above info.  SysLogHandler is
        monkey patched to map this log lvl to the LOG_NOTICE syslog
        priority.
        """
        self.log(NOTICE, msg, *args, **kwargs)

    def _log(self, level, msg, *args, **kwargs):
        store_name = self.store_in[level]
        cargs = [msg]
        if any(args):
            cargs.extend(args)
        captured = dict(kwargs)
        if 'exc_info' in kwargs and \
                not isinstance(kwargs['exc_info'], tuple):
            captured['exc_info'] = sys.exc_info()
        self.log_dict[store_name].append((tuple(cargs), captured))
        super(FakeLogger, self)._log(level, msg, *args, **kwargs)

    def _clear(self):
        self.log_dict = defaultdict(list)
        self.lines_dict = {'critical': [], 'error': [], 'info': [],
                           'warning': [], 'debug': [], 'notice': []}

    clear = _clear  # this is a public interface

    def get_lines_for_level(self, level):
        if level not in self.lines_dict:
            raise KeyError(
                "Invalid log level '%s'; valid levels are %s" %
                (level,
                 ', '.join("'%s'" % lvl for lvl in sorted(self.lines_dict))))
        return self.lines_dict[level]

    def all_log_lines(self):
        return dict((level, msgs) for level, msgs in self.lines_dict.items()
                    if len(msgs) > 0)

    def _store_in(store_name):
        def stub_fn(self, *args, **kwargs):
            self.log_dict[store_name].append((args, kwargs))
        return stub_fn

    # mock out the StatsD logging methods:
    update_stats = _store_in('update_stats')
    increment = _store_in('increment')
    decrement = _store_in('decrement')
    timing = _store_in('timing')
    timing_since = _store_in('timing_since')
    transfer_rate = _store_in('transfer_rate')
    set_statsd_prefix = _store_in('set_statsd_prefix')

    def get_increments(self):
        return [call[0][0] for call in self.log_dict['increment']]

    def get_increment_counts(self):
        counts = {}
        for metric in self.get_increments():
            if metric not in counts:
                counts[metric] = 0
            counts[metric] += 1
        return counts

    def setFormatter(self, obj):
        self.formatter = obj

    def close(self):
        self._clear()

    def set_name(self, name):
        # don't touch _handlers
        self._name = name

    def acquire(self):
        pass

    def release(self):
        pass

    def createLock(self):
        pass

    def emit(self, record):
        pass

    def _handle(self, record):
        try:
            line = record.getMessage()
        except TypeError:
            print('WARNING: unable to format log message %r %% %r' % (
                record.msg, record.args))
            raise
        self.lines_dict[record.levelname.lower()].append(line)

    def handle(self, record):
        self._handle(record)

    def flush(self):
        pass

    def handleError(self, record):
        pass

    def isEnabledFor(self, level):
        return True


class DebugSwiftLogFormatter(utils.SwiftLogFormatter):

    def format(self, record):
        msg = super(DebugSwiftLogFormatter, self).format(record)
        return msg.replace('#012', '\n')


class DebugLogger(FakeLogger):
    """A simple stdout logging version of FakeLogger"""

    def __init__(self, *args, **kwargs):
        FakeLogger.__init__(self, *args, **kwargs)
        self.formatter = DebugSwiftLogFormatter(
            "%(server)s %(levelname)s: %(message)s")

    def handle(self, record):
        self._handle(record)
        print(self.formatter.format(record))


class DebugLogAdapter(utils.LogAdapter):

    def _send_to_logger(name):
        def stub_fn(self, *args, **kwargs):
            return getattr(self.logger, name)(*args, **kwargs)
        return stub_fn

    # delegate to FakeLogger's mocks
    update_stats = _send_to_logger('update_stats')
    increment = _send_to_logger('increment')
    decrement = _send_to_logger('decrement')
    timing = _send_to_logger('timing')
    timing_since = _send_to_logger('timing_since')
    transfer_rate = _send_to_logger('transfer_rate')
    set_statsd_prefix = _send_to_logger('set_statsd_prefix')

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return getattr(self.__dict__['logger'], name)


def debug_logger(name='test'):
    """get a named adapted debug logger"""
    return DebugLogAdapter(DebugLogger(), name)


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

        def __init__(self, status, etag=None, body=b'', timestamp='1',
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
            self.headers = headers or {}
            self.expect_headers = expect_headers or {}
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
                self.body += " " * (self.SLOW_READS - len(self.body))

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
                    etag = '"' + md5(self.body).hexdigest() + '"'
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
            headers.update(self.headers)
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
                    slowly_read_byte = self.body[self.sent]
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

        def close(self):
            self.closed = True

    timestamps_iter = iter(kwargs.get('timestamps') or ['1'] * len(code_iter))
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
            unexpected_requests.append((args, kwargs))
            raise

        if 'give_connect' in kwargs:
            give_conn_fn = kwargs['give_connect']
            argspec = inspect.getargspec(give_conn_fn)
            if argspec.keywords or 'connection_id' in argspec.args:
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
        return FakeConn(status, etag, body=body, timestamp=timestamp,
                        headers=headers, expect_headers=expect_headers,
                        connection_id=i, give_send=kwargs.get('give_send'),
                        give_expect=kwargs.get('give_expect'))

    connect.unexpected_requests = unexpected_requests
    connect.code_iter = code_iter

    return connect


@contextmanager
def mocked_http_conn(*args, **kwargs):
    requests = []

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
    fake_conn = fake_http_connect(*args, **kwargs)
    fake_conn.requests = requests
    with mocklib.patch('swift.common.bufferedhttp.http_connect_raw',
                       new=fake_conn):
        yield fake_conn
        left_over_status = list(fake_conn.code_iter)
        if left_over_status:
            raise AssertionError('left over status %r' % left_over_status)
        if fake_conn.unexpected_requests:
            raise AssertionError('unexpected requests %r' %
                                 fake_conn.unexpected_requests)


def make_timestamp_iter(offset=0):
    return iter(Timestamp(t)
                for t in itertools.count(int(time.time()) + offset))


@contextmanager
def mock_timestamp_now(now=None):
    if now is None:
        now = Timestamp.now()
    with mocklib.patch('swift.common.utils.Timestamp.now',
                       classmethod(lambda c: now)):
        yield now


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


class StubResponse(object):

    def __init__(self, status, body=b'', headers=None, frag_index=None):
        self.status = status
        self.body = body
        self.readable = BytesIO(body)
        self.headers = HeaderKeyDict(headers)
        if frag_index is not None:
            self.headers['X-Object-Sysmeta-Ec-Frag-Index'] = frag_index
        fake_reason = ('Fake', 'This response is a lie.')
        self.reason = swob.RESPONSE_REASONS.get(status, fake_reason)[0]

    def getheader(self, header_name, default=None):
        return self.headers.get(header_name, default)

    def getheaders(self):
        if 'Content-Length' not in self.headers:
            self.headers['Content-Length'] = len(self.body)
        return self.headers.items()

    def read(self, amt=0):
        return self.readable.read(amt)


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
    etag = md5(test_body).hexdigest()
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
        node_key = lambda n: (n['ip'], n['port'])
        part = utils.split_path(req['path'], 5, 5, True)[1]
        all_nodes.extend(policy.object_ring.get_part_nodes(part))
        all_nodes.extend(policy.object_ring.get_more_nodes(part))
        for i, node in enumerate(all_nodes):
            node_map[node_key(node)] = i
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
        if errno.errorcode.get(e.errno) in ('ENOSPC', 'ENOTSUP', 'EOPNOTSUPP'):
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
