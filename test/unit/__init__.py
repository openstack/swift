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
import errno
import sys
from contextlib import contextmanager, closing
from collections import defaultdict, Iterable
import itertools
from numbers import Number
from tempfile import NamedTemporaryFile
import time
import eventlet
from eventlet.green import socket
from tempfile import mkdtemp
from shutil import rmtree
from swift.common.utils import Timestamp
from test import get_config
from swift.common import swob, utils
from swift.common.ring import Ring, RingData
from hashlib import md5
import logging.handlers
from httplib import HTTPException
from swift.common import storage_policy
from swift.common.storage_policy import StoragePolicy, ECStoragePolicy
import functools
import cPickle as pickle
from gzip import GzipFile
import mock as mocklib
import inspect

EMPTY_ETAG = md5().hexdigest()

# try not to import this module from swift
if not os.path.basename(sys.argv[0]).startswith('swift'):
    # never patch HASH_PATH_SUFFIX AGAIN!
    utils.HASH_PATH_SUFFIX = 'endcap'


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
                            ec_type='jerasure_rs_vand', ec_ndata=10,
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
    seemed to patch setUp at the wrong time (i.e. in setup the global wasn't
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
        orig_tearDown = cls.tearDown

        def setUp(cls_self):
            self._orig_POLICIES = storage_policy._POLICIES
            if not getattr(cls_self, '_policies_patched', False):
                storage_policy._POLICIES = self.policies
                self._setup_rings()
                cls_self._policies_patched = True

            orig_setUp(cls_self)

        def tearDown(cls_self):
            orig_tearDown(cls_self)
            storage_policy._POLICIES = self._orig_POLICIES

        cls.setUp = setUp
        cls.tearDown = tearDown

        return cls

    def _patch_method(self, f):
        @functools.wraps(f)
        def mywrapper(*args, **kwargs):
            self._orig_POLICIES = storage_policy._POLICIES
            try:
                storage_policy._POLICIES = self.policies
                self._setup_rings()
                return f(*args, **kwargs)
            finally:
                storage_policy._POLICIES = self._orig_POLICIES
        return mywrapper

    def __enter__(self):
        self._orig_POLICIES = storage_policy._POLICIES
        storage_policy._POLICIES = self.policies

    def __exit__(self, *args):
        storage_policy._POLICIES = self._orig_POLICIES


class FakeRing(Ring):

    def __init__(self, replicas=3, max_more_nodes=0, part_power=0,
                 base_port=1000):
        """
        :param part_power: make part calculation based on the path

        If you set a part_power when you setup your FakeRing the parts you get
        out of ring methods will actually be based on the path - otherwise we
        exercise the real ring code, but ignore the result and return 1.
        """
        self._base_port = base_port
        self.max_more_nodes = max_more_nodes
        self._part_shift = 32 - part_power
        # 9 total nodes (6 more past the initial 3) is the cap, no matter if
        # this is set higher, or R^2 for R replicas
        self.set_replicas(replicas)
        self._reload()

    def _reload(self):
        self._rtime = time.time()

    def set_replicas(self, replicas):
        self.replicas = replicas
        self._devs = []
        for x in range(self.replicas):
            ip = '10.0.0.%s' % x
            port = self._base_port + x
            self._devs.append({
                'ip': ip,
                'replication_ip': ip,
                'port': port,
                'replication_port': port,
                'device': 'sd' + (chr(ord('a') + x)),
                'zone': x % 3,
                'region': x % 2,
                'id': x,
            })

    @property
    def replica_count(self):
        return self.replicas

    def _get_part_nodes(self, part):
        return [dict(node, index=i) for i, node in enumerate(list(self._devs))]

    def get_more_nodes(self, part):
        # replicas^2 is the true cap
        for x in xrange(self.replicas, min(self.replicas + self.max_more_nodes,
                                           self.replicas * self.replicas)):
            yield {'ip': '10.0.0.%s' % x,
                   'replication_ip': '10.0.0.%s' % x,
                   'port': self._base_port + x,
                   'replication_port': self._base_port + x,
                   'device': 'sda',
                   'zone': x % 3,
                   'region': x % 2,
                   'id': x}


def write_fake_ring(path, *devs):
    """
    Pretty much just a two node, two replica, 2 part power ring...
    """
    dev1 = {'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
            'port': 6000}
    dev2 = {'id': 0, 'zone': 0, 'device': 'sdb1', 'ip': '127.0.0.1',
            'port': 6000}

    dev1_updates, dev2_updates = devs or ({}, {})

    dev1.update(dev1_updates)
    dev2.update(dev2_updates)

    replica2part2dev_id = [[0, 1, 0, 1], [1, 0, 1, 0]]
    devs = [dev1, dev2]
    part_shift = 30
    with closing(GzipFile(path, 'wb')) as f:
        pickle.dump(RingData(replica2part2dev_id, devs, part_shift), f)


class FabricatedRing(Ring):
    """
    When a FakeRing just won't do - you can fabricate one to meet
    your tests needs.
    """

    def __init__(self, replicas=6, devices=8, nodes=4, port=6000,
                 part_power=4):
        self.devices = devices
        self.nodes = nodes
        self.port = port
        self.replicas = 6
        self.part_power = part_power
        self._part_shift = 32 - self.part_power
        self._reload()

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
    rv = ''
    lc = ''
    crlfs = 0
    while crlfs < 2:
        c = fd.read(1)
        if not c:
            raise ValueError("didn't get two CRLFs; just got %r" % rv)
        rv = rv + c
        if c == '\r' and lc != '\n':
            crlfs = 0
        if lc == '\r' and c == '\n':
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

xattr_data = {}


def _get_inode(fd):
    if not isinstance(fd, int):
        try:
            fd = fd.fileno()
        except AttributeError:
            return os.stat(fd).st_ino
    return os.fstat(fd).st_ino


def _setxattr(fd, k, v):
    inode = _get_inode(fd)
    data = xattr_data.get(inode, {})
    data[k] = v
    xattr_data[inode] = data


def _getxattr(fd, k):
    inode = _get_inode(fd)
    data = xattr_data.get(inode, {}).get(k)
    if not data:
        raise IOError(errno.ENODATA, "Fake IOError")
    return data

import xattr
xattr.setxattr = _setxattr
xattr.getxattr = _getxattr


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
    }

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
                           'warning': [], 'debug': []}

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
            print 'WARNING: unable to format log message %r %% %r' % (
                record.msg, record.args)
            raise
        self.lines_dict[record.levelname.lower()].append(line)

    def handle(self, record):
        self._handle(record)

    def flush(self):
        pass

    def handleError(self, record):
        pass


class DebugLogger(FakeLogger):
    """A simple stdout logging version of FakeLogger"""

    def __init__(self, *args, **kwargs):
        FakeLogger.__init__(self, *args, **kwargs)
        self.formatter = logging.Formatter(
            "%(server)s %(levelname)s: %(message)s")

    def handle(self, record):
        self._handle(record)
        print self.formatter.format(record)


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


class MockTrue(object):
    """
    Instances of MockTrue evaluate like True
    Any attr accessed on an instance of MockTrue will return a MockTrue
    instance. Any method called on an instance of MockTrue will return
    a MockTrue instance.

    >>> thing = MockTrue()
    >>> thing
    True
    >>> thing == True # True == True
    True
    >>> thing == False # True == False
    False
    >>> thing != True # True != True
    False
    >>> thing != False # True != False
    True
    >>> thing.attribute
    True
    >>> thing.method()
    True
    >>> thing.attribute.method()
    True
    >>> thing.method().attribute
    True

    """

    def __getattribute__(self, *args, **kwargs):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __repr__(*args, **kwargs):
        return repr(True)

    def __eq__(self, other):
        return other is True

    def __ne__(self, other):
        return other is not True


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

        def __init__(self, status, etag=None, body='', timestamp='1',
                     headers=None, expect_headers=None, connection_id=None,
                     give_send=None):
            # connect exception
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
            if 'slow' in kwargs and isinstance(kwargs['slow'], list):
                try:
                    self._next_sleep = kwargs['slow'].pop(0)
                except IndexError:
                    self._next_sleep = None
            # be nice to trixy bits with node_iter's
            eventlet.sleep()

        def getresponse(self):
            if self.expect_status and self.explicit_expect_list:
                raise Exception('Test did not consume all fake '
                                'expect status: %r' % (self.expect_status,))
            if isinstance(self.status, (Exception, eventlet.Timeout)):
                raise self.status
            exc = kwargs.get('raise_exc')
            if exc:
                if isinstance(exc, (Exception, eventlet.Timeout)):
                    raise exc
                raise Exception('test')
            if kwargs.get('raise_timeout_exc'):
                raise eventlet.Timeout()
            return self

        def getexpect(self):
            expect_status = self.expect_status.pop(0)
            if isinstance(self.expect_status, (Exception, eventlet.Timeout)):
                raise self.expect_status
            headers = dict(self.expect_headers)
            if expect_status == 409:
                headers['X-Backend-Timestamp'] = self.timestamp
            return FakeConn(expect_status, headers=headers)

        def getheaders(self):
            etag = self.etag
            if not etag:
                if isinstance(self.body, str):
                    etag = '"' + md5(self.body).hexdigest() + '"'
                else:
                    etag = '"68b329da9893e34099c7d8ad5cb9c940"'

            headers = swob.HeaderKeyDict({
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
                if container_ts_iter.next() is False:
                    headers['x-container-timestamp'] = '1'
            except StopIteration:
                pass
            am_slow, value = self.get_slow()
            if am_slow:
                headers['content-length'] = '4'
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
                if self.sent < 4:
                    self.sent += 1
                    eventlet.sleep(value)
                    return ' '
            rv = self.body[:amt]
            self.body = self.body[amt:]
            return rv

        def send(self, amt=None):
            if self.give_send:
                self.give_send(self.connection_id, amt)
            am_slow, value = self.get_slow()
            if am_slow:
                if self.received < 4:
                    self.received += 1
                    eventlet.sleep(value)

        def getheader(self, name, default=None):
            return swob.HeaderKeyDict(self.getheaders()).get(name, default)

        def close(self):
            pass

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

    def connect(*args, **ckwargs):
        if kwargs.get('slow_connect', False):
            eventlet.sleep(0.1)
        if 'give_content_type' in kwargs:
            if len(args) >= 7 and 'Content-Type' in args[6]:
                kwargs['give_content_type'](args[6]['Content-Type'])
            else:
                kwargs['give_content_type']('')
        i, status = conn_id_and_code_iter.next()
        if 'give_connect' in kwargs:
            give_conn_fn = kwargs['give_connect']
            argspec = inspect.getargspec(give_conn_fn)
            if argspec.keywords or 'connection_id' in argspec.args:
                ckwargs['connection_id'] = i
            give_conn_fn(*args, **ckwargs)
        etag = etag_iter.next()
        headers = headers_iter.next()
        expect_headers = expect_headers_iter.next()
        timestamp = timestamps_iter.next()

        if status <= 0:
            raise HTTPException()
        if body_iter is None:
            body = static_body or ''
        else:
            body = body_iter.next()
        return FakeConn(status, etag, body=body, timestamp=timestamp,
                        headers=headers, expect_headers=expect_headers,
                        connection_id=i, give_send=kwargs.get('give_send'))

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


def make_timestamp_iter():
    return iter(Timestamp(t) for t in itertools.count(int(time.time())))
