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

from __future__ import print_function
import mock
import os
from six.moves.urllib.parse import urlparse
import sys
import pickle
import socket
import locale
import eventlet
import eventlet.debug
import functools
import random

from time import time, sleep
from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp
from unittest2 import SkipTest

from six.moves.configparser import ConfigParser, NoSectionError
from six.moves import http_client
from six.moves.http_client import HTTPException

from swift.common.middleware.memcache import MemcacheMiddleware
from swift.common.storage_policy import parse_storage_policies, PolicyError

from test import get_config
from test.functional.swift_test_client import Account, Connection, Container, \
    ResponseError
# This has the side effect of mocking out the xattr module so that unit tests
# (and in this case, when in-process functional tests are called for) can run
# on file systems that don't support extended attributes.
from test.unit import debug_logger, FakeMemcache

from swift.common import constraints, utils, ring, storage_policy
from swift.common.ring import Ring
from swift.common.wsgi import monkey_patch_mimetools, loadapp
from swift.common.utils import config_true_value, split_path
from swift.account import server as account_server
from swift.container import server as container_server
from swift.obj import server as object_server, mem_server as mem_object_server
import swift.proxy.controllers.obj

http_client._MAXHEADERS = constraints.MAX_HEADER_COUNT
DEBUG = True

# In order to get the proper blocking behavior of sockets without using
# threads, where we can set an arbitrary timeout for some piece of code under
# test, we use eventlet with the standard socket library patched. We have to
# perform this setup at module import time, since all the socket module
# bindings in the swiftclient code will have been made by the time nose
# invokes the package or class setup methods.
eventlet.hubs.use_hub(utils.get_hub())
eventlet.patcher.monkey_patch(all=False, socket=True)
eventlet.debug.hub_exceptions(False)

from swiftclient import get_auth, http_connection

has_insecure = False
try:
    from swiftclient import __version__ as client_version
    # Prevent a ValueError in StrictVersion with '2.0.3.68.ga99c2ff'
    client_version = '.'.join(client_version.split('.')[:3])
except ImportError:
    # Pre-PBR we had version, not __version__. Anyhow...
    client_version = '1.2'
from distutils.version import StrictVersion
if StrictVersion(client_version) >= StrictVersion('2.0'):
    has_insecure = True


config = {}
web_front_end = None
normalized_urls = None

# If no config was read, we will fall back to old school env vars
swift_test_auth_version = None
swift_test_auth = os.environ.get('SWIFT_TEST_AUTH')
swift_test_user = [os.environ.get('SWIFT_TEST_USER'), None, None, '', '', '']
swift_test_key = [os.environ.get('SWIFT_TEST_KEY'), None, None, '', '', '']
swift_test_tenant = ['', '', '', '', '', '']
swift_test_perm = ['', '', '', '', '', '']
swift_test_domain = ['', '', '', '', '', '']
swift_test_user_id = ['', '', '', '', '', '']
swift_test_tenant_id = ['', '', '', '', '', '']

skip, skip2, skip3, skip_service_tokens, skip_if_no_reseller_admin = \
    False, False, False, False, False

orig_collate = ''
insecure = False

orig_hash_path_suff_pref = ('', '')
orig_swift_conf_name = None

in_process = False
_testdir = _test_servers = _test_coros = _test_socks = None
policy_specified = None


class FakeMemcacheMiddleware(MemcacheMiddleware):
    """
    Caching middleware that fakes out caching in swift if memcached
    does not appear to be running.
    """

    def __init__(self, app, conf):
        super(FakeMemcacheMiddleware, self).__init__(app, conf)
        self.memcache = FakeMemcache()


class InProcessException(BaseException):
    pass


def _info(msg):
    print(msg, file=sys.stderr)


def _debug(msg):
    if DEBUG:
        _info('DEBUG: ' + msg)


def _in_process_setup_swift_conf(swift_conf_src, testdir):
    # override swift.conf contents for in-process functional test runs
    conf = ConfigParser()
    conf.read(swift_conf_src)
    try:
        section = 'swift-hash'
        conf.set(section, 'swift_hash_path_suffix', 'inprocfunctests')
        conf.set(section, 'swift_hash_path_prefix', 'inprocfunctests')
        section = 'swift-constraints'
        max_file_size = (8 * 1024 * 1024) + 2  # 8 MB + 2
        conf.set(section, 'max_file_size', max_file_size)
    except NoSectionError:
        msg = 'Conf file %s is missing section %s' % (swift_conf_src, section)
        raise InProcessException(msg)

    test_conf_file = os.path.join(testdir, 'swift.conf')
    with open(test_conf_file, 'w') as fp:
        conf.write(fp)

    return test_conf_file


def _in_process_find_conf_file(conf_src_dir, conf_file_name, use_sample=True):
    """
    Look for a file first in conf_src_dir, if it exists, otherwise optionally
    look in the source tree sample 'etc' dir.

    :param conf_src_dir: Directory in which to search first for conf file. May
                         be None
    :param conf_file_name: Name of conf file
    :param use_sample: If True and the conf_file_name is not found, then return
                       any sample conf file found in the source tree sample
                       'etc' dir by appending '-sample' to conf_file_name
    :returns: Path to conf file
    :raises InProcessException: If no conf file is found
    """
    dflt_src_dir = os.path.normpath(os.path.join(os.path.abspath(__file__),
                                    os.pardir, os.pardir, os.pardir,
                                    'etc'))
    conf_src_dir = dflt_src_dir if conf_src_dir is None else conf_src_dir
    conf_file_path = os.path.join(conf_src_dir, conf_file_name)
    if os.path.exists(conf_file_path):
        return conf_file_path

    if use_sample:
        # fall back to using the corresponding sample conf file
        conf_file_name += '-sample'
        conf_file_path = os.path.join(dflt_src_dir, conf_file_name)
        if os.path.exists(conf_file_path):
            return conf_file_path

    msg = 'Failed to find config file %s' % conf_file_name
    raise InProcessException(msg)


def _in_process_setup_ring(swift_conf, conf_src_dir, testdir):
    """
    If SWIFT_TEST_POLICY is set:
    - look in swift.conf file for specified policy
    - move this to be policy-0 but preserving its options
    - copy its ring file to test dir, changing its devices to suit
      in process testing, and renaming it to suit policy-0
    Otherwise, create a default ring file.
    """
    conf = ConfigParser()
    conf.read(swift_conf)
    sp_prefix = 'storage-policy:'

    try:
        # policy index 0 will be created if no policy exists in conf
        policies = parse_storage_policies(conf)
    except PolicyError as e:
        raise InProcessException(e)

    # clear all policies from test swift.conf before adding test policy back
    for policy in policies:
        conf.remove_section(sp_prefix + str(policy.idx))

    if policy_specified:
        policy_to_test = policies.get_by_name(policy_specified)
        if policy_to_test is None:
            raise InProcessException('Failed to find policy name "%s"'
                                     % policy_specified)
        _info('Using specified policy %s' % policy_to_test.name)
    else:
        policy_to_test = policies.default
        _info('Defaulting to policy %s' % policy_to_test.name)

    # make policy_to_test be policy index 0 and default for the test config
    sp_zero_section = sp_prefix + '0'
    conf.add_section(sp_zero_section)
    for (k, v) in policy_to_test.get_info(config=True).items():
        conf.set(sp_zero_section, k, v)
    conf.set(sp_zero_section, 'default', True)

    with open(swift_conf, 'w') as fp:
        conf.write(fp)

    # look for a source ring file
    ring_file_src = ring_file_test = 'object.ring.gz'
    if policy_to_test.idx:
        ring_file_src = 'object-%s.ring.gz' % policy_to_test.idx
    try:
        ring_file_src = _in_process_find_conf_file(conf_src_dir, ring_file_src,
                                                   use_sample=False)
    except InProcessException as e:
        if policy_specified:
            raise InProcessException('Failed to find ring file %s'
                                     % ring_file_src)
        ring_file_src = None

    ring_file_test = os.path.join(testdir, ring_file_test)
    if ring_file_src:
        # copy source ring file to a policy-0 test ring file, re-homing servers
        _info('Using source ring file %s' % ring_file_src)
        ring_data = ring.RingData.load(ring_file_src)
        obj_sockets = []
        for dev in ring_data.devs:
            device = 'sd%c1' % chr(len(obj_sockets) + ord('a'))
            utils.mkdirs(os.path.join(_testdir, 'sda1'))
            utils.mkdirs(os.path.join(_testdir, 'sda1', 'tmp'))
            obj_socket = eventlet.listen(('localhost', 0))
            obj_sockets.append(obj_socket)
            dev['port'] = obj_socket.getsockname()[1]
            dev['ip'] = '127.0.0.1'
            dev['device'] = device
            dev['replication_port'] = dev['port']
            dev['replication_ip'] = dev['ip']
        ring_data.save(ring_file_test)
    else:
        # make default test ring, 2 replicas, 4 partitions, 2 devices
        _info('No source object ring file, creating 2rep/4part/2dev ring')
        obj_sockets = [eventlet.listen(('localhost', 0)) for _ in (0, 1)]
        ring_data = ring.RingData(
            [[0, 1, 0, 1], [1, 0, 1, 0]],
            [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
              'port': obj_sockets[0].getsockname()[1]},
             {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
              'port': obj_sockets[1].getsockname()[1]}],
            30)
        with closing(GzipFile(ring_file_test, 'wb')) as f:
            pickle.dump(ring_data, f)

    for dev in ring_data.devs:
        _debug('Ring file dev: %s' % dev)

    return obj_sockets


def _load_encryption(proxy_conf_file, **kwargs):
    """
    Load encryption configuration and override proxy-server.conf contents.

    :param proxy_conf_file: Source proxy conf filename
    :returns: Path to the test proxy conf file to use
    :raises InProcessException: raised if proxy conf contents are invalid
    """
    _debug('Setting configuration for encryption')

    # The global conf dict cannot be used to modify the pipeline.
    # The pipeline loader requires the pipeline to be set in the local_conf.
    # If pipeline is set in the global conf dict (which in turn populates the
    # DEFAULTS options) then it prevents pipeline being loaded into the local
    # conf during wsgi load_app.
    # Therefore we must modify the [pipeline:main] section.

    conf = ConfigParser()
    conf.read(proxy_conf_file)
    try:
        section = 'pipeline:main'
        pipeline = conf.get(section, 'pipeline')
        pipeline = pipeline.replace(
            "proxy-logging proxy-server",
            "keymaster encryption proxy-logging proxy-server")
        conf.set(section, 'pipeline', pipeline)
        root_secret = os.urandom(32).encode("base64")
        conf.set('filter:keymaster', 'encryption_root_secret', root_secret)
    except NoSectionError as err:
        msg = 'Error problem with proxy conf file %s: %s' % \
              (proxy_conf_file, err)
        raise InProcessException(msg)

    test_conf_file = os.path.join(_testdir, 'proxy-server.conf')
    with open(test_conf_file, 'w') as fp:
        conf.write(fp)

    return test_conf_file


# Mapping from possible values of the variable
# SWIFT_TEST_IN_PROCESS_CONF_LOADER
# to the method to call for loading the associated configuration
# The expected signature for these methods is:
# conf_filename_to_use loader(input_conf_filename, **kwargs)
conf_loaders = {
    'encryption': _load_encryption
}


def in_process_setup(the_object_server=object_server):
    _info('IN-PROCESS SERVERS IN USE FOR FUNCTIONAL TESTS')
    _info('Using object_server class: %s' % the_object_server.__name__)
    conf_src_dir = os.environ.get('SWIFT_TEST_IN_PROCESS_CONF_DIR')
    show_debug_logs = os.environ.get('SWIFT_TEST_DEBUG_LOGS')

    if conf_src_dir is not None:
        if not os.path.isdir(conf_src_dir):
            msg = 'Config source %s is not a dir' % conf_src_dir
            raise InProcessException(msg)
        _info('Using config source dir: %s' % conf_src_dir)

    # If SWIFT_TEST_IN_PROCESS_CONF specifies a config source dir then
    # prefer config files from there, otherwise read config from source tree
    # sample files. A mixture of files from the two sources is allowed.
    proxy_conf = _in_process_find_conf_file(conf_src_dir, 'proxy-server.conf')
    _info('Using proxy config from %s' % proxy_conf)
    swift_conf_src = _in_process_find_conf_file(conf_src_dir, 'swift.conf')
    _info('Using swift config from %s' % swift_conf_src)

    monkey_patch_mimetools()

    global _testdir
    _testdir = os.path.join(mkdtemp(), 'tmp_functional')
    utils.mkdirs(_testdir)
    rmtree(_testdir)
    utils.mkdirs(os.path.join(_testdir, 'sda1'))
    utils.mkdirs(os.path.join(_testdir, 'sda1', 'tmp'))
    utils.mkdirs(os.path.join(_testdir, 'sdb1'))
    utils.mkdirs(os.path.join(_testdir, 'sdb1', 'tmp'))

    # Call the associated method for the value of
    # 'SWIFT_TEST_IN_PROCESS_CONF_LOADER', if one exists
    conf_loader_label = os.environ.get(
        'SWIFT_TEST_IN_PROCESS_CONF_LOADER')
    if conf_loader_label is not None:
        try:
            conf_loader = conf_loaders[conf_loader_label]
            _debug('Calling method %s mapped to conf loader %s' %
                   (conf_loader.__name__, conf_loader_label))
        except KeyError as missing_key:
            raise InProcessException('No function mapped for conf loader %s' %
                                     missing_key)

        try:
            # Pass-in proxy_conf
            proxy_conf = conf_loader(proxy_conf)
            _debug('Now using proxy conf %s' % proxy_conf)
        except Exception as err:  # noqa
            raise InProcessException(err)

    swift_conf = _in_process_setup_swift_conf(swift_conf_src, _testdir)
    obj_sockets = _in_process_setup_ring(swift_conf, conf_src_dir, _testdir)

    global orig_swift_conf_name
    orig_swift_conf_name = utils.SWIFT_CONF_FILE
    utils.SWIFT_CONF_FILE = swift_conf
    constraints.reload_constraints()
    storage_policy.SWIFT_CONF_FILE = swift_conf
    storage_policy.reload_storage_policies()
    global config
    if constraints.SWIFT_CONSTRAINTS_LOADED:
        # Use the swift constraints that are loaded for the test framework
        # configuration
        _c = dict((k, str(v))
                  for k, v in constraints.EFFECTIVE_CONSTRAINTS.items())
        config.update(_c)
    else:
        # In-process swift constraints were not loaded, somethings wrong
        raise SkipTest
    global orig_hash_path_suff_pref
    orig_hash_path_suff_pref = utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX
    utils.validate_hash_conf()

    global _test_socks
    _test_socks = []
    # We create the proxy server listening socket to get its port number so
    # that we can add it as the "auth_port" value for the functional test
    # clients.
    prolis = eventlet.listen(('localhost', 0))
    _test_socks.append(prolis)

    # The following set of configuration values is used both for the
    # functional test frame work and for the various proxy, account, container
    # and object servers.
    config.update({
        # Values needed by the various in-process swift servers
        'devices': _testdir,
        'swift_dir': _testdir,
        'mount_check': 'false',
        'client_timeout': '4',
        'allow_account_management': 'true',
        'account_autocreate': 'true',
        'allow_versions': 'True',
        'allow_versioned_writes': 'True',
        # Below are values used by the functional test framework, as well as
        # by the various in-process swift servers
        'auth_host': '127.0.0.1',
        'auth_port': str(prolis.getsockname()[1]),
        'auth_ssl': 'no',
        'auth_prefix': '/auth/',
        # Primary functional test account (needs admin access to the
        # account)
        'account': 'test',
        'username': 'tester',
        'password': 'testing',
        # User on a second account (needs admin access to the account)
        'account2': 'test2',
        'username2': 'tester2',
        'password2': 'testing2',
        # User on same account as first, but without admin access
        'username3': 'tester3',
        'password3': 'testing3',
        # Service user and prefix (emulates glance, cinder, etc. user)
        'account5': 'test5',
        'username5': 'tester5',
        'password5': 'testing5',
        'service_prefix': 'SERVICE',
        # For tempauth middleware. Update reseller_prefix
        'reseller_prefix': 'AUTH, SERVICE',
        'SERVICE_require_group': 'service',
        # Reseller admin user (needs reseller_admin_role)
        'account6': 'test6',
        'username6': 'tester6',
        'password6': 'testing6'
    })

    # If an env var explicitly specifies the proxy-server object_post_as_copy
    # option then use its value, otherwise leave default config unchanged.
    object_post_as_copy = os.environ.get(
        'SWIFT_TEST_IN_PROCESS_OBJECT_POST_AS_COPY')
    if object_post_as_copy is not None:
        object_post_as_copy = config_true_value(object_post_as_copy)
        config['object_post_as_copy'] = str(object_post_as_copy)
        _debug('Setting object_post_as_copy to %r' % object_post_as_copy)

    acc1lis = eventlet.listen(('localhost', 0))
    acc2lis = eventlet.listen(('localhost', 0))
    con1lis = eventlet.listen(('localhost', 0))
    con2lis = eventlet.listen(('localhost', 0))
    _test_socks += [acc1lis, acc2lis, con1lis, con2lis] + obj_sockets

    account_ring_path = os.path.join(_testdir, 'account.ring.gz')
    with closing(GzipFile(account_ring_path, 'wb')) as f:
        pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                    [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
                      'port': acc1lis.getsockname()[1]},
                     {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                      'port': acc2lis.getsockname()[1]}], 30),
                    f)
    container_ring_path = os.path.join(_testdir, 'container.ring.gz')
    with closing(GzipFile(container_ring_path, 'wb')) as f:
        pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                    [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
                      'port': con1lis.getsockname()[1]},
                     {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                      'port': con2lis.getsockname()[1]}], 30),
                    f)

    eventlet.wsgi.HttpProtocol.default_request_version = "HTTP/1.0"
    # Turn off logging requests by the underlying WSGI software.
    eventlet.wsgi.HttpProtocol.log_request = lambda *a: None
    logger = utils.get_logger(config, 'wsgi-server', log_route='wsgi')
    # Redirect logging other messages by the underlying WSGI software.
    eventlet.wsgi.HttpProtocol.log_message = \
        lambda s, f, *a: logger.error('ERROR WSGI: ' + f % a)
    # Default to only 4 seconds for in-process functional test runs
    eventlet.wsgi.WRITE_TIMEOUT = 4

    def get_logger_name(name):
        if show_debug_logs:
            return debug_logger(name)
        else:
            return None

    acc1srv = account_server.AccountController(
        config, logger=get_logger_name('acct1'))
    acc2srv = account_server.AccountController(
        config, logger=get_logger_name('acct2'))
    con1srv = container_server.ContainerController(
        config, logger=get_logger_name('cont1'))
    con2srv = container_server.ContainerController(
        config, logger=get_logger_name('cont2'))

    objsrvs = [
        (obj_sockets[index],
         the_object_server.ObjectController(
             config, logger=get_logger_name('obj%d' % (index + 1))))
        for index in range(len(obj_sockets))
    ]

    if show_debug_logs:
        logger = debug_logger('proxy')

    def get_logger(name, *args, **kwargs):
        return logger

    with mock.patch('swift.common.utils.get_logger', get_logger):
        with mock.patch('swift.common.middleware.memcache.MemcacheMiddleware',
                        FakeMemcacheMiddleware):
            try:
                app = loadapp(proxy_conf, global_conf=config)
            except Exception as e:
                raise InProcessException(e)

    nl = utils.NullLogger()
    global proxy_srv
    proxy_srv = prolis
    prospa = eventlet.spawn(eventlet.wsgi.server, prolis, app, nl)
    acc1spa = eventlet.spawn(eventlet.wsgi.server, acc1lis, acc1srv, nl)
    acc2spa = eventlet.spawn(eventlet.wsgi.server, acc2lis, acc2srv, nl)
    con1spa = eventlet.spawn(eventlet.wsgi.server, con1lis, con1srv, nl)
    con2spa = eventlet.spawn(eventlet.wsgi.server, con2lis, con2srv, nl)

    objspa = [eventlet.spawn(eventlet.wsgi.server, objsrv[0], objsrv[1], nl)
              for objsrv in objsrvs]

    global _test_coros
    _test_coros = \
        (prospa, acc1spa, acc2spa, con1spa, con2spa) + tuple(objspa)

    # Create accounts "test" and "test2"
    def create_account(act):
        ts = utils.normalize_timestamp(time())
        account_ring = Ring(_testdir, ring_name='account')
        partition, nodes = account_ring.get_nodes(act)
        for node in nodes:
            # Note: we are just using the http_connect method in the object
            # controller here to talk to the account server nodes.
            conn = swift.proxy.controllers.obj.http_connect(
                node['ip'], node['port'], node['device'], partition, 'PUT',
                '/' + act, {'X-Timestamp': ts, 'x-trans-id': act})
            resp = conn.getresponse()
            assert(resp.status == 201)

    create_account('AUTH_test')
    create_account('AUTH_test2')

cluster_info = {}


def get_cluster_info():
    # The fallback constraints used for testing will come from the current
    # effective constraints.
    eff_constraints = dict(constraints.EFFECTIVE_CONSTRAINTS)

    # We'll update those constraints based on what the /info API provides, if
    # anything.
    global cluster_info
    global config
    try:
        conn = Connection(config)
        conn.authenticate()
        cluster_info.update(conn.cluster_info())
    except (ResponseError, socket.error):
        # Failed to get cluster_information via /info API, so fall back on
        # test.conf data
        pass
    else:
        try:
            eff_constraints.update(cluster_info['swift'])
        except KeyError:
            # Most likely the swift cluster has "expose_info = false" set
            # in its proxy-server.conf file, so we'll just do the best we
            # can.
            print("** Swift Cluster not exposing /info **", file=sys.stderr)

    # Finally, we'll allow any constraint present in the swift-constraints
    # section of test.conf to override everything. Note that only those
    # constraints defined in the constraints module are converted to integers.
    test_constraints = get_config('swift-constraints')
    for k in constraints.DEFAULT_CONSTRAINTS:
        try:
            test_constraints[k] = int(test_constraints[k])
        except KeyError:
            pass
        except ValueError:
            print("Invalid constraint value: %s = %s" % (
                k, test_constraints[k]), file=sys.stderr)
    eff_constraints.update(test_constraints)

    # Just make it look like these constraints were loaded from a /info call,
    # even if the /info call failed, or when they are overridden by values
    # from the swift-constraints section of test.conf
    cluster_info['swift'] = eff_constraints


def setup_package():

    global policy_specified
    policy_specified = os.environ.get('SWIFT_TEST_POLICY')
    in_process_env = os.environ.get('SWIFT_TEST_IN_PROCESS')
    if in_process_env is not None:
        use_in_process = utils.config_true_value(in_process_env)
    else:
        use_in_process = None

    global in_process

    global config
    if use_in_process:
        # Explicitly set to True, so barrel on ahead with in-process
        # functional test setup.
        in_process = True
        # NOTE: No attempt is made to a read local test.conf file.
    else:
        if use_in_process is None:
            # Not explicitly set, default to using in-process functional tests
            # if the test.conf file is not found, or does not provide a usable
            # configuration.
            config.update(get_config('func_test'))
            if not config:
                in_process = True
            # else... leave in_process value unchanged. It may be that
            # setup_package is called twice, in which case in_process_setup may
            # have loaded config before we reach here a second time, so the
            # existence of config is not reliable to determine that in_process
            # should be False. Anyway, it's default value is False.
        else:
            # Explicitly set to False, do not attempt to use in-process
            # functional tests, be sure we attempt to read from local
            # test.conf file.
            in_process = False
            config.update(get_config('func_test'))

    if in_process:
        in_mem_obj_env = os.environ.get('SWIFT_TEST_IN_MEMORY_OBJ')
        in_mem_obj = utils.config_true_value(in_mem_obj_env)
        try:
            in_process_setup(the_object_server=(
                mem_object_server if in_mem_obj else object_server))
        except InProcessException as exc:
            print(('Exception during in-process setup: %s'
                   % str(exc)), file=sys.stderr)
            raise

    global web_front_end
    web_front_end = config.get('web_front_end', 'integral')
    global normalized_urls
    normalized_urls = config.get('normalized_urls', False)

    global orig_collate
    orig_collate = locale.setlocale(locale.LC_COLLATE)
    locale.setlocale(locale.LC_COLLATE, config.get('collate', 'C'))

    global insecure
    insecure = config_true_value(config.get('insecure', False))

    global swift_test_auth_version
    global swift_test_auth
    global swift_test_user
    global swift_test_key
    global swift_test_tenant
    global swift_test_perm
    global swift_test_domain
    global swift_test_service_prefix

    swift_test_service_prefix = None

    if config:
        swift_test_auth_version = str(config.get('auth_version', '1'))

        swift_test_auth = 'http'
        if config_true_value(config.get('auth_ssl', 'no')):
            swift_test_auth = 'https'
        if 'auth_prefix' not in config:
            config['auth_prefix'] = '/'
        try:
            suffix = '://%(auth_host)s:%(auth_port)s%(auth_prefix)s' % config
            swift_test_auth += suffix
        except KeyError:
            pass  # skip

        if 'service_prefix' in config:
                swift_test_service_prefix = utils.append_underscore(
                    config['service_prefix'])

        if swift_test_auth_version == "1":
            swift_test_auth += 'v1.0'

            try:
                if 'account' in config:
                    swift_test_user[0] = '%(account)s:%(username)s' % config
                else:
                    swift_test_user[0] = '%(username)s' % config
                swift_test_key[0] = config['password']
            except KeyError:
                # bad config, no account/username configured, tests cannot be
                # run
                pass
            try:
                swift_test_user[1] = '%s%s' % (
                    '%s:' % config['account2'] if 'account2' in config else '',
                    config['username2'])
                swift_test_key[1] = config['password2']
            except KeyError:
                pass  # old config, no second account tests can be run
            try:
                swift_test_user[2] = '%s%s' % (
                    '%s:' % config['account'] if 'account'
                    in config else '', config['username3'])
                swift_test_key[2] = config['password3']
            except KeyError:
                pass  # old config, no third account tests can be run
            try:
                swift_test_user[4] = '%s%s' % (
                    '%s:' % config['account5'], config['username5'])
                swift_test_key[4] = config['password5']
                swift_test_tenant[4] = config['account5']
            except KeyError:
                pass  # no service token tests can be run

            for _ in range(3):
                swift_test_perm[_] = swift_test_user[_]

        else:
            swift_test_user[0] = config['username']
            swift_test_tenant[0] = config['account']
            swift_test_key[0] = config['password']
            swift_test_user[1] = config['username2']
            swift_test_tenant[1] = config['account2']
            swift_test_key[1] = config['password2']
            swift_test_user[2] = config['username3']
            swift_test_tenant[2] = config['account']
            swift_test_key[2] = config['password3']
            if 'username4' in config:
                swift_test_user[3] = config['username4']
                swift_test_tenant[3] = config['account4']
                swift_test_key[3] = config['password4']
                swift_test_domain[3] = config['domain4']
            if 'username5' in config:
                swift_test_user[4] = config['username5']
                swift_test_tenant[4] = config['account5']
                swift_test_key[4] = config['password5']
            if 'username6' in config:
                swift_test_user[5] = config['username6']
                swift_test_tenant[5] = config['account6']
                swift_test_key[5] = config['password6']

            for _ in range(5):
                swift_test_perm[_] = swift_test_tenant[_] + ':' \
                    + swift_test_user[_]

    global skip
    skip = not all([swift_test_auth, swift_test_user[0], swift_test_key[0]])
    if skip:
        print('SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG', file=sys.stderr)

    global skip2
    skip2 = not all([not skip, swift_test_user[1], swift_test_key[1]])
    if not skip and skip2:
        print('SKIPPING SECOND ACCOUNT FUNCTIONAL TESTS '
              'DUE TO NO CONFIG FOR THEM', file=sys.stderr)

    global skip3
    skip3 = not all([not skip, swift_test_user[2], swift_test_key[2]])
    if not skip and skip3:
        print('SKIPPING THIRD ACCOUNT FUNCTIONAL TESTS'
              'DUE TO NO CONFIG FOR THEM', file=sys.stderr)

    global skip_if_not_v3
    skip_if_not_v3 = (swift_test_auth_version != '3'
                      or not all([not skip,
                                  swift_test_user[3],
                                  swift_test_key[3]]))
    if not skip and skip_if_not_v3:
        print('SKIPPING FUNCTIONAL TESTS SPECIFIC TO AUTH VERSION 3',
              file=sys.stderr)

    global skip_service_tokens
    skip_service_tokens = not all([not skip, swift_test_user[4],
                                   swift_test_key[4], swift_test_tenant[4],
                                   swift_test_service_prefix])
    if not skip and skip_service_tokens:
        print(
            'SKIPPING FUNCTIONAL TESTS SPECIFIC TO SERVICE TOKENS',
            file=sys.stderr)

    if policy_specified:
        policies = FunctionalStoragePolicyCollection.from_info()
        for p in policies:
            # policy names are case-insensitive
            if policy_specified.lower() == p['name'].lower():
                _info('Using specified policy %s' % policy_specified)
                FunctionalStoragePolicyCollection.policy_specified = p
                Container.policy_specified = policy_specified
                break
        else:
            _info(
                'SKIPPING FUNCTIONAL TESTS: Failed to find specified policy %s'
                % policy_specified)
            raise Exception('Failed to find specified policy %s'
                            % policy_specified)

    global skip_if_no_reseller_admin
    skip_if_no_reseller_admin = not all([not skip, swift_test_user[5],
                                         swift_test_key[5],
                                         swift_test_tenant[5]])
    if not skip and skip_if_no_reseller_admin:
        print(
            'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG FOR RESELLER ADMIN',
            file=sys.stderr)

    get_cluster_info()


def teardown_package():
    global orig_collate
    locale.setlocale(locale.LC_COLLATE, orig_collate)

    # clean up containers and objects left behind after running tests
    global config

    if config:
        conn = Connection(config)
        conn.authenticate()
        account = Account(conn, config.get('account', config['username']))
        account.delete_containers()

    global in_process
    global _test_socks
    if in_process:
        try:
            for i, server in enumerate(_test_coros):
                server.kill()
                if not server.dead:
                    # kill it from the socket level
                    _test_socks[i].close()
        except Exception:
            pass
        try:
            rmtree(os.path.dirname(_testdir))
        except Exception:
            pass
        utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX = \
            orig_hash_path_suff_pref
        utils.SWIFT_CONF_FILE = orig_swift_conf_name
        constraints.reload_constraints()
        reset_globals()


class AuthError(Exception):
    pass


class InternalServerError(Exception):
    pass


url = [None, None, None, None, None]
token = [None, None, None, None, None]
service_token = [None, None, None, None, None]
parsed = [None, None, None, None, None]
conn = [None, None, None, None, None]


def reset_globals():
    global url, token, service_token, parsed, conn, config
    url = [None, None, None, None, None]
    token = [None, None, None, None, None]
    service_token = [None, None, None, None, None]
    parsed = [None, None, None, None, None]
    conn = [None, None, None, None, None]
    if config:
        config = {}


def connection(url):
    if has_insecure:
        parsed_url, http_conn = http_connection(url, insecure=insecure)
    else:
        parsed_url, http_conn = http_connection(url)

    orig_request = http_conn.request

    # Add the policy header if policy_specified is set
    def request_with_policy(method, url, body=None, headers={}):
        version, account, container, obj = split_path(url, 1, 4, True)
        if policy_specified and method == 'PUT' and container and not obj \
                and 'X-Storage-Policy' not in headers:
            headers['X-Storage-Policy'] = policy_specified

        return orig_request(method, url, body, headers)

    http_conn.request = request_with_policy

    return parsed_url, http_conn


def get_url_token(user_index, os_options):
    authargs = dict(snet=False,
                    tenant_name=swift_test_tenant[user_index],
                    auth_version=swift_test_auth_version,
                    os_options=os_options,
                    insecure=insecure)
    return get_auth(swift_test_auth,
                    swift_test_user[user_index],
                    swift_test_key[user_index],
                    **authargs)


def retry(func, *args, **kwargs):
    """
    You can use the kwargs to override:
      'retries' (default: 5)
      'use_account' (default: 1) - which user's token to pass
      'url_account' (default: matches 'use_account') - which user's storage URL
      'resource' (default: url[url_account] - URL to connect to; retry()
          will interpolate the variable :storage_url: if present
      'service_user' - add a service token from this user (1 indexed)
    """
    global url, token, service_token, parsed, conn
    retries = kwargs.get('retries', 5)
    attempts, backoff = 0, 1

    # use account #1 by default; turn user's 1-indexed account into 0-indexed
    use_account = kwargs.pop('use_account', 1) - 1
    service_user = kwargs.pop('service_user', None)
    if service_user:
        service_user -= 1  # 0-index

    # access our own account by default
    url_account = kwargs.pop('url_account', use_account + 1) - 1
    os_options = {'user_domain_name': swift_test_domain[use_account],
                  'project_domain_name': swift_test_domain[use_account]}
    while attempts <= retries:
        auth_failure = False
        attempts += 1
        try:
            if not url[use_account] or not token[use_account]:
                url[use_account], token[use_account] = get_url_token(
                    use_account, os_options)
                parsed[use_account] = conn[use_account] = None
            if not parsed[use_account] or not conn[use_account]:
                parsed[use_account], conn[use_account] = \
                    connection(url[use_account])

            # default resource is the account url[url_account]
            resource = kwargs.pop('resource', '%(storage_url)s')
            template_vars = {'storage_url': url[url_account]}
            parsed_result = urlparse(resource % template_vars)
            if isinstance(service_user, int):
                if not service_token[service_user]:
                    dummy, service_token[service_user] = get_url_token(
                        service_user, os_options)
                kwargs['service_token'] = service_token[service_user]
            return func(url[url_account], token[use_account],
                        parsed_result, conn[url_account],
                        *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed[use_account] = conn[use_account] = None
            if service_user:
                service_token[service_user] = None
        except AuthError:
            auth_failure = True
            url[use_account] = token[use_account] = None
            if service_user:
                service_token[service_user] = None
        except InternalServerError:
            pass
        if attempts <= retries:
            if not auth_failure:
                sleep(backoff)
            backoff *= 2
    raise Exception('No result after %s retries.' % retries)


def check_response(conn):
    resp = conn.getresponse()
    if resp.status == 401:
        resp.read()
        raise AuthError()
    elif resp.status // 100 == 5:
        resp.read()
        raise InternalServerError()
    return resp


def load_constraint(name):
    global cluster_info
    try:
        c = cluster_info['swift'][name]
    except KeyError:
        raise SkipTest("Missing constraint: %s" % name)
    if not isinstance(c, int):
        raise SkipTest("Bad value, %r, for constraint: %s" % (c, name))
    return c


def get_storage_policy_from_cluster_info(info):
    policies = info['swift'].get('policies', {})
    default_policy = []
    non_default_policies = []
    for p in policies:
        if p.get('default', {}):
            default_policy.append(p)
        else:
            non_default_policies.append(p)
    return default_policy, non_default_policies


def reset_acl():
    def post(url, token, parsed, conn):
        conn.request('POST', parsed.path, '', {
            'X-Auth-Token': token,
            'X-Account-Access-Control': '{}'
        })
        return check_response(conn)
    resp = retry(post, use_account=1)
    resp.read()


def requires_acls(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        global skip, cluster_info
        if skip or not cluster_info:
            raise SkipTest('Requires account ACLs')
        # Determine whether this cluster has account ACLs; if not, skip test
        if not cluster_info.get('tempauth', {}).get('account_acls'):
            raise SkipTest('Requires account ACLs')
        if swift_test_auth_version != '1':
            # remove when keystoneauth supports account acls
            raise SkipTest('Requires account ACLs')
        reset_acl()
        try:
            rv = f(*args, **kwargs)
        finally:
            reset_acl()
        return rv
    return wrapper


class FunctionalStoragePolicyCollection(object):

    # policy_specified is set in __init__.py when tests are being set up.
    policy_specified = None

    def __init__(self, policies):
        self._all = policies
        self.default = None
        for p in self:
            if p.get('default', False):
                assert self.default is None, 'Found multiple default ' \
                    'policies %r and %r' % (self.default, p)
                self.default = p

    @classmethod
    def from_info(cls, info=None):
        if not (info or cluster_info):
            get_cluster_info()
        info = info or cluster_info
        try:
            policy_info = info['swift']['policies']
        except KeyError:
            raise AssertionError('Did not find any policy info in %r' % info)
        policies = cls(policy_info)
        assert policies.default, \
            'Did not find default policy in %r' % policy_info
        return policies

    def __len__(self):
        return len(self._all)

    def __iter__(self):
        return iter(self._all)

    def __getitem__(self, index):
        return self._all[index]

    def filter(self, **kwargs):
        return self.__class__([p for p in self if all(
            p.get(k) == v for k, v in kwargs.items())])

    def exclude(self, **kwargs):
        return self.__class__([p for p in self if all(
            p.get(k) != v for k, v in kwargs.items())])

    def select(self):
        # check that a policy was specified and that it is available
        # in the current list (i.e., hasn't been excluded of the current list)
        if self.policy_specified and self.policy_specified in self:
            return self.policy_specified
        else:
            return random.choice(self)


def requires_policies(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if skip:
            raise SkipTest
        try:
            self.policies = FunctionalStoragePolicyCollection.from_info()
        except AssertionError:
            raise SkipTest("Unable to determine available policies")
        if len(self.policies) < 2:
            raise SkipTest("Multiple policies not enabled")
        return f(self, *args, **kwargs)

    return wrapper
