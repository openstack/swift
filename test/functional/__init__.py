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

import httplib
import os
import sys
import pickle
import socket
import locale
import eventlet
import eventlet.debug
import functools
import random
from time import time, sleep
from httplib import HTTPException
from urlparse import urlparse
from nose import SkipTest
from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp

from test import get_config
from test.functional.swift_test_client import Account, Connection, \
    ResponseError
# This has the side effect of mocking out the xattr module so that unit tests
# (and in this case, when in-process functional tests are called for) can run
# on file systems that don't support extended attributes.
from test.unit import debug_logger, FakeMemcache

from swift.common import constraints, utils, ring, storage_policy
from swift.common.wsgi import monkey_patch_mimetools
from swift.common.middleware import catch_errors, gatekeeper, healthcheck, \
    proxy_logging, container_sync, bulk, tempurl, slo, dlo, ratelimit, \
    tempauth, container_quotas, account_quotas
from swift.common.utils import config_true_value
from swift.proxy import server as proxy_server
from swift.account import server as account_server
from swift.container import server as container_server
from swift.obj import server as object_server, mem_server as mem_object_server
import swift.proxy.controllers.obj

httplib._MAXHEADERS = constraints.MAX_HEADER_COUNT

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
swift_test_user = [os.environ.get('SWIFT_TEST_USER'), None, None, '']
swift_test_key = [os.environ.get('SWIFT_TEST_KEY'), None, None, '']
swift_test_tenant = ['', '', '', '']
swift_test_perm = ['', '', '', '']
swift_test_domain = ['', '', '', '']
swift_test_user_id = ['', '', '', '']
swift_test_tenant_id = ['', '', '', '']

skip, skip2, skip3 = False, False, False

orig_collate = ''
insecure = False

orig_hash_path_suff_pref = ('', '')
orig_swift_conf_name = None

in_process = False
_testdir = _test_servers = _test_sockets = _test_coros = None


class FakeMemcacheMiddleware(object):
    """
    Caching middleware that fakes out caching in swift.
    """

    def __init__(self, app, conf):
        self.app = app
        self.memcache = FakeMemcache()

    def __call__(self, env, start_response):
        env['swift.cache'] = self.memcache
        return self.app(env, start_response)


def fake_memcache_filter_factory(conf):
    def filter_app(app):
        return FakeMemcacheMiddleware(app, conf)
    return filter_app


# swift.conf contents for in-process functional test runs
functests_swift_conf = '''
[swift-hash]
swift_hash_path_suffix = inprocfunctests
swift_hash_path_prefix = inprocfunctests

[swift-constraints]
max_file_size = %d
''' % ((8 * 1024 * 1024) + 2)  # 8 MB + 2


def in_process_setup(the_object_server=object_server):
    print >>sys.stderr, 'IN-PROCESS SERVERS IN USE FOR FUNCTIONAL TESTS'
    print >>sys.stderr, 'Using object_server: %s' % the_object_server.__name__

    monkey_patch_mimetools()

    global _testdir
    _testdir = os.path.join(mkdtemp(), 'tmp_functional')
    utils.mkdirs(_testdir)
    rmtree(_testdir)
    utils.mkdirs(os.path.join(_testdir, 'sda1'))
    utils.mkdirs(os.path.join(_testdir, 'sda1', 'tmp'))
    utils.mkdirs(os.path.join(_testdir, 'sdb1'))
    utils.mkdirs(os.path.join(_testdir, 'sdb1', 'tmp'))

    swift_conf = os.path.join(_testdir, "swift.conf")
    with open(swift_conf, "w") as scfp:
        scfp.write(functests_swift_conf)

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
        config.update(constraints.EFFECTIVE_CONSTRAINTS)
    else:
        # In-process swift constraints were not loaded, somethings wrong
        raise SkipTest
    global orig_hash_path_suff_pref
    orig_hash_path_suff_pref = utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX
    utils.validate_hash_conf()

    # We create the proxy server listening socket to get its port number so
    # that we can add it as the "auth_port" value for the functional test
    # clients.
    prolis = eventlet.listen(('localhost', 0))

    # The following set of configuration values is used both for the
    # functional test frame work and for the various proxy, account, container
    # and object servers.
    config.update({
        # Values needed by the various in-process swift servers
        'devices': _testdir,
        'swift_dir': _testdir,
        'mount_check': 'false',
        'client_timeout': 4,
        'allow_account_management': 'true',
        'account_autocreate': 'true',
        'allowed_headers':
        'content-disposition, content-encoding, x-delete-at,'
        ' x-object-manifest, x-static-large-object',
        'allow_versions': 'True',
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
        # For tempauth middleware
        'user_admin_admin': 'admin .admin .reseller_admin',
        'user_test_tester': 'testing .admin',
        'user_test2_tester2': 'testing2 .admin',
        'user_test_tester3': 'testing3'
    })

    acc1lis = eventlet.listen(('localhost', 0))
    acc2lis = eventlet.listen(('localhost', 0))
    con1lis = eventlet.listen(('localhost', 0))
    con2lis = eventlet.listen(('localhost', 0))
    obj1lis = eventlet.listen(('localhost', 0))
    obj2lis = eventlet.listen(('localhost', 0))
    global _test_sockets
    _test_sockets = \
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis)

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
    object_ring_path = os.path.join(_testdir, 'object.ring.gz')
    with closing(GzipFile(object_ring_path, 'wb')) as f:
        pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                    [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
                      'port': obj1lis.getsockname()[1]},
                     {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                      'port': obj2lis.getsockname()[1]}], 30),
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

    prosrv = proxy_server.Application(config, logger=debug_logger('proxy'))
    acc1srv = account_server.AccountController(
        config, logger=debug_logger('acct1'))
    acc2srv = account_server.AccountController(
        config, logger=debug_logger('acct2'))
    con1srv = container_server.ContainerController(
        config, logger=debug_logger('cont1'))
    con2srv = container_server.ContainerController(
        config, logger=debug_logger('cont2'))
    obj1srv = the_object_server.ObjectController(
        config, logger=debug_logger('obj1'))
    obj2srv = the_object_server.ObjectController(
        config, logger=debug_logger('obj2'))
    global _test_servers
    _test_servers = \
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv, obj2srv)

    pipeline = [
        catch_errors.filter_factory,
        gatekeeper.filter_factory,
        healthcheck.filter_factory,
        proxy_logging.filter_factory,
        fake_memcache_filter_factory,
        container_sync.filter_factory,
        bulk.filter_factory,
        tempurl.filter_factory,
        slo.filter_factory,
        dlo.filter_factory,
        ratelimit.filter_factory,
        tempauth.filter_factory,
        container_quotas.filter_factory,
        account_quotas.filter_factory,
        proxy_logging.filter_factory,
    ]
    app = prosrv
    import mock
    for filter_factory in reversed(pipeline):
        app_filter = filter_factory(config)
        with mock.patch('swift.common.utils') as mock_utils:
            mock_utils.get_logger.return_value = None
            app = app_filter(app)
        app.logger = prosrv.logger

    nl = utils.NullLogger()
    prospa = eventlet.spawn(eventlet.wsgi.server, prolis, app, nl)
    acc1spa = eventlet.spawn(eventlet.wsgi.server, acc1lis, acc1srv, nl)
    acc2spa = eventlet.spawn(eventlet.wsgi.server, acc2lis, acc2srv, nl)
    con1spa = eventlet.spawn(eventlet.wsgi.server, con1lis, con1srv, nl)
    con2spa = eventlet.spawn(eventlet.wsgi.server, con2lis, con2srv, nl)
    obj1spa = eventlet.spawn(eventlet.wsgi.server, obj1lis, obj1srv, nl)
    obj2spa = eventlet.spawn(eventlet.wsgi.server, obj2lis, obj2srv, nl)
    global _test_coros
    _test_coros = \
        (prospa, acc1spa, acc2spa, con1spa, con2spa, obj1spa, obj2spa)

    # Create accounts "test" and "test2"
    def create_account(act):
        ts = utils.normalize_timestamp(time())
        partition, nodes = prosrv.account_ring.get_nodes(act)
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
            print >>sys.stderr, "** Swift Cluster not exposing /info **"

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
            print >>sys.stderr, "Invalid constraint value: %s = %s" % (
                k, test_constraints[k])
    eff_constraints.update(test_constraints)

    # Just make it look like these constraints were loaded from a /info call,
    # even if the /info call failed, or when they are overridden by values
    # from the swift-constraints section of test.conf
    cluster_info['swift'] = eff_constraints


def setup_package():
    in_process_env = os.environ.get('SWIFT_TEST_IN_PROCESS')
    if in_process_env is not None:
        use_in_process = utils.config_true_value(in_process_env)
    else:
        use_in_process = None

    global in_process

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
            if config:
                in_process = False
            else:
                in_process = True
        else:
            # Explicitly set to False, do not attempt to use in-process
            # functional tests, be sure we attempt to read from local
            # test.conf file.
            in_process = False
            config.update(get_config('func_test'))

    if in_process:
        in_mem_obj_env = os.environ.get('SWIFT_TEST_IN_MEMORY_OBJ')
        in_mem_obj = utils.config_true_value(in_mem_obj_env)
        in_process_setup(the_object_server=(
            mem_object_server if in_mem_obj else object_server))

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

            for _ in range(4):
                swift_test_perm[_] = swift_test_tenant[_] + ':' \
                    + swift_test_user[_]

    global skip
    skip = not all([swift_test_auth, swift_test_user[0], swift_test_key[0]])
    if skip:
        print >>sys.stderr, 'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG'

    global skip2
    skip2 = not all([not skip, swift_test_user[1], swift_test_key[1]])
    if not skip and skip2:
        print >>sys.stderr, \
            'SKIPPING SECOND ACCOUNT FUNCTIONAL TESTS' \
            ' DUE TO NO CONFIG FOR THEM'

    global skip3
    skip3 = not all([not skip, swift_test_user[2], swift_test_key[2]])
    if not skip and skip3:
        print >>sys.stderr, \
            'SKIPPING THIRD ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'

    global skip_if_not_v3
    skip_if_not_v3 = (swift_test_auth_version != '3'
                      or not all([not skip,
                                  swift_test_user[3],
                                  swift_test_key[3]]))
    if not skip and skip_if_not_v3:
        print >>sys.stderr, \
            'SKIPPING FUNCTIONAL TESTS SPECIFIC TO AUTH VERSION 3'

    get_cluster_info()


def teardown_package():
    global orig_collate
    locale.setlocale(locale.LC_COLLATE, orig_collate)

    # clean up containers and objects left behind after running tests
    conn = Connection(config)
    conn.authenticate()
    account = Account(conn, config.get('account', config['username']))
    account.delete_containers()

    global in_process
    if in_process:
        try:
            for server in _test_coros:
                server.kill()
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


class AuthError(Exception):
    pass


class InternalServerError(Exception):
    pass


url = [None, None, None, None]
token = [None, None, None, None]
parsed = [None, None, None, None]
conn = [None, None, None, None]


def connection(url):
    if has_insecure:
        return http_connection(url, insecure=insecure)
    return http_connection(url)


def retry(func, *args, **kwargs):
    """
    You can use the kwargs to override:
      'retries' (default: 5)
      'use_account' (default: 1) - which user's token to pass
      'url_account' (default: matches 'use_account') - which user's storage URL
      'resource' (default: url[url_account] - URL to connect to; retry()
          will interpolate the variable :storage_url: if present
    """
    global url, token, parsed, conn
    retries = kwargs.get('retries', 5)
    attempts, backoff = 0, 1

    # use account #1 by default; turn user's 1-indexed account into 0-indexed
    use_account = kwargs.pop('use_account', 1) - 1

    # access our own account by default
    url_account = kwargs.pop('url_account', use_account + 1) - 1
    os_options = {'user_domain_name': swift_test_domain[use_account],
                  'project_domain_name': swift_test_domain[use_account]}
    while attempts <= retries:
        attempts += 1
        try:
            if not url[use_account] or not token[use_account]:
                url[use_account], token[use_account] = \
                    get_auth(swift_test_auth, swift_test_user[use_account],
                             swift_test_key[use_account],
                             snet=False,
                             tenant_name=swift_test_tenant[use_account],
                             auth_version=swift_test_auth_version,
                             os_options=os_options)
                parsed[use_account] = conn[use_account] = None
            if not parsed[use_account] or not conn[use_account]:
                parsed[use_account], conn[use_account] = \
                    connection(url[use_account])

            # default resource is the account url[url_account]
            resource = kwargs.pop('resource', '%(storage_url)s')
            template_vars = {'storage_url': url[url_account]}
            parsed_result = urlparse(resource % template_vars)
            return func(url[url_account], token[use_account],
                        parsed_result, conn[url_account],
                        *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed[use_account] = conn[use_account] = None
        except AuthError:
            url[use_account] = token[use_account] = None
            continue
        except InternalServerError:
            pass
        if attempts <= retries:
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
            raise SkipTest
        # Determine whether this cluster has account ACLs; if not, skip test
        if not cluster_info.get('tempauth', {}).get('account_acls'):
            raise SkipTest
        if 'keystoneauth' in cluster_info:
            # remove when keystoneauth supports account acls
            raise SkipTest
        reset_acl()
        try:
            rv = f(*args, **kwargs)
        finally:
            reset_acl()
        return rv
    return wrapper


class FunctionalStoragePolicyCollection(object):

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
