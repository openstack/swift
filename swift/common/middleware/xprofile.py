# Copyright (c) 2010-2012 OpenStack, LLC.
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
Profiling middleware for Swift Servers.

.. note::
    This middleware is intended for development and testing environments only,
    not production. No authentication is expected or required for the web UI,
    and profiling may incur noticeable performance penalties.

The current implementation is based on eventlet aware profiler.(For the
future, more profilers could be added in to collect more data for analysis.)
Profiling all incoming requests and accumulating cpu timing statistics
information for performance tuning and optimization. An mini web UI is also
provided for profiling data analysis. It can be accessed from the URL as
below.

Index page for browse profile data::

    http://SERVER_IP:PORT/__profile__

List all profiles to return profile ids in json format::

    http://SERVER_IP:PORT/__profile__/
    http://SERVER_IP:PORT/__profile__/all

Retrieve specific profile data in different formats::

    http://SERVER_IP:PORT/__profile__/PROFILE_ID?format=[default|json|csv|ods]
    http://SERVER_IP:PORT/__profile__/current?format=[default|json|csv|ods]
    http://SERVER_IP:PORT/__profile__/all?format=[default|json|csv|ods]

Retrieve metrics from specific function in json format::

    http://SERVER_IP:PORT/__profile__/PROFILE_ID/NFL?format=json
    http://SERVER_IP:PORT/__profile__/current/NFL?format=json
    http://SERVER_IP:PORT/__profile__/all/NFL?format=json

    NFL is defined by concatenation of file name, function name and the first
    line number.
    e.g.::
        account.py:50(GETorHEAD)
    or with full path:
        opt/stack/swift/swift/proxy/controllers/account.py:50(GETorHEAD)

    A list of URL examples:

    http://localhost:8080/__profile__    (proxy server)
    http://localhost:6200/__profile__/all    (object server)
    http://localhost:6201/__profile__/current    (container server)
    http://localhost:6202/__profile__/12345?format=json    (account server)

The profiling middleware can be configured in paste file for WSGI servers such
as proxy, account, container and object servers. Please refer to the sample
configuration files in etc directory.

The profiling data is provided with four formats such as binary(by default),
json, csv and odf spreadsheet which requires installing odfpy library::

    sudo pip install odfpy

There's also a simple visualization capability which is enabled by using
matplotlib toolkit. it is also required to be installed if you want to use
it to visualize statistic data::

    sudo apt-get install python-matplotlib
"""

import os
import sys
import time

from eventlet import greenthread, GreenPool, patcher
import eventlet.green.profile as eprofile
import urllib

from swift.common.utils import get_logger, config_true_value
from swift.common.swob import Request
from swift.common.middleware.x_profile.exceptions import MethodNotAllowed
from swift.common.middleware.x_profile.exceptions import NotFoundException
from swift.common.middleware.x_profile.exceptions import ProfileException
from swift.common.middleware.x_profile.html_viewer import HTMLViewer
from swift.common.middleware.x_profile.profile_model import ProfileLog


DEFAULT_PROFILE_PREFIX = '/tmp/log/swift/profile/default.profile'  # nosec B108

# unwind the iterator; it may call start_response, do lots of work, etc
PROFILE_EXEC_EAGER = """
app_iter = self.app(environ, start_response)
app_iter_ = list(app_iter)
if hasattr(app_iter, 'close'):
    app_iter.close()
"""

# don't unwind the iterator (don't consume resources)
PROFILE_EXEC_LAZY = """
app_iter_ = self.app(environ, start_response)
"""

thread = patcher.original('_thread')  # non-monkeypatched module needed


# This monkey patch code fix the problem of eventlet profile tool
# which can not accumulate profiling results across multiple calls
# of runcalls and runctx.
def new_setup(self):
    self._has_setup = True
    self.cur = None
    self.timings = {}
    self.current_tasklet = greenthread.getcurrent()
    self.thread_id = thread.get_ident()
    self.simulate_call("profiler")


def new_runctx(self, cmd, globals, locals):
    if not getattr(self, '_has_setup', False):
        self._setup()
    try:
        return self.base.runctx(self, cmd, globals, locals)
    finally:
        self.TallyTimings()


def new_runcall(self, func, *args, **kw):
    if not getattr(self, '_has_setup', False):
        self._setup()
    try:
        return self.base.runcall(self, func, *args, **kw)
    finally:
        self.TallyTimings()


class ProfileMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='profile')
        self.log_filename_prefix = conf.get('log_filename_prefix',
                                            DEFAULT_PROFILE_PREFIX)
        dirname = os.path.dirname(self.log_filename_prefix)
        # Notes: this effort may fail due to permission denied.
        # it is better to be created and authorized to current
        # user in advance.
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.dump_interval = float(conf.get('dump_interval', 5.0))
        self.dump_timestamp = config_true_value(conf.get(
            'dump_timestamp', 'no'))
        self.flush_at_shutdown = config_true_value(conf.get(
            'flush_at_shutdown', 'no'))
        self.path = conf.get('path', '__profile__').replace('/', '')
        self.unwind = config_true_value(conf.get('unwind', 'no'))
        self.profile_module = conf.get('profile_module',
                                       'eventlet.green.profile')
        self.profiler = get_profiler(self.profile_module)
        self.profile_log = ProfileLog(self.log_filename_prefix,
                                      self.dump_timestamp)
        self.viewer = HTMLViewer(self.path, self.profile_module,
                                 self.profile_log)
        self.dump_pool = GreenPool(1000)
        self.last_dump_at = None

    def __del__(self):
        if self.flush_at_shutdown:
            self.profile_log.clear(str(os.getpid()))

    def _combine_body_qs(self, request):
        wsgi_input = request.environ['wsgi.input']
        query_dict = request.params
        qs_in_body = wsgi_input.read().decode('utf-8')
        query_dict.update(urllib.parse.parse_qs(qs_in_body,
                                                keep_blank_values=True,
                                                strict_parsing=False))
        return query_dict

    def dump_checkpoint(self):
        current_time = time.time()
        if self.last_dump_at is None or self.last_dump_at +\
                self.dump_interval < current_time:
            self.dump_pool.spawn_n(self.profile_log.dump_profile,
                                   self.profiler, os.getpid())
            self.last_dump_at = current_time

    def __call__(self, environ, start_response):
        request = Request(environ)
        path_entry = request.path_info.split('/')
        # hijack favicon request sent by browser so that it doesn't
        # invoke profiling hook and contaminate the data.
        if path_entry[1] == 'favicon.ico':
            start_response('200 OK', [])
            return ''
        elif path_entry[1] == self.path:
            try:
                self.dump_checkpoint()
                query_dict = self._combine_body_qs(request)
                content, headers = self.viewer.render(request.url,
                                                      request.method,
                                                      path_entry,
                                                      query_dict,
                                                      self.renew_profile)
                start_response('200 OK', headers)
                if isinstance(content, str):
                    content = content.encode('utf-8')
                return [content]
            except MethodNotAllowed as mx:
                start_response('405 Method Not Allowed', [])
                return '%s' % mx
            except NotFoundException as nx:
                start_response('404 Not Found', [])
                return '%s' % nx
            except ProfileException as pf:
                start_response('500 Internal Server Error', [])
                return '%s' % pf
            except Exception as ex:
                start_response('500 Internal Server Error', [])
                return 'Error on render profiling results: %s' % ex
        else:
            _locals = locals()
            code = self.unwind and PROFILE_EXEC_EAGER or\
                PROFILE_EXEC_LAZY
            self.profiler.runctx(code, globals(), _locals)
            app_iter = _locals['app_iter_']
            self.dump_checkpoint()
            return app_iter

    def renew_profile(self):
        self.profiler = get_profiler(self.profile_module)


def get_profiler(profile_module):
    if profile_module == 'eventlet.green.profile':
        eprofile.Profile._setup = new_setup
        eprofile.Profile.runctx = new_runctx
        eprofile.Profile.runcall = new_runcall
    # hacked method to import profile module supported in python 2.6
    __import__(profile_module)
    return sys.modules[profile_module].Profile()


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def profile_filter(app):
        return ProfileMiddleware(app, conf)

    return profile_filter
