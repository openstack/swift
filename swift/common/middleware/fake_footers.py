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
"""
This middleware is not intended to ever be merged on master branch.

This middleware mimics behaviour that we expect to eventually integrate into
the proxy app/object controller once feature/ec has merged to master.
Specifically, we need to be able to add trailing headers (aka footers) to PUT
requests, after the request body has been read by a downstream app, and have
these footers sent on to the object server.

As a workaround, while we wait for feature/ec to merge to master,this
middleware provides a callback mechanism for other (upstream) middlewares to
provide footers. FakeFooters will store these footers in memcache (since we
have no way currently to forward them on to the object server) using a key
based on the request path.

Then, when handling any response, FakeFooters will look in memcache for any
footers stored under the current request path, and append those footers to the
response headers.

Middleware wishing to send footers to FakeFooters should add a reference to a
callback function in the request environ under key 'swift.update.footers'.
Fake Footers will call this function with a single argument - a dict - after it
completes reading the request body. The middleware callback should add any
footers to this dict before returning.

To use FakeFooters you will need to add it to the proxy pipeline in
proxy-server.conf and also add a filter section::

  pipeline = catch_errors proxy-logging cache  tempauth fake_footers \
      proxy-logging proxy-server

  [filter:fake_footers]
  use = egg:swift#fake_footers

An example of a middleware taking advantage of FakeFooters is given below::

    from swift.common.utils import get_logger
    from swift.common.wsgi import WSGIContext


    FAKE_KEY = 'X-Object-Meta-FakeFooter'


    class TestFakeFootersContext(WSGIContext):
        def __init__(self, app, logger):
            super(TestFakeFootersContext, self).__init__(app)
            self.logger = logger

        def footers_callback(self, footers):
            footers.update(
                {FAKE_KEY: self.env.get('swift.trans_id', 'bogus_tran_id')})

        def handle_request(self, env, start_response):
            self.env = env
            env['swift.update.footers'] = self.footers_callback
            resp = self._app_call(env)
            for item in self._response_headers:
                if item[0].lower() == FAKE_KEY.lower():
                    self.logger.info('TestFakeFooters resp found %s=%s' % item)
                    break
            else:
                self.logger.info('TestFakeFooters resp MISSING test footer')
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return resp


    class TestFakeFootersMiddleware(object):
        def __init__(self, app, conf):
            self.app = app
            self.logger = get_logger(conf, log_route='fake-footers')

        def __call__(self, env, start_response):
            context = TestFakeFootersContext(self.app, self.logger)
            return context.handle_request(env, start_response)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)

        def except_filter(app):
            return TestFakeFootersMiddleware(app, conf)
        return except_filter


"""

from swift.common.utils import get_logger, cache_from_env
from swift.common.wsgi import WSGIContext


MEMCACHE_TIMEOUT = 3600


class FakeFootersContext(WSGIContext):

    def __init__(self, app, logger):
        super(FakeFootersContext, self).__init__(app)
        self.logger = logger

    def retrieve_footers(self, env):
        key = 'swift.footer.%s' % env['PATH_INFO']
        footers = self.memcache_client.get(key) or {}
        self.logger.info('retrieve_footers %s %s' % (key, footers))
        return footers

    class ReaderWrapper(object):
        def __init__(self, env, callback, context):
            self.env = env
            self.footer_callback = callback
            self.original_read = env['wsgi.input'].read
            self.context = context

        def store_footers(self, footers):
            key = 'swift.footer.%s' % self.env['PATH_INFO']
            self.context.memcache_client.set(key, footers, MEMCACHE_TIMEOUT)
            self.context.logger.info('store_footers %s %s' % (key, footers))

        def read(self, size):
            chunk = self.original_read(size)
            if chunk == '':
                # end of stream, call back for footers
                footers = {}
                self.footer_callback(footers)
                self.store_footers(footers)
            return chunk

    def handle_request(self, env, start_response):
        self.memcache_client = cache_from_env(env)
        if not self.memcache_client:
            raise Exception('Memcache required for FakeFooters')
        callback = env.get('swift.update.footers')
        if callback:
            env['wsgi.input'] = self.ReaderWrapper(env, callback, self)

        resp = self._app_call(env)
        footers = self.retrieve_footers(env)
        for item in footers.items():
            self._response_headers.append(item)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp


class FakeFootersMiddleware(object):
    """
    Middleware that fakes footer handling by storing footer values in memcache.
    """

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='fake-footers')

    def __call__(self, env, start_response):
        context = FakeFootersContext(self.app,
                                     self.logger)
        return context.handle_request(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def except_filter(app):
        return FakeFootersMiddleware(app, conf)
    return except_filter
