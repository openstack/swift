# Copyright (c) 2013 OpenStack Foundation
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
# This stuff can't live in test/unit/__init__.py due to its swob dependency.
from collections import defaultdict
from urllib import parse
from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.request_helpers import is_user_meta, \
    is_object_transient_sysmeta, resolve_etag_is_at_header, \
    resolve_ignore_range_header
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import split_path, md5

from test.debug_logger import debug_logger
from test.unit import FakeRing


class LeakTrackingIter(object):
    def __init__(self, inner_iter, mark_closed, mark_read, key):
        if isinstance(inner_iter, bytes):
            inner_iter = (inner_iter, )
        self.inner_iter = iter(inner_iter)
        self.mark_closed = mark_closed
        self.mark_read = mark_read
        self.key = key

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.inner_iter)
        except StopIteration:
            self.mark_read(self.key)
            raise

    def close(self):
        self.mark_closed(self.key)


class FakeSwiftCall(object):
    """
    Encapsulate properties of a request captured by FakeSwift.
    """
    DUMMY_VALUE = object()

    def __init__(self, req):
        self.req = req
        self.method = req.method
        path = req.environ['PATH_INFO']
        if req.environ.get('QUERY_STRING'):
            path += '?' + req.environ['QUERY_STRING']
        self.path = normalize_path(path)
        self.headers = HeaderKeyDict(req.headers)
        # footers is populated if/when it is passed by FakeSwift to any
        # update_footers callback that has been set in the request environment.
        self.footers = HeaderKeyDict()
        self._env = self._partial_copy(req.environ)
        # leave FakeSwift to read body and set this attribute after the call
        # has been captured
        self.body = None

    def _partial_copy(self, value):
        # Ideally we want a snapshot of the environ, with deep copies of
        # mutable values. However, some things (e.g. EncInputWrapper) won't
        # deepcopy. To avoid a confusing mixed of copied and original
        # values, replace un-copied values with DUMMY_VALUE.
        if value is None:
            return value
        elif isinstance(value, (bool, int, float, str, bytes)):
            return value
        elif isinstance(value, (list, tuple, set)):
            return value.__class__([self._partial_copy(v) for v in value])
        elif isinstance(value, dict):
            return dict([(k, self._partial_copy(v))
                         for k, v in value.items()])
        else:
            return self.DUMMY_VALUE

    @property
    def env(self):
        """
        Returns a partial deepcopy of the request environ as it was received by
        ``FakeSwift``.

        It may not be possible to deepcopy everything in a request environ, so
        only values of type ``bool``, ``int``, ``float``, ``str``, ``bytes``,
        ``list``, ``tuple``, ``set``, or ``dict`` are copied. Other values are
        replaced with a ``FakeSwiftCall.DUMMY_VALUE`` sentinel so that tests
        can still check for that key being in ``env``, but not mistakenly
        assume that the value is a copy.

        Tests that need to make assertions about values not copied into
        ``FakeSwiftCall.env`` can access the original request environ via
        ``FakeSwiftCall.req.environ``.

        Writing tests that assert the equality of ``env`` is discouraged (e.g.
        ``self.assertEqual(expected, call.env)``) because those assertions will
        break when new default keys are added to the request environ. Tests
        should instead make assertions about individual items in ``env`` (e.g.
        ``self.assertEqual(expected, call.env['swift.source')``).
        """
        return self._env


def normalize_query_string(qs):
    if qs.startswith('?'):
        qs = qs[1:]
    if not qs:
        return ''
    else:
        # N.B. sort params so app.call asserts can hard code qs
        return '?%s' % parse.urlencode(sorted(parse.parse_qsl(qs)))


def normalize_path(path):
    parsed = parse.urlparse(path)
    return parsed.path + normalize_query_string(parsed.query)


class FakeSwift(object):
    """
    A good-enough fake Swift proxy server to use in testing middleware.

    Responses for expected requests should be registered using the ``register``
    method. Registered requests are keyed by their method and path *including
    query string*.

    Received requests are matched to registered requests with the same method
    as follows, in order of preference:

      * A received request matches a registered request if the received
        request's path, including query string, is the same as the registered
        request's path, including query string.
      * A received request matches a registered request if the received
        request's path, excluding query string, is the same as the registered
        request's path, including query string.

    A received ``HEAD`` request will be matched to a registered ``GET``,
    according to the same path preferences, if a match cannot be made to a
    registered ``HEAD`` request.

    A ``PUT`` request that matches a registered ``PUT`` request will create an
    entry in the ``uploaded`` object cache that is keyed by the received
    request's path, excluding query string. A subsequent ``GET`` or ``HEAD``
    request that does not match a registered request will match an ``uploaded``
    object based on the ``GET`` or ``HEAD`` request's path, excluding query
    string.

    A ``POST`` request whose path, excluding query string, matches an object in
    the ``uploaded`` cache will modify the metadata of the object in the
    ``uploaded`` cache. However, the ``POST`` request must first match a
    registered ``POST`` request.

    Examples:

      * received ``GET /v1/a/c/o`` will match registered ``GET /v1/a/c/o``
      * received ``GET /v1/a/c/o?x=y`` will match registered ``GET /v1/a/c/o``
      * received ``HEAD /v1/a/c/o?x=y`` will match registered ``GET /v1/a/c/o``
      * received ``GET /v1/a/c/o`` will NOT match registered
        ``GET /v1/a/c/o?x=y``
      * received ``PUT /v1/a/c/o?x=y``, if it matches a registered ``PUT``,
        will create uploaded ``/v1/a/c/o``
      * received ``POST /v1/a/c/o?x=y``, if it matches a registered ``POST``,
        will update uploaded ``/v1/a/c/o``
    """
    ALLOWED_METHODS = [
        'PUT', 'POST', 'DELETE', 'GET', 'HEAD', 'OPTIONS', 'REPLICATE',
        'SSYNC', 'UPDATE']
    container_existence_skip_cache = 0.0
    account_existence_skip_cache = 0.0

    def __init__(self, capture_unexpected_calls=True):
        self.capture_unexpected_calls = capture_unexpected_calls
        self._calls = []
        self._unclosed_req_keys = defaultdict(int)
        self._unread_req_paths = defaultdict(int)
        self.uploaded = {}
        # mapping of (method, path) --> (response class, headers, body)
        self._responses = {}
        self._sticky_headers = {}
        self.logger = debug_logger('fake-swift')
        self.account_ring = FakeRing()
        self.container_ring = FakeRing()
        self.get_object_ring = lambda policy_index: FakeRing()
        self.auto_create_account_prefix = '.'
        self.backend_user_agent = "fake_swift"
        self._pipeline_final_app = self
        # Object Servers learned to resolve_ignore_range_header in Jan-2020,
        # and although we still maintain some middleware tests that assert
        # proper behavior across rolling upgrades, having a FakeSwift not act
        # like modern swift is now opt-in.
        self.can_ignore_range = True

    def _find_response(self, method, path):
        path = normalize_path(path)
        resps = self._responses[(method, path)]
        if len(resps) == 1:
            # we'll return the last registered response forever
            return resps[0]
        else:
            return resps.pop(0)

    def _select_response(self, env):
        # in some cases we can borrow different registered response
        # ... the order is brittle and significant
        method = env['REQUEST_METHOD']
        path = self._parse_path(env)[0]
        preferences = [(method, path)]
        if env.get('QUERY_STRING'):
            # we can always reuse response w/o query string
            preferences.append((method, env['PATH_INFO']))
        if method == 'HEAD':
            # any path suitable for GET always works for HEAD
            # N.B. list(preferences) to avoid iter+modify/sigkill
            preferences.extend(('GET', p) for _, p in list(preferences))
        for m, p in preferences:
            try:
                resp_class, headers, body = self._find_response(m, p)
            except KeyError:
                pass
            else:
                break
        else:
            # special case for re-reading an uploaded file
            # ... uploaded is only objects and always raw path
            if method in ('GET', 'HEAD') and env['PATH_INFO'] in self.uploaded:
                resp_class = swob.HTTPOk
                headers, body = self.uploaded[env['PATH_INFO']]
            else:
                raise KeyError("Didn't find %r in allowed responses" % (
                    (method, path),))

        if method == 'HEAD':
            # HEAD resp never has body
            body = None

        try:
            is_success = resp_class().is_success
        except Exception:
            # test_reconciler passes in an exploding response
            is_success = False
        if is_success and method in ('GET', 'HEAD'):
            # update sticky resp headers with headers from registered resp
            sticky_headers = self._sticky_headers.get(env['PATH_INFO'], {})
            resp_headers = HeaderKeyDict(sticky_headers)
            resp_headers.update(headers)
        else:
            # error responses don't get sticky resp headers
            resp_headers = HeaderKeyDict(headers)

        return resp_class, resp_headers, body

    def _get_policy_index(self, acc, cont):
        path = '/v1/%s/%s' % (acc, cont)
        env = {'PATH_INFO': path,
               'REQUEST_METHOD': 'HEAD'}
        try:
            resp_class, headers, _ = self._select_response(env)
            policy_index = headers.get('X-Backend-Storage-Policy-Index')
        except KeyError:
            policy_index = None
        if policy_index is None:
            policy_index = str(int(POLICIES.default))
        return policy_index

    def _parse_path(self, env):
        path = env['PATH_INFO']

        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)
        if env.get('QUERY_STRING'):
            path += '?' + env['QUERY_STRING']
        path = normalize_path(path)
        return path, acc, cont, obj

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        if method not in self.ALLOWED_METHODS:
            return HTTPMethodNotAllowed()(env, start_response)

        path, acc, cont, obj = self._parse_path(env)

        if 'swift.authorize' in env:
            resp = env['swift.authorize'](swob.Request(env))
            if resp:
                return resp(env, start_response)

        req = swob.Request(env)

        # Capture the request before reading the body, in case the iter raises
        # an exception.
        call = FakeSwiftCall(req)
        try:
            resp_class, headers, body = self._select_response(env)
        except KeyError:
            if self.capture_unexpected_calls:
                self._calls.append(call)
            raise
        else:
            self._calls.append(call)

        if (cont and not obj and method == 'UPDATE') or (
                obj and method == 'PUT'):
            call.body = b''.join(iter(env['wsgi.input'].read, b''))

        # simulate object PUT
        if method == 'PUT' and obj:
            if 'swift.callback.update_footers' in env:
                footers = HeaderKeyDict()
                env['swift.callback.update_footers'](footers)
                call.footers.update(footers)
            etag = md5(call.body, usedforsecurity=False).hexdigest()
            headers.setdefault('Etag', etag)
            headers.setdefault('Content-Length', len(call.body))

            # keep it for subsequent GET requests later
            metadata = dict(call.headers, **call.footers)
            if "CONTENT_TYPE" in env:
                metadata['Content-Type'] = env["CONTENT_TYPE"]
            self.uploaded[env['PATH_INFO']] = (metadata, call.body)

        # simulate object POST
        elif method == 'POST' and obj:
            metadata, data = self.uploaded.get(env['PATH_INFO'], ({}, None))
            # select items to keep from existing...
            new_metadata = dict(
                (k, v) for k, v in metadata.items()
                if (not is_user_meta('object', k) and not
                    is_object_transient_sysmeta(k)))
            # apply from new
            new_metadata.update(
                dict((k, v)
                     for k, v in dict(call.headers, **call.footers).items()
                     if (is_user_meta('object', k) or
                         is_object_transient_sysmeta(k) or
                         k.lower == 'content-type')))
            self.uploaded[env['PATH_INFO']] = new_metadata, data

        # Some middlewares (e.g. proxy_logging) inspect the request headers
        # after it has been handled, so simulate some request headers updates
        # that the real proxy makes. Do this *after* the request has been
        # captured in the state it was received.
        if obj:
            req.headers.setdefault('X-Backend-Storage-Policy-Index',
                                   self._get_policy_index(acc, cont))

        # Apply conditional etag overrides
        conditional_etag = resolve_etag_is_at_header(req, headers)

        if self.can_ignore_range:
            # avoid popping range from original environ
            req = swob.Request(dict(req.environ))
            resolve_ignore_range_header(req, headers)

        # range requests ought to work, hence conditional_response=True
        if isinstance(body, list):
            resp = resp_class(
                req=req, headers=headers, app_iter=body,
                conditional_response=req.method in ('GET', 'HEAD'),
                conditional_etag=conditional_etag)
        else:
            resp = resp_class(
                req=req, headers=headers, body=body,
                conditional_response=req.method in ('GET', 'HEAD'),
                conditional_etag=conditional_etag)
        wsgi_iter = resp(req.environ, start_response)
        self.mark_opened((method, path))
        return LeakTrackingIter(wsgi_iter, self.mark_closed,
                                self.mark_read, (method, path))

    def clear_calls(self):
        del self._calls[:]

    def mark_opened(self, key):
        self._unclosed_req_keys[key] += 1
        self._unread_req_paths[key] += 1

    def mark_closed(self, key):
        self._unclosed_req_keys[key] -= 1

    def mark_read(self, key):
        self._unread_req_paths[key] -= 1

    @property
    def unclosed_requests(self):
        return {key: count
                for key, count in self._unclosed_req_keys.items()
                if count > 0}

    @property
    def unread_requests(self):
        return {path: count
                for path, count in self._unread_req_paths.items()
                if count > 0}

    @property
    def call_list(self):
        """
        Returns a list of instances of ``FakeSwiftCall``.
        """
        return self._calls

    @property
    def calls(self):
        """
        Returns a list of 2-tuples (<method>, <path>) for each item in
        ``call_list``.
        """
        return [(call.method, call.path) for call in self._calls]

    @property
    def headers(self):
        """
        Returns a list of headers for each item in ``call_list``.
        """
        return [call.headers for call in self._calls]

    @property
    def calls_with_headers(self):
        """
        Returns a list of 3-tuples (<method>, <path>, <headers>) for each item
        in ``call_list``.
        """
        return [(call.method, call.path, call.headers)
                for call in self._calls]

    @property
    def call_count(self):
        return len(self.call_list)

    @property
    def txn_ids(self):
        return [call.env.get('swift.trans_id') for call in self.call_list]

    @property
    def swift_sources(self):
        return [call.env.get('swift.source') for call in self.call_list]

    def update_sticky_response_headers(self, path, headers):
        """
        Tests setUp can use this to ensure any successful GET/HEAD response for
        a given path will include these headers.
        """
        sticky_headers = self._sticky_headers.setdefault(path, {})
        sticky_headers.update(headers)

    def register(self, method, path, response_class, headers, body=b''):
        path = normalize_path(path)
        self._responses[(method, path)] = [(response_class, headers, body)]

    def register_next_response(self, method, path,
                               response_class, headers, body=b''):
        resp_key = (method, normalize_path(path))
        next_resp = (response_class, headers, body)
        self._responses.setdefault(resp_key, []).append(next_resp)


class FakeAppThatExcepts(object):
    MESSAGE = b"We take exception to that!"

    def __init__(self, exception_class=Exception):
        self.exception_class = exception_class

    def __call__(self, env, start_response):
        raise self.exception_class(self.MESSAGE)
