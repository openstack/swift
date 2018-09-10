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

from collections import defaultdict, namedtuple
from hashlib import md5
from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.request_helpers import is_user_meta, \
    is_object_transient_sysmeta, resolve_etag_is_at_header
from swift.common.swob import HTTPNotImplemented
from swift.common.utils import split_path

from test.unit import FakeLogger, FakeRing


class LeakTrackingIter(object):
    def __init__(self, inner_iter, mark_closed, path):
        if isinstance(inner_iter, bytes):
            inner_iter = (inner_iter, )
        self.inner_iter = inner_iter
        self.mark_closed = mark_closed
        self.path = path

    def __iter__(self):
        for x in self.inner_iter:
            yield x

    def close(self):
        self.mark_closed(self.path)


FakeSwiftCall = namedtuple('FakeSwiftCall', ['method', 'path', 'headers'])


class FakeSwift(object):
    """
    A good-enough fake Swift proxy server to use in testing middleware.
    """
    ALLOWED_METHODS = [
        'PUT', 'POST', 'DELETE', 'GET', 'HEAD', 'OPTIONS', 'REPLICATE']

    def __init__(self):
        self._calls = []
        self._unclosed_req_paths = defaultdict(int)
        self.req_method_paths = []
        self.swift_sources = []
        self.txn_ids = []
        self.uploaded = {}
        # mapping of (method, path) --> (response class, headers, body)
        self._responses = {}
        self.logger = FakeLogger('fake-swift')
        self.account_ring = FakeRing()
        self.container_ring = FakeRing()
        self.get_object_ring = lambda policy_index: FakeRing()

    def _find_response(self, method, path):
        resp = self._responses[(method, path)]
        if isinstance(resp, list):
            try:
                resp = resp.pop(0)
            except IndexError:
                raise IndexError("Didn't find any more %r "
                                 "in allowed responses" % (
                                     (method, path),))
        return resp

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        if method not in self.ALLOWED_METHODS:
            raise HTTPNotImplemented()

        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)
        if env.get('QUERY_STRING'):
            path += '?' + env['QUERY_STRING']

        if 'swift.authorize' in env:
            resp = env['swift.authorize'](swob.Request(env))
            if resp:
                return resp(env, start_response)

        req = swob.Request(env)
        self.swift_sources.append(env.get('swift.source'))
        self.txn_ids.append(env.get('swift.trans_id'))

        try:
            resp_class, raw_headers, body = self._find_response(method, path)
            headers = HeaderKeyDict(raw_headers)
        except KeyError:
            if (env.get('QUERY_STRING')
                    and (method, env['PATH_INFO']) in self._responses):
                resp_class, raw_headers, body = self._find_response(
                    method, env['PATH_INFO'])
                headers = HeaderKeyDict(raw_headers)
            elif method == 'HEAD' and ('GET', path) in self._responses:
                resp_class, raw_headers, body = self._find_response(
                    'GET', path)
                body = None
                headers = HeaderKeyDict(raw_headers)
            elif method == 'GET' and obj and path in self.uploaded:
                resp_class = swob.HTTPOk
                headers, body = self.uploaded[path]
            else:
                raise KeyError("Didn't find %r in allowed responses" % (
                    (method, path),))

        # simulate object PUT
        if method == 'PUT' and obj:
            put_body = b''.join(iter(env['wsgi.input'].read, b''))
            if 'swift.callback.update_footers' in env:
                footers = HeaderKeyDict()
                env['swift.callback.update_footers'](footers)
                req.headers.update(footers)
            etag = md5(put_body).hexdigest()
            headers.setdefault('Etag', etag)
            headers.setdefault('Content-Length', len(put_body))

            # keep it for subsequent GET requests later
            self.uploaded[path] = (dict(req.headers), put_body)
            if "CONTENT_TYPE" in env:
                self.uploaded[path][0]['Content-Type'] = env["CONTENT_TYPE"]

        # simulate object POST
        elif method == 'POST' and obj:
            metadata, data = self.uploaded.get(path, ({}, None))
            # select items to keep from existing...
            new_metadata = dict(
                (k, v) for k, v in metadata.items()
                if (not is_user_meta('object', k) and not
                    is_object_transient_sysmeta(k)))
            # apply from new
            new_metadata.update(
                dict((k, v) for k, v in req.headers.items()
                     if (is_user_meta('object', k) or
                         is_object_transient_sysmeta(k) or
                         k.lower == 'content-type')))
            self.uploaded[path] = new_metadata, data

        # note: tests may assume this copy of req_headers is case insensitive
        # so we deliberately use a HeaderKeyDict
        self._calls.append(
            FakeSwiftCall(method, path, HeaderKeyDict(req.headers)))

        # Apply conditional etag overrides
        conditional_etag = resolve_etag_is_at_header(req, headers)

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
        wsgi_iter = resp(env, start_response)
        self.mark_opened(path)
        return LeakTrackingIter(wsgi_iter, self.mark_closed, path)

    def mark_opened(self, path):
        self._unclosed_req_paths[path] += 1

    def mark_closed(self, path):
        self._unclosed_req_paths[path] -= 1

    @property
    def unclosed_requests(self):
        return {path: count
                for path, count in self._unclosed_req_paths.items()
                if count > 0}

    @property
    def calls(self):
        return [(method, path) for method, path, headers in self._calls]

    @property
    def headers(self):
        return [headers for method, path, headers in self._calls]

    @property
    def calls_with_headers(self):
        return self._calls

    @property
    def call_count(self):
        return len(self._calls)

    def register(self, method, path, response_class, headers, body=b''):
        self._responses[(method, path)] = (response_class, headers, body)

    def register_responses(self, method, path, responses):
        self._responses[(method, path)] = list(responses)


class FakeAppThatExcepts(object):
    MESSAGE = b"We take exception to that!"

    def __init__(self, exception_class=Exception):
        self.exception_class = exception_class

    def __call__(self, env, start_response):
        raise self.exception_class(self.MESSAGE)
