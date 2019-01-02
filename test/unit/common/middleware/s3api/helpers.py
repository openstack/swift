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

from copy import deepcopy
from hashlib import md5
from swift.common import swob
from swift.common.utils import split_path
from swift.common.request_helpers import is_sys_meta


class FakeSwift(object):
    """
    A good-enough fake Swift proxy server to use in testing middleware.
    """

    def __init__(self, s3_acl=False):
        self._calls = []
        self.req_method_paths = []
        self.swift_sources = []
        self.uploaded = {}
        # mapping of (method, path) --> (response class, headers, body)
        self._responses = {}
        self.s3_acl = s3_acl
        self.remote_user = 'authorized'

    def _fake_auth_middleware(self, env):
        if 'swift.authorize_override' in env:
            return

        if 's3api.auth_details' not in env:
            return

        tenant_user = env['s3api.auth_details']['access_key']
        tenant, user = tenant_user.rsplit(':', 1)

        path = env['PATH_INFO']
        env['PATH_INFO'] = path.replace(tenant_user, 'AUTH_' + tenant)

        if self.remote_user:
            env['REMOTE_USER'] = self.remote_user

        if env['REQUEST_METHOD'] == 'TEST':

            def authorize_cb(req):
                # Assume swift owner, if not yet set
                req.environ.setdefault('swift_owner', True)
                # But then default to blocking authz, to ensure we've replaced
                # the default auth system
                return swob.HTTPForbidden(request=req)

            env['swift.authorize'] = authorize_cb
        else:
            env['swift.authorize'] = lambda req: None

    def __call__(self, env, start_response):
        if self.s3_acl:
            self._fake_auth_middleware(env)

        req = swob.Request(env)
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)
        if env.get('QUERY_STRING'):
            path += '?' + env['QUERY_STRING']

        if 'swift.authorize' in env:
            resp = env['swift.authorize'](req)
            if resp:
                return resp(env, start_response)

        headers = req.headers
        self._calls.append((method, path, headers))
        self.swift_sources.append(env.get('swift.source'))

        try:
            resp_class, raw_headers, body = self._responses[(method, path)]
            headers = swob.HeaderKeyDict(raw_headers)
        except KeyError:
            # FIXME: suppress print state error for python3 compatibility.
            # pylint: disable-msg=E1601
            if (env.get('QUERY_STRING')
                    and (method, env['PATH_INFO']) in self._responses):
                resp_class, raw_headers, body = self._responses[
                    (method, env['PATH_INFO'])]
                headers = swob.HeaderKeyDict(raw_headers)
            elif method == 'HEAD' and ('GET', path) in self._responses:
                resp_class, raw_headers, _ = self._responses[('GET', path)]
                body = None
                headers = swob.HeaderKeyDict(raw_headers)
            elif method == 'GET' and obj and path in self.uploaded:
                resp_class = swob.HTTPOk
                headers, body = self.uploaded[path]
            else:
                print("Didn't find %r in allowed responses" %
                      ((method, path),))
                raise

        # simulate object PUT
        if method == 'PUT' and obj:
            input = env['wsgi.input'].read()
            etag = md5(input).hexdigest()
            headers.setdefault('Etag', etag)
            headers.setdefault('Content-Length', len(input))

            # keep it for subsequent GET requests later
            self.uploaded[path] = (deepcopy(headers), input)
            if "CONTENT_TYPE" in env:
                self.uploaded[path][0]['Content-Type'] = env["CONTENT_TYPE"]

        # range requests ought to work, but copies are special
        support_range_and_conditional = not (
            method == 'PUT' and
            'X-Copy-From' in req.headers and
            'Range' in req.headers)
        if isinstance(body, list):
            app_iter = body
            body = None
        else:
            app_iter = None
        resp = resp_class(
            req=req, headers=headers, body=body, app_iter=app_iter,
            conditional_response=support_range_and_conditional)
        return resp(env, start_response)

    @property
    def calls(self):
        return [(method, path) for method, path, headers in self._calls]

    @property
    def calls_with_headers(self):
        return self._calls

    @property
    def call_count(self):
        return len(self._calls)

    def register(self, method, path, response_class, headers, body):
        # assuming the path format like /v1/account/container/object
        resource_map = ['account', 'container', 'object']
        index = len(list(filter(None, split_path(path, 0, 4, True)[1:]))) - 1
        resource = resource_map[index]
        if (method, path) in self._responses:
            old_headers = self._responses[(method, path)][1]
            headers = headers.copy()
            for key, value in old_headers.items():
                if is_sys_meta(resource, key) and key not in headers:
                    # keep old sysmeta for s3acl
                    headers.update({key: value})

        self._responses[(method, path)] = (response_class, headers, body)

    def register_unconditionally(self, method, path, response_class, headers,
                                 body):
        # register() keeps old sysmeta around, but
        # register_unconditionally() keeps nothing.
        self._responses[(method, path)] = (response_class, headers, body)

    def clear_calls(self):
        del self._calls[:]


class UnreadableInput(object):
    # Some clients will send neither a Content-Length nor a Transfer-Encoding
    # header, which will cause (some versions of?) eventlet to bomb out on
    # reads. This class helps us simulate that behavior.
    def __init__(self, test_case):
        self.calls = 0
        self.test_case = test_case

    def read(self, *a, **kw):
        self.calls += 1
        # Calling wsgi.input.read with neither a Content-Length nor
        # a Transfer-Encoding header will raise TypeError (See
        # https://bugs.launchpad.net/swift3/+bug/1593870 in detail)
        # This unreadable class emulates the behavior
        raise TypeError

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.test_case.assertEqual(0, self.calls)
