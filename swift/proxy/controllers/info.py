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

import json
from time import time

from swift.common.utils import public, streq_const_time
from swift.common.digest import get_hmac
from swift.common.registry import get_swift_info
from swift.proxy.controllers.base import Controller, delay_denial
from swift.common.swob import HTTPOk, HTTPForbidden, HTTPUnauthorized


class InfoController(Controller):
    """WSGI controller for info requests"""
    server_type = 'Info'

    def __init__(self, app, version, expose_info, disallowed_sections,
                 admin_key):
        super(InfoController, self).__init__(app)
        self.expose_info = expose_info
        self.disallowed_sections = disallowed_sections
        self.admin_key = admin_key
        self.allowed_hmac_methods = {
            'HEAD': ['HEAD', 'GET'],
            'GET': ['GET']}

    @public
    @delay_denial
    def GET(self, req):
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def HEAD(self, req):
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def OPTIONS(self, req):
        return HTTPOk(request=req, headers={'Allow': 'HEAD, GET, OPTIONS'})

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        """
        Handles requests to /info
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        if not self.expose_info:
            return HTTPForbidden(request=req)

        admin_request = False
        sig = req.params.get('swiftinfo_sig', '')
        expires = req.params.get('swiftinfo_expires', '')

        if sig != '' or expires != '':
            admin_request = True
            if not self.admin_key:
                return HTTPForbidden(request=req)
            try:
                expires = int(expires)
            except ValueError:
                return HTTPUnauthorized(request=req)
            if expires < time():
                return HTTPUnauthorized(request=req)

            valid_sigs = []
            for method in self.allowed_hmac_methods[req.method]:
                valid_sigs.append(get_hmac(method,
                                           '/info',
                                           expires,
                                           self.admin_key))

            # While it's true that any() will short-circuit, this doesn't
            # affect the timing-attack resistance since the only way this will
            # short-circuit is when a valid signature is passed in.
            is_valid_hmac = any(streq_const_time(valid_sig, sig)
                                for valid_sig in valid_sigs)
            if not is_valid_hmac:
                return HTTPUnauthorized(request=req)

        headers = {}
        if 'Origin' in req.headers:
            headers['Access-Control-Allow-Origin'] = req.headers['Origin']
            headers['Access-Control-Expose-Headers'] = ', '.join(
                ['x-trans-id'])

        info = json.dumps(get_swift_info(
            admin=admin_request, disallowed_sections=self.disallowed_sections))

        return HTTPOk(request=req,
                      headers=headers,
                      body=info.encode('ascii'),
                      content_type='application/json; charset=UTF-8')
