# Copyright (c) 2010-2017 OpenStack Foundation
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

import re
from swift.common.wsgi import WSGIContext


def app_property(name):
    return property(lambda self: getattr(self.app, name))


class RewriteContext(WSGIContext):
    base_re = None

    def __init__(self, app, requested, rewritten):
        super(RewriteContext, self).__init__(app)
        self.requested = requested
        self.rewritten_re = re.compile(self.base_re % re.escape(rewritten))

    def handle_request(self, env, start_response):
        resp_iter = self._app_call(env)
        for i, (header, value) in enumerate(self._response_headers):
            if header.lower() in ('location', 'content-location'):
                self._response_headers[i] = (header, self.rewritten_re.sub(
                    r'\1%s\2' % self.requested, value))
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp_iter
