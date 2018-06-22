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

import os

from swift.common.swob import Request, Response


class HealthCheckMiddleware(object):
    """
    Healthcheck middleware used for monitoring.

    If the path is /healthcheck, it will respond 200 with "OK" as the body.

    If the optional config parameter "disable_path" is set, and a file is
    present at that path, it will respond 503 with "DISABLED BY FILE" as the
    body.
    """

    def __init__(self, app, conf):
        self.app = app
        self.disable_path = conf.get('disable_path', '')

    def GET(self, req):
        """Returns a 200 response with "OK" in the body."""
        return Response(request=req, body=b"OK", content_type="text/plain")

    def DISABLED(self, req):
        """Returns a 503 response with "DISABLED BY FILE" in the body."""
        return Response(request=req, status=503, body=b"DISABLED BY FILE",
                        content_type="text/plain")

    def __call__(self, env, start_response):
        req = Request(env)
        if req.path == '/healthcheck':
            handler = self.GET
            if self.disable_path and os.path.exists(self.disable_path):
                handler = self.DISABLED
            return handler(req)(env, start_response)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def healthcheck_filter(app):
        return HealthCheckMiddleware(app, conf)
    return healthcheck_filter
