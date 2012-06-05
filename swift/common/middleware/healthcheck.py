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

from webob import Request, Response


class HealthCheckMiddleware(object):
    """
    Healthcheck middleware used for monitoring.

    If the path is /healthcheck, it will respond with "OK" in the body
    """

    def __init__(self, app, *args, **kwargs):
        self.app = app

    def GET(self, req):
        """Returns a 200 response with "OK" in the body."""
        return Response(request=req, body="OK", content_type="text/plain")

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            if req.path == '/healthcheck':
                return self.GET(req)(env, start_response)
        except UnicodeError:
            # definitely, this is not /healthcheck
            pass
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    def healthcheck_filter(app):
        return HealthCheckMiddleware(app)
    return healthcheck_filter
