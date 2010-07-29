# Copyright (c) 2010 OpenStack, LLC.
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

from webob import Response


class HealthCheckController(object):
    """Basic controller used for monitoring."""

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def GET(self, req):
        return Response(request=req, body="OK", content_type="text/plain")

healthcheck = HealthCheckController.GET
