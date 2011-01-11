# Copyright (c) 2010-2011 OpenStack, LLC.
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

from webob import Request
from webob.exc import HTTPServerError

from swift.common.utils import get_logger


class CatchErrorMiddleware(object):
    """
    Middleware that provides high-level error handling.
    """

    def __init__(self, app, conf):
        self.app = app
        # if the application already has a logger we should use that one
        self.logger = getattr(app, 'logger', None)
        if not self.logger:
            # and only call get_logger if we have to
            self.logger = get_logger(conf)

    def __call__(self, env, start_response):
        try:
            return self.app(env, start_response)
        except Exception, err:
            self.logger.exception(_('Error: %s'), err)
            resp = HTTPServerError(request=Request(env),
                                   body='An error occurred',
                                   content_type='text/plain')
            return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def except_filter(app):
        return CatchErrorMiddleware(app, conf)
    return except_filter
