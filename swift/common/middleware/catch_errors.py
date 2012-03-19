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

from eventlet import Timeout
from webob import Request
from webob.exc import HTTPServerError
import uuid

from swift.common.utils import get_logger


class CatchErrorMiddleware(object):
    """
    Middleware that provides high-level error handling and ensures that a
    transaction id will be set for every request.
    """

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='catch-errors')

    def __call__(self, env, start_response):
        """
        If used, this should be the first middleware in pipeline.
        """
        trans_id = 'tx' + uuid.uuid4().hex
        env['swift.trans_id'] = trans_id
        self.logger.txn_id = trans_id
        try:

            def my_start_response(status, response_headers, exc_info=None):
                trans_header = ('x-trans-id', trans_id)
                response_headers.append(trans_header)
                return start_response(status, response_headers, exc_info)
            return self.app(env, my_start_response)
        except (Exception, Timeout), err:
            self.logger.exception(_('Error: %s'), err)
            resp = HTTPServerError(request=Request(env),
                                   body='An error occurred',
                                   content_type='text/plain')
            resp.headers['x-trans-id'] = trans_id
            return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def except_filter(app):
        return CatchErrorMiddleware(app, conf)
    return except_filter
