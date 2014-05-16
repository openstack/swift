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

from swift import gettext_ as _

from swift.common.swob import Request, HTTPServerError
from swift.common.utils import get_logger, generate_trans_id
from swift.common.wsgi import WSGIContext


class CatchErrorsContext(WSGIContext):

    def __init__(self, app, logger, trans_id_suffix=''):
        super(CatchErrorsContext, self).__init__(app)
        self.logger = logger
        self.trans_id_suffix = trans_id_suffix

    def handle_request(self, env, start_response):
        trans_id_suffix = self.trans_id_suffix
        trans_id_extra = env.get('HTTP_X_TRANS_ID_EXTRA')
        if trans_id_extra:
            trans_id_suffix += '-' + trans_id_extra[:32]

        trans_id = generate_trans_id(trans_id_suffix)
        env['swift.trans_id'] = trans_id
        self.logger.txn_id = trans_id
        try:
            # catch any errors in the pipeline
            resp = self._app_call(env)
        except:  # noqa
            self.logger.exception(_('Error: An error occurred'))
            resp = HTTPServerError(request=Request(env),
                                   body='An error occurred',
                                   content_type='text/plain')
            resp.headers['X-Trans-Id'] = trans_id
            return resp(env, start_response)

        # make sure the response has the trans_id
        if self._response_headers is None:
            self._response_headers = []
        self._response_headers.append(('X-Trans-Id', trans_id))
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp


class CatchErrorMiddleware(object):
    """
    Middleware that provides high-level error handling and ensures that a
    transaction id will be set for every request.
    """

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='catch-errors')
        self.trans_id_suffix = conf.get('trans_id_suffix', '')

    def __call__(self, env, start_response):
        """
        If used, this should be the first middleware in pipeline.
        """
        context = CatchErrorsContext(self.app,
                                     self.logger,
                                     self.trans_id_suffix)
        return context.handle_request(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def except_filter(app):
        return CatchErrorMiddleware(app, conf)
    return except_filter
