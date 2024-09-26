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

from swift.common.swob import Request, HTTPServerError
from swift.common.utils import get_logger, generate_trans_id, close_if_possible
from swift.common.wsgi import WSGIContext


class BadResponseLength(Exception):
    pass


def enforce_byte_count(inner_iter, nbytes):
    """
    Enforces that inner_iter yields exactly <nbytes> bytes before
    exhaustion.

    If inner_iter fails to do so, BadResponseLength is raised.

    :param inner_iter: iterable of bytestrings
    :param nbytes: number of bytes expected
    """
    try:
        bytes_left = nbytes
        for chunk in inner_iter:
            if bytes_left >= len(chunk):
                yield chunk
                bytes_left -= len(chunk)
            else:
                yield chunk[:bytes_left]
                raise BadResponseLength(
                    "Too many bytes; truncating after %d bytes "
                    "with at least %d surplus bytes remaining" % (
                        nbytes, len(chunk) - bytes_left))

        if bytes_left:
            raise BadResponseLength('Expected another %d bytes' % (
                bytes_left,))
    finally:
        close_if_possible(inner_iter)


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
            self.logger.exception('Error: An error occurred')
            resp = HTTPServerError(request=Request(env),
                                   body=b'An error occurred',
                                   content_type='text/plain')
            resp.headers['X-Trans-Id'] = trans_id
            resp.headers['X-Openstack-Request-Id'] = trans_id
            return resp(env, start_response)

        # If the app specified a Content-Length, enforce that it sends that
        # many bytes.
        #
        # If an app gives too few bytes, then the client will wait for the
        # remainder before sending another HTTP request on the same socket;
        # since no more bytes are coming, this will result in either an
        # infinite wait or a timeout. In this case, we want to raise an
        # exception to signal to the WSGI server that it should close the
        # TCP connection.
        #
        # If an app gives too many bytes, then we can deadlock with the
        # client; if the client reads its N bytes and then sends a large-ish
        # request (enough to fill TCP buffers), it'll block until we read
        # some of the request. However, we won't read the request since
        # we'll be trying to shove the rest of our oversized response out
        # the socket. In that case, we truncate the response body at N bytes
        # and raise an exception to stop any more bytes from being
        # generated and also to kill the TCP connection.
        if env['REQUEST_METHOD'] == 'HEAD':
            resp = enforce_byte_count(resp, 0)

        elif self._response_headers:
            content_lengths = [val for header, val in self._response_headers
                               if header.lower() == "content-length"]
            if len(content_lengths) == 1:
                try:
                    content_length = int(content_lengths[0])
                except ValueError:
                    pass
                else:
                    resp = enforce_byte_count(resp, content_length)

        # make sure the response has the trans_id
        if self._response_headers is None:
            self._response_headers = []
        self._response_headers.append(('X-Trans-Id', trans_id))
        self._response_headers.append(('X-Openstack-Request-Id', trans_id))
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
