# Copyright (c) 2010-2014 OpenStack Foundation.
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
"""
The s3api middleware will emulate the S3 REST api on top of swift.

To enable this middleware to your configuration, add the s3api middleware
in front of the auth middleware. See ``proxy-server.conf-sample`` for more
detail and configurable options.

To set up your client, the access key will be the concatenation of the
account and user strings that should look like test:tester, and the
secret access key is the account password.  The host should also point
to the swift storage hostname.

An example client using the python boto library is as follows::

    from boto.s3.connection import S3Connection
    connection = S3Connection(
        aws_access_key_id='test:tester',
        aws_secret_access_key='testing',
        port=8080,
        host='127.0.0.1',
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat())

"""

from paste.deploy import loadwsgi

from swift.common.wsgi import PipelineWrapper, loadcontext

from swift.common.middleware.s3api.exception import NotS3Request, \
    InvalidSubresource
from swift.common.middleware.s3api.request import get_request_class
from swift.common.middleware.s3api.response import ErrorResponse, \
    InternalError, MethodNotAllowed, ResponseBase, S3NotImplemented
from swift.common.middleware.s3api.cfg import CONF
from swift.common.utils import get_logger, register_swift_info
from swift.common.middleware.s3api.acl_handlers import get_acl_handler


class S3ApiMiddleware(object):
    """S3Api: S3 compatibility middleware"""
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        self.logger = get_logger(
            conf, log_route=conf.get('log_name', 's3api'))
        self.slo_enabled = conf['allow_multipart_uploads']
        self.check_pipeline(conf)

    def __call__(self, env, start_response):
        try:
            req_class = get_request_class(env)
            req = req_class(env, self.app, self.slo_enabled)
            resp = self.handle_request(req)
        except NotS3Request:
            resp = self.app
        except InvalidSubresource as e:
            self.logger.debug(e.cause)
        except ErrorResponse as err_resp:
            if isinstance(err_resp, InternalError):
                self.logger.exception(err_resp)
            resp = err_resp
        except Exception as e:
            self.logger.exception(e)
            resp = InternalError(reason=e)

        if isinstance(resp, ResponseBase) and 'swift.trans_id' in env:
            resp.headers['x-amz-id-2'] = env['swift.trans_id']
            resp.headers['x-amz-request-id'] = env['swift.trans_id']

        return resp(env, start_response)

    def handle_request(self, req):
        self.logger.debug('Calling S3Api Middleware')
        self.logger.debug(req.__dict__)
        try:
            controller = req.controller(self.app, self.conf, self.logger)
        except S3NotImplemented:
            # TODO: Probably we should distinct the error to log this warning
            self.logger.warning('multipart: No SLO middleware in pipeline')
            raise

        acl_handler = get_acl_handler(req.controller_name)(req, self.logger)
        req.set_acl_handler(acl_handler)

        if hasattr(controller, req.method):
            handler = getattr(controller, req.method)
            if not getattr(handler, 'publicly_accessible', False):
                raise MethodNotAllowed(req.method,
                                       req.controller.resource_type())
            res = handler(req)
        else:
            raise MethodNotAllowed(req.method,
                                   req.controller.resource_type())

        return res

    def check_pipeline(self, conf):
        """
        Check that proxy-server.conf has an appropriate pipeline for s3api.
        """
        if conf.get('__file__', None) is None:
            return

        ctx = loadcontext(loadwsgi.APP, conf.__file__)
        pipeline = str(PipelineWrapper(ctx)).split(' ')

        # Add compatible with 3rd party middleware.
        self.check_filter_order(pipeline, ['s3api', 'proxy-server'])

        auth_pipeline = pipeline[pipeline.index('s3api') + 1:
                                 pipeline.index('proxy-server')]

        # Check SLO middleware
        if self.slo_enabled and 'slo' not in auth_pipeline:
            self.slo_enabled = False
            self.logger.warning('s3api middleware requires SLO middleware '
                                'to support multi-part upload, please add it '
                                'in pipeline')

        if not conf.auth_pipeline_check:
            self.logger.debug('Skip pipeline auth check.')
            return

        if 'tempauth' in auth_pipeline:
            self.logger.debug('Use tempauth middleware.')
        elif 'keystoneauth' in auth_pipeline:
            self.check_filter_order(
                auth_pipeline,
                ['s3token', 'keystoneauth'])
            self.logger.debug('Use keystone middleware.')
        elif len(auth_pipeline):
            self.logger.debug('Use third party(unknown) auth middleware.')
        else:
            raise ValueError('Invalid pipeline %r: expected auth between '
                             's3api and proxy-server ' % pipeline)

    def check_filter_order(self, pipeline, required_filters):
        """
        Check that required filters are present in order in the pipeline.
        """
        indexes = []
        missing_filters = []
        for required_filter in required_filters:
            try:
                indexes.append(pipeline.index(required_filter))
            except ValueError as e:
                self.logger.debug(e)
                missing_filters.append(required_filter)

        if missing_filters:
            raise ValueError('Invalid pipeline %r: missing filters %r' % (
                pipeline, missing_filters))

        if indexes != sorted(indexes):
            raise ValueError('Invalid pipeline %r: expected filter %s' % (
                pipeline, ' before '.join(required_filters)))


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    CONF.update(global_conf)
    CONF.update(local_conf)

    register_swift_info(
        's3api',
        max_bucket_listing=CONF['max_bucket_listing'],
        max_parts_listing=CONF['max_parts_listing'],
        max_upload_part_num=CONF['max_upload_part_num'],
        max_multi_delete_objects=CONF['max_multi_delete_objects'],
        allow_multipart_uploads=CONF['allow_multipart_uploads'],
    )

    def s3api_filter(app):
        return S3ApiMiddleware(app, CONF)

    return s3api_filter
