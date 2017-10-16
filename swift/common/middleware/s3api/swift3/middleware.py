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
The swift3 middleware will emulate the S3 REST api on top of swift.

The following operations are currently supported:

    * GET Service
    * DELETE Bucket
    * GET Bucket (List Objects)
    * PUT Bucket
    * DELETE Object
    * Delete Multiple Objects
    * GET Object
    * HEAD Object
    * PUT Object
    * PUT Object (Copy)

To add this middleware to your configuration, add the swift3 middleware
in front of the auth middleware, and before any other middleware that
look at swift requests (like rate limiting).

To set up your client, the access key will be the concatenation of the
account and user strings that should look like test:tester, and the
secret access key is the account password.  The host should also point
to the swift storage hostname.  It also will have to use the old style
calling format, and not the hostname based container format.

An example client using the python boto library might look like the
following for an SAIO setup::

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

from swift3 import __version__ as swift3_version
from swift3.exception import NotS3Request
from swift3.request import get_request_class
from swift3.response import ErrorResponse, InternalError, MethodNotAllowed, \
    ResponseBase
from swift3.cfg import CONF
from swift3.utils import LOGGER
from swift.common.utils import get_logger, register_swift_info


class Swift3Middleware(object):
    """Swift3 S3 compatibility middleware"""
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.slo_enabled = conf['allow_multipart_uploads']
        self.check_pipeline(conf)

    def __call__(self, env, start_response):
        try:
            req_class = get_request_class(env)
            req = req_class(env, self.app, self.slo_enabled)
            resp = self.handle_request(req)
        except NotS3Request:
            resp = self.app
        except ErrorResponse as err_resp:
            if isinstance(err_resp, InternalError):
                LOGGER.exception(err_resp)
            resp = err_resp
        except Exception as e:
            LOGGER.exception(e)
            resp = InternalError(reason=e)

        if isinstance(resp, ResponseBase) and 'swift.trans_id' in env:
            resp.headers['x-amz-id-2'] = env['swift.trans_id']
            resp.headers['x-amz-request-id'] = env['swift.trans_id']

        return resp(env, start_response)

    def handle_request(self, req):
        LOGGER.debug('Calling Swift3 Middleware')
        LOGGER.debug(req.__dict__)

        controller = req.controller(self.app)
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
        Check that proxy-server.conf has an appropriate pipeline for swift3.
        """
        if conf.get('__file__', None) is None:
            return

        ctx = loadcontext(loadwsgi.APP, conf.__file__)
        pipeline = str(PipelineWrapper(ctx)).split(' ')

        # Add compatible with 3rd party middleware.
        check_filter_order(pipeline, ['swift3', 'proxy-server'])

        auth_pipeline = pipeline[pipeline.index('swift3') + 1:
                                 pipeline.index('proxy-server')]

        # Check SLO middleware
        if self.slo_enabled and 'slo' not in auth_pipeline:
            self.slo_enabled = False
            LOGGER.warning('swift3 middleware requires SLO middleware '
                           'to support multi-part upload, please add it '
                           'in pipeline')

        if not conf.auth_pipeline_check:
            LOGGER.debug('Skip pipeline auth check.')
            return

        if 'tempauth' in auth_pipeline:
            LOGGER.debug('Use tempauth middleware.')
        elif 'keystoneauth' in auth_pipeline:
            check_filter_order(auth_pipeline,
                               ['s3token',
                                'keystoneauth'])
            LOGGER.debug('Use keystone middleware.')
        elif len(auth_pipeline):
            LOGGER.debug('Use third party(unknown) auth middleware.')
        else:
            raise ValueError('Invalid pipeline %r: expected auth between '
                             'swift3 and proxy-server ' % pipeline)


def check_filter_order(pipeline, required_filters):
    """
    Check that required filters are present in order in the pipeline.
    """
    indexes = []
    missing_filters = []
    for filter in required_filters:
        try:
            indexes.append(pipeline.index(filter))
        except ValueError as e:
            LOGGER.debug(e)
            missing_filters.append(filter)

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

    # Reassign config to logger
    global LOGGER
    LOGGER = get_logger(CONF, log_route=CONF.get('log_name', 'swift3'))

    register_swift_info(
        'swift3',
        max_bucket_listing=CONF['max_bucket_listing'],
        max_parts_listing=CONF['max_parts_listing'],
        max_upload_part_num=CONF['max_upload_part_num'],
        max_multi_delete_objects=CONF['max_multi_delete_objects'],
        allow_multipart_uploads=CONF['allow_multipart_uploads'],
        version=swift3_version,
    )

    def swift3_filter(app):
        return Swift3Middleware(app, CONF)

    return swift3_filter
