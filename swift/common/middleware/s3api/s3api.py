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

To set up your client, ensure you are using the tempauth or keystone auth
system for swift project.
When your swift on a SAIO environment, make sure you have setting the tempauth
middleware configuration in ``proxy-server.conf``, and the access key will be
the concatenation of the account and user strings that should look like
test:tester, and the secret access key is the account password. The host should
also point to the swift storage hostname.

The tempauth option example:

.. code-block:: ini

   [filter:tempauth]
   use = egg:swift#tempauth
   user_admin_admin = admin .admin .reseller_admin
   user_test_tester = testing

An example client using tempauth with the python boto library is as follows:

.. code-block:: python

    from boto.s3.connection import S3Connection
    connection = S3Connection(
        aws_access_key_id='test:tester',
        aws_secret_access_key='testing',
        port=8080,
        host='127.0.0.1',
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat())

And if you using keystone auth, you need the ec2 credentials, which can
be downloaded from the API Endpoints tab of the dashboard or by openstack
ec2 command.

Here is showing to create an EC2 credential:

.. code-block:: console

  # openstack ec2 credentials create
  +------------+---------------------------------------------------+
  | Field      | Value                                             |
  +------------+---------------------------------------------------+
  | access     | c2e30f2cd5204b69a39b3f1130ca8f61                  |
  | links      | {u'self': u'http://controller:5000/v3/......'}    |
  | project_id | 407731a6c2d0425c86d1e7f12a900488                  |
  | secret     | baab242d192a4cd6b68696863e07ed59                  |
  | trust_id   | None                                              |
  | user_id    | 00f0ee06afe74f81b410f3fe03d34fbc                  |
  +------------+---------------------------------------------------+

An example client using keystone auth with the python boto library will be:

.. code-block:: python

    from boto.s3.connection import S3Connection
    connection = S3Connection(
        aws_access_key_id='c2e30f2cd5204b69a39b3f1130ca8f61',
        aws_secret_access_key='baab242d192a4cd6b68696863e07ed59',
        port=8080,
        host='127.0.0.1',
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat())

----------
Deployment
----------

Proxy-Server Setting
^^^^^^^^^^^^^^^^^^^^

Set s3api before your auth in your pipeline in ``proxy-server.conf`` file.
To enable all compatibility currently supported, you should make sure that
bulk, slo, and your auth middleware are also included in your proxy
pipeline setting.

Using tempauth, the minimum example config is:

.. code-block:: ini

    [pipeline:main]
    pipeline = proxy-logging cache s3api tempauth bulk slo proxy-logging \
proxy-server

When using keystone, the config will be:

.. code-block:: ini

    [pipeline:main]
    pipeline = proxy-logging cache authtoken s3api s3token keystoneauth bulk \
slo proxy-logging proxy-server

Finally, add the s3api middleware section:

.. code-block:: ini

   [filter:s3api]
   use = egg:swift#s3api

.. note::
    ``keystonemiddleware.authtoken`` can be located before/after s3api but
    we recommend to put it before s3api because when authtoken is after s3api,
    both authtoken and s3token will issue the acceptable token to keystone
    (i.e. authenticate twice). And in the ``keystonemiddleware.authtoken``
    middleware , you should set ``delay_auth_decision`` option to ``True``.

-----------
Constraints
-----------
Currently, the s3api is being ported from https://github.com/openstack/swift3
so any existing issues in swift3 are still remaining. Please make sure
descriptions in the example ``proxy-server.conf`` and what happens with the
config, before enabling the options.

-------------
Supported API
-------------
The compatibility will continue to be improved upstream, you can keep and
eye on compatibility via a check tool build by SwiftStack. See
https://github.com/swiftstack/s3compat in detail.

"""

import json
from paste.deploy import loadwsgi
from urllib.parse import parse_qs

from swift.common.constraints import valid_api_version
from swift.common.middleware.listing_formats import \
    MAX_CONTAINER_LISTING_CONTENT_LENGTH
from swift.common.request_helpers import append_log_info
from swift.common.wsgi import PipelineWrapper, loadcontext, WSGIContext

from swift.common.middleware import app_property
from swift.common.middleware.s3api.exception import NotS3Request, \
    InvalidSubresource
from swift.common.middleware.s3api.s3request import get_request_class
from swift.common.middleware.s3api.s3response import ErrorResponse, \
    InternalError, MethodNotAllowed, S3ResponseBase, S3NotImplemented
from swift.common.utils import get_logger, config_true_value, \
    config_positive_int_value, split_path, closing_if_possible, \
    list_from_csv, parse_header, checksum
from swift.common.middleware.s3api.utils import Config
from swift.common.middleware.s3api.acl_handlers import get_acl_handler
from swift.common.registry import register_swift_info, \
    register_sensitive_header, register_sensitive_param


class ListingEtagMiddleware(object):
    def __init__(self, app):
        self.app = app

    # Pass these along so get_container_info will have the configured
    # odds to skip cache
    _pipeline_final_app = app_property('_pipeline_final_app')
    _pipeline_request_logging_app = app_property(
        '_pipeline_request_logging_app')

    def __call__(self, env, start_response):
        # a lot of this is cribbed from listing_formats / swob.Request
        if env['REQUEST_METHOD'] != 'GET':
            # Nothing to translate
            return self.app(env, start_response)

        try:
            v, a, c = split_path(env.get('SCRIPT_NAME', '') +
                                 env['PATH_INFO'], 3, 3)
            if not valid_api_version(v):
                raise ValueError
        except ValueError:
            is_container_req = False
        else:
            is_container_req = True
        if not is_container_req:
            # pass through
            return self.app(env, start_response)

        ctx = WSGIContext(self.app)
        resp_iter = ctx._app_call(env)

        content_type = content_length = cl_index = None
        for index, (header, value) in enumerate(ctx._response_headers):
            header = header.lower()
            if header == 'content-type':
                content_type = value.split(';', 1)[0].strip()
                if content_length:
                    break
            elif header == 'content-length':
                cl_index = index
                try:
                    content_length = int(value)
                except ValueError:
                    pass  # ignore -- we'll bail later
                if content_type:
                    break

        if content_type != 'application/json' or content_length is None or \
                content_length > MAX_CONTAINER_LISTING_CONTENT_LENGTH:
            start_response(ctx._response_status, ctx._response_headers,
                           ctx._response_exc_info)
            return resp_iter

        # We've done our sanity checks, slurp the response into memory
        with closing_if_possible(resp_iter):
            body = b''.join(resp_iter)

        try:
            listing = json.loads(body)
            for item in listing:
                if 'subdir' in item:
                    continue
                value, params = parse_header(item['hash'])
                if 's3_etag' in params:
                    item['s3_etag'] = '"%s"' % params.pop('s3_etag')
                    item['hash'] = value + ''.join(
                        '; %s=%s' % kv for kv in params.items())
        except (TypeError, KeyError, ValueError):
            # If anything goes wrong above, drop back to original response
            start_response(ctx._response_status, ctx._response_headers,
                           ctx._response_exc_info)
            return [body]

        body = json.dumps(listing).encode('ascii')
        ctx._response_headers[cl_index] = (
            ctx._response_headers[cl_index][0],
            str(len(body)),
        )
        start_response(ctx._response_status, ctx._response_headers,
                       ctx._response_exc_info)
        return [body]


class S3ApiMiddleware(object):
    """S3Api: S3 compatibility middleware"""
    def __init__(self, app, wsgi_conf, *args, **kwargs):
        self.app = app
        self.conf = Config()

        # Set default values if they are not configured
        self.conf.allow_no_owner = config_true_value(
            wsgi_conf.get('allow_no_owner', False))
        self.conf.location = wsgi_conf.get('location', 'us-east-1')
        self.conf.dns_compliant_bucket_names = config_true_value(
            wsgi_conf.get('dns_compliant_bucket_names', True))
        self.conf.max_bucket_listing = config_positive_int_value(
            wsgi_conf.get('max_bucket_listing', 1000))
        self.conf.max_parts_listing = config_positive_int_value(
            wsgi_conf.get('max_parts_listing', 1000))
        self.conf.max_multi_delete_objects = config_positive_int_value(
            wsgi_conf.get('max_multi_delete_objects', 1000))
        self.conf.multi_delete_concurrency = config_positive_int_value(
            wsgi_conf.get('multi_delete_concurrency', 2))
        self.conf.s3_acl = config_true_value(
            wsgi_conf.get('s3_acl', False))
        self.conf.storage_domains = list_from_csv(
            wsgi_conf.get('storage_domain', ''))
        self.conf.auth_pipeline_check = config_true_value(
            wsgi_conf.get('auth_pipeline_check', True))
        self.conf.max_upload_part_num = config_positive_int_value(
            wsgi_conf.get('max_upload_part_num', 1000))
        self.conf.check_bucket_owner = config_true_value(
            wsgi_conf.get('check_bucket_owner', False))
        self.conf.force_swift_request_proxy_log = config_true_value(
            wsgi_conf.get('force_swift_request_proxy_log', False))
        self.conf.allow_multipart_uploads = config_true_value(
            wsgi_conf.get('allow_multipart_uploads', True))
        self.conf.min_segment_size = config_positive_int_value(
            wsgi_conf.get('min_segment_size', 5242880))
        self.conf.allowable_clock_skew = config_positive_int_value(
            wsgi_conf.get('allowable_clock_skew', 15 * 60))
        self.conf.cors_preflight_allow_origin = list_from_csv(wsgi_conf.get(
            'cors_preflight_allow_origin', ''))
        if '*' in self.conf.cors_preflight_allow_origin and \
                len(self.conf.cors_preflight_allow_origin) > 1:
            raise ValueError('if cors_preflight_allow_origin should include '
                             'all domains, * must be the only entry')
        self.conf.ratelimit_as_client_error = config_true_value(
            wsgi_conf.get('ratelimit_as_client_error', False))

        self.logger = get_logger(
            wsgi_conf, log_route='s3api', statsd_tail_prefix='s3api')
        self.check_pipeline(wsgi_conf)
        checksum.log_selected_implementation(self.logger)

    def is_s3_cors_preflight(self, env):
        if env['REQUEST_METHOD'] != 'OPTIONS' or not env.get('HTTP_ORIGIN'):
            # Not a CORS preflight
            return False
        acrh = env.get('HTTP_ACCESS_CONTROL_REQUEST_HEADERS', '').lower()
        if 'authorization' in acrh and \
                not env['PATH_INFO'].startswith(('/v1/', '/v1.0/')):
            return True
        q = parse_qs(env.get('QUERY_STRING', ''))
        if 'AWSAccessKeyId' in q or 'X-Amz-Credential' in q:
            return True
        # Not S3, apparently
        return False

    def __call__(self, env, start_response):
        origin = env.get('HTTP_ORIGIN')
        if self.conf.cors_preflight_allow_origin and \
                self.is_s3_cors_preflight(env):
            # I guess it's likely going to be an S3 request? *shrug*
            if self.conf.cors_preflight_allow_origin != ['*'] and \
                    origin not in self.conf.cors_preflight_allow_origin:
                start_response('401 Unauthorized', [
                    ('Allow', 'GET, HEAD, PUT, POST, DELETE, OPTIONS'),
                ])
                return [b'']

            headers = [
                ('Allow', 'GET, HEAD, PUT, POST, DELETE, OPTIONS'),
                ('Access-Control-Allow-Origin', origin),
                ('Access-Control-Allow-Methods',
                 'GET, HEAD, PUT, POST, DELETE, OPTIONS'),
                ('Vary', 'Origin, Access-Control-Request-Headers'),
            ]
            acrh = set(list_from_csv(
                env.get('HTTP_ACCESS_CONTROL_REQUEST_HEADERS', '').lower()))
            if acrh:
                headers.append((
                    'Access-Control-Allow-Headers',
                    ', '.join(acrh)))

            start_response('200 OK', headers)
            return [b'']

        try:
            req_class = get_request_class(env, self.conf.s3_acl)
            req = req_class(env, self.app, self.conf)
            resp = self.handle_request(req)
        except NotS3Request:
            resp = self.app
        except InvalidSubresource as e:
            self.logger.debug(e.cause)
        except ErrorResponse as err_resp:
            self.logger.increment(err_resp.metric_name)
            append_log_info(env, 's3:err:%s' % err_resp.summary)
            if isinstance(err_resp, InternalError):
                self.logger.exception(err_resp)
            resp = err_resp
        except Exception as e:
            self.logger.exception(e)
            resp = InternalError(reason=str(e))

        if isinstance(resp, S3ResponseBase) and 'swift.trans_id' in env:
            resp.headers['x-amz-id-2'] = env['swift.trans_id']
            resp.headers['x-amz-request-id'] = env['swift.trans_id']

        if 's3api.backend_path' in env and 'swift.backend_path' not in env:
            env['swift.backend_path'] = env['s3api.backend_path']

        return resp(env, start_response)

    def handle_request(self, req):
        self.logger.debug('Calling S3Api Middleware')
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

        if req.policy_index is not None:
            res.headers.setdefault('X-Backend-Storage-Policy-Index',
                                   req.policy_index)
        return res

    def check_pipeline(self, wsgi_conf):
        """
        Check that proxy-server.conf has an appropriate pipeline for s3api.
        """
        if wsgi_conf.get('__file__', None) is None:
            return

        ctx = loadcontext(loadwsgi.APP, wsgi_conf['__file__'])
        pipeline = str(PipelineWrapper(ctx)).split(' ')

        # Add compatible with 3rd party middleware.
        self.check_filter_order(pipeline, ['s3api', 'proxy-server'])

        auth_pipeline = pipeline[pipeline.index('s3api') + 1:
                                 pipeline.index('proxy-server')]

        # Check SLO middleware
        if self.conf.allow_multipart_uploads and 'slo' not in auth_pipeline:
            self.conf.allow_multipart_uploads = False
            self.logger.warning('s3api middleware requires SLO middleware '
                                'to support multi-part upload, please add it '
                                'in pipeline')

        if not self.conf.auth_pipeline_check:
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
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info(
        's3api',
        # TODO: make default values as variables
        max_bucket_listing=int(conf.get('max_bucket_listing', 1000)),
        max_parts_listing=int(conf.get('max_parts_listing', 1000)),
        max_upload_part_num=int(conf.get('max_upload_part_num', 1000)),
        max_multi_delete_objects=int(
            conf.get('max_multi_delete_objects', 1000)),
        allow_multipart_uploads=config_true_value(
            conf.get('allow_multipart_uploads', True)),
        min_segment_size=int(conf.get('min_segment_size', 5242880)),
        s3_acl=config_true_value(conf.get('s3_acl', False)),
    )

    register_sensitive_header('authorization')
    register_sensitive_param('Signature')
    register_sensitive_param('X-Amz-Signature')

    def s3api_filter(app):
        return S3ApiMiddleware(ListingEtagMiddleware(app), conf)

    return s3api_filter
