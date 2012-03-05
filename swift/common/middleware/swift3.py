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

"""
The swift3 middleware will emulate the S3 REST api on top of swift.

The following opperations are currently supported:

    * GET Service
    * DELETE Bucket
    * GET Bucket (List Objects)
    * PUT Bucket
    * DELETE Object
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

    connection = boto.s3.Connection(
        aws_access_key_id='test:tester',
        aws_secret_access_key='testing',
        port=8080,
        host='127.0.0.1',
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat())
"""

from urllib import unquote, quote
import rfc822
import hmac
import base64
import errno
from xml.sax.saxutils import escape as xml_escape
import urlparse

from webob import Request, Response
from webob.exc import HTTPNotFound
from simplejson import loads

from swift.common.utils import split_path


MAX_BUCKET_LISTING = 1000


def get_err_response(code):
    """
    Given an HTTP response code, create a properly formatted xml error response

    :param code: error code
    :returns: webob.response object
    """
    error_table = {
        'AccessDenied':
            (403, 'Access denied'),
        'BucketAlreadyExists':
            (409, 'The requested bucket name is not available'),
        'BucketNotEmpty':
            (409, 'The bucket you tried to delete is not empty'),
        'InvalidArgument':
            (400, 'Invalid Argument'),
        'InvalidBucketName':
            (400, 'The specified bucket is not valid'),
        'InvalidURI':
            (400, 'Could not parse the specified URI'),
        'NoSuchBucket':
            (404, 'The specified bucket does not exist'),
        'SignatureDoesNotMatch':
            (403, 'The calculated request signature does not match '\
            'your provided one'),
        'NoSuchKey':
            (404, 'The resource you requested does not exist')}

    resp = Response(content_type='text/xml')
    resp.status = error_table[code][0]
    resp.body = error_table[code][1]
    resp.body = '<?xml version="1.0" encoding="UTF-8"?>\r\n<Error>\r\n  ' \
                '<Code>%s</Code>\r\n  <Message>%s</Message>\r\n</Error>\r\n' \
                 % (code, error_table[code][1])
    return resp


def get_acl(account_name):
    body = ('<AccessControlPolicy>'
            '<Owner>'
            '<ID>%s</ID>'
            '</Owner>'
            '<AccessControlList>'
            '<Grant>'
            '<Grantee xmlns:xsi="http://www.w3.org/2001/'\
            'XMLSchema-instance" xsi:type="CanonicalUser">'
            '<ID>%s</ID>'
            '</Grantee>'
            '<Permission>FULL_CONTROL</Permission>'
            '</Grant>'
            '</AccessControlList>'
            '</AccessControlPolicy>' %
            (account_name, account_name))
    return Response(body=body, content_type="text/plain")


def canonical_string(req):
    """
    Canonicalize a request to a token that can be signed.
    """
    amz_headers = {}

    buf = "%s\n%s\n%s\n" % (req.method, req.headers.get('Content-MD5', ''),
            req.headers.get('Content-Type') or '')

    for amz_header in sorted((key.lower() for key in req.headers
                              if key.lower().startswith('x-amz-'))):
        amz_headers[amz_header] = req.headers[amz_header]

    if 'x-amz-date' in amz_headers:
        buf += "\n"
    elif 'Date' in req.headers:
        buf += "%s\n" % req.headers['Date']

    for k in sorted(key.lower() for key in amz_headers):
        buf += "%s:%s\n" % (k, amz_headers[k])

    path = req.path_qs
    if '?' in path:
        path, args = path.split('?', 1)
        for key in urlparse.parse_qs(args, keep_blank_values=True):
            if key in ('acl', 'logging', 'torrent', 'location',
                       'requestPayment'):
                return "%s%s?%s" % (buf, path, key)
    return buf + path


class Controller(object):
    def __init__(self, app):
        self.app = app
        self.response_args = []

    def do_start_response(self, *args):
        self.response_args.extend(args)


class ServiceController(Controller):
    """
    Handles account level requests.
    """
    def __init__(self, env, app, account_name, token, **kwargs):
        Controller.__init__(self, app)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s' % account_name

    def GET(self, env, start_response):
        """
        Handle GET Service request
        """
        env['QUERY_STRING'] = 'format=json'
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 200:
            if status == 401:
                return get_err_response('AccessDenied')
            else:
                return get_err_response('InvalidURI')

        containers = loads(''.join(list(body_iter)))
        # we don't keep the creation time of a backet (s3cmd doesn't
        # work without that) so we use something bogus.
        body = '<?xml version="1.0" encoding="UTF-8"?>' \
            '<ListAllMyBucketsResult ' \
              'xmlns="http://doc.s3.amazonaws.com/2006-03-01">' \
            '<Buckets>%s</Buckets>' \
            '</ListAllMyBucketsResult>' \
            % ("".join(['<Bucket><Name>%s</Name><CreationDate>' \
                         '2009-02-03T16:45:09.000Z</CreationDate></Bucket>' %
                         xml_escape(i['name']) for i in containers]))
        resp = Response(status=200, content_type='application/xml', body=body)
        return resp


class BucketController(Controller):
    """
    Handles bucket request.
    """
    def __init__(self, env, app, account_name, token, container_name,
                    **kwargs):
        Controller.__init__(self, app)
        self.container_name = unquote(container_name)
        self.account_name = unquote(account_name)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s/%s' % (account_name, container_name)

    def GET(self, env, start_response):
        """
        Handle GET Bucket (List Objects) request
        """
        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}
        max_keys = min(int(args.get('max-keys', MAX_BUCKET_LISTING)),
                        MAX_BUCKET_LISTING)
        env['QUERY_STRING'] = 'format=json&limit=%s' % (max_keys + 1)
        if 'marker' in args:
            env['QUERY_STRING'] += '&marker=%s' % quote(args['marker'])
        if 'prefix' in args:
            env['QUERY_STRING'] += '&prefix=%s' % quote(args['prefix'])
        if 'delimiter' in args:
            env['QUERY_STRING'] += '&delimiter=%s' % quote(args['delimiter'])
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 200:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchBucket')
            else:
                return get_err_response('InvalidURI')

        if 'acl' in args:
            return get_acl(self.account_name)

        objects = loads(''.join(list(body_iter)))
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01">'
            '<Prefix>%s</Prefix>'
            '<Marker>%s</Marker>'
            '<Delimiter>%s</Delimiter>'
            '<IsTruncated>%s</IsTruncated>'
            '<MaxKeys>%s</MaxKeys>'
            '<Name>%s</Name>'
            '%s'
            '%s'
            '</ListBucketResult>' %
            (
                xml_escape(args.get('prefix', '')),
                xml_escape(args.get('marker', '')),
                xml_escape(args.get('delimiter', '')),
                'true' if len(objects) == (max_keys + 1) else 'false',
                max_keys,
                xml_escape(self.container_name),
                "".join(['<Contents><Key>%s</Key><LastModified>%sZ</LastModif'\
                        'ied><ETag>%s</ETag><Size>%s</Size><StorageClass>STA'\
                        'NDARD</StorageClass></Contents>' %
                        (xml_escape(i['name']), i['last_modified'], i['hash'],
                           i['bytes'])
                           for i in objects[:max_keys] if 'subdir' not in i]),
                "".join(['<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>'
                         % xml_escape(i['subdir'])
                         for i in objects[:max_keys] if 'subdir' in i])))
        return Response(body=body, content_type='application/xml')

    def PUT(self, env, start_response):
        """
        Handle PUT Bucket request
        """
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 201:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 202:
                return get_err_response('BucketAlreadyExists')
            else:
                return get_err_response('InvalidURI')

        resp = Response()
        resp.headers.add('Location', self.container_name)
        resp.status = 200
        return resp

    def DELETE(self, env, start_response):
        """
        Handle DELETE Bucket request
        """
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 204:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchBucket')
            elif status == 409:
                return get_err_response('BucketNotEmpty')
            else:
                return get_err_response('InvalidURI')

        resp = Response()
        resp.status = 204
        return resp


class ObjectController(Controller):
    """
    Handles requests on objects
    """
    def __init__(self, env, app, account_name, token, container_name,
                    object_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s/%s/%s' % (account_name, container_name,
                                             object_name)

    def GETorHEAD(self, env, start_response):
        app_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if 200 <= status < 300:
            if 'QUERY_STRING' in env:
                args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
            else:
                args = {}
            if 'acl' in args:
                return get_acl(self.account_name)

            new_hdrs = {}
            for key, val in headers.iteritems():
                _key = key.lower()
                if _key.startswith('x-object-meta-'):
                    new_hdrs['x-amz-meta-' + key[14:]] = val
                elif _key in ('content-length', 'content-type',
                             'content-encoding', 'etag', 'last-modified'):
                    new_hdrs[key] = val
            return Response(status=status, headers=new_hdrs, app_iter=app_iter)
        elif status == 401:
            return get_err_response('AccessDenied')
        elif status == 404:
            return get_err_response('NoSuchKey')
        else:
            return get_err_response('InvalidURI')

    def HEAD(self, env, start_response):
        """
        Handle HEAD Object request
        """
        return self.GETorHEAD(env, start_response)

    def GET(self, env, start_response):
        """
        Handle GET Object request
        """
        return self.GETorHEAD(env, start_response)

    def PUT(self, env, start_response):
        """
        Handle PUT Object and PUT Object (Copy) request
        """
        for key, value in env.items():
            if key.startswith('HTTP_X_AMZ_META_'):
                del env[key]
                env['HTTP_X_OBJECT_META_' + key[16:]] = value
            elif key == 'HTTP_CONTENT_MD5':
                env['HTTP_ETAG'] = value.decode('base64').encode('hex')
            elif key == 'HTTP_X_AMZ_COPY_SOURCE':
                env['HTTP_X_COPY_FROM'] = value

        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 201:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchBucket')
            else:
                return get_err_response('InvalidURI')

        if 'HTTP_X_COPY_FROM' in env:
            body = '<CopyObjectResult>' \
                   '<ETag>"%s"</ETag>' \
                   '</CopyObjectResult>' % headers['etag']
            return Response(status=200, body=body)

        return Response(status=200, etag=headers['etag'])

    def DELETE(self, env, start_response):
        """
        Handle DELETE Object request
        """
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 204:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchKey')
            else:
                return get_err_response('InvalidURI')

        resp = Response()
        resp.status = 204
        return resp


class Swift3Middleware(object):
    """Swift3 S3 compatibility midleware"""
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app

    def get_controller(self, path):
        container, obj = split_path(path, 0, 2, True)
        d = dict(container_name=container, object_name=obj)

        if container and obj:
            return ObjectController, d
        elif container:
            return BucketController, d
        return ServiceController, d

    def __call__(self, env, start_response):
        req = Request(env)

        if 'AWSAccessKeyId' in req.GET:
            try:
                req.headers['Date'] = req.GET['Expires']
                req.headers['Authorization'] = \
                    'AWS %(AWSAccessKeyId)s:%(Signature)s' % req.GET
            except KeyError:
                return get_err_response('InvalidArgument')(env, start_response)

        if not 'Authorization' in req.headers:
            return self.app(env, start_response)

        try:
            account, signature = \
                req.headers['Authorization'].split(' ')[-1].rsplit(':', 1)
        except Exception:
            return get_err_response('InvalidArgument')(env, start_response)

        try:
            controller, path_parts = self.get_controller(req.path)
        except ValueError:
            return get_err_response('InvalidURI')(env, start_response)

        token = base64.urlsafe_b64encode(canonical_string(req))

        controller = controller(env, self.app, account, token, **path_parts)

        if hasattr(controller, req.method):
            res = getattr(controller, req.method)(env, start_response)
        else:
            return get_err_response('InvalidURI')(env, start_response)

        return res(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    def swift3_filter(app):
        return Swift3Middleware(app, conf)

    return swift3_filter
