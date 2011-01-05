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

from webob import Request, Response
from webob.exc import HTTPNotFound
from simplejson import loads
from swift.common.utils import split_path
from urllib import unquote, quote
import rfc822
import hmac
import base64
import errno

def get_err_response(code):
    error_table = {'AccessDenied':
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
                   (403, 'The calculated request signature does not match your provided one'),
                   'NoSuchKey':
                   (404, 'The resource you requested does not exist')}

    resp = Response(content_type='text/xml')
    resp.status = error_table[code][0]
    resp.body = error_table[code][1]
    resp.body = """<?xml version="1.0" encoding="UTF-8"?>\r\n<Error>\r\n  <Code>%s</Code>\r\n  <Message>%s</Message>\r\n</Error>\r\n""" % (code, error_table[code][1])
    return resp

class Controller(object):
    def __init__(self, app):
        self.app = app
        self.response_args = []

    def do_start_response(self, *args):
        self.response_args.extend(args)

class ServiceController(Controller):
    def __init__(self, env, app, account_name, token, **kwargs):
        Controller.__init__(self, app)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s' % account_name

    def GET(self, env, start_response):
        env['QUERY_STRING'] = 'format=json'
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 200:
            if status == 401:
                return get_err_response('AccessDenied')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        containers = loads(''.join(list(body_iter)))
        resp = Response(content_type='text/xml')
        resp.status = 200
        # we don't keep the creation time of a backet (s3cmd doesn't
        # work without that) so we use something bogus.
        resp.body = """<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01"><Buckets>%s</Buckets></ListAllMyBucketsResult>""" % ("".join(['<Bucket><Name>%s</Name><CreationDate>2009-02-03T16:45:09.000Z</CreationDate></Bucket>' % i['name'] for i in containers]))
        return resp

class BucketController(Controller):
    def __init__(self, env, app, account_name, token, container_name, **kwargs):
        Controller.__init__(self, app)
        self.container_name = unquote(container_name)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s/%s' % (account_name, container_name)
               
    def GET(self, env, start_response):
        env['QUERY_STRING'] = 'format=json'
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 200:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('InvalidBucketName')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        objects = loads(''.join(list(body_iter)))
        resp = Response(content_type='text/xml')
        resp.status = 200
        resp.body = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01"><Name>%s</Name>%s</ListBucketResult>""" % (self.container_name, "".join(['<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>%s</Size><StorageClass>STANDARD</StorageClass></Contents>' % (i['name'], i['last_modified'], i['hash'], i['bytes']) for i in objects]))
        return resp

    def PUT(self, env, start_response):
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 201:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 202:
                return get_err_response('BucketAlreadyExists')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        resp = Response()
        resp.headers.add('Location', self.container_name)
        resp.status = 200
        return resp

    def DELETE(self, env, start_response):
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 204:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('InvalidBucketName')
            elif status == 409:
                return get_err_response('BucketNotEmpty')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        resp = Response()
        resp.status = 204
        return resp

class ObjectController(Controller):
    def __init__(self, env, app, account_name, token, container_name, object_name, **kwargs):
        Controller.__init__(self, app)
        self.container_name = unquote(container_name)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s/%s/%s' % (account_name, container_name, object_name)
    def GETorHEAD(self, env, start_response):
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 200:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchKey')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        resp = Response(content_type=headers['Content-Type'])
        resp.etag = headers['etag']
        resp.status = 200
        req = Request(env)
        if req.method == 'GET':
            resp.body = ''.join(list(body_iter))
        return resp

    def HEAD(self, env, start_response):
        return self.GETorHEAD(env, start_response)

    def GET(self, env, start_response):
        return self.GETorHEAD(env, start_response)
        
    def PUT(self, env, start_response):
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])

        if status != 201:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('InvalidBucketName')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        resp = Response()
        resp.etag = headers['etag']
        resp.status = 200
        return resp

    def DELETE(self, env, start_response):
        body_iter = self.app(env, self.do_start_response)
        status = int(self.response_args[0].split()[0])
        headers = dict(self.response_args[1])
            
        if status != 204:
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('NoSuchKey')
            else:
                print status, headers, body_iter
                return get_err_response('InvalidURI')

        resp = Response()
        resp.status = 204
        return resp

class Swift3Middleware(object):
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app

    def get_controller(self, path):
        container, obj = split_path(path, 0, 2)
        d = dict(container_name=container, object_name=obj)

        if container and obj:
            return ObjectController, d
        elif container:
            return BucketController, d
        return ServiceController, d
    
    def get_account_info(self, env, req):
        if req.headers.get("content-md5"):
            md5 = req.headers.get("content-md5")
        else:
            md5 = ""

        if req.headers.get("content-type"):
            content_type = req.headers.get("content-type")
        else:
            content_type = ""

        if req.headers.get("date"):
            date = req.headers.get("date")
        else:
            date = ""

        h = req.method + "\n" + md5 + "\n" + content_type + "\n" + date + "\n"
        for header in req.headers:
            if header.startswith("X-Amz-"):
                h += header.lower()+":"+str(req.headers[header])+"\n"
        h += req.path
        try:
            account, _ = req.headers['Authorization'].split(' ')[-1].split(':')
        except:
            return None, None
        token = base64.urlsafe_b64encode(h)
        return account, token

    def __call__(self, env, start_response):
        req = Request(env)
        if not'Authorization' in req.headers:
            return self.app(env, start_response)
        try:
            controller, path_parts = self.get_controller(req.path)
        except ValueError:
            return get_err_response('InvalidURI')(env, start_response)

        account_name, token = self.get_account_info(env, req)
        if not account_name:
            return get_err_response('InvalidArgument')(env, start_response)
                
        controller = controller(env, self.app, account_name, token, **path_parts)
        if hasattr(controller, req.method):
            res = getattr(controller, req.method)(env, start_response)
        else:
            return get_err_response('InvalidURI')(env, start_response)
            
        return res(env, start_response)

def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    def swift3_filter(app):
        return Swift3Middleware(app, conf)
    return swift3_filter
