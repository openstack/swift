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
from hashlib import md5, sha1
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

class ServiceController(Controller):
    def __init__(self, env, app, account_name, token, **kwargs):
        Controller.__init__(self, app)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s' % account_name

    def GET(self, env, start_response):
        req = Request(env)
        env['QUERY_STRING'] = 'format=json'
        resp = self.app(env, start_response)
        try:
            containers = loads(''.join(list(resp)))
        except:
            return get_err_response('AccessDenied')
            
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
        req = Request(env)
        env['QUERY_STRING'] = 'format=json'
        resp = self.app(env, start_response)
        try:
            objects = loads(''.join(list(resp)))
        except:
            status = int(resp[0].split()[0])
            resp = Response(content_type='text/xml')
            if status == 401:
                return get_err_response('AccessDenied')
            elif status == 404:
                return get_err_response('InvalidBucketName')
            else:
                print resp
                return get_err_response('InvalidURI')

        resp = Response(content_type='text/xml')
        resp.status = 200
        resp.body = """<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01"><Name>%s</Name>%s</ListBucketResult>""" % (self.container_name, "".join(['<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>%s</Size><StorageClass>STANDARD</StorageClass></Contents>' % (i['name'], i['last_modified'], i['hash'], i['bytes']) for i in objects]))
        return resp

    def PUT(self, env, start_response):
        req = Request(env)
        resp = self.app(env, start_response)
        status = int(resp[0].split()[0])
        if status == 401:
            return get_err_response('AccessDenied')
        elif status == 202:
            return get_err_response('BucketAlreadyExists')
        else:
            print resp

        resp = Response()
        resp.headers.add('Location', self.container_name)
        resp.status = 200
        return resp

    def DELETE(self, env, start_response):
        req = Request(env)
        resp = self.app(env, start_response)
        try:
            status = int(resp[0].split()[0])
        except:
            resp = Response()
            resp.status = 204
            return resp

        if status == 401:
            return get_err_response('AccessDenied')
        elif status == 404:
            return get_err_response('InvalidBucketName')
        elif status == 409:
            return get_err_response('BucketNotEmpty')
        else:
            print resp
            return get_err_response('InvalidURI')

class ObjectController(Controller):
    def __init__(self, env, app, account_name, token, container_name, object_name, **kwargs):
        Controller.__init__(self, app)
        self.container_name = unquote(container_name)
        env['HTTP_X_AUTH_TOKEN'] = token
        env['PATH_INFO'] = '/v1/%s/%s/%s' % (account_name, container_name, object_name)

    def GETorHEAD(self, env, start_response):
        # there should be better ways.
        # TODO:
        # - we can't handle various errors properly (autorization, etc)
        # - hide GETorHEAD
        req = Request(env)
        method = req.method
        req.method = 'GET'
        data = self.app(env, start_response)
        if type(data) == list:
            status = int(data[0][data[0].find('<title>') + 7:].split(' ')[0])
            if status == 404:
                return get_err_response('NoSuchKey')
            else:
                return get_err_response('AccessDenied')
            
        if method == 'GET':
            resp = Response(content_type='text/xml')
            resp.body = ''.join(list(data))
            resp.status = 200
        else:
            resp = Response()
            etag = md5()
            etag.update(''.join(list(data)))
            etag = etag.hexdigest()
            resp.etag = etag
            resp.status = 200
        return resp

    def HEAD(self, env, start_response):
        return self.GETorHEAD(env, start_response)

    def GET(self, env, start_response):
        return self.GETorHEAD(env, start_response)
        
    def PUT(self, env, start_response):
        # TODO: how can we get etag from the response header?
        req = Request(env)
        etag = md5()
        etag.update(req.body)
        etag = etag.hexdigest()
        resp = self.app(env, start_response)
        status = int(resp[0].split()[0])
        if status == 401:
            return get_err_response('AccessDenied')
        elif status == 404:
            return get_err_response('InvalidBucketName')
        elif status == 201:
            resp = Response()
            resp.etag = etag
            resp.status = 200
            return resp
        else:
            print resp
            return get_err_response('InvalidURI')

    def DELETE(self, env, start_response):
        # TODO: how can we get the response result?
        req = Request(env)
        resp = self.app(env, start_response)
        try:
            status = int(resp[0].split()[0])
        except:
            resp = Response()
            resp.status = 204
            return resp
            
        print resp
        if status == 401:
            return get_err_response('AccessDenied')
        elif status == 404:
            return get_err_response('NoSuchKey')
        else:
            return get_err_response('AccessDenied')

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
#        print req.method
#        print req.path
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
