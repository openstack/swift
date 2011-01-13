# Copyright (c) 2011 OpenStack, LLC.
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

import unittest

from webob import Request, Response
from webob.exc import HTTPUnauthorized, HTTPCreated, HTTPNoContent,\
     HTTPAccepted, HTTPBadRequest, HTTPNotFound, HTTPConflict
import xml.dom.minidom
import simplejson

from swift.common.middleware import swift3


class FakeApp(object):
    def __init__(self):
        self.app = self
        self.response_args = []

    def __call__(self, env, start_response):
        return "FAKE APP"

    def do_start_response(self, *args):
        self.response_args.extend(args)


class FakeAppService(FakeApp):
    def __init__(self, status=200):
        FakeApp.__init__(self)
        self.status = status
        self.buckets = (('apple', 1, 200), ('orange', 3, 430))

    def __call__(self, env, start_response):
        if self.status == 200:
            start_response(Response().status)
            start_response({'Content-Type': 'text/xml'})
            json_pattern = ['"name":%s', '"count":%s', '"bytes":%s']
            json_pattern = '{' + ','.join(json_pattern) + '}'
            json_out = []
            for b in self.buckets:
                name = simplejson.dumps(b[0])
                json_out.append(json_pattern %
                                (name, b[1], b[2]))
            account_list = '[' + ','.join(json_out) + ']'
            return account_list
        elif self.status == 401:
            start_response(HTTPUnauthorized().status)
            start_response({})
        else:
            start_response(HTTPBadRequest().status)
            start_response({})


class FakeAppBucket(FakeApp):
    def __init__(self, status=200):
        FakeApp.__init__(self)
        self.status = status
        self.objects = (('rose', '2011-01-05T02:19:14.275290', 0, 303),
                        ('viola', '2011-01-05T02:19:14.275290', 0, 3909),
                        ('lily', '2011-01-05T02:19:14.275290', 0, 3909))

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] == 'GET':
            if self.status == 200:
                start_response(Response().status)
                start_response({'Content-Type': 'text/xml'})
                json_pattern = ['"name":%s', '"last_modified":%s', '"hash":%s',
                                '"bytes":%s']
                json_pattern = '{' + ','.join(json_pattern) + '}'
                json_out = []
                for b in self.objects:
                    name = simplejson.dumps(b[0])
                    time = simplejson.dumps(b[1])
                    json_out.append(json_pattern %
                                    (name, time, b[2], b[3]))
                account_list = '[' + ','.join(json_out) + ']'
                return account_list
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 404:
                start_response(HTTPNotFound().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})
        elif env['REQUEST_METHOD'] == 'PUT':
            if self.status == 201:
                start_response(HTTPCreated().status)
                start_response({})
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 202:
                start_response(HTTPAccepted().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})
        elif env['REQUEST_METHOD'] == 'DELETE':
            if self.status == 204:
                start_response(HTTPNoContent().status)
                start_response({})
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 404:
                start_response(HTTPNotFound().status)
                start_response({})
            elif self.status == 409:
                start_response(HTTPConflict().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})


class FakeAppObject(FakeApp):
    def __init__(self, status=200):
        FakeApp.__init__(self)
        self.status = status
        self.object_body = 'hello'
        self.response_headers = {'Content-Type': 'text/html',
                                 'Content-Length': len(self.object_body),
                                 'x-object-meta-test': 'swift',
                                 'etag': '1b2cf535f27731c974343645a3985328',
                                 'last-modified': '2011-01-05T02:19:14.275290'}

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] == 'GET' or env['REQUEST_METHOD'] == 'HEAD':
            if self.status == 200:
                start_response(Response().status)
                start_response(self.response_headers)
                if env['REQUEST_METHOD'] == 'GET':
                    return self.object_body
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 404:
                start_response(HTTPNotFound().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})
        elif env['REQUEST_METHOD'] == 'PUT':
            if self.status == 201:
                start_response(HTTPCreated().status)
                start_response({'etag': self.response_headers['etag']})
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 404:
                start_response(HTTPNotFound().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})
        elif env['REQUEST_METHOD'] == 'DELETE':
            if self.status == 204:
                start_response(HTTPNoContent().status)
                start_response({})
            elif self.status == 401:
                start_response(HTTPUnauthorized().status)
                start_response({})
            elif self.status == 404:
                start_response(HTTPNotFound().status)
                start_response({})
            else:
                start_response(HTTPBadRequest().status)
                start_response({})


def start_response(*args):
    pass


class TestSwift3(unittest.TestCase):
    def setUp(self):
        self.app = swift3.filter_factory({})(FakeApp())

    def test_non_s3_request_passthrough(self):
        req = Request.blank('/something')
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

    def test_bad_format_authorization(self):
        req = Request.blank('/something',
                            headers={'Authorization': 'hoge'})
        resp = self.app(req.environ, start_response)
        dom = xml.dom.minidom.parseString("".join(resp))
        self.assertEquals(dom.firstChild.nodeName, 'Error')
        code = dom.getElementsByTagName('Code')[0].childNodes[0].nodeValue
        self.assertEquals(code, 'InvalidArgument')

    def test_bad_path(self):
        req = Request.blank('/bucket/object/bad',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AUTH_something:hoge'})
        resp = self.app(req.environ, start_response)
        dom = xml.dom.minidom.parseString("".join(resp))
        self.assertEquals(dom.firstChild.nodeName, 'Error')
        code = dom.getElementsByTagName('Code')[0].childNodes[0].nodeValue
        self.assertEquals(code, 'InvalidURI')

    def _test_method_error(self, cl, method, path, status):
        local_app = swift3.filter_factory({})(cl(status))
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, start_response)
        dom = xml.dom.minidom.parseString("".join(resp))
        self.assertEquals(dom.firstChild.nodeName, 'Error')
        return dom.getElementsByTagName('Code')[0].childNodes[0].nodeValue

    def test_service_GET_error(self):
        code = self._test_method_error(FakeAppService, 'GET', '/', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppService, 'GET', '/', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_service_GET(self):
        local_app = swift3.filter_factory({})(FakeAppService())
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '200')

        dom = xml.dom.minidom.parseString("".join(resp))
        self.assertEquals(dom.firstChild.nodeName, 'ListAllMyBucketsResult')

        buckets = [n for n in dom.getElementsByTagName('Bucket')]
        listing = [n for n in buckets[0].childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 2)

        names = []
        for b in buckets:
            if b.childNodes[0].nodeName == 'Name':
                names.append(b.childNodes[0].childNodes[0].nodeValue)

        self.assertEquals(len(names), len(FakeAppService().buckets))
        for i in FakeAppService().buckets:
            self.assertTrue(i[0] in names)

    def test_bucket_GET_error(self):
        code = self._test_method_error(FakeAppBucket, 'GET', '/bucket', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppBucket, 'GET', '/bucket', 404)
        self.assertEquals(code, 'InvalidBucketName')
        code = self._test_method_error(FakeAppBucket, 'GET', '/bucket', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_bucket_GET(self):
        local_app = swift3.filter_factory({})(FakeAppBucket())
        bucket_name = 'junk'
        req = Request.blank('/%s' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '200')

        dom = xml.dom.minidom.parseString("".join(resp))
        self.assertEquals(dom.firstChild.nodeName, 'ListBucketResult')
        name = dom.getElementsByTagName('Name')[0].childNodes[0].nodeValue
        self.assertEquals(name, bucket_name)

        objects = [n for n in dom.getElementsByTagName('Contents')]
        listing = [n for n in objects[0].childNodes if n.nodeName != '#text']

        names = []
        for o in objects:
            if o.childNodes[0].nodeName == 'Key':
                names.append(o.childNodes[0].childNodes[0].nodeValue)

        self.assertEquals(len(names), len(FakeAppBucket().objects))
        for i in FakeAppBucket().objects:
            self.assertTrue(i[0] in names)

    def test_bucket_PUT_error(self):
        code = self._test_method_error(FakeAppBucket, 'PUT', '/bucket', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppBucket, 'PUT', '/bucket', 202)
        self.assertEquals(code, 'BucketAlreadyExists')
        code = self._test_method_error(FakeAppBucket, 'PUT', '/bucket', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_bucket_PUT(self):
        local_app = swift3.filter_factory({})(FakeAppBucket(201))
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '200')

    def test_bucket_DELETE_error(self):
        code = self._test_method_error(FakeAppBucket, 'DELETE', '/bucket', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppBucket, 'DELETE', '/bucket', 404)
        self.assertEquals(code, 'InvalidBucketName')
        code = self._test_method_error(FakeAppBucket, 'DELETE', '/bucket', 409)
        self.assertEquals(code, 'BucketNotEmpty')
        code = self._test_method_error(FakeAppBucket, 'DELETE', '/bucket', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_bucket_DELETE(self):
        local_app = swift3.filter_factory({})(FakeAppBucket(204))
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '204')

    def _test_object_GETorHEAD(self, method):
        local_app = swift3.filter_factory({})(FakeAppObject())
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '200')

        headers = dict(local_app.app.response_args[1])
        for key, val in local_app.app.response_headers.iteritems():
            if key in ('Content-Length', 'Content-Type', 'Content-Encoding',
                       'etag', 'last-modified'):
                self.assertTrue(key in headers)
                self.assertEquals(headers[key], val)

            elif key.startswith('x-object-meta-'):
                self.assertTrue('x-amz-meta-' + key[14:] in headers)
                self.assertEquals(headers['x-amz-meta-' + key[14:]], val)

        if method == 'GET':
            self.assertEquals(resp, local_app.app.object_body)

    def test_object_HEAD(self):
        self._test_object_GETorHEAD('HEAD')

    def test_object_GET(self):
        self._test_object_GETorHEAD('GET')

    def test_object_PUT_error(self):
        code = self._test_method_error(FakeAppObject, 'PUT',
                                       '/bucket/object', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppObject, 'PUT',
                                       '/bucket/object', 404)
        self.assertEquals(code, 'InvalidBucketName')
        code = self._test_method_error(FakeAppObject, 'PUT',
                                       '/bucket/object', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_object_PUT(self):
        local_app = swift3.filter_factory({})(FakeAppObject(201))
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '200')

        headers = dict(local_app.app.response_args[1])
        self.assertEquals(headers['ETag'],
                          "\"%s\"" % local_app.app.response_headers['etag'])

    def test_object_DELETE_error(self):
        code = self._test_method_error(FakeAppObject, 'DELETE',
                                       '/bucket/object', 401)
        self.assertEquals(code, 'AccessDenied')
        code = self._test_method_error(FakeAppObject, 'DELETE',
                                       '/bucket/object', 404)
        self.assertEquals(code, 'NoSuchKey')
        code = self._test_method_error(FakeAppObject, 'DELETE',
                                       '/bucket/object', 0)
        self.assertEquals(code, 'InvalidURI')

    def test_object_DELETE(self):
        local_app = swift3.filter_factory({})(FakeAppObject(204))
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AUTH_who:password'})
        resp = local_app(req.environ, local_app.app.do_start_response)
        self.assertEquals(local_app.app.response_args[0].split()[0], '204')


if __name__ == '__main__':
    unittest.main()
