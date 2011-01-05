# Copyright (c) 2010-2011 OpenStack, LLC.
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

import webob
from urllib import quote, unquote
from json import loads as json_loads

from swift.common.compressing_file_reader import CompressingFileReader
from swift.proxy.server import BaseApplication


class MemcacheStub(object):

    def get(self, *a, **kw):
        return None

    def set(self, *a, **kw):
        return None

    def incr(self, *a, **kw):
        return 0

    def delete(self, *a, **kw):
        return None

    def set_multi(self, *a, **kw):
        return None

    def get_multi(self, *a, **kw):
        return []


class InternalProxy(object):
    """
    Set up a private instance of a proxy server that allows normal requests
    to be made without having to actually send the request to the proxy.
    This also doesn't log the requests to the normal proxy logs.

    :param proxy_server_conf: proxy server configuration dictionary
    :param logger: logger to log requests to
    :param retries: number of times to retry each request
    """

    def __init__(self, proxy_server_conf=None, logger=None, retries=0):
        self.upload_app = BaseApplication(proxy_server_conf,
                                          memcache=MemcacheStub(),
                                          logger=logger)
        self.retries = retries

    def upload_file(self, source_file, account, container, object_name,
                    compress=True, content_type='application/x-gzip',
                    etag=None):
        """
        Upload a file to cloud files.

        :param source_file: path to or file like object to upload
        :param account: account to upload to
        :param container: container to upload to
        :param object_name: name of object being uploaded
        :param compress: if True, compresses object as it is uploaded
        :param content_type: content-type of object
        :param etag: etag for object to check successful upload
        :returns: True if successful, False otherwise
        """
        target_name = '/v1/%s/%s/%s' % (account, container, object_name)

        # create the container
        if not self.create_container(account, container):
            return False

        # upload the file to the account
        req = webob.Request.blank(target_name,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Transfer-Encoding': 'chunked'})
        if compress:
            if hasattr(source_file, 'read'):
                compressed_file = CompressingFileReader(source_file)
            else:
                compressed_file = CompressingFileReader(
                                    open(source_file, 'rb'))
            req.body_file = compressed_file
        else:
            if not hasattr(source_file, 'read'):
                source_file = open(source_file, 'rb')
            req.body_file = source_file
        req.account = account
        req.content_type = content_type
        req.content_length = None   # to make sure we send chunked data
        if etag:
            req.etag = etag
        resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
        tries = 1
        while (resp.status_int < 200 or resp.status_int > 299) \
                and tries <= self.retries:
            resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
            tries += 1
        if not (200 <= resp.status_int < 300):
            return False
        return True

    def get_object(self, account, container, object_name):
        """
        Get object.

        :param account: account name object is in
        :param container: container name object is in
        :param object_name: name of object to get
        :returns: iterator for object data
        """
        req = webob.Request.blank('/v1/%s/%s/%s' %
                            (account, container, object_name),
                            environ={'REQUEST_METHOD': 'GET'})
        req.account = account
        resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
        tries = 1
        while (resp.status_int < 200 or resp.status_int > 299) \
                and tries <= self.retries:
            resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
            tries += 1
        return resp.status_int, resp.app_iter

    def create_container(self, account, container):
        """
        Create container.

        :param account: account name to put the container in
        :param container: container name to create
        :returns: True if successful, otherwise False
        """
        req = webob.Request.blank('/v1/%s/%s' % (account, container),
                            environ={'REQUEST_METHOD': 'PUT'})
        req.account = account
        resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
        tries = 1
        while (resp.status_int < 200 or resp.status_int > 299) \
                and tries <= self.retries:
            resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
            tries += 1
        return 200 <= resp.status_int < 300

    def get_container_list(self, account, container, marker=None,
                           end_marker=None, limit=None, prefix=None,
                           delimiter=None, full_listing=True):
        """
        Get container listing.

        :param account: account name for the container
        :param container: container name to get the listing of
        :param marker: marker query
        :param end_marker: end marker query
        :param limit: limit to query
        :param prefix: prefix query
        :param delimeter: delimeter for query
        :param full_listing: if True, make enough requests to get all listings
        :returns: list of objects
        """
        if full_listing:
            rv = []
            listing = self.get_container_list(account, container, marker,
                end_marker, limit, prefix, delimiter, full_listing=False)
            while listing:
                rv.extend(listing)
                if not delimiter:
                    marker = listing[-1]['name']
                else:
                    marker = listing[-1].get('name', listing[-1].get('subdir'))
                listing = self.get_container_list(account, container, marker,
                    end_marker, limit, prefix, delimiter, full_listing=False)
            return rv
        path = '/v1/%s/%s' % (account, container)
        qs = 'format=json'
        if marker:
            qs += '&marker=%s' % quote(marker)
        if end_marker:
            qs += '&end_marker=%s' % quote(end_marker)
        if limit:
            qs += '&limit=%d' % limit
        if prefix:
            qs += '&prefix=%s' % quote(prefix)
        if delimiter:
            qs += '&delimiter=%s' % quote(delimiter)
        path += '?%s' % qs
        req = webob.Request.blank(path, environ={'REQUEST_METHOD': 'GET'})
        req.account = account
        resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
        tries = 1
        while (resp.status_int < 200 or resp.status_int > 299) \
                and tries <= self.retries:
            resp = self.upload_app.handle_request(
                                self.upload_app.update_request(req))
            tries += 1
        if resp.status_int == 204:
            return []
        if 200 <= resp.status_int < 300:
            return json_loads(resp.body)
