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

import hashlib
import io
import json
import os
import random
import sys
import socket
import time

from unittest2 import SkipTest
from xml.dom import minidom

import six
from six.moves import http_client
from six.moves import urllib
from swiftclient import get_auth

from swift.common import constraints
from swift.common.http import is_success
from swift.common.utils import config_true_value

from test import safe_repr

http_client._MAXHEADERS = constraints.MAX_HEADER_COUNT


class AuthenticationFailed(Exception):
    pass


class RequestError(Exception):
    pass


class ResponseError(Exception):
    def __init__(self, response, method=None, path=None, details=None):
        self.status = getattr(response, 'status', 0)
        self.reason = getattr(response, 'reason', '[unknown]')
        self.method = method
        self.path = path
        self.headers = getattr(response, 'getheaders', lambda: [])()
        self.details = details

        for name, value in self.headers:
            if name.lower() == 'x-trans-id':
                self.txid = value
                break
        else:
            self.txid = None

        super(ResponseError, self).__init__()

    def __str__(self):
        return repr(self)

    def __repr__(self):
        msg = '%d: %r (%r %r) txid=%s' % (
            self.status, self.reason, self.method, self.path, self.txid)
        if self.details:
            msg += '\n%s' % self.details
        return msg


def listing_empty(method):
    for i in range(6):
        if len(method()) == 0:
            return True

        time.sleep(2 ** i)

    return False


def listing_items(method):
    marker = None
    once = True
    items = []

    while once or items:
        for i in items:
            yield i

        if once or marker:
            if marker:
                items = method(parms={'marker': marker})
            else:
                items = method()

            if len(items) == 10000:
                marker = items[-1]
            else:
                marker = None

            once = False
        else:
            items = []


class Connection(object):
    def __init__(self, config):
        for key in 'auth_host auth_port auth_ssl username password'.split():
            if key not in config:
                raise SkipTest(
                    "Missing required configuration parameter: %s" % key)

        self.auth_host = config['auth_host']
        self.auth_port = int(config['auth_port'])
        self.auth_ssl = config['auth_ssl'] in ('on', 'true', 'yes', '1')
        self.insecure = config_true_value(config.get('insecure', 'false'))
        self.auth_prefix = config.get('auth_prefix', '/')
        self.auth_version = str(config.get('auth_version', '1'))

        self.account = config.get('account')
        self.username = config['username']
        self.password = config['password']

        self.storage_netloc = None
        self.storage_path = None
        self.conn_class = None

    @property
    def storage_url(self):
        return '%s://%s/%s' % (self.storage_scheme, self.storage_netloc,
                               self.storage_path)

    @storage_url.setter
    def storage_url(self, value):
        url = urllib.parse.urlparse(value)

        if url.scheme == 'http':
            self.conn_class = http_client.HTTPConnection
        elif url.scheme == 'https':
            self.conn_class = http_client.HTTPSConnection
        else:
            raise ValueError('unexpected protocol %s' % (url.scheme))

        self.storage_netloc = url.netloc
        # Make sure storage_path is a string and not unicode, since
        # keystoneclient (called by swiftclient) returns them in
        # unicode and this would cause troubles when doing
        # no_safe_quote query.
        x = url.path.split('/')
        self.storage_path = str('/%s/%s' % (x[1], x[2]))
        self.account_name = str(x[2])

    @property
    def storage_scheme(self):
        if self.conn_class is None:
            return None
        if issubclass(self.conn_class, http_client.HTTPSConnection):
            return 'https'
        return 'http'

    def get_account(self):
        return Account(self, self.account)

    def authenticate(self):
        if self.auth_version == "1":
            auth_path = '%sv1.0' % (self.auth_prefix)
            if self.account:
                auth_user = '%s:%s' % (self.account, self.username)
            else:
                auth_user = self.username
        else:
            auth_user = self.username
            auth_path = self.auth_prefix
        auth_scheme = 'https://' if self.auth_ssl else 'http://'
        auth_netloc = "%s:%d" % (self.auth_host, self.auth_port)
        auth_url = auth_scheme + auth_netloc + auth_path

        if self.insecure:
            try:
                import requests
                from requests.packages.urllib3.exceptions import \
                    InsecureRequestWarning
            except ImportError:
                pass
            else:
                requests.packages.urllib3.disable_warnings(
                    InsecureRequestWarning)
        authargs = dict(snet=False, tenant_name=self.account,
                        auth_version=self.auth_version, os_options={},
                        insecure=self.insecure)
        (storage_url, storage_token) = get_auth(
            auth_url, auth_user, self.password, **authargs)

        if not (storage_url and storage_token):
            raise AuthenticationFailed()

        self.storage_url = storage_url
        self.auth_user = auth_user
        # With v2 keystone, storage_token is unicode.
        # We want it to be string otherwise this would cause
        # troubles when doing query with already encoded
        # non ascii characters in its headers.
        self.storage_token = str(storage_token)
        self.user_acl = '%s:%s' % (self.account, self.username)

        self.http_connect()
        return self.storage_path, self.storage_token

    def cluster_info(self):
        """
        Retrieve the data in /info, or {} on 404
        """
        status = self.make_request('GET', '/info',
                                   cfg={'absolute_path': True})
        if status // 100 == 4:
            return {}
        if not is_success(status):
            raise ResponseError(self.response, 'GET', '/info')
        return json.loads(self.response.read())

    def http_connect(self):
        if self.storage_scheme == 'https' and \
                self.insecure and sys.version_info >= (2, 7, 9):
            import ssl
            self.connection = self.conn_class(
                self.storage_netloc,
                context=ssl._create_unverified_context())
        else:
            self.connection = self.conn_class(self.storage_netloc)

    def make_path(self, path=None, cfg=None):
        if path is None:
            path = []
        if cfg is None:
            cfg = {}

        if cfg.get('version_only_path'):
            return '/' + self.storage_path.split('/')[1]

        if path:
            quote = urllib.parse.quote
            if cfg.get('no_quote') or cfg.get('no_path_quote'):
                quote = lambda x: x
            return '%s/%s' % (self.storage_path,
                              '/'.join([quote(i) for i in path]))
        else:
            return self.storage_path

    def make_headers(self, hdrs, cfg=None):
        if cfg is None:
            cfg = {}
        headers = {}

        if not cfg.get('no_auth_token'):
            headers['X-Auth-Token'] = self.storage_token

        if cfg.get('use_token'):
            headers['X-Auth-Token'] = cfg.get('use_token')

        if isinstance(hdrs, dict):
            headers.update(hdrs)
        return headers

    def make_request(self, method, path=None, data=b'', hdrs=None, parms=None,
                     cfg=None):
        if path is None:
            path = []
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if not cfg.get('absolute_path'):
            # Set absolute_path=True to make a request to exactly the given
            # path, not storage path + given path. Useful for
            # non-account/container/object requests.
            path = self.make_path(path, cfg=cfg)
        headers = self.make_headers(hdrs, cfg=cfg)
        if isinstance(parms, dict) and parms:
            quote = urllib.parse.quote
            if cfg.get('no_quote') or cfg.get('no_parms_quote'):
                quote = lambda x: x
            query_args = ['%s=%s' % (quote(x), quote(str(y)))
                          for (x, y) in parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))
        if not cfg.get('no_content_length'):
            if cfg.get('set_content_length'):
                headers['Content-Length'] = str(cfg.get('set_content_length'))
            else:
                headers['Content-Length'] = str(len(data))

        def try_request():
            self.http_connect()
            self.connection.request(method, path, data, headers)
            return self.connection.getresponse()

        try:
            self.response = self.request_with_retry(try_request)
        except RequestError as e:
            details = "{method} {path} headers: {headers} data: {data}".format(
                method=method, path=path, headers=headers, data=data)
            raise RequestError('Unable to complete request: %s.\n%s' % (
                details, str(e)))
        return self.response.status

    def request_with_retry(self, try_request):
        self.response = None
        try_count = 0
        fail_messages = []
        while try_count < 5:
            try_count += 1

            try:
                self.response = try_request()
            except socket.timeout as e:
                fail_messages.append(safe_repr(e))
                continue
            except http_client.HTTPException as e:
                fail_messages.append(safe_repr(e))
                continue

            if self.response.status == 401:
                fail_messages.append("Response 401")
                self.authenticate()
                continue
            elif self.response.status == 503:
                fail_messages.append("Response 503")
                if try_count != 5:
                    time.sleep(5)
                continue
            break

        if self.response:
            return self.response

        raise RequestError('Attempts: %s, Failures: %s' % (
            len(fail_messages), fail_messages))

    def put_start(self, path, hdrs=None, parms=None, cfg=None, chunked=False):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        self.http_connect()

        path = self.make_path(path, cfg)
        headers = self.make_headers(hdrs, cfg=cfg)

        if chunked:
            headers['Transfer-Encoding'] = 'chunked'
            headers.pop('Content-Length', None)

        if isinstance(parms, dict) and parms:
            quote = urllib.parse.quote
            if cfg.get('no_quote') or cfg.get('no_parms_quote'):
                quote = lambda x: x
            query_args = ['%s=%s' % (quote(x), quote(str(y)))
                          for (x, y) in parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))

        self.connection.putrequest('PUT', path)
        for key, value in headers.items():
            self.connection.putheader(key, value)
        self.connection.endheaders()

    def put_data(self, data, chunked=False):
        if chunked:
            self.connection.send(b'%x\r\n%s\r\n' % (len(data), data))
        else:
            self.connection.send(data)

    def put_end(self, chunked=False):
        if chunked:
            self.connection.send(b'0\r\n\r\n')

        self.response = self.connection.getresponse()
        # Hope it isn't big!
        self.response.body = self.response.read()
        self.connection.close()
        return self.response.status


class Base(object):
    def __str__(self):
        return self.name

    def header_fields(self, required_fields, optional_fields=None):
        if optional_fields is None:
            optional_fields = ()

        def is_int_header(header):
            if header.startswith('x-account-storage-policy-') and \
                    header.endswith(('-bytes-used', '-object-count')):
                return True
            return header in (
                'content-length',
                'x-account-container-count',
                'x-account-object-count',
                'x-account-bytes-used',
                'x-container-object-count',
                'x-container-bytes-used',
            )

        headers = dict(self.conn.response.getheaders())
        ret = {}

        for return_key, header in required_fields:
            if header not in headers:
                raise ValueError("%s was not found in response headers: %r" %
                                 (header, headers))

            if is_int_header(header):
                ret[return_key] = int(headers[header])
            else:
                ret[return_key] = headers[header]

        for return_key, header in optional_fields:
            if header not in headers:
                continue
            if is_int_header(header):
                ret[return_key] = int(headers[header])
            else:
                ret[return_key] = headers[header]

        return ret


class Account(Base):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = str(name)

    def update_metadata(self, metadata=None, cfg=None):
        if metadata is None:
            metadata = {}
        if cfg is None:
            cfg = {}
        headers = dict(("X-Account-Meta-%s" % k, v)
                       for k, v in metadata.items())

        self.conn.make_request('POST', self.path, hdrs=headers, cfg=cfg)
        if not is_success(self.conn.response.status):
            raise ResponseError(self.conn.response, 'POST',
                                self.conn.make_path(self.path))
        return True

    def container(self, container_name):
        return Container(self.conn, self.name, container_name)

    def containers(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        format_type = parms.get('format', None)
        if format_type not in [None, 'json', 'xml']:
            raise RequestError('Invalid format: %s' % format_type)
        if format_type is None and 'format' in parms:
            del parms['format']

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
                                        parms=parms, cfg=cfg)
        if status == 200:
            if format_type == 'json':
                conts = json.loads(self.conn.response.read())
                if six.PY2:
                    for cont in conts:
                        cont['name'] = cont['name'].encode('utf-8')
                return conts
            elif format_type == 'xml':
                conts = []
                tree = minidom.parseString(self.conn.response.read())
                for x in tree.getElementsByTagName('container'):
                    cont = {}
                    for key in ['name', 'count', 'bytes', 'last_modified']:
                        cont[key] = x.getElementsByTagName(key)[0].\
                            childNodes[0].nodeValue
                    conts.append(cont)
                for cont in conts:
                    if six.PY2:
                        cont['name'] = cont['name'].encode('utf-8')
                    for key in ('count', 'bytes'):
                        cont[key] = int(cont[key])
                return conts
            else:
                lines = self.conn.response.read().split(b'\n')
                if lines and not lines[-1]:
                    lines = lines[:-1]
                if six.PY2:
                    return lines
                return [line.decode('utf-8') for line in lines]
        elif status == 204:
            return []

        raise ResponseError(self.conn.response, 'GET',
                            self.conn.make_path(self.path))

    def delete_containers(self):
        for c in listing_items(self.containers):
            cont = self.container(c)
            cont.update_metadata(hdrs={'x-versions-location': ''},
                                 tolerate_missing=True)
            if not cont.delete_recursive():
                return False

        return listing_empty(self.containers)

    def info(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if self.conn.make_request('HEAD', self.path, hdrs=hdrs,
                                  parms=parms, cfg=cfg) != 204:

            raise ResponseError(self.conn.response, 'HEAD',
                                self.conn.make_path(self.path))

        fields = [['object_count', 'x-account-object-count'],
                  ['container_count', 'x-account-container-count'],
                  ['bytes_used', 'x-account-bytes-used']]
        optional_fields = [
            ['temp-url-key', 'x-account-meta-temp-url-key'],
            ['temp-url-key-2', 'x-account-meta-temp-url-key-2']]

        return self.header_fields(fields, optional_fields=optional_fields)

    @property
    def path(self):
        return []


class Container(Base):
    # policy_specified is set in __init__.py when tests are being set up.
    policy_specified = None

    def __init__(self, conn, account, name):
        self.conn = conn
        self.account = str(account)
        self.name = str(name)

    def create(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if self.policy_specified and 'X-Storage-Policy' not in hdrs:
            hdrs['X-Storage-Policy'] = self.policy_specified
        return self.conn.make_request('PUT', self.path, hdrs=hdrs,
                                      parms=parms, cfg=cfg) in (201, 202)

    def update_metadata(self, hdrs=None, cfg=None, tolerate_missing=False):
        if hdrs is None:
            hdrs = {}
        if cfg is None:
            cfg = {}

        self.conn.make_request('POST', self.path, hdrs=hdrs, cfg=cfg)
        if is_success(self.conn.response.status):
            return True
        if tolerate_missing and self.conn.response.status == 404:
            return True
        raise ResponseError(self.conn.response, 'POST',
                            self.conn.make_path(self.path))

    def delete(self, hdrs=None, parms=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        return self.conn.make_request('DELETE', self.path, hdrs=hdrs,
                                      parms=parms) == 204

    def delete_files(self):
        for f in listing_items(self.files):
            file_item = self.file(f)
            if not file_item.delete(tolerate_missing=True):
                return False

        return listing_empty(self.files)

    def delete_recursive(self):
        return self.delete_files() and self.delete()

    def file(self, file_name):
        return File(self.conn, self.account, self.name, file_name)

    def files(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        format_type = parms.get('format', None)
        if format_type not in [None, 'json', 'xml']:
            raise RequestError('Invalid format: %s' % format_type)
        if format_type is None and 'format' in parms:
            del parms['format']

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
                                        parms=parms, cfg=cfg)
        if status == 200:
            if format_type == 'json':
                files = json.loads(self.conn.response.read())

                if six.PY2:
                    for file_item in files:
                        for key in ('name', 'subdir', 'content_type'):
                            if key in file_item:
                                file_item[key] = file_item[key].encode('utf-8')
                return files
            elif format_type == 'xml':
                files = []
                tree = minidom.parseString(self.conn.response.read())
                container = tree.getElementsByTagName('container')[0]
                for x in container.childNodes:
                    file_item = {}
                    if x.tagName == 'object':
                        for key in ['name', 'hash', 'bytes', 'content_type',
                                    'last_modified']:
                            file_item[key] = x.getElementsByTagName(key)[0].\
                                childNodes[0].nodeValue
                    elif x.tagName == 'subdir':
                        file_item['subdir'] = x.getElementsByTagName(
                            'name')[0].childNodes[0].nodeValue
                    else:
                        raise ValueError('Found unexpected element %s'
                                         % x.tagName)
                    files.append(file_item)

                for file_item in files:
                    if 'subdir' in file_item:
                        if six.PY2:
                            file_item['subdir'] = \
                                file_item['subdir'].encode('utf-8')
                    else:
                        if six.PY2:
                            file_item.update({
                                k: file_item[k].encode('utf-8')
                                for k in ('name', 'content_type')})
                        file_item['bytes'] = int(file_item['bytes'])
                return files
            else:
                content = self.conn.response.read()
                if content:
                    lines = content.split(b'\n')
                    if lines and not lines[-1]:
                        lines = lines[:-1]
                    if six.PY2:
                        return lines
                    return [line.decode('utf-8') for line in lines]
                else:
                    return []
        elif status == 204:
            return []

        raise ResponseError(self.conn.response, 'GET',
                            self.conn.make_path(self.path, cfg=cfg))

    def info(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        self.conn.make_request('HEAD', self.path, hdrs=hdrs,
                               parms=parms, cfg=cfg)

        if self.conn.response.status == 204:
            required_fields = [['bytes_used', 'x-container-bytes-used'],
                               ['object_count', 'x-container-object-count'],
                               ['last_modified', 'last-modified']]
            optional_fields = [
                # N.B. swift doesn't return both x-versions-location
                # and x-history-location at a response so that this is safe
                # using same variable "versions" for both and it means
                # versioning is enabled.
                ['versions', 'x-versions-location'],
                ['versions', 'x-history-location'],
                ['tempurl_key', 'x-container-meta-temp-url-key'],
                ['tempurl_key2', 'x-container-meta-temp-url-key-2']]

            return self.header_fields(required_fields, optional_fields)

        raise ResponseError(self.conn.response, 'HEAD',
                            self.conn.make_path(self.path))

    @property
    def path(self):
        return [self.name]


class File(Base):
    def __init__(self, conn, account, container, name):
        self.conn = conn
        self.account = str(account)
        self.container = str(container)
        self.name = str(name)

        self.chunked_write_in_progress = False
        self.content_type = None
        self.content_range = None
        self.size = None
        self.metadata = {}

    def make_headers(self, cfg=None):
        if cfg is None:
            cfg = {}
        headers = {}
        if not cfg.get('no_content_length'):
            if cfg.get('set_content_length'):
                headers['Content-Length'] = str(cfg.get('set_content_length'))
            elif self.size:
                headers['Content-Length'] = str(self.size)
            else:
                headers['Content-Length'] = '0'

        if cfg.get('use_token'):
            headers['X-Auth-Token'] = cfg.get('use_token')

        if cfg.get('no_content_type'):
            pass
        elif self.content_type:
            headers['Content-Type'] = self.content_type
        else:
            headers['Content-Type'] = 'application/octet-stream'

        for key in self.metadata:
            headers['X-Object-Meta-' + key] = self.metadata[key]

        return headers

    @classmethod
    def compute_md5sum(cls, data):
        block_size = 4096

        if isinstance(data, bytes):
            data = six.BytesIO(data)

        checksum = hashlib.md5()
        buff = data.read(block_size)
        while buff:
            checksum.update(buff)
            buff = data.read(block_size)
        data.seek(0)
        return checksum.hexdigest()

    def copy(self, dest_cont, dest_file, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if 'destination' in cfg:
            headers = {'Destination': cfg['destination']}
        elif cfg.get('no_destination'):
            headers = {}
        else:
            headers = {'Destination': '%s/%s' % (dest_cont, dest_file)}
        headers.update(hdrs)

        if 'Destination' in headers:
            headers['Destination'] = urllib.parse.quote(headers['Destination'])

        if self.conn.make_request('COPY', self.path, hdrs=headers,
                                  cfg=cfg, parms=parms) != 201:
            raise ResponseError(self.conn.response, 'COPY',
                                self.conn.make_path(self.path))
        return True

    def copy_account(self, dest_account, dest_cont, dest_file,
                     hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if 'destination' in cfg:
            headers = {'Destination': cfg['destination']}
        elif cfg.get('no_destination'):
            headers = {}
        else:
            headers = {'Destination-Account': dest_account,
                       'Destination': '%s/%s' % (dest_cont, dest_file)}
        headers.update(hdrs)

        if 'Destination-Account' in headers:
            headers['Destination-Account'] = \
                urllib.parse.quote(headers['Destination-Account'])
        if 'Destination' in headers:
            headers['Destination'] = urllib.parse.quote(headers['Destination'])

        if self.conn.make_request('COPY', self.path, hdrs=headers,
                                  cfg=cfg, parms=parms) != 201:
            raise ResponseError(self.conn.response, 'COPY',
                                self.conn.make_path(self.path))
        return True

    def delete(self, hdrs=None, parms=None, cfg=None, tolerate_missing=False):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if tolerate_missing:
            allowed_statuses = (204, 404)
        else:
            allowed_statuses = (204,)

        if self.conn.make_request(
                'DELETE', self.path, hdrs=hdrs, cfg=cfg,
                parms=parms) not in allowed_statuses:
            raise ResponseError(self.conn.response, 'DELETE',
                                self.conn.make_path(self.path))

        return True

    def info(self, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}
        if self.conn.make_request('HEAD', self.path, hdrs=hdrs,
                                  parms=parms, cfg=cfg) != 200:

            raise ResponseError(self.conn.response, 'HEAD',
                                self.conn.make_path(self.path))

        fields = [['content_length', 'content-length'],
                  ['content_type', 'content-type'],
                  ['last_modified', 'last-modified'],
                  ['etag', 'etag']]
        optional_fields = [['x_object_manifest', 'x-object-manifest'],
                           ['x_symlink_target', 'x-symlink-target']]

        header_fields = self.header_fields(fields,
                                           optional_fields=optional_fields)
        header_fields['etag'] = header_fields['etag']
        return header_fields

    def initialize(self, hdrs=None, parms=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if not self.name:
            return False

        status = self.conn.make_request('HEAD', self.path, hdrs=hdrs,
                                        parms=parms)
        if status == 404:
            return False
        elif not is_success(status):
            raise ResponseError(self.conn.response, 'HEAD',
                                self.conn.make_path(self.path))

        for hdr in self.conn.response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]
            if hdr[0].lower().startswith('x-object-meta-'):
                self.metadata[hdr[0][14:]] = hdr[1]
            if hdr[0].lower() == 'etag':
                self.etag = hdr[1]
            if hdr[0].lower() == 'content-length':
                self.size = int(hdr[1])
            if hdr[0].lower() == 'last-modified':
                self.last_modified = hdr[1]

        return True

    def load_from_filename(self, filename, callback=None):
        fobj = open(filename, 'rb')
        self.write(fobj, callback=callback)
        fobj.close()

    @property
    def path(self):
        return [self.container, self.name]

    @classmethod
    def random_data(cls, size=None):
        if size is None:
            size = random.randint(1, 32768)
        fd = open('/dev/urandom', 'rb')
        data = fd.read(size)
        fd.close()
        return data

    def read(self, size=-1, offset=0, hdrs=None, buffer=None,
             callback=None, cfg=None, parms=None):
        if cfg is None:
            cfg = {}
        if parms is None:
            parms = {}

        if size > 0:
            range_string = 'bytes=%d-%d' % (offset, (offset + size) - 1)
            if hdrs:
                hdrs['Range'] = range_string
            else:
                hdrs = {'Range': range_string}

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
                                        cfg=cfg, parms=parms)

        if not is_success(status):
            raise ResponseError(self.conn.response, 'GET',
                                self.conn.make_path(self.path))

        for hdr in self.conn.response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]
            if hdr[0].lower() == 'content-range':
                self.content_range = hdr[1]

        if hasattr(buffer, 'write'):
            scratch = self.conn.response.read(8192)
            transferred = 0

            while len(scratch) > 0:
                buffer.write(scratch)
                transferred += len(scratch)
                if callable(callback):
                    callback(transferred, self.size)
                scratch = self.conn.response.read(8192)
            return None
        else:
            return self.conn.response.read()

    def read_md5(self):
        status = self.conn.make_request('GET', self.path)

        if not is_success(status):
            raise ResponseError(self.conn.response, 'GET',
                                self.conn.make_path(self.path))

        checksum = hashlib.md5()

        scratch = self.conn.response.read(8192)
        while len(scratch) > 0:
            checksum.update(scratch)
            scratch = self.conn.response.read(8192)

        return checksum.hexdigest()

    def save_to_filename(self, filename, callback=None):
        try:
            fobj = open(filename, 'wb')
            self.read(buffer=fobj, callback=callback)
        finally:
            fobj.close()

    def sync_metadata(self, metadata=None, cfg=None, parms=None):
        if cfg is None:
            cfg = {}

        self.metadata = self.metadata if metadata is None else metadata

        if self.metadata:
            headers = self.make_headers(cfg=cfg)
            if not cfg.get('no_content_length'):
                if cfg.get('set_content_length'):
                    headers['Content-Length'] = str(
                        cfg.get('set_content_length'))
                else:
                    headers['Content-Length'] = '0'

            self.conn.make_request('POST', self.path, hdrs=headers,
                                   parms=parms, cfg=cfg)

            if self.conn.response.status != 202:
                raise ResponseError(self.conn.response, 'POST',
                                    self.conn.make_path(self.path))

        return True

    def chunked_write(self, data=None, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        if data is not None and self.chunked_write_in_progress:
            self.conn.put_data(data, True)
        elif data is not None:
            self.chunked_write_in_progress = True

            headers = self.make_headers(cfg=cfg)
            headers.update(hdrs)

            self.conn.put_start(self.path, hdrs=headers, parms=parms,
                                cfg=cfg, chunked=True)

            self.conn.put_data(data, True)
        elif self.chunked_write_in_progress:
            self.chunked_write_in_progress = False
            return self.conn.put_end(True) == 201
        else:
            raise RuntimeError

    def write(self, data=b'', hdrs=None, parms=None, callback=None, cfg=None,
              return_resp=False):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        block_size = 2 ** 20

        if all(hasattr(data, attr) for attr in ('flush', 'seek', 'fileno')):
            try:
                data.flush()
                data.seek(0)
            except IOError:
                pass
            self.size = int(os.fstat(data.fileno())[6])
        else:
            data = io.BytesIO(data)
            self.size = data.seek(0, os.SEEK_END)
            data.seek(0)

        headers = self.make_headers(cfg=cfg)
        headers.update(hdrs)

        def try_request():
            # rewind to be ready for another attempt
            data.seek(0)
            self.conn.put_start(self.path, hdrs=headers, parms=parms, cfg=cfg)

            transferred = 0
            for buff in iter(lambda: data.read(block_size), b''):
                self.conn.put_data(buff)
                transferred += len(buff)
                if callable(callback):
                    callback(transferred, self.size)

            self.conn.put_end()
            return self.conn.response

        try:
            self.response = self.conn.request_with_retry(try_request)
        except RequestError as e:
            raise ResponseError(self.conn.response, 'PUT',
                                self.conn.make_path(self.path), details=str(e))
        if not is_success(self.response.status):
            raise ResponseError(self.conn.response, 'PUT',
                                self.conn.make_path(self.path))

        try:
            data.seek(0)
        except IOError:
            pass
        self.md5 = self.compute_md5sum(data)

        if return_resp:
            return self.conn.response

        return True

    def write_random(self, size=None, hdrs=None, parms=None, cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        data = self.random_data(size)
        if not self.write(data, hdrs=hdrs, parms=parms, cfg=cfg):
            raise ResponseError(self.conn.response, 'PUT',
                                self.conn.make_path(self.path))
        self.md5 = self.compute_md5sum(six.BytesIO(data))
        return data

    def write_random_return_resp(self, size=None, hdrs=None, parms=None,
                                 cfg=None):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        data = self.random_data(size)
        resp = self.write(data, hdrs=hdrs, parms=parms, cfg=cfg,
                          return_resp=True)
        if not resp:
            raise ResponseError(self.conn.response)
        self.md5 = self.compute_md5sum(six.BytesIO(data))
        return resp

    def post(self, hdrs=None, parms=None, cfg=None, return_resp=False):
        if hdrs is None:
            hdrs = {}
        if parms is None:
            parms = {}
        if cfg is None:
            cfg = {}

        headers = self.make_headers(cfg=cfg)
        headers.update(hdrs)

        self.conn.make_request('POST', self.path, hdrs=headers,
                               parms=parms, cfg=cfg)

        if self.conn.response.status != 202:
            raise ResponseError(self.conn.response, 'POST',
                                self.conn.make_path(self.path))

        if return_resp:
            return self.conn.response

        return True
