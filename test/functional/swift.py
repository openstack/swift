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

import hashlib
import httplib
import os
import random
import socket
import StringIO
import time
import urllib

import simplejson as json

from nose import SkipTest
from xml.dom import minidom

class AuthenticationFailed(Exception):
    pass

class RequestError(Exception):
    pass

class ResponseError(Exception):
    def __init__(self, response):
        self.status = response.status
        self.reason = response.reason
        Exception.__init__(self)

    def __str__(self):
        return '%d: %s' % (self.status, self.reason)

    def __repr__(self):
        return '%d: %s' % (self.status, self.reason)

def listing_empty(method):
    for i in xrange(0, 6):
        if len(method()) == 0:
            return True

        time.sleep(2**i)

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
                 items = method(parms={'marker':marker})
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
            if not config.has_key(key):
                raise SkipTest

        self.auth_host = config['auth_host']
        self.auth_port = int(config['auth_port'])
        self.auth_ssl = config['auth_ssl'] in ('on', 'true', 'yes', '1')
        self.auth_prefix = config.get('auth_prefix', '/')

        self.account = config.get('account')
        self.username = config['username']
        self.password = config['password']

        self.storage_host = None
        self.storage_port = None

        self.conn_class = None

    def get_account(self):
        return Account(self, self.account)

    def authenticate(self, clone_conn=None):
        if clone_conn:
            self.conn_class = clone_conn.conn_class
            self.storage_host = clone_conn.storage_host
            self.storage_url = clone_conn.storage_url
            self.storage_port = clone_conn.storage_port
            self.storage_token = clone_conn.storage_token
            return

        if self.account:
            auth_user = '%s:%s' % (self.account, self.username)
        else:
            auth_user = self.username
        headers = {
            'x-auth-user': auth_user,
            'x-auth-key': self.password,
        }

        path = '%sv1.0' % (self.auth_prefix)
        if self.auth_ssl:
            connection = httplib.HTTPSConnection(self.auth_host,
                port=self.auth_port)
        else:
            connection = httplib.HTTPConnection(self.auth_host,
                port=self.auth_port)
        #connection.set_debuglevel(3)
        connection.request('GET', path, '', headers)
        response = connection.getresponse()
        connection.close()

        if response.status == 401:
            raise AuthenticationFailed()

        if response.status not in (200, 204):
            raise ResponseError(response)

        for hdr in response.getheaders():
            if hdr[0].lower() == "x-storage-url":
                storage_url = hdr[1]
            elif hdr[0].lower() == "x-storage-token":
                storage_token = hdr[1]

        if not (storage_url and storage_token):
            raise AuthenticationFailed()

        x = storage_url.split('/')

        if x[0] == 'http:':
            self.conn_class = httplib.HTTPConnection
            self.storage_port = 80
        elif x[0] == 'https:':
            self.conn_class = httplib.HTTPSConnection
            self.storage_port = 443
        else:
            raise ValueError, 'unexpected protocol %s' % (x[0])

        self.storage_host = x[2].split(':')[0]
        if ':' in x[2]:
            self.storage_port = int(x[2].split(':')[1])
        self.storage_url = '/%s/%s' % (x[3], x[4])

        self.storage_token = storage_token

        self.http_connect()
        return self.storage_url, self.storage_token

    def http_connect(self):
        self.connection = self.conn_class(self.storage_host,
            port=self.storage_port)
        #self.connection.set_debuglevel(3)
        
    def make_path(self, path=[], cfg={}):
        if cfg.get('version_only_path'):
            return '/' + self.storage_url.split('/')[1]

        if path:
            quote = urllib.quote
            if cfg.get('no_quote') or cfg.get('no_path_quote'):
                quote = lambda x: x
            return '%s/%s' % (self.storage_url, '/'.join([quote(i) for i
                in path]))
        else:
            return self.storage_url

    def make_headers(self, hdrs, cfg={}):
        headers = {}

        if not cfg.get('no_auth_token'):
            headers['X-Auth-Token'] = self.storage_token

        if isinstance(hdrs, dict):
            headers.update(hdrs)
        return headers

    def make_request(self, method, path=[], data='', hdrs={}, parms={},
        cfg={}):

        path = self.make_path(path, cfg=cfg)
        headers = self.make_headers(hdrs, cfg=cfg)

        if isinstance(parms, dict) and parms:
            quote = urllib.quote
            if cfg.get('no_quote') or cfg.get('no_parms_quote'):
                quote = lambda x: x
            query_args = ['%s=%s' % (quote(x), quote(str(y))) for (x,y) in
                parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))

        if not cfg.get('no_content_length'):
            if cfg.get('set_content_length'):
                headers['Content-Length'] = cfg.get('set_content_length')
            else:
                headers['Content-Length'] = len(data)

        def try_request():
            self.http_connect()
            self.connection.request(method, path, data, headers)
            return self.connection.getresponse()

        self.response = None
        try_count = 0
        while try_count < 5:
            try_count += 1

            try:
                self.response = try_request()
            except httplib.HTTPException:
                continue
            
            if self.response.status == 401:
                self.authenticate()
                continue
            elif self.response.status == 503:
                if try_count != 5:
                    time.sleep(5)
                continue

            break

        if self.response:
            return self.response.status

        raise RequestError('Unable to compelte http request')

    def put_start(self, path, hdrs={}, parms={}, cfg={}, chunked=False):
        self.http_connect()

        path = self.make_path(path, cfg)
        headers = self.make_headers(hdrs, cfg=cfg)

        if chunked:
            headers['Transfer-Encoding'] = 'chunked'
            headers.pop('Content-Length', None)

        if isinstance(parms, dict) and parms:
            quote = urllib.quote
            if cfg.get('no_quote') or cfg.get('no_parms_quote'):
                quote = lambda x: x
            query_args = ['%s=%s' % (quote(x), quote(str(y))) for (x,y) in
                parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))

            query_args = ['%s=%s' % (urllib.quote(x),
                urllib.quote(str(y))) for (x,y) in parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))

        self.connection = self.conn_class(self.storage_host,
            port=self.storage_port)
        #self.connection.set_debuglevel(3)
        self.connection.putrequest('PUT', path)
        for key, value in headers.iteritems():
            self.connection.putheader(key, value)
        self.connection.endheaders()

    def put_data(self, data, chunked=False):
        if chunked:
            self.connection.send('%s\r\n%s\r\n' % (hex(len(data)), data))
        else:
            self.connection.send(data)

    def put_end(self, chunked=False):
        if chunked:
            self.connection.send('0\r\n\r\n')

        self.response = self.connection.getresponse()
        self.connection.close()
        return self.response.status

class Base:
    def __str__(self):
        return self.name

    def header_fields(self, fields):
        headers = dict(self.conn.response.getheaders())

        ret = {}
        for field in fields:
            if not headers.has_key(field[1]):
                raise ValueError("%s was not found in response header" %
                    (field[1]))

            try:
                ret[field[0]] = int(headers[field[1]])
            except ValueError:
                ret[field[0]] = headers[field[1]]
        return ret

class Account(Base):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = str(name)

    def container(self, container_name):
        return Container(self.conn, self.name, container_name)

    def containers(self, hdrs={}, parms={}, cfg={}):
        format = parms.get('format', None)
        if format not in [None, 'json', 'xml']:
            raise RequestError('Invalid format: %s' % format)
        if format is None and parms.has_key('format'):
            del parms['format']

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg)
        if status == 200:
            if format == 'json':
                conts = json.loads(self.conn.response.read())
                for cont in conts:
                    cont['name'] = cont['name'].encode('utf-8')
                return conts
            elif format == 'xml':
                conts = []
                tree = minidom.parseString(self.conn.response.read())
                for x in tree.getElementsByTagName('container'):
                    cont = {}
                    for key in ['name', 'count', 'bytes']:
                        cont[key] = x.getElementsByTagName(key)[0].\
                            childNodes[0].nodeValue
                    conts.append(cont)
                for cont in conts:
                    cont['name'] = cont['name'].encode('utf-8')
                return conts
            else:
                lines = self.conn.response.read().split('\n')
                if lines and not lines[-1]:
                    lines = lines[:-1]
                return lines
        elif status == 204:
            return []

        raise ResponseError(self.conn.response)

    def delete_containers(self):
        for c in listing_items(self.containers):
            cont = self.container(c)
            if not cont.delete_recursive():
                return False

        return listing_empty(self.containers)

    def info(self, hdrs={}, parms={}, cfg={}):
        if self.conn.make_request('HEAD', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg) != 204:

            raise ResponseError(self.conn.response)

        fields = [['object_count', 'x-account-object-count'],
            ['container_count', 'x-account-container-count'],
            ['bytes_used', 'x-account-bytes-used']]

        return self.header_fields(fields)

    @property
    def path(self):
        return []

class Container(Base):
    def __init__(self, conn, account, name):
        self.conn = conn
        self.account = str(account)
        self.name = str(name)

    def create(self, hdrs={}, parms={}, cfg={}):
        return self.conn.make_request('PUT', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg) in (201, 202)

    def delete(self, hdrs={}, parms={}):
        return self.conn.make_request('DELETE', self.path, hdrs=hdrs,
            parms=parms) == 204

    def delete_files(self):
        for f in listing_items(self.files):
            file = self.file(f)
            if not file.delete():
                return False

        return listing_empty(self.files)

    def delete_recursive(self):
        return self.delete_files() and self.delete()

    def file(self, file_name):
        return File(self.conn, self.account, self.name, file_name)

    def files(self, hdrs={}, parms={}, cfg={}):
        format = parms.get('format', None)
        if format not in [None, 'json', 'xml']:
            raise RequestError('Invalid format: %s' % format)
        if format is None and parms.has_key('format'):
            del parms['format']

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg)
        if status == 200:
            if format == 'json':
                files = json.loads(self.conn.response.read())

                for file in files:
                    file['name'] = file['name'].encode('utf-8')
                    file['content_type'] = file['content_type'].encode('utf-8')
                return files
            elif format == 'xml':
                files = []
                tree = minidom.parseString(self.conn.response.read())
                for x in tree.getElementsByTagName('object'):
                    file = {}
                    for key in ['name', 'hash', 'bytes', 'content_type',
                        'last_modified']:

                        file[key] = x.getElementsByTagName(key)[0].\
                            childNodes[0].nodeValue
                    files.append(file)

                for file in files:
                    file['name'] = file['name'].encode('utf-8')
                    file['content_type'] = file['content_type'].encode('utf-8')
                return files
            else:
                content = self.conn.response.read()
                if content:
                    lines = content.split('\n')
                    if lines and not lines[-1]:
                        lines = lines[:-1]
                    return lines
                else:
                    return []
        elif status == 204:
            return []

        raise ResponseError(self.conn.response)

    def info(self, hdrs={}, parms={}, cfg={}):
        status = self.conn.make_request('HEAD', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg)

        if self.conn.response.status == 204:
            fields = [['bytes_used', 'x-container-bytes-used'],
                ['object_count', 'x-container-object-count']]

            return self.header_fields(fields)

        raise ResponseError(self.conn.response)

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
        self.size = None
        self.metadata = {}

    def make_headers(self, cfg={}):
        headers = {}
        if not cfg.get('no_content_length'):
            if cfg.get('set_content_length'):
                headers['Content-Length'] = cfg.get('set_content_length')
            elif self.size:
                headers['Content-Length'] = self.size
            else:
                headers['Content-Length'] = 0

        if cfg.get('no_content_type'):
            pass
        elif self.content_type:
            headers['Content-Type'] = self.content_type
        else:
            headers['Content-Type'] = 'application/octet-stream'

        for key in self.metadata:
            headers['X-Object-Meta-'+key] = self.metadata[key]

        return headers

    @classmethod
    def compute_md5sum(cls, data):
        block_size = 4096

        if isinstance(data, str):
            data = StringIO.StringIO(data)

        checksum = hashlib.md5()
        buff = data.read(block_size)
        while buff:
            checksum.update(buff)
            buff = data.read(block_size)
        data.seek(0)
        return checksum.hexdigest()

    def copy(self, dest_cont, dest_file, hdrs={}, parms={}, cfg={}):
        if cfg.has_key('destination'):
            headers = {'Destination': cfg['destination']}
        elif cfg.get('no_destination'):
            headers = {}
        else:
            headers = {'Destination': '%s/%s' % (dest_cont, dest_file)}
        headers.update(hdrs)

        if headers.has_key('Destination'):
            headers['Destination'] = urllib.quote(headers['Destination'])

        return self.conn.make_request('COPY', self.path, hdrs=headers,
            parms=parms) == 201

    def delete(self, hdrs={}, parms={}):
        if self.conn.make_request('DELETE', self.path, hdrs=hdrs,
            parms=parms) != 204:

            raise ResponseError(self.conn.response)

        return True

    def info(self, hdrs={}, parms={}, cfg={}):
        if self.conn.make_request('HEAD', self.path, hdrs=hdrs,
            parms=parms, cfg=cfg) != 200:

            raise ResponseError(self.conn.response)

        fields = [['content_length', 'content-length'], ['content_type',
            'content-type'], ['last_modified', 'last-modified'], ['etag',
            'etag']]

        header_fields = self.header_fields(fields)
        header_fields['etag'] = header_fields['etag'].strip('"')
        return header_fields

    def initialize(self, hdrs={}, parms={}):
        if not self.name:
            return False

        status = self.conn.make_request('HEAD', self.path, hdrs=hdrs,
            parms=parms)
        if status == 404:
            return False
        elif (status < 200) or (status > 299):
            raise ResponseError(self.conn.response)

        for hdr in self.conn.response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]
            if hdr[0].lower().startswith('x-object-meta-'):
                self.metadata[hdr[0][14:]] = hdr[1]
            if hdr[0].lower() == 'etag':
                self.etag = hdr[1].strip('"')
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
        fd = open('/dev/urandom', 'r')
        data = fd.read(size)
        fd.close()
        return data

    def read(self, size=-1, offset=0, hdrs=None, buffer=None,
        callback=None, cfg={}):

        if size > 0:
            range = 'bytes=%d-%d' % (offset, (offset + size) - 1)
            if hdrs:
                hdrs['Range'] = range
            else:
                hdrs = {'Range': range}

        status = self.conn.make_request('GET', self.path, hdrs=hdrs,
            cfg=cfg)

        if(status < 200) or (status > 299):
            raise ResponseError(self.conn.response)

        for hdr in self.conn.response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]

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

        if(status < 200) or (status > 299):
            raise ResponseError(self.conn.response)

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

    def sync_metadata(self, metadata={}, cfg={}):
        self.metadata.update(metadata)

        if self.metadata:
            headers = self.make_headers(cfg=cfg)
            if not cfg.get('no_content_length'):
                if cfg.get('set_content_length'):
                    headers['Content-Length'] = \
                        cfg.get('set_content_length')
                else:
                    headers['Content-Length'] = 0

            self.conn.make_request('POST', self.path, hdrs=headers, cfg=cfg)

            if self.conn.response.status not in (201, 202):
                raise ResponseError(self.conn.response)

        return True

    def chunked_write(self, data=None, hdrs={}, parms={}, cfg={}):
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

    def write(self, data='', hdrs={}, parms={}, callback=None, cfg={}):
        block_size = 2**20

        if isinstance(data, file):
            try:
                data.flush()
                data.seek(0)
            except IOError:
                pass
            self.size = int(os.fstat(data.fileno())[6])
        else:
            data = StringIO.StringIO(data)
            self.size = data.len

        headers = self.make_headers(cfg=cfg)
        headers.update(hdrs)

        self.conn.put_start(self.path, hdrs=headers, parms=parms, cfg=cfg)

        transfered = 0
        buff = data.read(block_size)
        try:
            while len(buff) > 0:
                self.conn.put_data(buff)
                buff = data.read(block_size)
                transfered += len(buff)
                if callable(callback):
                    callback(transfered, self.size)

            self.conn.put_end()
        except socket.timeout, err:
            raise err

        if (self.conn.response.status < 200) or \
            (self.conn.response.status > 299):

            raise ResponseError(self.conn.response)

        self.md5 = self.compute_md5sum(data)

        return True

    def write_random(self, size=None, hdrs={}, parms={}, cfg={}):
        data = self.random_data(size)
        if not self.write(data, hdrs=hdrs, parms=parms, cfg=cfg):
            raise ResponseError(self.conn.response)
        self.md5 = self.compute_md5sum(StringIO.StringIO(data))
        return data
