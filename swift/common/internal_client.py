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

from eventlet import sleep, Timeout, spawn
from eventlet.green import socket
from eventlet.green.http import client as http_client
from eventlet.green.urllib import request as urllib_request
import json
import urllib
import struct
from sys import exit
import zlib
from time import gmtime, strftime, time
from zlib import compressobj

from swift.common.exceptions import ClientException
from swift.common.http import (HTTP_NOT_FOUND, HTTP_MULTIPLE_CHOICES,
                               is_client_error, is_server_error)
from swift.common.middleware.gatekeeper import GatekeeperMiddleware
from swift.common.request_helpers import USE_REPLICATION_NETWORK_HEADER
from swift.common.swob import Request, bytes_to_wsgi
from swift.common.utils import quote, close_if_possible, drain_and_close
from swift.common.wsgi import loadapp


class UnexpectedResponse(Exception):
    """
    Exception raised on invalid responses to InternalClient.make_request().

    :param message: Exception message.
    :param resp: The unexpected response.
    """

    def __init__(self, message, resp):
        super(UnexpectedResponse, self).__init__(message)
        self.resp = resp


class CompressingFileReader(object):
    """
    Wrapper for file object to compress object while reading.

    Can be used to wrap file objects passed to InternalClient.upload_object().

    Used in testing of InternalClient.

    :param file_obj: File object to wrap.
    :param compresslevel:  Compression level, defaults to 9.
    :param chunk_size:  Size of chunks read when iterating using object,
                        defaults to 4096.
    """

    def __init__(self, file_obj, compresslevel=9, chunk_size=4096):
        self._f = file_obj
        self.compresslevel = compresslevel
        self.chunk_size = chunk_size
        self.set_initial_state()

    def set_initial_state(self):
        """
        Sets the object to the state needed for the first read.
        """

        self._f.seek(0)
        self._compressor = compressobj(
            self.compresslevel, zlib.DEFLATED, -zlib.MAX_WBITS,
            zlib.DEF_MEM_LEVEL, 0)
        self.done = False
        self.first = True
        self.crc32 = 0
        self.total_size = 0

    def read(self, *a, **kw):
        """
        Reads a chunk from the file object.

        Params are passed directly to the underlying file object's read().

        :returns: Compressed chunk from file object.
        """

        if self.done:
            return b''
        x = self._f.read(*a, **kw)
        if x:
            self.crc32 = zlib.crc32(x, self.crc32) & 0xffffffff
            self.total_size += len(x)
            compressed = self._compressor.compress(x)
            if not compressed:
                compressed = self._compressor.flush(zlib.Z_SYNC_FLUSH)
        else:
            compressed = self._compressor.flush(zlib.Z_FINISH)
            crc32 = struct.pack("<L", self.crc32 & 0xffffffff)
            size = struct.pack("<L", self.total_size & 0xffffffff)
            footer = crc32 + size
            compressed += footer
            self.done = True
        if self.first:
            self.first = False
            header = b'\037\213\010\000\000\000\000\000\002\377'
            compressed = header + compressed
        return compressed

    def __iter__(self):
        return self

    def __next__(self):
        chunk = self.read(self.chunk_size)
        if chunk:
            return chunk
        raise StopIteration

    def seek(self, offset, whence=0):
        if not (offset == 0 and whence == 0):
            raise NotImplementedError('Seek implemented on offset 0 only')
        self.set_initial_state()


class InternalClient(object):
    """
    An internal client that uses a swift proxy app to make requests to Swift.

    This client will exponentially slow down for retries.

    :param conf_path: Full path to proxy config.
    :param user_agent: User agent to be sent to requests to Swift.
    :param request_tries: Number of tries before InternalClient.make_request()
                          gives up.
    :param use_replication_network: Force the client to use the replication
        network over the cluster.
    :param global_conf: a dict of options to update the loaded proxy config.
        Options in ``global_conf`` will override those in ``conf_path`` except
        where the ``conf_path`` option is preceded by ``set``.
    :param app: Optionally provide a WSGI app for the internal client to use.
    """

    def __init__(self, conf_path, user_agent, request_tries,
                 use_replication_network=False, global_conf=None, app=None,
                 **kwargs):
        if request_tries < 1:
            raise ValueError('request_tries must be positive')
        # Internal clients don't use the gatekeeper and the pipeline remains
        # static so we never allow anything to modify the proxy pipeline.
        if kwargs.get('allow_modify_pipeline'):
            raise ValueError("'allow_modify_pipeline' is no longer supported")
        self.app = app or loadapp(conf_path, global_conf=global_conf,
                                  allow_modify_pipeline=False,)
        self.check_gatekeeper_not_loaded(self.app)
        self.user_agent = \
            self.app._pipeline_final_app.backend_user_agent = user_agent
        self.request_tries = request_tries
        self.use_replication_network = use_replication_network
        self.get_object_ring = self.app._pipeline_final_app.get_object_ring
        self.container_ring = self.app._pipeline_final_app.container_ring
        self.account_ring = self.app._pipeline_final_app.account_ring
        self.auto_create_account_prefix = \
            self.app._pipeline_final_app.auto_create_account_prefix

    @staticmethod
    def check_gatekeeper_not_loaded(app):
        # the Gatekeeper middleware would prevent an InternalClient passing
        # X-Backend-* headers to the proxy app, so ensure it's not present
        try:
            for app in app._pipeline:
                if isinstance(app, GatekeeperMiddleware):
                    raise ValueError(
                        "Gatekeeper middleware is not allowed in the "
                        "InternalClient proxy pipeline")
        except AttributeError:
            pass

    def make_request(
            self, method, path, headers, acceptable_statuses, body_file=None,
            params=None):
        """Makes a request to Swift with retries.

        :param method: HTTP method of request.
        :param path: Path of request.
        :param headers: Headers to be sent with request.
        :param acceptable_statuses: List of acceptable statuses for request.
        :param body_file: Body file to be passed along with request,
                          defaults to None.
        :param params: A dict of params to be set in request query string,
                       defaults to None.

        :returns: Response object on success.

        :raises UnexpectedResponse: Exception raised when make_request() fails
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = dict(headers)
        headers['user-agent'] = self.user_agent
        headers.setdefault('x-backend-allow-reserved-names', 'true')
        if self.use_replication_network:
            headers.setdefault(USE_REPLICATION_NETWORK_HEADER, 'true')

        for attempt in range(self.request_tries):
            resp = err = None
            req = Request.blank(
                path, environ={'REQUEST_METHOD': method}, headers=headers)
            if body_file is not None:
                if hasattr(body_file, 'seek'):
                    body_file.seek(0)
                req.body_file = body_file
            if params:
                req.params = params
            try:
                # execute in a separate greenthread to not polute corolocals
                resp = spawn(req.get_response, self.app).wait()
            except (Exception, Timeout) as e:
                err = e
            else:
                if resp.status_int in acceptable_statuses or \
                        resp.status_int // 100 in acceptable_statuses:
                    return resp
                elif not is_server_error(resp.status_int):
                    # No sense retrying when we expect the same result
                    break
            # sleep only between tries, not after each one
            if attempt < self.request_tries - 1:
                if resp:
                    # for non 2XX requests it's safe and useful to drain
                    # the response body so we log the correct status code
                    if resp.status_int // 100 != 2:
                        drain_and_close(resp)
                    else:
                        # Just close; the 499 is appropriate
                        close_if_possible(resp.app_iter)
                sleep(2 ** (attempt + 1))
        if resp:
            msg = 'Unexpected response: %s' % resp.status
            if resp.status_int // 100 != 2 and resp.body:
                # provide additional context (and drain the response body) for
                # non 2XX responses
                msg += ' (%s)' % resp.body
            raise UnexpectedResponse(msg, resp)
        if err:
            raise err

    def handle_request(self, *args, **kwargs):
        resp = self.make_request(*args, **kwargs)
        # Drain the response body to prevent unexpected disconnect
        # in proxy-server
        drain_and_close(resp)

    def _get_metadata(
            self, path, metadata_prefix='', acceptable_statuses=(2,),
            headers=None, params=None):
        """
        Gets metadata by doing a HEAD on a path and using the metadata_prefix
        to get values from the headers returned.

        :param path: Path to do HEAD on.
        :param metadata_prefix: Used to filter values from the headers
                                returned.  Will strip that prefix from the
                                keys in the dict returned.  Defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).
        :param headers: extra headers to send

        :returns: A dict of metadata with metadata_prefix stripped from keys.
                  Keys will be lowercase.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = headers or {}
        resp = self.make_request('HEAD', path, headers, acceptable_statuses,
                                 params=params)
        metadata_prefix = metadata_prefix.lower()
        metadata = {}
        for k, v in resp.headers.items():
            if k.lower().startswith(metadata_prefix):
                metadata[k[len(metadata_prefix):].lower()] = v
        return metadata

    def _iter_items(
            self, path, marker='', end_marker='', prefix='',
            acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Returns an iterator of items from a json listing.  Assumes listing has
        'name' key defined and uses markers.

        :param path: Path to do GET on.
        :param marker: Prefix of first desired item, defaults to ''.
        :param end_marker: Last item returned will be 'less' than this,
                           defaults to ''.
        :param prefix: Prefix of items
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """
        if not isinstance(marker, bytes):
            marker = marker.encode('utf8')
        if not isinstance(end_marker, bytes):
            end_marker = end_marker.encode('utf8')
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf8')

        while True:
            resp = self.make_request(
                'GET', '%s?format=json&marker=%s&end_marker=%s&prefix=%s' %
                (path, bytes_to_wsgi(quote(marker)),
                 bytes_to_wsgi(quote(end_marker)),
                 bytes_to_wsgi(quote(prefix))),
                {}, acceptable_statuses)
            if not resp.status_int == 200:
                if resp.status_int >= HTTP_MULTIPLE_CHOICES:
                    b''.join(resp.app_iter)
                break
            data = json.loads(resp.body)
            if not data:
                break
            for item in data:
                yield item
            marker = data[-1]['name'].encode('utf8')

    def make_path(self, account, container=None, obj=None):
        """
        Returns a swift path for a request quoting and utf-8 encoding the path
        parts as need be.

        :param account: swift account
        :param container: container, defaults to None
        :param obj: object, defaults to None

        :raises ValueError: Is raised if obj is specified and container is
                            not.
        """

        path = '/v1/%s' % quote(account)
        if container:
            path += '/%s' % quote(container)

            if obj:
                path += '/%s' % quote(obj)
        elif obj:
            raise ValueError('Object specified without container')
        return path

    def _set_metadata(
            self, path, metadata, metadata_prefix='',
            acceptable_statuses=(2,)):
        """
        Sets metadata on path using metadata_prefix to set values in headers of
        POST request.

        :param path: Path to do POST on.
        :param metadata: Dict of metadata to set.
        :param metadata_prefix: Prefix used to set metadata values in headers
                                of requests, used to prefix keys in metadata
                                when setting metadata, defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = {}
        for k, v in metadata.items():
            if k.lower().startswith(metadata_prefix):
                headers[k] = v
            else:
                headers['%s%s' % (metadata_prefix, k)] = v
        self.handle_request('POST', path, headers, acceptable_statuses)

    # account methods

    def iter_containers(
            self, account, marker='', end_marker='', prefix='',
            acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Returns an iterator of containers dicts from an account.

        :param account: Account on which to do the container listing.
        :param marker: Prefix of first desired item, defaults to ''.
        :param end_marker: Last item returned will be 'less' than this,
                           defaults to ''.
        :param prefix: Prefix of containers
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account)
        return self._iter_items(path, marker, end_marker, prefix,
                                acceptable_statuses)

    def create_account(self, account):
        """
        Creates an account.

        :param account: Account to create.
        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """
        path = self.make_path(account)
        self.handle_request('PUT', path, {}, (201, 202))

    def delete_account(self, account, acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Deletes an account.

        :param account: Account to delete.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).
        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """
        path = self.make_path(account)
        self.handle_request('DELETE', path, {}, acceptable_statuses)

    def get_account_info(
            self, account, acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Returns (container_count, object_count) for an account.

        :param account: Account on which to get the information.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account)
        resp = self.make_request('HEAD', path, {}, acceptable_statuses)
        if not resp.status_int // 100 == 2:
            return (0, 0)
        return (int(resp.headers.get('x-account-container-count', 0)),
                int(resp.headers.get('x-account-object-count', 0)))

    def get_account_metadata(
            self, account, metadata_prefix='', acceptable_statuses=(2,),
            params=None):
        """Gets account metadata.

        :param account: Account on which to get the metadata.
        :param metadata_prefix: Used to filter values from the headers
                                returned.  Will strip that prefix from the
                                keys in the dict returned.  Defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :returns: Returns dict of account metadata.  Keys will be lowercase.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account)
        return self._get_metadata(path, metadata_prefix, acceptable_statuses,
                                  headers=None, params=params)

    def set_account_metadata(
            self, account, metadata, metadata_prefix='',
            acceptable_statuses=(2,)):
        """
        Sets account metadata.  A call to this will add to the account
        metadata and not overwrite all of it with values in the metadata dict.
        To clear an account metadata value, pass an empty string as
        the value for the key in the metadata dict.

        :param account: Account on which to get the metadata.
        :param metadata: Dict of metadata to set.
        :param metadata_prefix: Prefix used to set metadata values in headers
                                of requests, used to prefix keys in metadata
                                when setting metadata, defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account)
        self._set_metadata(
            path, metadata, metadata_prefix, acceptable_statuses)

    # container methods

    def container_exists(self, account, container):
        """Checks to see if a container exists.

        :param account: The container's account.
        :param container: Container to check.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.

        :returns: True if container exists, false otherwise.
        """

        path = self.make_path(account, container)
        resp = self.make_request('HEAD', path, {}, (2, HTTP_NOT_FOUND))
        return not resp.status_int == HTTP_NOT_FOUND

    def create_container(
            self, account, container, headers=None, acceptable_statuses=(2,)):
        """
        Creates container.

        :param account: The container's account.
        :param container: Container to create.
        :param headers: Defaults to empty dict.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = headers or {}
        path = self.make_path(account, container)
        self.handle_request('PUT', path, headers, acceptable_statuses)

    def delete_container(
            self, account, container, headers=None,
            acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Deletes a container.

        :param account: The container's account.
        :param container: Container to delete.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = headers or {}
        path = self.make_path(account, container)
        self.handle_request('DELETE', path, headers, acceptable_statuses)

    def get_container_metadata(
            self, account, container, metadata_prefix='',
            acceptable_statuses=(2,), params=None):
        """Gets container metadata.

        :param account: The container's account.
        :param container: Container to get metadata on.
        :param metadata_prefix: Used to filter values from the headers
                                returned.  Will strip that prefix from the
                                keys in the dict returned.  Defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :returns: Returns dict of container metadata.  Keys will be lowercase.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container)
        return self._get_metadata(path, metadata_prefix, acceptable_statuses,
                                  params=params)

    def iter_objects(
            self, account, container, marker='', end_marker='', prefix='',
            acceptable_statuses=(2, HTTP_NOT_FOUND)):
        """
        Returns an iterator of object dicts from a container.

        :param account: The container's account.
        :param container: Container to iterate objects on.
        :param marker: Prefix of first desired item, defaults to ''.
        :param end_marker: Last item returned will be 'less' than this,
                           defaults to ''.
        :param prefix: Prefix of objects
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container)
        return self._iter_items(path, marker, end_marker, prefix,
                                acceptable_statuses)

    def set_container_metadata(
            self, account, container, metadata, metadata_prefix='',
            acceptable_statuses=(2,)):
        """
        Sets container metadata.  A call to this will add to the container
        metadata and not overwrite all of it with values in the metadata dict.
        To clear a container metadata value, pass an empty string as the value
        for the key in the metadata dict.

        :param account: The container's account.
        :param container: Container to set metadata on.
        :param metadata: Dict of metadata to set.
        :param metadata_prefix: Prefix used to set metadata values in headers
                                of requests, used to prefix keys in metadata
                                when setting metadata, defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container)
        self._set_metadata(
            path, metadata, metadata_prefix, acceptable_statuses)

    # object methods

    def delete_object(
            self, account, container, obj,
            acceptable_statuses=(2, HTTP_NOT_FOUND),
            headers=None):
        """
        Deletes an object.

        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2, HTTP_NOT_FOUND).
        :param headers: extra headers to send with request

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container, obj)
        self.handle_request('DELETE', path, (headers or {}),
                            acceptable_statuses)

    def get_object_metadata(
            self, account, container, obj, metadata_prefix='',
            acceptable_statuses=(2,), headers=None, params=None):
        """Gets object metadata.

        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object.
        :param metadata_prefix: Used to filter values from the headers
                                returned.  Will strip that prefix from the
                                keys in the dict returned.  Defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).
        :param headers: extra headers to send with request

        :returns: Dict of object metadata.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container, obj)
        return self._get_metadata(path, metadata_prefix, acceptable_statuses,
                                  headers=headers, params=params)

    def get_object(self, account, container, obj, headers=None,
                   acceptable_statuses=(2,), params=None):
        """
        Gets an object.

        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object name.
        :param headers: Headers to send with request, defaults to empty dict.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).
        :param params: A dict of params to be set in request query string,
                       defaults to None.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        :returns: A 3-tuple (status, headers, iterator of object body)
        """

        headers = headers or {}
        path = self.make_path(account, container, obj)
        resp = self.make_request(
            'GET', path, headers, acceptable_statuses, params=params)
        return (resp.status_int, resp.headers, resp.app_iter)

    def iter_object_lines(
            self, account, container, obj, headers=None,
            acceptable_statuses=(2,)):
        """
        Returns an iterator of object lines from an uncompressed or compressed
        text object.

        Uncompress object as it is read if the object's name ends with '.gz'.

        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = headers or {}
        path = self.make_path(account, container, obj)
        resp = self.make_request('GET', path, headers, acceptable_statuses)
        if not resp.status_int // 100 == 2:
            return

        last_part = b''
        compressed = obj.endswith('.gz')
        # magic in the following zlib.decompressobj argument is courtesy of
        # Python decompressing gzip chunk-by-chunk
        # http://stackoverflow.com/questions/2423866
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        for chunk in resp.app_iter:
            if compressed:
                chunk = d.decompress(chunk)
            parts = chunk.split(b'\n')
            if len(parts) == 1:
                last_part = last_part + parts[0]
            else:
                parts[0] = last_part + parts[0]
                for part in parts[:-1]:
                    yield part
                last_part = parts[-1]
        if last_part:
            yield last_part

    def set_object_metadata(
            self, account, container, obj, metadata,
            metadata_prefix='', acceptable_statuses=(2,)):
        """
        Sets an object's metadata.  The object's metadata will be overwritten
        by the values in the metadata dict.

        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object.
        :param metadata: Dict of metadata to set.
        :param metadata_prefix: Prefix used to set metadata values in headers
                                of requests, used to prefix keys in metadata
                                when setting metadata, defaults to ''.
        :param acceptable_statuses: List of status for valid responses,
                                    defaults to (2,).

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        path = self.make_path(account, container, obj)
        self._set_metadata(
            path, metadata, metadata_prefix, acceptable_statuses)

    def upload_object(
            self, fobj, account, container, obj, headers=None,
            acceptable_statuses=(2,), params=None):
        """
        :param fobj: File object to read object's content from.
        :param account: The object's account.
        :param container: The object's container.
        :param obj: The object.
        :param headers: Headers to send with request, defaults to empty dict.
        :param acceptable_statuses: List of acceptable statuses for request.
        :param params: A dict of params to be set in request query string,
                       defaults to None.

        :raises UnexpectedResponse: Exception raised when requests fail
                                    to get a response with an acceptable status
        :raises Exception: Exception is raised when code fails in an
                           unexpected way.
        """

        headers = dict(headers or {})
        if 'Content-Length' not in headers:
            headers['Transfer-Encoding'] = 'chunked'
        path = self.make_path(account, container, obj)
        self.handle_request('PUT', path, headers, acceptable_statuses, fobj,
                            params=params)


def get_auth(url, user, key, auth_version='1.0', **kwargs):
    if auth_version != '1.0':
        exit('ERROR: swiftclient missing, only auth v1.0 supported')
    req = urllib_request.Request(url)
    req.add_header('X-Auth-User', user)
    req.add_header('X-Auth-Key', key)
    conn = urllib_request.urlopen(req)
    headers = conn.info()
    return (
        headers.getheader('X-Storage-Url'),
        headers.getheader('X-Auth-Token'))


class SimpleClient(object):
    """
    Simple client that is used in bin/swift-dispersion-* and container sync
    """
    def __init__(self, url=None, token=None, starting_backoff=1,
                 max_backoff=5, retries=5):
        self.url = url
        self.token = token
        self.attempts = 0  # needed in swif-dispersion-populate
        self.starting_backoff = starting_backoff
        self.max_backoff = max_backoff
        self.retries = retries

    def base_request(self, method, container=None, name=None, prefix=None,
                     headers=None, proxy=None, contents=None,
                     full_listing=None, logger=None, additional_info=None,
                     timeout=None, marker=None):
        # Common request method
        trans_start = time()
        url = self.url

        if full_listing:
            info, body_data = self.base_request(
                method, container, name, prefix, headers, proxy,
                timeout=timeout, marker=marker)
            listing = body_data
            while listing:
                marker = listing[-1]['name']
                info, listing = self.base_request(
                    method, container, name, prefix, headers, proxy,
                    timeout=timeout, marker=marker)
                if listing:
                    body_data.extend(listing)
            return [info, body_data]

        if headers is None:
            headers = {}

        if self.token:
            headers['X-Auth-Token'] = self.token

        if container:
            url = '%s/%s' % (url.rstrip('/'), quote(container))

        if name:
            url = '%s/%s' % (url.rstrip('/'), quote(name))
        else:
            params = ['format=json']
            if prefix:
                params.append('prefix=%s' % prefix)

            if marker:
                params.append('marker=%s' % quote(marker))

            url += '?' + '&'.join(params)

        req = urllib_request.Request(url, headers=headers, data=contents)
        if proxy:
            proxy = urllib.parse.urlparse(proxy)
            req.set_proxy(proxy.netloc, proxy.scheme)
        req.get_method = lambda: method
        conn = urllib_request.urlopen(req, timeout=timeout)
        body = conn.read()
        info = conn.info()
        try:
            body_data = json.loads(body)
        except ValueError:
            body_data = None
        trans_stop = time()
        if logger:
            sent_content_length = 0
            for n, v in headers.items():
                nl = n.lower()
                if nl == 'content-length':
                    try:
                        sent_content_length = int(v)
                        break
                    except ValueError:
                        pass
            logger.debug("-> " + " ".join(
                quote(str(x) if x else "-", ":/")
                for x in (
                    strftime('%Y-%m-%dT%H:%M:%S', gmtime(trans_stop)),
                    method,
                    url,
                    conn.getcode(),
                    sent_content_length,
                    info['content-length'],
                    trans_start,
                    trans_stop,
                    trans_stop - trans_start,
                    additional_info
                )))
        return [info, body_data]

    def retry_request(self, method, **kwargs):
        retries = kwargs.pop('retries', self.retries)
        self.attempts = 0
        backoff = self.starting_backoff
        while self.attempts <= retries:
            self.attempts += 1
            try:
                return self.base_request(method, **kwargs)
            except urllib_request.HTTPError as err:
                if is_client_error(err.getcode() or 500):
                    raise ClientException('Client error',
                                          http_status=err.getcode())
                elif self.attempts > retries:
                    raise ClientException('Raise too many retries',
                                          http_status=err.getcode())
            except (socket.error, http_client.HTTPException,
                    urllib_request.URLError):
                if self.attempts > retries:
                    raise
            sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff)

    def get_account(self, *args, **kwargs):
        # Used in swift-dispersion-populate
        return self.retry_request('GET', **kwargs)

    def put_container(self, container, **kwargs):
        # Used in swift-dispersion-populate
        return self.retry_request('PUT', container=container, **kwargs)

    def get_container(self, container, **kwargs):
        # Used in swift-dispersion-populate
        return self.retry_request('GET', container=container, **kwargs)

    def put_object(self, container, name, contents, **kwargs):
        # Used in swift-dispersion-populate
        return self.retry_request('PUT', container=container, name=name,
                                  contents=contents.read(), **kwargs)


def head_object(url, **kwargs):
    """For usage with container sync """
    client = SimpleClient(url=url)
    return client.retry_request('HEAD', **kwargs)


def put_object(url, **kwargs):
    """For usage with container sync """
    client = SimpleClient(url=url)
    client.retry_request('PUT', **kwargs)


def delete_object(url, **kwargs):
    """For usage with container sync """
    client = SimpleClient(url=url)
    client.retry_request('DELETE', **kwargs)
