# Copyright (c) 2013 OpenStack Foundation
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

from eventlet import sleep
import urllib

from swift.common import bufferedhttp
from swift.common import exceptions
from swift.common import http
from swift.common import utils
from swift.common.swob import wsgi_to_bytes


def encode_missing(object_hash, ts_data, ts_meta=None, ts_ctype=None,
                   **kwargs):
    """
    Returns a string representing the object hash, its data file timestamp,
    the delta forwards to its metafile and content-type timestamps, if
    non-zero, and its durability, in the form:
    ``<hash> <ts_data> [m:<hex delta to ts_meta>[,t:<hex delta to ts_ctype>]
    [,durable:False]``

    The decoder for this line is
    :py:func:`~swift.obj.ssync_receiver.decode_missing`
    """
    msg = ('%s %s'
           % (urllib.parse.quote(object_hash),
              urllib.parse.quote(ts_data.internal)))
    extra_parts = []
    if ts_meta and ts_meta != ts_data:
        delta = ts_meta.raw - ts_data.raw
        extra_parts.append('m:%x' % delta)
        if ts_meta.offset:
            extra_parts[-1] += '__%x' % ts_meta.offset
        if ts_ctype and ts_ctype != ts_data:
            delta = ts_ctype.raw - ts_data.raw
            extra_parts.append('t:%x' % delta)
            if ts_ctype.offset:
                extra_parts[-1] += '__%x' % ts_ctype.offset
    if 'durable' in kwargs and kwargs['durable'] is False:
        # only send durable in the less common case that it is False
        extra_parts.append('durable:%s' % kwargs['durable'])
    if extra_parts:
        msg = '%s %s' % (msg, ','.join(extra_parts))
    return msg.encode('ascii')


def decode_wanted(parts):
    """
    Parse missing_check line parts to determine which parts of local
    diskfile were wanted by the receiver.

    The encoder for parts is
    :py:func:`~swift.obj.ssync_receiver.encode_wanted`
    """
    wanted = {}
    key_map = {'d': 'data', 'm': 'meta'}
    if parts:
        # receiver specified data and/or meta wanted, so use those as
        # conditions for sending PUT and/or POST subrequests
        for k in key_map:
            if k in parts[0]:
                wanted[key_map[k]] = True
    if not wanted:
        # assume legacy receiver which will only accept PUTs. There is no
        # way to send any meta file content without morphing the timestamp
        # of either the data or the metadata, so we just send data file
        # content to a legacy receiver. Once the receiver gets updated we
        # will be able to send it the meta file content.
        wanted['data'] = True
    return wanted


class SsyncBufferedHTTPResponse(bufferedhttp.BufferedHTTPResponse, object):
    def __init__(self, *args, **kwargs):
        super(SsyncBufferedHTTPResponse, self).__init__(*args, **kwargs)
        self.ssync_response_buffer = b''
        self.ssync_response_chunk_left = 0

    def readline(self, size=1024):
        """
        Reads a line from the SSYNC response body.

        httplib has no readline and will block on read(x) until x is
        read, so we have to do the work ourselves. A bit of this is
        taken from Python's httplib itself.
        """
        data = self.ssync_response_buffer
        self.ssync_response_buffer = b''
        while b'\n' not in data and len(data) < size:
            if self.ssync_response_chunk_left == -1:  # EOF-already indicator
                break
            if self.ssync_response_chunk_left == 0:
                line = self.fp.readline()
                i = line.find(b';')
                if i >= 0:
                    line = line[:i]  # strip chunk-extensions
                try:
                    self.ssync_response_chunk_left = int(line.strip(), 16)
                except ValueError:
                    # close the connection as protocol synchronisation is
                    # probably lost
                    self.close()
                    raise exceptions.ReplicationException('Early disconnect')
                if self.ssync_response_chunk_left == 0:
                    self.ssync_response_chunk_left = -1
                    break
            chunk = self.fp.read(min(self.ssync_response_chunk_left,
                                     size - len(data)))
            if not chunk:
                # close the connection as protocol synchronisation is
                # probably lost
                self.close()
                raise exceptions.ReplicationException('Early disconnect')
            self.ssync_response_chunk_left -= len(chunk)
            if self.ssync_response_chunk_left == 0:
                self.fp.read(2)  # discard the trailing \r\n
            data += chunk
        if b'\n' in data:
            data, self.ssync_response_buffer = data.split(b'\n', 1)
            data += b'\n'
        return data


class SsyncBufferedHTTPConnection(bufferedhttp.BufferedHTTPConnection):
    response_class = SsyncBufferedHTTPResponse


class Sender(object):
    """
    Sends SSYNC requests to the object server.

    These requests are eventually handled by
    :py:mod:`.ssync_receiver` and full documentation about the
    process is there.
    """

    def __init__(self, daemon, node, job, suffixes, remote_check_objs=None,
                 include_non_durable=False, max_objects=0):
        self.daemon = daemon
        self.df_mgr = self.daemon._df_router[job['policy']]
        self.node = node
        self.job = job
        self.suffixes = suffixes
        # When remote_check_objs is given in job, ssync_sender trys only to
        # make sure those objects exist or not in remote.
        self.remote_check_objs = remote_check_objs
        self.include_non_durable = include_non_durable
        self.max_objects = max_objects
        self.limited_by_max_objects = False

    def __call__(self):
        """
        Perform ssync with remote node.

        :returns: a 2-tuple, in the form (success, can_delete_objs) where
                  success is a boolean and can_delete_objs is the map of
                  objects that are in sync with the receiver. Each entry in
                  can_delete_objs maps a hash => timestamp of data file or
                  tombstone file
        """
        if not self.suffixes:
            return True, {}
        connection = response = None
        try:
            # Double try blocks in case our main error handler fails.
            try:
                # The general theme for these functions is that they should
                # raise exceptions.MessageTimeout for client timeouts and
                # exceptions.ReplicationException for common issues that will
                # abort the replication attempt and log a simple error. All
                # other exceptions will be logged with a full stack trace.
                connection, response = self.connect()
                # available_map has an entry for each object in given suffixes
                # that is available to be sync'd;
                # each entry is a hash => dict of timestamps of data file or
                # tombstone file and/or meta file
                # send_map has an entry for each object that the receiver wants
                # to be sync'ed;
                # each entry maps an object hash => dict of wanted parts
                available_map, send_map = self.missing_check(connection,
                                                             response)
                if self.remote_check_objs is None:
                    self.updates(connection, response, send_map)
                    can_delete_obj = available_map
                else:
                    # when we are initialized with remote_check_objs we don't
                    # *send* any requested updates; instead we only collect
                    # what's already in sync and safe for deletion
                    in_sync_hashes = (set(available_map.keys()) -
                                      set(send_map.keys()))
                    can_delete_obj = dict((hash_, available_map[hash_])
                                          for hash_ in in_sync_hashes)
                self.daemon.logger.debug(
                    'ssync completed ok: dev: %s, part: %s, policy: %d, '
                    'num suffixes: %s, available: %d, sent: %d, deletable: %d',
                    self.job['device'], self.job['partition'],
                    self.job['policy'].idx, len(self.suffixes),
                    len(available_map), len(send_map), len(can_delete_obj))
                return True, can_delete_obj
            except (exceptions.MessageTimeout,
                    exceptions.ReplicationException) as err:
                node_str = utils.node_to_string(self.node, replication=True)
                self.daemon.logger.error('%s/%s %s', node_str,
                                         self.job['partition'], err)
            except Exception:
                # We don't want any exceptions to escape our code and possibly
                # mess up the original replicator code that called us since it
                # was originally written to shell out to rsync which would do
                # no such thing.
                node_str = utils.node_to_string(self.node, replication=True)
                self.daemon.logger.exception(
                    '%s/%s EXCEPTION in ssync.Sender',
                    node_str, self.job['partition'])
            finally:
                self.disconnect(connection)
        except Exception:
            # We don't want any exceptions to escape our code and possibly
            # mess up the original replicator code that called us since it
            # was originally written to shell out to rsync which would do
            # no such thing.
            # This particular exception handler does the minimal amount as it
            # would only get called if the above except Exception handler
            # failed (bad node or job data).
            self.daemon.logger.exception('EXCEPTION in ssync.Sender')
        return False, {}

    def connect(self):
        """
        Establishes a connection and starts an SSYNC request
        with the object server.
        """
        connection = response = None
        node_addr = '%s:%s' % (self.node['replication_ip'],
                               self.node['replication_port'])
        with exceptions.MessageTimeout(
                self.daemon.conn_timeout, 'connect send'):
            connection = SsyncBufferedHTTPConnection(node_addr)
            connection.putrequest('SSYNC', '/%s/%s' % (
                self.node['device'], self.job['partition']))
            connection.putheader('Transfer-Encoding', 'chunked')
            connection.putheader('X-Backend-Storage-Policy-Index',
                                 int(self.job['policy']))
            # a sync job must use the node's backend_index for the frag_index
            # of the rebuilt fragments instead of the frag_index from the job
            # which will be rebuilding them
            frag_index = self.node.get('backend_index')
            if frag_index is not None:
                connection.putheader('X-Backend-Ssync-Frag-Index', frag_index)
                # Node-Index header is for backwards compat 2.4.0-2.20.0
                connection.putheader('X-Backend-Ssync-Node-Index', frag_index)
            connection.endheaders()
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'connect receive'):
            response = connection.getresponse()
            if response.status != http.HTTP_OK:
                err_msg = utils.cap_length(response.read(), 1024)
                raise exceptions.ReplicationException(
                    'Expected status %s; got %s (%s)' %
                    (http.HTTP_OK, response.status, err_msg))
            if self.include_non_durable and not utils.config_true_value(
                    response.getheader('x-backend-accept-no-commit', False)):
                # fall back to legacy behaviour if receiver does not understand
                # X-Backend-Commit
                self.daemon.logger.warning(
                    'ssync receiver %s does not accept non-durable fragments' %
                    node_addr)
                self.include_non_durable = False
        return connection, response

    def missing_check(self, connection, response):
        """
        Handles the sender-side of the MISSING_CHECK step of a
        SSYNC request.

        Full documentation of this can be found at
        :py:meth:`.Receiver.missing_check`.
        """
        self.limited_by_max_objects = False
        available_map = {}
        send_map = {}
        # First, send our list.
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'missing_check start'):
            msg = b':MISSING_CHECK: START\r\n'
            connection.send(b'%x\r\n%s\r\n' % (len(msg), msg))
        # an empty frag_prefs list is sufficient to get non-durable frags
        # yielded, in which case an older durable frag will not be yielded
        frag_prefs = [] if self.include_non_durable else None
        hash_gen = self.df_mgr.yield_hashes(
            self.job['device'], self.job['partition'],
            self.job['policy'], self.suffixes,
            frag_index=self.job.get('frag_index'),
            frag_prefs=frag_prefs)
        if self.remote_check_objs is not None:
            hash_gen = filter(
                lambda objhash_timestamps:
                objhash_timestamps[0] in
                self.remote_check_objs, hash_gen)
        nlines = 0
        nbytes = 0
        object_hash = None
        for object_hash, timestamps in hash_gen:
            available_map[object_hash] = timestamps
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout,
                    'missing_check send line: %d lines (%d bytes) sent'
                    % (nlines, nbytes)):
                msg = b'%s\r\n' % encode_missing(object_hash, **timestamps)
                msg = b'%x\r\n%s\r\n' % (len(msg), msg)
                connection.send(msg)
            if nlines % 5 == 0:
                sleep()  # Gives a chance for other greenthreads to run
            nlines += 1
            nbytes += len(msg)
            if 0 < self.max_objects <= nlines:
                break
        for _ in hash_gen:
            # only log truncation if there were more hashes to come...
            self.limited_by_max_objects = True
            self.daemon.logger.info(
                'ssync missing_check truncated after %d objects: '
                'device: %s, part: %s, policy: %s, last object hash: '
                '%s', nlines, self.job['device'],
                self.job['partition'], int(self.job['policy']),
                object_hash)
            break
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'missing_check end'):
            msg = b':MISSING_CHECK: END\r\n'
            connection.send(b'%x\r\n%s\r\n' % (len(msg), msg))
        # Now, retrieve the list of what they want.
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'missing_check start wait'):
                line = response.readline(size=self.daemon.network_chunk_size)
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == b':MISSING_CHECK: START':
                break
            elif line:
                try:
                    line = line.decode('ascii')
                except UnicodeDecodeError:
                    pass
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % utils.cap_length(line, 1024))
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'missing_check line wait'):
                line = response.readline(size=self.daemon.network_chunk_size)
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == b':MISSING_CHECK: END':
                break
            parts = line.decode('ascii').split()
            if parts:
                send_map[parts[0]] = decode_wanted(parts[1:])
        return available_map, send_map

    def updates(self, connection, response, send_map):
        """
        Handles the sender-side of the UPDATES step of an SSYNC
        request.

        Full documentation of this can be found at
        :py:meth:`.Receiver.updates`.
        """
        # First, send all our subrequests based on the send_map.
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'updates start'):
            msg = b':UPDATES: START\r\n'
            connection.send(b'%x\r\n%s\r\n' % (len(msg), msg))
        frag_prefs = [] if self.include_non_durable else None
        updates = 0
        for object_hash, want in send_map.items():
            object_hash = urllib.parse.unquote(object_hash)
            try:
                df = self.df_mgr.get_diskfile_from_hash(
                    self.job['device'], self.job['partition'], object_hash,
                    self.job['policy'], frag_index=self.job.get('frag_index'),
                    open_expired=True, frag_prefs=frag_prefs)
            except exceptions.DiskFileNotExist:
                continue
            url_path = urllib.parse.quote(
                '/%s/%s/%s' % (df.account, df.container, df.obj))
            try:
                df.open()
                if want.get('data'):
                    is_durable = (df.durable_timestamp == df.data_timestamp)
                    # EC reconstructor may have passed a callback to build an
                    # alternative diskfile - construct it using the metadata
                    # from the data file only.
                    df_alt = self.job.get(
                        'sync_diskfile_builder', lambda *args: df)(
                            self.job, self.node, df)
                    self.send_put(connection, url_path, df_alt,
                                  durable=is_durable)
                if want.get('meta') and df.data_timestamp != df.timestamp:
                    self.send_post(connection, url_path, df)
            except exceptions.DiskFileDeleted as err:
                if want.get('data'):
                    self.send_delete(connection, url_path, err.timestamp)
            except exceptions.DiskFileError:
                # DiskFileErrors are expected while opening the diskfile,
                # before any data is read and sent. Since there is no partial
                # state on the receiver it's ok to ignore this diskfile and
                # continue. The diskfile may however be deleted after a
                # successful ssync since it remains in the send_map.
                pass
            if updates % 5 == 0:
                sleep()  # Gives a chance for other greenthreads to run
            updates += 1
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'updates end'):
            msg = b':UPDATES: END\r\n'
            connection.send(b'%x\r\n%s\r\n' % (len(msg), msg))
        # Now, read their response for any issues.
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'updates start wait'):
                line = response.readline(size=self.daemon.network_chunk_size)
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == b':UPDATES: START':
                break
            elif line:
                try:
                    line = line.decode('ascii')
                except UnicodeDecodeError:
                    pass
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % utils.cap_length(line, 1024))
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'updates line wait'):
                line = response.readline(size=self.daemon.network_chunk_size)
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == b':UPDATES: END':
                break
            elif line:
                try:
                    line = line.decode('ascii')
                except UnicodeDecodeError:
                    pass
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % utils.cap_length(line, 1024))

    def send_subrequest(self, connection, method, url_path, headers, df):
        msg = [b'%s %s' % (method.encode('ascii'), url_path.encode('utf8'))]
        for key, value in sorted(headers.items()):
            msg.append(wsgi_to_bytes('%s: %s' % (key, value)))
        msg = b'\r\n'.join(msg) + b'\r\n\r\n'
        with exceptions.MessageTimeout(self.daemon.node_timeout,
                                       'send_%s' % method.lower()):
            connection.send(b'%x\r\n%s\r\n' % (len(msg), msg))

        if df:
            bytes_read = 0
            for chunk in df.reader():
                bytes_read += len(chunk)
                with exceptions.MessageTimeout(self.daemon.node_timeout,
                                               'send_%s chunk' %
                                               method.lower()):
                    connection.send(b'%x\r\n%s\r\n' % (len(chunk), chunk))
            if bytes_read != df.content_length:
                # Since we may now have partial state on the receiver we have
                # to prevent the receiver finalising what may well be a bad or
                # partially written diskfile. Unfortunately we have no other
                # option than to pull the plug on this ssync session. If ssync
                # supported multiphase PUTs like the proxy uses for EC we could
                # send a bad etag in a footer of this subrequest, but that is
                # not supported.
                raise exceptions.ReplicationException(
                    'Sent data length does not match content-length')

    def send_delete(self, connection, url_path, timestamp):
        """
        Sends a DELETE subrequest with the given information.
        """
        headers = {'X-Timestamp': timestamp.internal}
        self.send_subrequest(connection, 'DELETE', url_path, headers, None)

    def send_put(self, connection, url_path, df, durable=True):
        """
        Sends a PUT subrequest for the url_path using the source df
        (DiskFile) and content_length.
        """
        headers = {'Content-Length': str(df.content_length)}
        if not durable:
            # only send this header for the less common case; without this
            # header object servers assume default commit behaviour
            headers['X-Backend-No-Commit'] = 'True'
        for key, value in df.get_datafile_metadata().items():
            if key not in ('name', 'Content-Length'):
                headers[key] = value
        self.send_subrequest(connection, 'PUT', url_path, headers, df)

    def send_post(self, connection, url_path, df):
        metadata = df.get_metafile_metadata()
        if metadata is None:
            return
        self.send_subrequest(connection, 'POST', url_path, metadata, None)

    def disconnect(self, connection):
        """
        Closes down the connection to the object server once done
        with the SSYNC request.
        """
        if not connection:
            return
        try:
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout, 'disconnect'):
                connection.send(b'0\r\n\r\n')
        except (Exception, exceptions.Timeout):
            pass  # We're okay with the above failing.
        connection.close()
