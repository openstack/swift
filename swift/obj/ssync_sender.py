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

import urllib
from itertools import ifilter
from swift.common import bufferedhttp
from swift.common import exceptions
from swift.common import http


class Sender(object):
    """
    Sends SSYNC requests to the object server.

    These requests are eventually handled by
    :py:mod:`.ssync_receiver` and full documentation about the
    process is there.
    """

    def __init__(self, daemon, node, job, suffixes, remote_check_objs=None):
        self.daemon = daemon
        self.df_mgr = self.daemon._diskfile_mgr
        self.node = node
        self.job = job
        self.suffixes = suffixes
        self.connection = None
        self.response = None
        self.response_buffer = ''
        self.response_chunk_left = 0
        # available_map has an entry for each object in given suffixes that
        # is available to be sync'd; each entry is a hash => timestamp
        self.available_map = {}
        # When remote_check_objs is given in job, ssync_sender trys only to
        # make sure those objects exist or not in remote.
        self.remote_check_objs = remote_check_objs
        # send_list has an entry for each object that the receiver wants to
        # be sync'ed; each entry is an object hash
        self.send_list = []
        self.failures = 0

    def __call__(self):
        """
        Perform ssync with remote node.

        :returns: a 2-tuple, in the form (success, can_delete_objs) where
                  success is a boolean and can_delete_objs is the map of
                  objects that are in sync with the receiver. Each entry in
                  can_delete_objs maps a hash => timestamp
        """
        if not self.suffixes:
            return True, {}
        try:
            # Double try blocks in case our main error handler fails.
            try:
                # The general theme for these functions is that they should
                # raise exceptions.MessageTimeout for client timeouts and
                # exceptions.ReplicationException for common issues that will
                # abort the replication attempt and log a simple error. All
                # other exceptions will be logged with a full stack trace.
                self.connect()
                self.missing_check()
                if self.remote_check_objs is None:
                    self.updates()
                    can_delete_obj = self.available_map
                else:
                    # when we are initialized with remote_check_objs we don't
                    # *send* any requested updates; instead we only collect
                    # what's already in sync and safe for deletion
                    in_sync_hashes = (set(self.available_map.keys()) -
                                      set(self.send_list))
                    can_delete_obj = dict((hash_, self.available_map[hash_])
                                          for hash_ in in_sync_hashes)
                self.disconnect()
                if not self.failures:
                    return True, can_delete_obj
                else:
                    return False, {}
            except (exceptions.MessageTimeout,
                    exceptions.ReplicationException) as err:
                self.daemon.logger.error(
                    '%s:%s/%s/%s %s', self.node.get('replication_ip'),
                    self.node.get('replication_port'), self.node.get('device'),
                    self.job.get('partition'), err)
            except Exception:
                # We don't want any exceptions to escape our code and possibly
                # mess up the original replicator code that called us since it
                # was originally written to shell out to rsync which would do
                # no such thing.
                self.daemon.logger.exception(
                    '%s:%s/%s/%s EXCEPTION in replication.Sender',
                    self.node.get('replication_ip'),
                    self.node.get('replication_port'),
                    self.node.get('device'), self.job.get('partition'))
        except Exception:
            # We don't want any exceptions to escape our code and possibly
            # mess up the original replicator code that called us since it
            # was originally written to shell out to rsync which would do
            # no such thing.
            # This particular exception handler does the minimal amount as it
            # would only get called if the above except Exception handler
            # failed (bad node or job data).
            self.daemon.logger.exception('EXCEPTION in replication.Sender')
        return False, {}

    def connect(self):
        """
        Establishes a connection and starts an SSYNC request
        with the object server.
        """
        with exceptions.MessageTimeout(
                self.daemon.conn_timeout, 'connect send'):
            self.connection = bufferedhttp.BufferedHTTPConnection(
                '%s:%s' % (self.node['replication_ip'],
                           self.node['replication_port']))
            self.connection.putrequest('SSYNC', '/%s/%s' % (
                self.node['device'], self.job['partition']))
            self.connection.putheader('Transfer-Encoding', 'chunked')
            self.connection.putheader('X-Backend-Storage-Policy-Index',
                                      int(self.job['policy']))
            self.connection.putheader('X-Backend-Ssync-Frag-Index',
                                      self.node['index'])
            self.connection.endheaders()
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'connect receive'):
            self.response = self.connection.getresponse()
            if self.response.status != http.HTTP_OK:
                raise exceptions.ReplicationException(
                    'Expected status %s; got %s' %
                    (http.HTTP_OK, self.response.status))

    def readline(self):
        """
        Reads a line from the SSYNC response body.

        httplib has no readline and will block on read(x) until x is
        read, so we have to do the work ourselves. A bit of this is
        taken from Python's httplib itself.
        """
        data = self.response_buffer
        self.response_buffer = ''
        while '\n' not in data and len(data) < self.daemon.network_chunk_size:
            if self.response_chunk_left == -1:  # EOF-already indicator
                break
            if self.response_chunk_left == 0:
                line = self.response.fp.readline()
                i = line.find(';')
                if i >= 0:
                    line = line[:i]  # strip chunk-extensions
                try:
                    self.response_chunk_left = int(line.strip(), 16)
                except ValueError:
                    # close the connection as protocol synchronisation is
                    # probably lost
                    self.response.close()
                    raise exceptions.ReplicationException('Early disconnect')
                if self.response_chunk_left == 0:
                    self.response_chunk_left = -1
                    break
            chunk = self.response.fp.read(min(
                self.response_chunk_left,
                self.daemon.network_chunk_size - len(data)))
            if not chunk:
                # close the connection as protocol synchronisation is
                # probably lost
                self.response.close()
                raise exceptions.ReplicationException('Early disconnect')
            self.response_chunk_left -= len(chunk)
            if self.response_chunk_left == 0:
                self.response.fp.read(2)  # discard the trailing \r\n
            data += chunk
        if '\n' in data:
            data, self.response_buffer = data.split('\n', 1)
            data += '\n'
        return data

    def missing_check(self):
        """
        Handles the sender-side of the MISSING_CHECK step of a
        SSYNC request.

        Full documentation of this can be found at
        :py:meth:`.Receiver.missing_check`.
        """
        # First, send our list.
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'missing_check start'):
            msg = ':MISSING_CHECK: START\r\n'
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        hash_gen = self.df_mgr.yield_hashes(
            self.job['device'], self.job['partition'],
            self.job['policy'], self.suffixes,
            frag_index=self.job.get('frag_index'))
        if self.remote_check_objs is not None:
            hash_gen = ifilter(lambda (path, object_hash, timestamp):
                               object_hash in self.remote_check_objs, hash_gen)
        for path, object_hash, timestamp in hash_gen:
            self.available_map[object_hash] = timestamp
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout,
                    'missing_check send line'):
                msg = '%s %s\r\n' % (
                    urllib.quote(object_hash),
                    urllib.quote(timestamp))
                self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'missing_check end'):
            msg = ':MISSING_CHECK: END\r\n'
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        # Now, retrieve the list of what they want.
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'missing_check start wait'):
                line = self.readline()
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == ':MISSING_CHECK: START':
                break
            elif line:
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % line[:1024])
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'missing_check line wait'):
                line = self.readline()
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == ':MISSING_CHECK: END':
                break
            parts = line.split()
            if parts:
                self.send_list.append(parts[0])

    def updates(self):
        """
        Handles the sender-side of the UPDATES step of an SSYNC
        request.

        Full documentation of this can be found at
        :py:meth:`.Receiver.updates`.
        """
        # First, send all our subrequests based on the send_list.
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'updates start'):
            msg = ':UPDATES: START\r\n'
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        for object_hash in self.send_list:
            try:
                df = self.df_mgr.get_diskfile_from_hash(
                    self.job['device'], self.job['partition'], object_hash,
                    self.job['policy'], frag_index=self.job.get('frag_index'))
            except exceptions.DiskFileNotExist:
                continue
            url_path = urllib.quote(
                '/%s/%s/%s' % (df.account, df.container, df.obj))
            try:
                df.open()
                # EC reconstructor may have passed a callback to build
                # an alternative diskfile...
                df = self.job.get('sync_diskfile_builder', lambda *args: df)(
                    self.job, self.node, df.get_metadata())
            except exceptions.DiskFileDeleted as err:
                self.send_delete(url_path, err.timestamp)
            except exceptions.DiskFileError:
                pass
            else:
                self.send_put(url_path, df)
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'updates end'):
            msg = ':UPDATES: END\r\n'
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        # Now, read their response for any issues.
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'updates start wait'):
                line = self.readline()
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == ':UPDATES: START':
                break
            elif line:
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % line[:1024])
        while True:
            with exceptions.MessageTimeout(
                    self.daemon.http_timeout, 'updates line wait'):
                line = self.readline()
            if not line:
                raise exceptions.ReplicationException('Early disconnect')
            line = line.strip()
            if line == ':UPDATES: END':
                break
            elif line:
                raise exceptions.ReplicationException(
                    'Unexpected response: %r' % line[:1024])

    def send_delete(self, url_path, timestamp):
        """
        Sends a DELETE subrequest with the given information.
        """
        msg = ['DELETE ' + url_path, 'X-Timestamp: ' + timestamp.internal]
        msg = '\r\n'.join(msg) + '\r\n\r\n'
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'send_delete'):
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))

    def send_put(self, url_path, df):
        """
        Sends a PUT subrequest for the url_path using the source df
        (DiskFile) and content_length.
        """
        msg = ['PUT ' + url_path, 'Content-Length: ' + str(df.content_length)]
        # Sorted to make it easier to test.
        for key, value in sorted(df.get_metadata().iteritems()):
            if key not in ('name', 'Content-Length'):
                msg.append('%s: %s' % (key, value))
        msg = '\r\n'.join(msg) + '\r\n\r\n'
        with exceptions.MessageTimeout(self.daemon.node_timeout, 'send_put'):
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
        for chunk in df.reader():
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout, 'send_put chunk'):
                self.connection.send('%x\r\n%s\r\n' % (len(chunk), chunk))

    def disconnect(self):
        """
        Closes down the connection to the object server once done
        with the SSYNC request.
        """
        try:
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout, 'disconnect'):
                self.connection.send('0\r\n\r\n')
        except (Exception, exceptions.Timeout):
            pass  # We're okay with the above failing.
        self.connection.close()
