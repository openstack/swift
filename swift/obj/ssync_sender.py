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
import os
from swift.common import bufferedhttp
from swift.common import exceptions
from swift.common import http
from swift.common.storage_policy import EC_POLICY
from swift.common.utils import Timestamp


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
        self.available_set = set()
        # When remote_check_objs is given in job, ssync_sender trys only to
        # make sure those objects exist or not in remote.
        self.remote_check_objs = remote_check_objs
        self.send_list = []
        self.failures = 0
        self.delete_list = []

    @property
    def frag_index(self):
        return self.job.get('frag_index')

    def __call__(self):
        """
        Perform ssync with remote node.

        :returns: a 2-tuple, in the form (success, can_delete_objs).

        Success is a boolean, and can_delete_objs is an iterable of strings
        representing the hashes which are in sync with the remote node.
        """
        if not self.suffixes:
            return True, set()
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
                    can_delete_obj = self.available_set
                else:
                    # when we are initialized with remote_check_objs we don't
                    # *send* any requested updates; instead we only collect
                    # what's already in sync and safe for deletion
                    can_delete_obj = self.available_set.difference(
                        self.send_list)
                self.disconnect()
                if not self.failures:
                    return True, can_delete_obj
                else:
                    return False, set()
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
        return False, set()

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
            self.job['policy'], self.suffixes, frag_index=self.frag_index)
        if self.remote_check_objs is not None:
            hash_gen = ifilter(lambda (path, object_hash, timestamp):
                               object_hash in self.remote_check_objs, hash_gen)
        for path, object_hash, timestamp in hash_gen:
            self.available_set.add(object_hash)
            with exceptions.MessageTimeout(
                    self.daemon.node_timeout,
                    'missing_check send line'):
                msg = '%s %s\r\n' % (
                    urllib.quote(object_hash),
                    urllib.quote(timestamp))
                self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))
                # this FI doesn't belong here, we'll send it if the other side
                # wants it or not (another reconstructor may have rebuilt it
                # already) but either way we'll delete it - well unless they
                # asked for it but there was an error
                if self.job['policy'].policy_type == EC_POLICY and \
                        self.job['sync_type'] == 'sync_revert' and \
                        self.job['local_index'] != self.node['index']:
                    self.delete_list.append((object_hash, timestamp))
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
            if line:
                self.send_list.append(line)

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
                    self.job['policy'], frag_index=self.frag_index)
            except exceptions.DiskFileNotExist:
                continue
            url_path = urllib.quote(
                '/%s/%s/%s' % (df.account, df.container, df.obj))
            try:
                df.open()
            except exceptions.DiskFileDeleted as err:
                self.send_delete(url_path, err.timestamp)
            except exceptions.DiskFileError:
                pass
            else:
                extra_headers = {}
                # If this is an EC policy, we need to reconstruct
                # the right fragment archive before sending it over
                # unless we're sync'ing because of an update_delete()
                # job in which case we just send it over "as is"
                if self.job['policy'].policy_type == EC_POLICY:
                    if self.job['sync_type'] == 'sync_revert':
                        extra_headers['X-Object-Sysmeta-Ec-Archive-Index'] = \
                            self.job['frag_index']
                        #extra_headers['X-Timestamp'] = timestamp
                    elif self.job['sync_type'] == 'sync_only':
                        # create a new df-like thing for the rebuilt object
                        df = self.daemon.reconstruct_fa(self.job, self.node,
                                                        self.job['policy'],
                                                        df.get_metadata())
                        if df is None:
                            # TODO, broken pipe maybe from here?
                            with exceptions.MessageTimeout(
                                    self.daemon.node_timeout, 'updates end'):
                                msg = ':UPDATES: END\r\n'
                                self.connection.send('%x\r\n%s\r\n' %
                                                     (len(msg), msg))
                            return
                self.send_put(url_path, df, extra_headers)
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
        # for EC we can potentially revert only some of a parittion
        # so we'll delete reverted objects here and clean up the
        # part dir, if empty, in the reconstructor, note we delete
        # the FI of the file we sent to the target...
        if self.job and self.job['policy'].policy_type == EC_POLICY:
            if self.job['sync_type'] == 'sync_revert':
                for object_hash, timestamp in self.delete_list:
                    try:
                        df = self.df_mgr.get_diskfile_from_hash(
                            self.job['device'], self.job['partition'],
                            object_hash, self.job['policy'],
                            frag_index=self.node['index'])
                        df.open()
                    except exceptions.DiskFileNotExist:
                        continue
                    ts_int = Timestamp(timestamp)
                    filename = \
                        self.df_mgr.make_on_disk_filename(ts_int, '.data',
                                                          self.node['index'])
                    delete_path = os.path.join(df._datadir, filename)
                    os.unlink(delete_path)
                    delete_path = \
                        os.path.join(df._datadir, timestamp + '.durable')
                    if os.path.isfile(delete_path):
                        os.unlink(delete_path)

    def send_delete(self, url_path, timestamp):
        """
        Sends a DELETE subrequest with the given information.
        """
        msg = ['DELETE ' + url_path, 'X-Timestamp: ' + timestamp.internal]
        msg = '\r\n'.join(msg) + '\r\n\r\n'
        with exceptions.MessageTimeout(
                self.daemon.node_timeout, 'send_delete'):
            self.connection.send('%x\r\n%s\r\n' % (len(msg), msg))

    def send_put(self, url_path, df, extra_headers=None):
        """
        Sends a PUT subrequest for the url_path using the source df
        (DiskFile) and content_length.
        """
        extra_headers = extra_headers or {}
        msg = ['PUT ' + url_path, 'Content-Length: ' + str(df.content_length)]

        # Sorted to make it easier to test.
        headers = dict(sorted(df.get_metadata().iteritems()))
        for key in extra_headers:
            headers[key] = extra_headers[key]
        for key in sorted(headers):  # stability for testing
            if key not in ('name', 'Content-Length'):
                msg.append('%s: %s' % (key, headers[key]))

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
