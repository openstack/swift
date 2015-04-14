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

import eventlet
import eventlet.wsgi
import eventlet.greenio

from swift.common import constraints
from swift.common import exceptions
from swift.common import http
from swift.common import swob
from swift.common import utils
from swift.common import request_helpers


class Receiver(object):
    """
    Handles incoming SSYNC requests to the object server.

    These requests come from the object-replicator daemon that uses
    :py:mod:`.ssync_sender`.

    The number of concurrent SSYNC requests is restricted by
    use of a replication_semaphore and can be configured with the
    object-server.conf [object-server] replication_concurrency
    setting.

    An SSYNC request is really just an HTTP conduit for
    sender/receiver replication communication. The overall
    SSYNC request should always succeed, but it will contain
    multiple requests within its request and response bodies. This
    "hack" is done so that replication concurrency can be managed.

    The general process inside an SSYNC request is:

        1. Initialize the request: Basic request validation, mount check,
           acquire semaphore lock, etc..

        2. Missing check: Sender sends the hashes and timestamps of
           the object information it can send, receiver sends back
           the hashes it wants (doesn't have or has an older
           timestamp).

        3. Updates: Sender sends the object information requested.

        4. Close down: Release semaphore lock, etc.
    """

    def __init__(self, app, request):
        self.app = app
        self.request = request
        self.device = None
        self.partition = None
        self.fp = None
        # We default to dropping the connection in case there is any exception
        # raised during processing because otherwise the sender could send for
        # quite some time before realizing it was all in vain.
        self.disconnect = True

    def __call__(self):
        """
        Processes an SSYNC request.

        Acquires a semaphore lock and then proceeds through the steps
        of the SSYNC process.
        """
        # The general theme for functions __call__ calls is that they should
        # raise exceptions.MessageTimeout for client timeouts (logged locally),
        # swob.HTTPException classes for exceptions to return to the caller but
        # not log locally (unmounted, for example), and any other Exceptions
        # will be logged with a full stack trace.
        #       This is because the client is never just some random user but
        # is instead also our code and we definitely want to know if our code
        # is broken or doing something unexpected.
        try:
            # Double try blocks in case our main error handlers fail.
            try:
                # initialize_request is for preamble items that can be done
                # outside a replication semaphore lock.
                for data in self.initialize_request():
                    yield data
                # If semaphore is in use, try to acquire it, non-blocking, and
                # return a 503 if it fails.
                if self.app.replication_semaphore:
                    if not self.app.replication_semaphore.acquire(False):
                        raise swob.HTTPServiceUnavailable()
                try:
                    with self.diskfile_mgr.replication_lock(self.device):
                        for data in self.missing_check():
                            yield data
                        for data in self.updates():
                            yield data
                    # We didn't raise an exception, so end the request
                    # normally.
                    self.disconnect = False
                finally:
                    if self.app.replication_semaphore:
                        self.app.replication_semaphore.release()
            except exceptions.ReplicationLockTimeout as err:
                self.app.logger.debug(
                    '%s/%s/%s SSYNC LOCK TIMEOUT: %s' % (
                        self.request.remote_addr, self.device, self.partition,
                        err))
                yield ':ERROR: %d %r\n' % (0, str(err))
            except exceptions.MessageTimeout as err:
                self.app.logger.error(
                    '%s/%s/%s TIMEOUT in replication.Receiver: %s' % (
                        self.request.remote_addr, self.device, self.partition,
                        err))
                yield ':ERROR: %d %r\n' % (408, str(err))
            except swob.HTTPException as err:
                body = ''.join(err({}, lambda *args: None))
                yield ':ERROR: %d %r\n' % (err.status_int, body)
            except Exception as err:
                self.app.logger.exception(
                    '%s/%s/%s EXCEPTION in replication.Receiver' %
                    (self.request.remote_addr, self.device, self.partition))
                yield ':ERROR: %d %r\n' % (0, str(err))
        except Exception:
            self.app.logger.exception('EXCEPTION in replication.Receiver')
        if self.disconnect:
            # This makes the socket close early so the remote side doesn't have
            # to send its whole request while the lower Eventlet-level just
            # reads it and throws it away. Instead, the connection is dropped
            # and the remote side will get a broken-pipe exception.
            try:
                socket = self.request.environ['wsgi.input'].get_socket()
                eventlet.greenio.shutdown_safe(socket)
                socket.close()
            except Exception:
                pass  # We're okay with the above failing.

    def _ensure_flush(self):
        """
        Sends a blank line sufficient to flush buffers.

        This is to ensure Eventlet versions that don't support
        eventlet.minimum_write_chunk_size will send any previous data
        buffered.

        If https://bitbucket.org/eventlet/eventlet/pull-request/37
        ever gets released in an Eventlet version, we should make
        this yield only for versions older than that.
        """
        yield ' ' * eventlet.wsgi.MINIMUM_CHUNK_SIZE + '\r\n'

    def initialize_request(self):
        """
        Basic validation of request and mount check.

        This function will be called before attempting to acquire a
        replication semaphore lock, so contains only quick checks.
        """
        # The following is the setting we talk about above in _ensure_flush.
        self.request.environ['eventlet.minimum_write_chunk_size'] = 0
        self.device, self.partition, self.policy = \
            request_helpers.get_name_and_placement(self.request, 2, 2, False)
        if 'X-Backend-Ssync-Frag-Index' in self.request.headers:
            self.frag_index = int(
                self.request.headers['X-Backend-Ssync-Frag-Index'])
        else:
            self.frag_index = None
        utils.validate_device_partition(self.device, self.partition)
        self.diskfile_mgr = self.app._diskfile_router[self.policy]
        if self.diskfile_mgr.mount_check and not constraints.check_mount(
                self.diskfile_mgr.devices, self.device):
            raise swob.HTTPInsufficientStorage(drive=self.device)
        self.fp = self.request.environ['wsgi.input']
        for data in self._ensure_flush():
            yield data

    def missing_check(self):
        """
        Handles the receiver-side of the MISSING_CHECK step of a
        SSYNC request.

        Receives a list of hashes and timestamps of object
        information the sender can provide and responds with a list
        of hashes desired, either because they're missing or have an
        older timestamp locally.

        The process is generally:

            1. Sender sends `:MISSING_CHECK: START` and begins
               sending `hash timestamp` lines.

            2. Receiver gets `:MISSING_CHECK: START` and begins
               reading the `hash timestamp` lines, collecting the
               hashes of those it desires.

            3. Sender sends `:MISSING_CHECK: END`.

            4. Receiver gets `:MISSING_CHECK: END`, responds with
               `:MISSING_CHECK: START`, followed by the list of
               hashes it collected as being wanted (one per line),
               `:MISSING_CHECK: END`, and flushes any buffers.

            5. Sender gets `:MISSING_CHECK: START` and reads the list
               of hashes desired by the receiver until reading
               `:MISSING_CHECK: END`.

        The collection and then response is so the sender doesn't
        have to read while it writes to ensure network buffers don't
        fill up and block everything.
        """
        with exceptions.MessageTimeout(
                self.app.client_timeout, 'missing_check start'):
            line = self.fp.readline(self.app.network_chunk_size)
        if line.strip() != ':MISSING_CHECK: START':
            raise Exception(
                'Looking for :MISSING_CHECK: START got %r' % line[:1024])
        object_hashes = []
        while True:
            with exceptions.MessageTimeout(
                    self.app.client_timeout, 'missing_check line'):
                line = self.fp.readline(self.app.network_chunk_size)
            if not line or line.strip() == ':MISSING_CHECK: END':
                break
            parts = line.split()
            object_hash, timestamp = [urllib.unquote(v) for v in parts[:2]]
            want = False
            try:
                df = self.diskfile_mgr.get_diskfile_from_hash(
                    self.device, self.partition, object_hash, self.policy,
                    frag_index=self.frag_index)
            except exceptions.DiskFileNotExist:
                want = True
            else:
                try:
                    df.open()
                except exceptions.DiskFileDeleted as err:
                    want = err.timestamp < timestamp
                except exceptions.DiskFileError as err:
                    want = True
                else:
                    want = df.timestamp < timestamp
            if want:
                object_hashes.append(object_hash)
        yield ':MISSING_CHECK: START\r\n'
        yield '\r\n'.join(object_hashes)
        yield '\r\n'
        yield ':MISSING_CHECK: END\r\n'
        for data in self._ensure_flush():
            yield data

    def updates(self):
        """
        Handles the UPDATES step of an SSYNC request.

        Receives a set of PUT and DELETE subrequests that will be
        routed to the object server itself for processing. These
        contain the information requested by the MISSING_CHECK step.

        The PUT and DELETE subrequests are formatted pretty much
        exactly like regular HTTP requests, excepting the HTTP
        version on the first request line.

        The process is generally:

            1. Sender sends `:UPDATES: START` and begins sending the
               PUT and DELETE subrequests.

            2. Receiver gets `:UPDATES: START` and begins routing the
               subrequests to the object server.

            3. Sender sends `:UPDATES: END`.

            4. Receiver gets `:UPDATES: END` and sends `:UPDATES:
               START` and `:UPDATES: END` (assuming no errors).

            5. Sender gets `:UPDATES: START` and `:UPDATES: END`.

        If too many subrequests fail, as configured by
        replication_failure_threshold and replication_failure_ratio,
        the receiver will hang up the request early so as to not
        waste any more time.

        At step 4, the receiver will send back an error if there were
        any failures (that didn't cause a hangup due to the above
        thresholds) so the sender knows the whole was not entirely a
        success. This is so the sender knows if it can remove an out
        of place partition, for example.
        """
        with exceptions.MessageTimeout(
                self.app.client_timeout, 'updates start'):
            line = self.fp.readline(self.app.network_chunk_size)
        if line.strip() != ':UPDATES: START':
            raise Exception('Looking for :UPDATES: START got %r' % line[:1024])
        successes = 0
        failures = 0
        while True:
            with exceptions.MessageTimeout(
                    self.app.client_timeout, 'updates line'):
                line = self.fp.readline(self.app.network_chunk_size)
            if not line or line.strip() == ':UPDATES: END':
                break
            # Read first line METHOD PATH of subrequest.
            method, path = line.strip().split(' ', 1)
            subreq = swob.Request.blank(
                '/%s/%s%s' % (self.device, self.partition, path),
                environ={'REQUEST_METHOD': method})
            # Read header lines.
            content_length = None
            replication_headers = []
            while True:
                with exceptions.MessageTimeout(self.app.client_timeout):
                    line = self.fp.readline(self.app.network_chunk_size)
                if not line:
                    raise Exception(
                        'Got no headers for %s %s' % (method, path))
                line = line.strip()
                if not line:
                    break
                header, value = line.split(':', 1)
                header = header.strip().lower()
                value = value.strip()
                subreq.headers[header] = value
                replication_headers.append(header)
                if header == 'content-length':
                    content_length = int(value)
            # Establish subrequest body, if needed.
            if method == 'DELETE':
                if content_length not in (None, 0):
                    raise Exception(
                        'DELETE subrequest with content-length %s' % path)
            elif method == 'PUT':
                if content_length is None:
                    raise Exception(
                        'No content-length sent for %s %s' % (method, path))

                def subreq_iter():
                    left = content_length
                    while left > 0:
                        with exceptions.MessageTimeout(
                                self.app.client_timeout,
                                'updates content'):
                            chunk = self.fp.read(
                                min(left, self.app.network_chunk_size))
                        if not chunk:
                            raise Exception(
                                'Early termination for %s %s' % (method, path))
                        left -= len(chunk)
                        yield chunk
                subreq.environ['wsgi.input'] = utils.FileLikeIter(
                    subreq_iter())
            else:
                raise Exception('Invalid subrequest method %s' % method)
            subreq.headers['X-Backend-Storage-Policy-Index'] = int(self.policy)
            subreq.headers['X-Backend-Replication'] = 'True'
            if replication_headers:
                subreq.headers['X-Backend-Replication-Headers'] = \
                    ' '.join(replication_headers)
            # Route subrequest and translate response.
            resp = subreq.get_response(self.app)
            if http.is_success(resp.status_int) or \
                    resp.status_int == http.HTTP_NOT_FOUND:
                successes += 1
            else:
                failures += 1
            if failures >= self.app.replication_failure_threshold and (
                    not successes or
                    float(failures) / successes >
                    self.app.replication_failure_ratio):
                raise Exception(
                    'Too many %d failures to %d successes' %
                    (failures, successes))
            # The subreq may have failed, but we want to read the rest of the
            # body from the remote side so we can continue on with the next
            # subreq.
            for junk in subreq.environ['wsgi.input']:
                pass
        if failures:
            raise swob.HTTPInternalServerError(
                'ERROR: With :UPDATES: %d failures to %d successes' %
                (failures, successes))
        yield ':UPDATES: START\r\n'
        yield ':UPDATES: END\r\n'
        for data in self._ensure_flush():
            yield data
