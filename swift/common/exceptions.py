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

from eventlet import Timeout
import swift.common.utils


class MessageTimeout(Timeout):

    def __init__(self, seconds=None, msg=None):
        Timeout.__init__(self, seconds=seconds)
        self.msg = msg

    def __str__(self):
        return '%s: %s' % (Timeout.__str__(self), self.msg)


class SwiftException(Exception):
    pass


class PutterConnectError(Exception):

    def __init__(self, status=None):
        self.status = status


class InvalidTimestamp(SwiftException):
    pass


class InsufficientStorage(SwiftException):
    pass


class FooterNotSupported(SwiftException):
    pass


class MultiphasePUTNotSupported(SwiftException):
    pass


class SuffixSyncError(SwiftException):
    pass


class RangeAlreadyComplete(SwiftException):
    pass


class DiskFileError(SwiftException):
    pass


class DiskFileNotOpen(DiskFileError):
    pass


class DiskFileQuarantined(DiskFileError):
    pass


class DiskFileCollision(DiskFileError):
    pass


class DiskFileNotExist(DiskFileError):
    pass


class DiskFileDeleted(DiskFileNotExist):

    def __init__(self, metadata=None):
        self.metadata = metadata or {}
        self.timestamp = swift.common.utils.Timestamp(
            self.metadata.get('X-Timestamp', 0))


class DiskFileExpired(DiskFileDeleted):
    pass


class DiskFileNoSpace(DiskFileError):
    pass


class DiskFileDeviceUnavailable(DiskFileError):
    pass


class DiskFileXattrNotSupported(DiskFileError):
    pass


class DiskFileBadMetadataChecksum(DiskFileError):
    pass


class DeviceUnavailable(SwiftException):
    pass


class DatabaseAuditorException(SwiftException):
    pass


class InvalidAccountInfo(DatabaseAuditorException):
    pass


class PathNotDir(OSError):
    pass


class ChunkReadError(SwiftException):
    pass


class ShortReadError(SwiftException):
    pass


class ChunkReadTimeout(Timeout):
    pass


class ChunkWriteTimeout(Timeout):
    pass


class ConnectionTimeout(Timeout):
    pass


class ResponseTimeout(Timeout):
    pass


class DriveNotMounted(SwiftException):
    pass


class LockTimeout(MessageTimeout):
    pass


class RingLoadError(SwiftException):
    pass


class RingBuilderError(SwiftException):
    pass


class RingValidationError(RingBuilderError):
    pass


class EmptyRingError(RingBuilderError):
    pass


class DuplicateDeviceError(RingBuilderError):
    pass


class UnPicklingError(SwiftException):
    pass


class FileNotFoundError(SwiftException):
    pass


class PermissionError(SwiftException):
    pass


class ListingIterError(SwiftException):
    pass


class ListingIterNotFound(ListingIterError):
    pass


class ListingIterNotAuthorized(ListingIterError):

    def __init__(self, aresp):
        self.aresp = aresp


class SegmentError(SwiftException):
    pass


class LinkIterError(SwiftException):
    pass


class ReplicationException(Exception):
    pass


class ReplicationLockTimeout(LockTimeout):
    pass


class PartitionLockTimeout(LockTimeout):
    pass


class MimeInvalid(SwiftException):
    pass


class APIVersionError(SwiftException):
    pass


class EncryptionException(SwiftException):
    pass


class UnknownSecretIdError(EncryptionException):
    pass


class QuarantineRequest(SwiftException):
    pass


class MemcacheConnectionError(Exception):
    pass


class MemcacheIncrNotFoundError(MemcacheConnectionError):
    pass


class MemcachePoolTimeout(Timeout):
    pass


class ClientException(Exception):

    def __init__(self, msg, http_scheme='', http_host='', http_port='',
                 http_path='', http_query='', http_status=None, http_reason='',
                 http_device='', http_response_content='', http_headers=None):
        super(ClientException, self).__init__(msg)
        self.msg = msg
        self.http_scheme = http_scheme
        self.http_host = http_host
        self.http_port = http_port
        self.http_path = http_path
        self.http_query = http_query
        self.http_status = http_status
        self.http_reason = http_reason
        self.http_device = http_device
        self.http_response_content = http_response_content
        self.http_headers = http_headers or {}

    def __str__(self):
        a = self.msg
        b = ''
        if self.http_scheme:
            b += '%s://' % self.http_scheme
        if self.http_host:
            b += self.http_host
        if self.http_port:
            b += ':%s' % self.http_port
        if self.http_path:
            b += self.http_path
        if self.http_query:
            b += '?%s' % self.http_query
        if self.http_status:
            if b:
                b = '%s %s' % (b, self.http_status)
            else:
                b = str(self.http_status)
        if self.http_reason:
            if b:
                b = '%s %s' % (b, self.http_reason)
            else:
                b = '- %s' % self.http_reason
        if self.http_device:
            if b:
                b = '%s: device %s' % (b, self.http_device)
            else:
                b = 'device %s' % self.http_device
        if self.http_response_content:
            if len(self.http_response_content) <= 60:
                b += '   %s' % self.http_response_content
            else:
                b += '  [first 60 chars of response] %s' \
                    % self.http_response_content[:60]
        return b and '%s: %s' % (a, b) or a


class InvalidPidFileException(Exception):
    pass
