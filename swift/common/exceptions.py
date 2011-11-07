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

from eventlet import Timeout


class MessageTimeout(Timeout):

    def __init__(self, seconds=None, msg=None):
        Timeout.__init__(self, seconds=seconds)
        self.msg = msg

    def __str__(self):
        return '%s: %s' % (Timeout.__str__(self), self.msg)


class AuditException(Exception):
    pass


class DiskFileError(Exception):
    pass


class DiskFileNotExist(Exception):
    pass


class AuthException(Exception):
    pass


class ChunkReadTimeout(Timeout):
    pass


class ChunkWriteTimeout(Timeout):
    pass


class ConnectionTimeout(Timeout):
    pass


class DriveNotMounted(Exception):
    pass


class LockTimeout(MessageTimeout):
    pass


class RingBuilderError(Exception):
    pass


class RingValidationError(RingBuilderError):
    pass


class EmptyRingError(RingBuilderError):
    pass


class DuplicateDeviceError(RingBuilderError):
    pass
