# Copyright (c) 2010-2012 OpenStack, LLC.
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

from swift import gettext_ as _


class ProfileException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Profiling Error: %s' % self.msg


class NotFoundException(ProfileException):
    pass


class MethodNotAllowed(ProfileException):
    pass


class ODFLIBNotInstalled(ProfileException):
    pass


class PLOTLIBNotInstalled(ProfileException):
    pass


class DataLoadFailure(ProfileException):
    pass
