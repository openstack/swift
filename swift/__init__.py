# Copyright (c) 2013 Hewlett-Packard Development Company, L.P.
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

import os
import gettext

import pbr.version


_version_info = pbr.version.VersionInfo('swift')
__version__ = _version_info.release_string()
__canonical_version__ = _version_info.version_string()


_localedir = os.environ.get('SWIFT_LOCALEDIR')
_t = gettext.translation('swift', localedir=_localedir, fallback=True)


def gettext_(msg):
    return _t.gettext(msg)
