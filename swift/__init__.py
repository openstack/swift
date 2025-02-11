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

import warnings

__version__ = None

# First, try to get our version out of PKG-INFO. If we're installed,
# this'll let us find our version without pulling in pbr. After all, if
# we're installed on a system, we're not in a Git-managed source tree, so
# pbr doesn't really buy us anything.
try:
    import importlib.metadata
except ImportError:
    # python < 3.8
    import pkg_resources
    try:
        __version__ = __canonical_version__ = pkg_resources.get_provider(
            pkg_resources.Requirement.parse('swift')).version
    except pkg_resources.DistributionNotFound:
        pass
else:
    try:
        __version__ = __canonical_version__ = importlib.metadata.distribution(
            'swift').version
    except importlib.metadata.PackageNotFoundError:
        pass

if __version__ is None:
    # No PKG-INFO? We're probably running from a checkout, then. Let pbr do
    # its thing to figure out a version number.
    import pbr.version
    _version_info = pbr.version.VersionInfo('swift')
    __version__ = _version_info.release_string()
    __canonical_version__ = _version_info.version_string()


warnings.filterwarnings('ignore', module='cryptography|OpenSSL', message=(
    'Python 2 is no longer supported by the Python core team. '
    'Support for it is now deprecated in cryptography'))
warnings.filterwarnings('ignore', message=(
    'Python 3.6 is no longer supported by the Python core team. '
    'Therefore, support for it is deprecated in cryptography'))
