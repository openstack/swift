# Copyright (c) 2010-2021 OpenStack Foundation
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

RECON_RELINKER_FILE = 'relinker.recon'
RECON_OBJECT_FILE = 'object.recon'
RECON_CONTAINER_FILE = 'container.recon'
RECON_ACCOUNT_FILE = 'account.recon'
RECON_DRIVE_FILE = 'drive.recon'
DEFAULT_RECON_CACHE_PATH = '/var/cache/swift'


def server_type_to_recon_file(server_type):
    if not isinstance(server_type, str) or \
            server_type.lower() not in ('account', 'container', 'object'):
        raise ValueError('Invalid server_type')
    return "%s.recon" % server_type.lower()
