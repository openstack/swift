# Copyright (c) 2014 OpenStack Foundation.
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

from swift.common.utils import config_true_value


class Config(dict):
    def __init__(self, base=None):
        if base is not None:
            self.update(base)

    def __getattr__(self, name):
        if name not in self:
            raise AttributeError("No attribute '%s'" % name)

        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def update(self, other):
        if hasattr(other, 'keys'):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value

    def __setitem__(self, key, value):
        if isinstance(self.get(key), bool):
            dict.__setitem__(self, key, config_true_value(value))
        elif isinstance(self.get(key), int):
            try:
                dict.__setitem__(self, key, int(value))
            except ValueError:
                if value:  # No need to raise the error if value is ''
                    raise
        else:
            dict.__setitem__(self, key, value)

# Global config dictionary.  The default values can be defined here.
CONF = Config({
    'allow_no_owner': False,
    'location': 'US',
    'dns_compliant_bucket_names': True,
    'max_bucket_listing': 1000,
    'max_parts_listing': 1000,
    'max_multi_delete_objects': 1000,
    's3_acl': False,
    'storage_domain': '',
    'auth_pipeline_check': True,
    'max_upload_part_num': 1000,
    'check_bucket_owner': False,
    'force_swift_request_proxy_log': False,
    'allow_multipart_uploads': True,
    'min_segment_size': 5242880,
})
