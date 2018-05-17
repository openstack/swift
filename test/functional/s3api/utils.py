# Copyright (c) 2015 OpenStack Foundation
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

from hashlib import md5
from swift.common.middleware.s3api.etree import fromstring


def get_error_code(body):
    elem = fromstring(body, 'Error')
    return elem.find('Code').text


def get_error_msg(body):
    elem = fromstring(body, 'Error')
    return elem.find('Message').text


def calculate_md5(body):
    return md5(body).digest().encode('base64').strip()
