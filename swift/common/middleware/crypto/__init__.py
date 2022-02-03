# Copyright (c) 2016 OpenStack Foundation
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
"""
Implements middleware for object encryption which comprises an instance of a
:class:`~swift.common.middleware.crypto.decrypter.Decrypter` combined with an
instance of an :class:`~swift.common.middleware.crypto.encrypter.Encrypter`.
"""
from swift.common.middleware.crypto.decrypter import Decrypter
from swift.common.middleware.crypto.encrypter import Encrypter

from swift.common.utils import config_true_value
from swift.common.registry import register_swift_info


def filter_factory(global_conf, **local_conf):
    """Provides a factory function for loading encryption middleware."""
    conf = global_conf.copy()
    conf.update(local_conf)
    enabled = not config_true_value(conf.get('disable_encryption', 'false'))
    register_swift_info('encryption', admin=True, enabled=enabled)

    def encryption_filter(app):
        return Decrypter(Encrypter(app, conf), conf)
    return encryption_filter
