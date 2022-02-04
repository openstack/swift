# Copyright (c) 2019 OpenStack Foundation
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
Implements middleware for object versioning which comprises an instance of a
:class:`~swift.common.middleware.versioned_writes.legacy.
VersionedWritesMiddleware` combined with an instance of an
:class:`~swift.common.middleware.versioned_writes.object_versioning.
ObjectVersioningMiddleware`.
"""
from swift.common.middleware.versioned_writes. \
    legacy import CLIENT_VERSIONS_LOC, CLIENT_HISTORY_LOC, \
    VersionedWritesMiddleware
from swift.common.middleware.versioned_writes. \
    object_versioning import ObjectVersioningMiddleware

from swift.common.utils import config_true_value
from swift.common.registry import register_swift_info, get_swift_info


def filter_factory(global_conf, **local_conf):
    """Provides a factory function for loading versioning middleware."""
    conf = global_conf.copy()
    conf.update(local_conf)
    if config_true_value(conf.get('allow_versioned_writes')):
        register_swift_info('versioned_writes', allowed_flags=(
            CLIENT_VERSIONS_LOC, CLIENT_HISTORY_LOC))

    allow_object_versioning = config_true_value(conf.get(
        'allow_object_versioning'))
    if allow_object_versioning:
        register_swift_info('object_versioning')

    def versioning_filter(app):
        if allow_object_versioning:
            if 'symlink' not in get_swift_info():
                raise ValueError('object versioning requires symlinks')
            app = ObjectVersioningMiddleware(app, conf)
        return VersionedWritesMiddleware(app, conf)
    return versioning_filter
