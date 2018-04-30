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

from swift.common.middleware.s3api.controllers.base import Controller, \
    UnsupportedController
from swift.common.middleware.s3api.controllers.service import ServiceController
from swift.common.middleware.s3api.controllers.bucket import BucketController
from swift.common.middleware.s3api.controllers.obj import ObjectController

from swift.common.middleware.s3api.controllers.acl import AclController
from swift.common.middleware.s3api.controllers.s3_acl import S3AclController
from swift.common.middleware.s3api.controllers.multi_delete import \
    MultiObjectDeleteController
from swift.common.middleware.s3api.controllers.multi_upload import \
    UploadController, PartController, UploadsController
from swift.common.middleware.s3api.controllers.location import \
    LocationController
from swift.common.middleware.s3api.controllers.logging import \
    LoggingStatusController
from swift.common.middleware.s3api.controllers.versioning import \
    VersioningController

__all__ = [
    'Controller',
    'ServiceController',
    'BucketController',
    'ObjectController',

    'AclController',
    'S3AclController',
    'MultiObjectDeleteController',
    'PartController',
    'UploadsController',
    'UploadController',
    'LocationController',
    'LoggingStatusController',
    'VersioningController',

    'UnsupportedController',
]
