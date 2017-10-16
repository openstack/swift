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

from swift3.controllers.base import Controller, UnsupportedController
from swift3.controllers.service import ServiceController
from swift3.controllers.bucket import BucketController
from swift3.controllers.obj import ObjectController

from swift3.controllers.acl import AclController
from swift3.controllers.s3_acl import S3AclController
from swift3.controllers.multi_delete import MultiObjectDeleteController
from swift3.controllers.multi_upload import UploadController, \
    PartController, UploadsController
from swift3.controllers.location import LocationController
from swift3.controllers.logging import LoggingStatusController
from swift3.controllers.versioning import VersioningController

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
