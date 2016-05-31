#!/usr/bin/python
# coding: UTF-8

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

import unittest
import uuid
from random import shuffle

from nose import SkipTest
from swiftclient import get_auth, http_connection

import test.functional as tf


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


TEST_CASE_FORMAT = (
    'http_method', 'header', 'account_name', 'container_name', 'object_name',
    'prep_container_header', 'reseller_prefix', 'target_user_name',
    'auth_user_name', 'service_user_name', 'expected')
# http_method           : HTTP methods such as PUT, GET, POST, HEAD and so on
# header                : headers for a request
# account_name          : Account name. Usually the name will be automatically
#                         created by keystone
# container_name        : Container name. If 'UUID' is specified, a container
#                         name will be created automatically
# object_name           : Object name. If 'UUID' is specified, a container
#                         name will be created automatically
# prep_container_header : headers which will be set on the container
# reseller_prefix       : Reseller prefix that will be used for request url.
#                         Can be None or SERVICE to select the user account
#                         prefix or the service prefix respectively
# target_user_name      : a user name which is used for getting the project id
#                         of the target
# auth_user_name        : a user name which is used for getting a token for
#                         X-Auth_Token
# service_user_name     : a user name which is used for getting a token for
#                         X-Service-Token
# expected              : expected status code
#
# a combination of account_name, container_name and object_name
# represents a target.
# +------------+--------------+-----------+---------+
# |account_name|container_name|object_name| target  |
# +------------+--------------+-----------+---------+
# |    None    |     None     |   None    | account |
# +------------+--------------+-----------+---------+
# |    None    |    'UUID'    |   None    |container|
# +------------+--------------+-----------+---------+
# |    None    |    'UUID'    |  'UUID'   | object  |
# +------------+--------------+-----------+---------+
#
# The following users are required to run this functional test.
# No.6, tester6, is added for this test.
# +----+-----------+-------+---------+-------------+
# |No. |  Domain   |Project|User name|    Role     |
# +----+-----------+-------+---------+-------------+
# | 1  |  default  | test  | tester  |    admin    |
# +----+-----------+-------+---------+-------------+
# | 2  |  default  | test2 | tester2 |    admin    |
# +----+-----------+-------+---------+-------------+
# | 3  |  default  | test  | tester3 |  _member_   |
# +----+-----------+-------+---------+-------------+
# | 4  |test-domain| test4 | tester4 |    admin    |
# +----+-----------+-------+---------+-------------+
# | 5  |  default  | test5 | tester5 |   service   |
# +----+-----------+-------+---------+-------------+
# | 6  |  default  | test  | tester6 |ResellerAdmin|
# +----+-----------+-------+---------+-------------+

# A scenario of put for account, container and object with
# several roles.
RBAC_PUT = [
    # PUT container in own account: ok
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 201),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 201),

    # PUT container in other users account: not allowed for role admin
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 403),

    # PUT container in other users account: not allowed for role _member_
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 403),

    # PUT container in other users account: allowed for role ResellerAdmin
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 201),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 201),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 201),

    # PUT object in own account: ok
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 201),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 201),

    # PUT object in other users account: not allowed for role admin
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 403),

    # PUT object in other users account: not allowed for role _member_
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 403),

    # PUT object in other users account: allowed for role ResellerAdmin
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 201),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 201),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 201)
]


RBAC_PUT_WITH_SERVICE_PREFIX = [
    # PUT container in own account: ok
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 201),

    # PUT container in other users account: not allowed for role service
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 401),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 403),

    # PUT object in own account: ok
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 201),

    # PUT object in other users account: not allowed for role service
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 401),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 403),

    # All following actions are using SERVICE prefix

    # PUT container in own account: ok
    ('PUT', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 201),

    # PUT container fails if wrong user, or only one token sent
    ('PUT', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('PUT', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('PUT', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('PUT', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # PUT object in own account: ok
    ('PUT', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 201),

    # PUT object fails if wrong user, or only one token sent
    ('PUT', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('PUT', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 401),
]


# A scenario of delete for account, container and object with
# several roles.
RBAC_DELETE = [
    # DELETE container in own account: ok
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 204),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 204),

    # DELETE container in other users account: not allowed for role admin
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 403),

    # DELETE container in other users account: not allowed for role _member_
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 403),

    # DELETE container in other users account: allowed for role ResellerAdmin
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 204),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 204),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 204),

    # DELETE object in own account: ok
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 204),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 204),

    # DELETE object in other users account: not allowed for role admin
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 403),

    # DELETE object in other users account: not allowed for role _member_
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 403),

    # DELETE object in other users account: allowed for role ResellerAdmin
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 204),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 204),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 204)
]


RBAC_DELETE_WITH_SERVICE_PREFIX = [
    # DELETE container in own account: ok
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 204),

    # DELETE container in other users account: not allowed for role service
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 401),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 403),

    # DELETE object in own account: ok
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 204),

    # DELETE object in other users account: not allowed for role service
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 401),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 403),

    # All following actions are using SERVICE prefix

    # DELETE container in own account: ok
    ('DELETE', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # DELETE container fails if wrong user, or only one token sent
    ('DELETE', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('DELETE', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('DELETE', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('DELETE', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # DELETE object in own account: ok
    ('DELETE', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # DELETE object fails if wrong user, or only one token sent
    ('DELETE', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('DELETE', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 401)
]


# A scenario of get for account, container and object with
# several roles.
RBAC_GET = [
    # GET own account: ok
    ('GET', None, None, None, None, None,
     None, 'tester', 'tester', None, 200),
    ('GET', None, None, None, None, None,
     None, 'tester', 'tester', 'tester', 200),

    # GET other users account: not allowed for role admin
    ('GET', None, None, None, None, None,
     None, 'tester2', 'tester', None, 403),
    ('GET', None, None, None, None, None,
     None, 'tester4', 'tester', None, 403),

    # GET other users account: not allowed for role _member_
    ('GET', None, None, None, None, None,
     None, 'tester3', 'tester3', None, 403),
    ('GET', None, None, None, None, None,
     None, 'tester2', 'tester3', None, 403),
    ('GET', None, None, None, None, None,
     None, 'tester4', 'tester3', None, 403),

    # GET other users account: allowed for role ResellerAdmin
    ('GET', None, None, None, None, None,
     None, 'tester6', 'tester6', None, 200),
    ('GET', None, None, None, None, None,
     None, 'tester2', 'tester6', None, 200),
    ('GET', None, None, None, None, None,
     None, 'tester4', 'tester6', None, 200),

    # GET container in own account: ok
    ('GET', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 200),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 200),

    # GET container in other users account: not allowed for role admin
    ('GET', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 403),

    # GET container in other users account: not allowed for role _member_
    ('GET', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 403),

    # GET container in other users account: allowed for role ResellerAdmin
    ('GET', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 200),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 200),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 200),

    # GET object in own account: ok
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 200),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 200),

    # GET object in other users account: not allowed for role admin
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 403),

    # GET object in other users account: not allowed for role _member_
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 403),

    # GET object in other users account: allowed for role ResellerAdmin
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 200),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 200),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 200)
]


RBAC_GET_WITH_SERVICE_PREFIX = [
    # GET own account: ok
    ('GET', None, None, None, None, None,
     None, 'tester', 'tester', 'tester5', 200),

    # GET other account: not allowed for role service
    ('GET', None, None, None, None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, None, None, None,
     None, 'tester', None, 'tester5', 401),
    ('GET', None, None, None, None, None,
     None, 'tester5', 'tester5', None, 403),
    ('GET', None, None, None, None, None,
     None, 'tester2', 'tester5', None, 403),
    ('GET', None, None, None, None, None,
     None, 'tester4', 'tester5', None, 403),

    # GET container in own account: ok
    ('GET', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 200),

    # GET container in other users account: not allowed for role service
    ('GET', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 401),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 403),
    ('GET', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 403),

    # GET object in own account: ok
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 200),

    # GET object fails if wrong user, or only one token sent
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 401),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 403),

    # All following actions are using SERVICE prefix

    # GET own account: ok
    ('GET', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),

    # GET other account: not allowed for role service
    ('GET', None, None, None, None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('GET', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('GET', None, None, None, None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # GET container in own account: ok
    ('GET', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),

    # GET container fails if wrong user, or only one token sent
    ('GET', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('GET', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('GET', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # GET object in own account: ok
    ('GET', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),

    # GET object fails if wrong user, or only one token sent
    ('GET', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('GET', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 401)
]


# A scenario of head for account, container and object with
# several roles.
RBAC_HEAD = [
    # HEAD own account: ok
    ('HEAD', None, None, None, None, None,
     None, 'tester', 'tester', None, 204),
    ('HEAD', None, None, None, None, None,
     None, 'tester', 'tester', 'tester', 204),

    # HEAD other users account: not allowed for role admin
    ('HEAD', None, None, None, None, None,
     None, 'tester2', 'tester', None, 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester4', 'tester', None, 403),

    # HEAD other users account: not allowed for role _member_
    ('HEAD', None, None, None, None, None,
     None, 'tester3', 'tester3', None, 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester2', 'tester3', None, 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester4', 'tester3', None, 403),

    # HEAD other users account: allowed for role ResellerAdmin
    ('HEAD', None, None, None, None, None,
     None, 'tester6', 'tester6', None, 204),
    ('HEAD', None, None, None, None, None,
     None, 'tester2', 'tester6', None, 204),
    ('HEAD', None, None, None, None, None,
     None, 'tester4', 'tester6', None, 204),

    # HEAD container in own account: ok
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 204),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 204),

    # HEAD container in other users account: not allowed for role admin
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 403),

    # HEAD container in other users account: not allowed for role _member_
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 403),

    # HEAD container in other users account: allowed for role ResellerAdmin
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 204),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 204),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 204),


    # HEAD object in own account: ok
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 200),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 200),

    # HEAD object in other users account: not allowed for role admin
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 403),

    # HEAD object in other users account: not allowed for role _member_
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 403),

    # HEAD object in other users account: allowed for role ResellerAdmin
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 200),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 200),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 200)
]


RBAC_HEAD_WITH_SERVICE_PREFIX = [
    # HEAD own account: ok
    ('HEAD', None, None, None, None, None,
     None, 'tester', 'tester', 'tester5', 204),

    # HEAD other account: not allowed for role service
    ('HEAD', None, None, None, None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester', None, 'tester5', 401),
    ('HEAD', None, None, None, None, None,
     None, 'tester5', 'tester5', None, 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester2', 'tester5', None, 403),
    ('HEAD', None, None, None, None, None,
     None, 'tester4', 'tester5', None, 403),

    # HEAD container in own account: ok
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 204),

    # HEAD container in other users account: not allowed for role service
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 401),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 403),

    # HEAD object in own account: ok
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 200),

    # HEAD object fails if wrong user, or only one token sent
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 401),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 403),

    # All following actions are using SERVICE prefix

    # HEAD own account: ok
    ('HEAD', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # HEAD other account: not allowed for role service
    ('HEAD', None, None, None, None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('HEAD', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('HEAD', None, None, None, None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # HEAD container in own account: ok
    ('HEAD', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # HEAD container in other users account: not allowed for role service
    ('HEAD', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('HEAD', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('HEAD', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # HEAD object in own account: ok
    ('HEAD', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),

    # HEAD object fails if wrong user, or only one token sent
    ('HEAD', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('HEAD', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 401)
]


# A scenario of post for account, container and object with
# several roles.
RBAC_POST = [
    # POST own account: ok
    ('POST', None, None, None, None, None,
     None, 'tester', 'tester', None, 204),
    ('POST', None, None, None, None, None,
     None, 'tester', 'tester', 'tester', 204),

    # POST other users account: not allowed for role admin
    ('POST', None, None, None, None, None,
     None, 'tester2', 'tester', None, 403),
    ('POST', None, None, None, None, None,
     None, 'tester4', 'tester', None, 403),

    # POST other users account: not allowed for role _member_
    ('POST', None, None, None, None, None,
     None, 'tester3', 'tester3', None, 403),
    ('POST', None, None, None, None, None,
     None, 'tester2', 'tester3', None, 403),
    ('POST', None, None, None, None, None,
     None, 'tester4', 'tester3', None, 403),

    # POST other users account: allowed for role ResellerAdmin
    ('POST', None, None, None, None, None,
     None, 'tester6', 'tester6', None, 204),
    ('POST', None, None, None, None, None,
     None, 'tester2', 'tester6', None, 204),
    ('POST', None, None, None, None, None,
     None, 'tester4', 'tester6', None, 204),

    # POST container in own account: ok
    ('POST', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 204),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 204),

    # POST container in other users account: not allowed for role admin
    ('POST', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 403),

    # POST container in other users account: not allowed for role _member_
    ('POST', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 403),

    # POST container in other users account: allowed for role ResellerAdmin
    ('POST', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 204),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 204),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 204),

    # POST object in own account: ok
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 202),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 202),

    # POST object in other users account: not allowed for role admin
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 403),

    # POST object in other users account: not allowed for role _member_
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 403),

    # POST object in other users account: allowed for role ResellerAdmin
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 202),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 202),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 202)
]


RBAC_POST_WITH_SERVICE_PREFIX = [
    # POST own account: ok
    ('POST', None, None, None, None, None,
     None, 'tester', 'tester', 'tester5', 204),

    # POST own account: ok
    ('POST', None, None, None, None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, None, None, None,
     None, 'tester', None, 'tester5', 401),
    ('POST', None, None, None, None, None,
     None, 'tester5', 'tester5', None, 403),
    ('POST', None, None, None, None, None,
     None, 'tester2', 'tester5', None, 403),
    ('POST', None, None, None, None, None,
     None, 'tester4', 'tester5', None, 403),

    # POST container in own account: ok
    ('POST', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 204),

    # POST container in other users account: not allowed for role service
    ('POST', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 401),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 403),
    ('POST', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 403),

    # POST object in own account: ok
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 202),

    # POST object fails if wrong user, or only one token sent
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 401),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 403),

    # All following actions are using SERVICE prefix

    # POST own account: ok
    ('POST', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # POST other account: not allowed for role service
    ('POST', None, None, None, None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('POST', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('POST', None, None, None, None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # POST container in own account: ok
    ('POST', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 204),

    # POST container in other users account: not allowed for role service
    ('POST', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('POST', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('POST', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 401),

    # POST object in own account: ok
    ('POST', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 202),

    # POST object fails if wrong user, or only one token sent
    ('POST', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 403),
    ('POST', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 401)
]


# A scenario of options for account, container and object with
# several roles.
RBAC_OPTIONS = [
    # OPTIONS request is always ok

    ('OPTIONS', None, None, None, None, None,
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester2', 'tester', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester4', 'tester', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester3', 'tester3', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester2', 'tester3', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester4', 'tester3', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester6', 'tester6', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester2', 'tester6', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester4', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester2', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester4', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester3', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester2', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester4', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester6', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester2', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester4', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester3', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester3', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester6', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester6', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester6', None, 200),
    ('OPTIONS', None, None, None, None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, None, None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID',
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID',
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, None, None, None, None, 'tester', 'tester', None, 200),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, None, None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, None, None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 200),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', None, None, None, 'tester', 'tester', None, 401),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', 'tester', None, 200),

    # Not OK for container: wrong origin
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', None,
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 401),

    # Not OK for object: missing X-Container-Meta-Access-Control-Allow-Origin
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', 'UUID', None, None, 'tester', 'tester', None, 401),
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', 'UUID',
     {"X-Container-Meta-Access-Control-Allow-Origin": "*"},
     None, 'tester', None, None, 200),

    # Not OK for object: wrong origin
    ('OPTIONS',
     {"Origin": "http://localhost", "Access-Control-Request-Method": "GET"},
     None, 'UUID', 'UUID',
     {"X-Container-Meta-Access-Control-Allow-Origin": "http://invalid.com"},
     None, 'tester', 'tester', None, 401)
]


RBAC_OPTIONS_WITH_SERVICE_PREFIX = [
    # OPTIONS request is always ok

    ('OPTIONS', None, None, None, None, None,
     None, 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester', None, 'tester5', 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester5', 'tester5', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester2', 'tester5', None, 200),
    ('OPTIONS', None, None, None, None, None,
     None, 'tester4', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester', None, 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester5', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester2', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     None, 'tester4', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester', None, 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester5', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester2', 'tester5', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     None, 'tester4', 'tester5', None, 200),
    ('OPTIONS', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, None, None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, None, None, None,
     'SERVICE', 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, None, None, None,
     'SERVICE', 'tester', None, 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     'SERVICE', 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, 'UUID', None, None,
     'SERVICE', 'tester', None, 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester3', 'tester5', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', None, 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', 'tester', 'tester', 200),
    ('OPTIONS', None, None, 'UUID', 'UUID', None,
     'SERVICE', 'tester', None, 'tester5', 200)
]


class SwiftClient(object):
    _tokens = {}

    def __init__(self):
        self._set_users()
        self.auth_url = tf.swift_test_auth
        self.insecure = tf.insecure
        self.auth_version = tf.swift_test_auth_version

    def _set_users(self):
        self.users = {}
        for index in range(6):
            self.users[tf.swift_test_user[index]] = {
                'account': tf.swift_test_tenant[index],
                'password': tf.swift_test_key[index],
                'domain': tf.swift_test_domain[index]}

    def _get_auth(self, user_name):
        info = self.users.get(user_name)
        if info is None:
            return None, None

        os_options = {'user_domain_name': info['domain'],
                      'project_domain_name': info['domain']}
        authargs = dict(snet=False, tenant_name=info['account'],
                        auth_version=self.auth_version, os_options=os_options,
                        insecure=self.insecure)
        storage_url, token = get_auth(
            self.auth_url, user_name, info['password'], **authargs)

        return storage_url, token

    def auth(self, user_name):
        storage_url, token = SwiftClient._tokens.get(user_name, (None, None))
        if not token:
            SwiftClient._tokens[user_name] = self._get_auth(user_name)
            storage_url, token = SwiftClient._tokens.get(user_name)
        return storage_url, token

    def send_request(self, method, url, token=None, headers=None,
                     service_token=None):
        headers = {} if headers is None else headers.copy()
        headers.update({'Content-Type': 'application/json',
                       'Accept': 'application/json'})
        if token:
            headers['X-Auth-Token'] = token
        if service_token:
            headers['X-Service-Token'] = service_token
        if self.insecure:
            parsed, conn = http_connection(url, insecure=self.insecure)
        else:
            parsed, conn = http_connection(url)

        conn.request(method, parsed.path, headers=headers)
        resp = conn.getresponse()

        return resp


class BaseTestAC(unittest.TestCase):
    def setUp(self):
        self.reseller_admin = tf.swift_test_user[5]
        self.client = SwiftClient()

    def _create_resource_url(self, storage_url, account=None,
                             container=None, obj=None, reseller_prefix=None):
        # e.g.
        #   storage_url = 'http://localhost/v1/AUTH_xxx'
        #   storage_url_list[:-1] is ['http:', '', 'localhost', 'v1']
        #   storage_url_list[-1] is 'AUTH_xxx'
        storage_url_list = storage_url.rstrip('/').split('/')
        base_url = '/'.join(storage_url_list[:-1])

        if account is None:
            account = storage_url_list[-1]
            if reseller_prefix == 'SERVICE':
                # replace endpoint reseller prefix with service reseller prefix
                i = (account.index('_') + 1) if '_' in account else 0
                account = tf.swift_test_service_prefix + account[i:]

        return '/'.join([part for part in (base_url, account, container, obj)
                         if part])

    def _put_container(self, storage_url, token, test_case):
        resource_url = self._create_resource_url(
            storage_url,
            test_case['account_name'],
            test_case['container_name'],
            reseller_prefix=test_case['reseller_prefix'])
        self.created_resources.append(resource_url)
        self.client.send_request('PUT', resource_url, token,
                                 headers=test_case['prep_container_header'])

    def _put_object(self, storage_url, token, test_case):
        resource_url = self._create_resource_url(
            storage_url,
            test_case['account_name'],
            test_case['container_name'],
            test_case['object_name'],
            reseller_prefix=test_case['reseller_prefix'])
        self.created_resources.append(resource_url)
        self.client.send_request('PUT', resource_url, token)

    def _get_storage_url_and_token(self, storage_url_user, token_user):
        storage_url, _junk = self.client.auth(storage_url_user)
        _junk, token = self.client.auth(token_user)

        return storage_url, token

    def _prepare(self, test_case):
        storage_url, reseller_token = self._get_storage_url_and_token(
            test_case['target_user_name'], self.reseller_admin)

        if test_case['http_method'] in ('GET', 'POST', 'DELETE', 'HEAD',
                                        'OPTIONS'):
            temp_test_case = test_case.copy()
            if test_case['container_name'] is None:
                # When the target is for account, dummy container will be
                # created to create an account. This account is created by
                # account_autocreate.
                temp_test_case['container_name'] = uuid.uuid4().hex
            self._put_container(storage_url, reseller_token, temp_test_case)

            if test_case['object_name']:
                self._put_object(storage_url, reseller_token, test_case)

        elif test_case['http_method'] in ('PUT',):
            if test_case['object_name']:
                self._put_container(storage_url, reseller_token, test_case)

    def _execute(self, test_case):
        storage_url, token = self._get_storage_url_and_token(
            test_case['target_user_name'], test_case['auth_user_name'])

        service_user = test_case['service_user_name']
        service_token = (None if service_user is None
                         else self.client.auth(service_user)[1])

        resource_url = self._create_resource_url(
            storage_url,
            test_case['account_name'],
            test_case['container_name'],
            test_case['object_name'],
            test_case['reseller_prefix'])

        if test_case['http_method'] in ('PUT'):
            self.created_resources.append(resource_url)

        resp = self.client.send_request(test_case['http_method'],
                                        resource_url,
                                        token,
                                        headers=test_case['header'],
                                        service_token=service_token)

        return resp.status

    def _cleanup(self):
        _junk, reseller_token = self.client.auth(self.reseller_admin)
        for resource_url in reversed(self.created_resources):
            resp = self.client.send_request('DELETE', resource_url,
                                            reseller_token)
            self.assertIn(resp.status, (204, 404))

    def _convert_data(self, data):
        test_case = dict(zip(TEST_CASE_FORMAT, data))
        if test_case['container_name'] == 'UUID':
            test_case['container_name'] = uuid.uuid4().hex
        if test_case['object_name'] == 'UUID':
            test_case['object_name'] = uuid.uuid4().hex
        return test_case

    def _run_scenario(self, scenario):
        for data in scenario:
            test_case = self._convert_data(data)
            self.created_resources = []
            try:
                self._prepare(test_case)
                result = self._execute(test_case)
                self.assertEqual(test_case['expected'],
                                 result,
                                 'Expected %s but got %s for test case %s' %
                                 (test_case['expected'], result, test_case))
            finally:
                self._cleanup()


class TestRBAC(BaseTestAC):

    def test_rbac(self):
        if any((tf.skip, tf.skip2, tf.skip3, tf.skip_if_not_v3,
                tf.skip_if_no_reseller_admin)):
            raise SkipTest
        scenario_rbac = RBAC_PUT + RBAC_DELETE + RBAC_GET +\
            RBAC_HEAD + RBAC_POST + RBAC_OPTIONS
        shuffle(scenario_rbac)
        self._run_scenario(scenario_rbac)

    def test_rbac_with_service_prefix(self):
        if any((tf.skip, tf.skip2, tf.skip3, tf.skip_if_not_v3,
                tf.skip_service_tokens, tf.skip_if_no_reseller_admin)):
            raise SkipTest
        scenario_rbac = RBAC_PUT_WITH_SERVICE_PREFIX +\
            RBAC_DELETE_WITH_SERVICE_PREFIX +\
            RBAC_GET_WITH_SERVICE_PREFIX +\
            RBAC_HEAD_WITH_SERVICE_PREFIX +\
            RBAC_POST_WITH_SERVICE_PREFIX +\
            RBAC_OPTIONS_WITH_SERVICE_PREFIX
        shuffle(scenario_rbac)
        self._run_scenario(scenario_rbac)


if __name__ == '__main__':
    unittest.main()
