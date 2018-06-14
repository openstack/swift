# Copyright (c) 2012 OpenStack Foundation
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
List endpoints for an object, account or container.

This middleware makes it possible to integrate swift with software
that relies on data locality information to avoid network overhead,
such as Hadoop.

Using the original API, answers requests of the form::

    /endpoints/{account}/{container}/{object}
    /endpoints/{account}/{container}
    /endpoints/{account}
    /endpoints/v1/{account}/{container}/{object}
    /endpoints/v1/{account}/{container}
    /endpoints/v1/{account}

with a JSON-encoded list of endpoints of the form::

    http://{server}:{port}/{dev}/{part}/{acc}/{cont}/{obj}
    http://{server}:{port}/{dev}/{part}/{acc}/{cont}
    http://{server}:{port}/{dev}/{part}/{acc}

correspondingly, e.g.::

    http://10.1.1.1:6200/sda1/2/a/c2/o1
    http://10.1.1.1:6200/sda1/2/a/c2
    http://10.1.1.1:6200/sda1/2/a

Using the v2 API, answers requests of the form::

    /endpoints/v2/{account}/{container}/{object}
    /endpoints/v2/{account}/{container}
    /endpoints/v2/{account}

with a JSON-encoded dictionary containing a key 'endpoints' that maps to a list
of endpoints having the same form as described above, and a key 'headers' that
maps to a dictionary of headers that should be sent with a request made to
the endpoints, e.g.::

    { "endpoints": {"http://10.1.1.1:6010/sda1/2/a/c3/o1",
                    "http://10.1.1.1:6030/sda3/2/a/c3/o1",
                    "http://10.1.1.1:6040/sda4/2/a/c3/o1"},
      "headers": {"X-Backend-Storage-Policy-Index": "1"}}

In this example, the 'headers' dictionary indicates that requests to the
endpoint URLs should include the header 'X-Backend-Storage-Policy-Index: 1'
because the object's container is using storage policy index 1.

The '/endpoints/' path is customizable ('list_endpoints_path'
configuration parameter).

Intended for consumption by third-party services living inside the
cluster (as the endpoints make sense only inside the cluster behind
the firewall); potentially written in a different language.

This is why it's provided as a REST API and not just a Python API:
to avoid requiring clients to write their own ring parsers in their
languages, and to avoid the necessity to distribute the ring file
to clients and keep it up-to-date.

Note that the call is not authenticated, which means that a proxy
with this middleware enabled should not be open to an untrusted
environment (everyone can query the locality data using this middleware).
"""

import json

from six.moves.urllib.parse import quote, unquote

from swift.common.ring import Ring
from swift.common.utils import get_logger, split_path
from swift.common.swob import Request, Response
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed
from swift.common.storage_policy import POLICIES
from swift.proxy.controllers.base import get_container_info

RESPONSE_VERSIONS = (1.0, 2.0)


class ListEndpointsMiddleware(object):
    """
    List endpoints for an object, account or container.

    See above for a full description.

    Uses configuration parameter `swift_dir` (default `/etc/swift`).

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='endpoints')
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.account_ring = Ring(self.swift_dir, ring_name='account')
        self.container_ring = Ring(self.swift_dir, ring_name='container')
        self.endpoints_path = conf.get('list_endpoints_path', '/endpoints/')
        if not self.endpoints_path.endswith('/'):
            self.endpoints_path += '/'
        self.default_response_version = 1.0
        self.response_map = {
            1.0: self.v1_format_response,
            2.0: self.v2_format_response,
        }

    def get_object_ring(self, policy_idx):
        """
        Get the ring object to use to handle a request based on its policy.

        :policy_idx: policy index as defined in swift.conf
        :returns: appropriate ring object
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

    def _parse_version(self, raw_version):
        err_msg = 'Unsupported version %r' % raw_version
        try:
            version = float(raw_version.lstrip('v'))
        except ValueError:
            raise ValueError(err_msg)
        if not any(version == v for v in RESPONSE_VERSIONS):
            raise ValueError(err_msg)
        return version

    def _parse_path(self, request):
        """
        Parse path parts of request into a tuple of version, account,
        container, obj.  Unspecified path parts are filled in as None,
        except version which is always returned as a float using the
        configured default response version if not specified in the
        request.

        :param request: the swob request

        :returns: parsed path parts as a tuple with version filled in as
                  configured default response version if not specified.
        :raises ValueError: if path is invalid, message will say why.
        """
        clean_path = request.path[len(self.endpoints_path) - 1:]
        # try to peel off version
        try:
            raw_version, rest = split_path(clean_path, 1, 2, True)
        except ValueError:
            raise ValueError('No account specified')
        try:
            version = self._parse_version(raw_version)
        except ValueError:
            if raw_version.startswith('v') and '_' not in raw_version:
                # looks more like an invalid version than an account
                raise
            # probably no version specified, but if the client really
            # said /endpoints/v_3/account they'll probably be sorta
            # confused by the useless response and lack of error.
            version = self.default_response_version
            rest = clean_path
        else:
            rest = '/' + rest if rest else '/'
        try:
            account, container, obj = split_path(rest, 1, 3, True)
        except ValueError:
            raise ValueError('No account specified')
        return version, account, container, obj

    def v1_format_response(self, req, endpoints, **kwargs):
        return Response(json.dumps(endpoints),
                        content_type='application/json')

    def v2_format_response(self, req, endpoints, storage_policy_index,
                           **kwargs):
        resp = {
            'endpoints': endpoints,
            'headers': {},
        }
        if storage_policy_index is not None:
            resp['headers'][
                'X-Backend-Storage-Policy-Index'] = str(storage_policy_index)
        return Response(json.dumps(resp),
                        content_type='application/json')

    def __call__(self, env, start_response):
        request = Request(env)
        if not request.path.startswith(self.endpoints_path):
            return self.app(env, start_response)

        if request.method != 'GET':
            return HTTPMethodNotAllowed(
                req=request, headers={"Allow": "GET"})(env, start_response)

        try:
            version, account, container, obj = self._parse_path(request)
        except ValueError as err:
            return HTTPBadRequest(str(err))(env, start_response)

        if account is not None:
            account = unquote(account)
        if container is not None:
            container = unquote(container)
        if obj is not None:
            obj = unquote(obj)

        storage_policy_index = None
        if obj is not None:
            container_info = get_container_info(
                {'PATH_INFO': '/v1/%s/%s' % (account, container)},
                self.app, swift_source='LE')
            storage_policy_index = container_info['storage_policy']
            obj_ring = self.get_object_ring(storage_policy_index)
            partition, nodes = obj_ring.get_nodes(
                account, container, obj)
            endpoint_template = 'http://{ip}:{port}/{device}/{partition}/' + \
                                '{account}/{container}/{obj}'
        elif container is not None:
            partition, nodes = self.container_ring.get_nodes(
                account, container)
            endpoint_template = 'http://{ip}:{port}/{device}/{partition}/' + \
                                '{account}/{container}'
        else:
            partition, nodes = self.account_ring.get_nodes(
                account)
            endpoint_template = 'http://{ip}:{port}/{device}/{partition}/' + \
                                '{account}'

        endpoints = []
        for node in nodes:
            endpoint = endpoint_template.format(
                ip=node['ip'],
                port=node['port'],
                device=node['device'],
                partition=partition,
                account=quote(account),
                container=quote(container or ''),
                obj=quote(obj or ''))
            endpoints.append(endpoint)

        resp = self.response_map[version](
            request, endpoints=endpoints,
            storage_policy_index=storage_policy_index)
        return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def list_endpoints_filter(app):
        return ListEndpointsMiddleware(app, conf)

    return list_endpoints_filter
