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

Answers requests of the form::

    /endpoints/{account}/{container}/{object}
    /endpoints/{account}/{container}
    /endpoints/{account}

with a JSON-encoded list of endpoints of the form::

    http://{server}:{port}/{dev}/{part}/{acc}/{cont}/{obj}
    http://{server}:{port}/{dev}/{part}/{acc}/{cont}
    http://{server}:{port}/{dev}/{part}/{acc}

correspondingly, e.g.::

    http://10.1.1.1:6000/sda1/2/a/c2/o1
    http://10.1.1.1:6000/sda1/2/a/c2
    http://10.1.1.1:6000/sda1/2/a

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

from urllib import quote, unquote

from swift.common.ring import Ring
from swift.common.utils import json, get_logger, split_path
from swift.common.swob import Request, Response
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed


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
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.account_ring = Ring(swift_dir, ring_name='account')
        self.container_ring = Ring(swift_dir, ring_name='container')
        self.object_ring = Ring(swift_dir, ring_name='object')
        self.endpoints_path = conf.get('list_endpoints_path', '/endpoints/')
        if not self.endpoints_path.endswith('/'):
            self.endpoints_path += '/'

    def __call__(self, env, start_response):
        request = Request(env)

        if not request.path.startswith(self.endpoints_path):
            return self.app(env, start_response)

        if request.method != 'GET':
            return HTTPMethodNotAllowed(
                req=request, headers={"Allow": "GET"})(env, start_response)

        try:
            clean_path = request.path[len(self.endpoints_path) - 1:]
            account, container, obj = \
                split_path(clean_path, 1, 3, True)
        except ValueError:
            return HTTPBadRequest('No account specified')(env, start_response)

        if account is not None:
            account = unquote(account)
        if container is not None:
            container = unquote(container)
        if obj is not None:
            obj = unquote(obj)

        if obj is not None:
            partition, nodes = self.object_ring.get_nodes(
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

        return Response(json.dumps(endpoints),
                        content_type='application/json')(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def list_endpoints_filter(app):
        return ListEndpointsMiddleware(app, conf)

    return list_endpoints_filter
