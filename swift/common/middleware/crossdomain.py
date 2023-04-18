# Copyright (c) 2013 OpenStack Foundation.
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

from swift.common.swob import Request, Response
from swift.common.registry import register_swift_info


class CrossDomainMiddleware(object):

    """
    Cross domain middleware used to respond to requests for cross domain
    policy information.

    If the path is ``/crossdomain.xml`` it will respond with an xml cross
    domain policy document. This allows web pages hosted elsewhere to use
    client side technologies such as Flash, Java and Silverlight to interact
    with the Swift API.

    To enable this middleware, add it to the pipeline in your proxy-server.conf
    file. It should be added before any authentication (e.g., tempauth or
    keystone) middleware. In this example ellipsis (...) indicate other
    middleware you may have chosen to use:

    .. code:: cfg

        [pipeline:main]
        pipeline =  ... crossdomain ... authtoken ... proxy-server

    And add a filter section, such as:

    .. code:: cfg

        [filter:crossdomain]
        use = egg:swift#crossdomain
        cross_domain_policy = <allow-access-from domain="*.example.com" />
            <allow-access-from domain="www.example.com" secure="false" />

    For continuation lines, put some whitespace before the continuation
    text. Ensure you put a completely blank line to terminate the
    ``cross_domain_policy`` value.

    The ``cross_domain_policy`` name/value is optional. If omitted, the policy
    defaults as if you had specified:

    .. code:: cfg

        cross_domain_policy = <allow-access-from domain="*" secure="false" />

    .. note::

       The default policy is very permissive; this is appropriate
       for most public cloud deployments, but may not be appropriate
       for all deployments. See also:
       `CWE-942 <https://cwe.mitre.org/data/definitions/942.html>`__


    """

    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        default_domain_policy = '<allow-access-from domain="*"' \
                                ' secure="false" />'
        self.cross_domain_policy = self.conf.get('cross_domain_policy',
                                                 default_domain_policy)

    def GET(self, req):
        """Returns a 200 response with cross domain policy information """
        body = '<?xml version="1.0"?>\n' \
               '<!DOCTYPE cross-domain-policy SYSTEM ' \
               '"http://www.adobe.com/xml/dtds/cross-domain-policy.dtd" >\n' \
               '<cross-domain-policy>\n' \
               '%s\n' \
               '</cross-domain-policy>' % self.cross_domain_policy
        return Response(request=req, body=body.encode('utf-8'),
                        content_type="application/xml")

    def __call__(self, env, start_response):
        req = Request(env)
        if req.path == '/crossdomain.xml' and req.method == 'GET':
            return self.GET(req)(env, start_response)
        else:
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    register_swift_info('crossdomain')

    def crossdomain_filter(app):
        return CrossDomainMiddleware(app, conf)
    return crossdomain_filter
