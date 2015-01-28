# Copyright (c) 2010-2014 OpenStack Foundation
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

import inspect
from swift import __version__ as swift_version
from swift.common.utils import public, timing_stats, config_true_value
from swift.common.swob import Response


class BaseStorageServer(object):
    """
    Implements common OPTIONS method for object, account, container servers.
    """

    def __init__(self, conf, **kwargs):
        self._allowed_methods = None
        replication_server = conf.get('replication_server', None)
        if replication_server is not None:
            replication_server = config_true_value(replication_server)
        self.replication_server = replication_server

    @property
    def server_type(self):
        raise NotImplementedError(
            'Storage nodes have not implemented the Server type.')

    @property
    def allowed_methods(self):
        if self._allowed_methods is None:
            self._allowed_methods = []
            all_methods = inspect.getmembers(self, predicate=callable)

            if self.replication_server is True:
                for name, m in all_methods:
                    if (getattr(m, 'publicly_accessible', False) and
                            getattr(m, 'replication', False)):
                        self._allowed_methods.append(name)
            elif self.replication_server is False:
                for name, m in all_methods:
                    if (getattr(m, 'publicly_accessible', False) and not
                            getattr(m, 'replication', False)):
                        self._allowed_methods.append(name)
            elif self.replication_server is None:
                for name, m in all_methods:
                    if getattr(m, 'publicly_accessible', False):
                        self._allowed_methods.append(name)

            self._allowed_methods.sort()
        return self._allowed_methods

    @public
    @timing_stats()
    def OPTIONS(self, req):
        """
        Base handler for OPTIONS requests

        :param req: swob.Request object
        :returns: swob.Response object
        """
        # Prepare the default response
        headers = {'Allow': ', '.join(self.allowed_methods),
                   'Server': '%s/%s' % (self.server_type, swift_version)}
        resp = Response(status=200, request=req, headers=headers)

        return resp
