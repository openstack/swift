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
from swift.common.utils import public, timing_stats, config_true_value, \
    LOG_LINE_DEFAULT_FORMAT
from swift.common.swob import Response


class BaseStorageServer(object):
    """
    Implements common OPTIONS method for object, account, container servers.
    """

    def __init__(self, conf, **kwargs):
        self._allowed_methods = None
        self.replication_server = config_true_value(
            conf.get('replication_server', 'true'))
        self.log_format = conf.get('log_format', LOG_LINE_DEFAULT_FORMAT)
        self.anonymization_method = conf.get('log_anonymization_method', 'md5')
        self.anonymization_salt = conf.get('log_anonymization_salt', '')

    @property
    def server_type(self):
        raise NotImplementedError(
            'Storage nodes have not implemented the Server type.')

    @property
    def allowed_methods(self):
        if self._allowed_methods is None:
            self._allowed_methods = []
            all_methods = inspect.getmembers(self, predicate=callable)
            for name, m in all_methods:
                if not getattr(m, 'publicly_accessible', False):
                    continue
                if getattr(m, 'replication', False) and \
                        not self.replication_server:
                    continue
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
