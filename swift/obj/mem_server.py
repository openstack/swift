# Copyright (c) 2010-2013 OpenStack, LLC.
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

""" In-Memory Object Server for Swift """


from swift.obj.mem_diskfile import InMemoryFileSystem
from swift.obj import server


class ObjectController(server.ObjectController):
    """
    Implements the WSGI application for the Swift In-Memory Object Server.
    """

    def setup(self, conf):
        """
        Nothing specific to do for the in-memory version.

        :param conf: WSGI configuration parameter
        """
        self._filesystem = InMemoryFileSystem()

    def get_diskfile(self, device, partition, account, container, obj,
                     **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a given
        REST API.

        An implementation of the object server that wants to use a different
        DiskFile class would simply over-ride this method to provide that
        behavior.
        """
        return self._filesystem.get_diskfile(account, container, obj, **kwargs)

    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        pass


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
