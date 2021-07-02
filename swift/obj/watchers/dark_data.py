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

#
# This is an audit watcher that manages the dark data in the cluster.
# Since the API for audit watchers is intended to use external plugins,
# this code is invoked as if it were external: through pkg_resources.
# Our setup.py comes pre-configured for convenience, but the operator has
# to enable this watcher honestly by additing DarkDataWatcher to watchers=
# in object-server.conf. The default is off, as if this does not exist.
# Which is for the best, because of a large performance impact of this.
#

"""
The name of "Dark Data" refers to the scientific hypothesis of Dark Matter,
which supposes that the universe contains a lot of matter than we cannot
observe. The Dark Data in Swift is the name of objects that are not
accounted in the containers.

The experience of running large scale clusters suggests that Swift does
not have any particular bugs that trigger creation of dark data. So,
this is an excercise in writing watchers, with a plausible function.

When enabled, Dark Data watcher definitely drags down the cluster's overall
performance. Of course, the load increase can be
mitigated as usual, but at the expense of the total time taken by
the pass of auditor.

Finally, keep in mind that Dark Data watcher needs the container
ring to operate, but runs on an object node. This can come up if
cluster has nodes separated by function.
"""

import os
import random
import shutil
import time

from eventlet import Timeout

from swift.common.direct_client import direct_get_container
from swift.common.exceptions import ClientException, QuarantineRequest
from swift.common.ring import Ring
from swift.common.utils import split_path, Timestamp


class ContainerError(Exception):
    pass


class DarkDataWatcher(object):
    def __init__(self, conf, logger):

        self.logger = logger

        swift_dir = '/etc/swift'
        self.container_ring = Ring(swift_dir, ring_name='container')
        self.dark_data_policy = conf.get('action')
        if self.dark_data_policy not in ['log', 'delete', 'quarantine']:
            if self.dark_data_policy is not None:
                self.logger.warning(
                    "Dark data action %r unknown, defaults to action = 'log'" %
                    (self.dark_data_policy,))
            self.dark_data_policy = 'log'
        self.grace_age = int(conf.get('grace_age', 604800))

    def start(self, audit_type, **other_kwargs):
        self.is_zbf = audit_type == 'ZBF'
        self.tot_unknown = 0
        self.tot_dark = 0
        self.tot_okay = 0

    def policy_based_object_handling(self, data_file_path, metadata):
        obj_path = metadata['name']

        if self.dark_data_policy == "quarantine":
            self.logger.info("quarantining dark data %s" % obj_path)
            raise QuarantineRequest
        elif self.dark_data_policy == "log":
            self.logger.info("reporting dark data %s" % obj_path)
        elif self.dark_data_policy == "delete":
            obj_dir = os.path.dirname(data_file_path)
            self.logger.info("deleting dark data %s" % obj_dir)
            shutil.rmtree(obj_dir)

    def see_object(self, object_metadata, data_file_path, **other_kwargs):

        # No point in loading the container servers with unnecessary requests.
        if self.is_zbf:
            return

        put_tstr = object_metadata['X-Timestamp']
        if float(Timestamp(put_tstr)) + self.grace_age >= time.time():
            # We can add "tot_new" if lumping these with the good objects
            # ever bothers anyone.
            self.tot_okay += 1
            return

        obj_path = object_metadata['name']
        try:
            obj_info = get_info_1(self.container_ring, obj_path, self.logger)
        except ContainerError:
            self.tot_unknown += 1
            return

        if obj_info is None:
            self.tot_dark += 1
            self.policy_based_object_handling(data_file_path, object_metadata)
        else:
            # OK, object is there, but in the future we might want to verify
            # more. Watch out for versioned objects, EC, and all that.
            self.tot_okay += 1

    def end(self, **other_kwargs):
        if self.is_zbf:
            return
        self.logger.info("total unknown %d ok %d dark %d" %
                         (self.tot_unknown, self.tot_okay, self.tot_dark))


#
# Get the information for 1 object from container server
#
def get_info_1(container_ring, obj_path, logger):

    path_comps = split_path(obj_path, 1, 3, True)
    account_name = path_comps[0]
    container_name = path_comps[1]
    obj_name = path_comps[2]

    container_part, container_nodes = \
        container_ring.get_nodes(account_name, container_name)

    if not container_nodes:
        raise ContainerError()

    # Perhaps we should do something about the way we select the container
    # nodes. For now we just shuffle. It spreads the load, but it does not
    # improve upon the the case when some nodes are down, so auditor slows
    # to a crawl (if this plugin is enabled).
    random.shuffle(container_nodes)

    dark_flag = 0
    for node in container_nodes:
        try:
            headers, objs = direct_get_container(
                node, container_part, account_name, container_name,
                prefix=obj_name, limit=1)
        except (ClientException, Timeout):
            # Something is wrong with that server, treat as an error.
            continue
        if not objs or objs[0]['name'] != obj_name:
            dark_flag += 1
            continue
        return objs[0]

    # We do not ask for a quorum of container servers to know the object.
    # Even if 1 server knows the object, we return with the info above.
    # So, we only end here when all servers either have no record of the
    # object or error out. In such case, even one non-error server means
    # that the object is dark.
    if dark_flag:
        return None
    raise ContainerError()
