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
# this code is invoked as if it were external: through load_pkg_resources().
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
performance. Of course, the load increase can be mitigated as usual,
but at the expense of the total time taken by the pass of auditor.

Because the watcher only deems an object dark when all container
servers agree, it will silently fail to detect anything if even one
of container servers in the ring is down or unreacheable. This is
done in the interest of operators who run with action=delete.

If a container is sharded, there is a small edgecase where an object row could
be misplaced. So it is recommended to always start with action=log, before
your confident to run action=delete.

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
from swift.common.utils import split_path, Namespace, Timestamp


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
            obj_info = get_info_1(self.container_ring, obj_path)
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
def get_info_1(container_ring, obj_path):

    path_comps = split_path(obj_path, 1, 3, True)
    account_name = path_comps[0]
    container_name = path_comps[1]
    obj_name = path_comps[2]
    visited = set()

    def check_container(account_name, container_name):
        record_type = 'auto'
        if (account_name, container_name) in visited:
            # Already queried; So we have a last ditch effort and specifically
            # ask for object data as this could be pointing back to the root
            # If the container doesn't have objects then this will return
            # no objects and break the loop.
            record_type = 'object'
        else:
            visited.add((account_name, container_name))

        container_part, container_nodes = \
            container_ring.get_nodes(account_name, container_name)
        if not container_nodes:
            raise ContainerError()

        # Perhaps we should do something about the way we select the container
        # nodes. For now we just shuffle. It spreads the load, but it does not
        # improve upon the the case when some nodes are down, so auditor slows
        # to a crawl (if this plugin is enabled).
        random.shuffle(container_nodes)

        err_flag = 0
        shards = set()
        for node in container_nodes:
            try:
                # The prefix+limit trick is used when a traditional listing
                # is returned, while includes is there for shards.
                # See the how GET routes it in swift/container/server.py.
                headers, objs_or_shards = direct_get_container(
                    node, container_part, account_name, container_name,
                    prefix=obj_name, limit=1,
                    extra_params={'includes': obj_name, 'states': 'listing'},
                    headers={'X-Backend-Record-Type': record_type})
            except (ClientException, Timeout):
                # Something is wrong with that server, treat as an error.
                err_flag += 1
                continue
            if headers.get('X-Backend-Record-Type') == 'shard':
                # When using includes=obj_name, we don't need to anything
                # like find_shard_range(obj_name, ... objs_or_shards).
                if len(objs_or_shards) != 0:
                    namespace = Namespace(objs_or_shards[0]['name'],
                                          objs_or_shards[0]['lower'],
                                          objs_or_shards[0]['upper'])
                    shards.add((namespace.account, namespace.container))
                continue
            if objs_or_shards and objs_or_shards[0]['name'] == obj_name:
                return objs_or_shards[0]

        # If we got back some shards, recurse
        for account_name, container_name in shards:
            res = check_container(account_name, container_name)
            if res:
                return res

        # We only report the object as dark if all known servers agree to it.
        if err_flag:
            raise ContainerError()
        return None

    return check_container(account_name, container_name)
