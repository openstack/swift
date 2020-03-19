# Copyright (c) 2010-2015 OpenStack Foundation
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
import os
import subprocess
import six
from eventlet import sleep

from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device
from swift.common.storage_policy import POLICIES, get_policy_string
from swift.common.utils import PrefixLoggerAdapter, get_logger, \
    config_true_value, whataremyips
from swift.obj import rpc_http as rpc


class ObjectRpcManager(Daemon):
    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = PrefixLoggerAdapter(
            logger or get_logger(conf, log_route='object-rpcmanager'), {})
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        # use native golang leveldb implementation
        self.use_go_leveldb = config_true_value(
            conf.get('use_go_leveldb', 'false'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.bind_ip = conf.get('bind_ip', '0.0.0.0')
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6200))
        self.volcheck = conf.get('volcheck', '')
        self.losf_bin = conf.get('losf_bin', '')
        self.healthcheck_interval = int(conf.get('healthcheck_interval', 10))

        # check if the path to LOSF binary and volume checker exist
        if not os.path.exists(self.volcheck):
            raise AttributeError(
                "Invalid or missing volcheck in your config file")
        if not os.path.exists(self.losf_bin):
            raise AttributeError(
                "Invalid or missing losf_bin in your config file")

        # this should select only kv enabled policies
        # (requires loading policies?)
        self.policies = POLICIES
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))

        # add RPC state check interval
        self.kv_disks = self.get_kv_disks()

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

    # add filter for KV only
    def get_policy2devices(self):
        ips = whataremyips(self.bind_ip)
        policy2devices = {}
        for policy in self.policies:
            self.load_object_ring(policy)
            local_devices = list(six.moves.filter(
                lambda dev: dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port']),
                policy.object_ring.devs))
            policy2devices[policy] = local_devices
        return policy2devices

    def get_kv_disks(self):
        """
        Returns a dict of KV backed policies to list of devices
        :return: dict
        """
        policy2devices = self.get_policy2devices()
        kv_disks = {}
        for policy, devs in policy2devices.items():
            if policy.diskfile_module.endswith(('.kv', '.hybrid')):
                kv_disks[policy.idx] = [d['device'] for d in devs]

        return kv_disks

    def get_worker_args(self, once=False, **kwargs):
        """
        Take the set of all local devices for this node from all the KV
        backed policies rings.

        :param once: False if the worker(s) will be daemonized, True if the
            worker(s) will be run once
        :param kwargs: optional overrides from the command line
        """

        # Note that this get re-used in is_healthy
        self.kv_disks = self.get_kv_disks()

        # TODO: what to do in this case ?
        if not self.kv_disks:
            # we only need a single worker to do nothing until a ring change
            yield dict(multiprocess_worker_index=0)
            return

        for policy_idx, devs in self.kv_disks.iteritems():
            for dev in devs:
                disk_path = os.path.join(self.devices_dir, dev)
                losf_dir = get_policy_string('losf', policy_idx)
                socket_path = os.path.join(disk_path, losf_dir, 'rpc.socket')
                yield dict(policy_idx=policy_idx, disk_path=disk_path)
                yield dict(policy_idx=policy_idx, disk_path=disk_path,
                           socket_path=socket_path, statecheck=True)

    def is_healthy(self):
        return self.get_kv_disks() == self.kv_disks

    def run_forever(self, policy_idx=None, disk_path=None, socket_path=None,
                    statecheck=None, *args, **kwargs):

        if statecheck:
            volcheck_args = [self.volcheck, '--disk_path', str(disk_path),
                             '--policy_idx', str(policy_idx),
                             '--keepuser', '--repair', '--no_prompt']
            # sleep a bit to let the RPC server start. Otherwise it will
            # timeout and take longer to get the checks started.
            sleep(2)
            while True:
                try:
                    state = rpc.get_kv_state(socket_path)
                    if not state.isClean:
                        self.logger.debug(volcheck_args)
                        subprocess.call(volcheck_args)
                except Exception:
                    self.logger.exception("state check failed, continue")
                sleep(10)
        else:
            losf_args = ['swift-losf-rpc', '-diskPath', str(disk_path),
                         '-debug', 'info',
                         '-policyIdx', str(policy_idx),
                         '-waitForMount={}'.format(str(self.mount_check))]
            if self.use_go_leveldb:
                losf_args.append('-useGoLevelDB')
            os.execv(self.losf_bin, losf_args)
