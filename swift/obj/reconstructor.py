# Copyright (c) 2010-2012 OpenStack Foundation
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
from os.path import join
import random
import time
import itertools
import cPickle as pickle
from swift import gettext_ as _

import eventlet
from eventlet import GreenPile, GreenPool, Timeout, sleep, hubs
from eventlet.support.greenlets import GreenletExit

from swift.common.utils import (
    whataremyips, unlink_older_than, compute_eta, get_logger,
    dump_recon_cache, ismount, mkdirs, config_true_value, list_from_csv,
    get_hub, tpool_reraise, config_auto_int_value, GreenAsyncPile, Timestamp,
    remove_file)
from swift.common.swob import HeaderKeyDict
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device
from swift.obj.ssync_sender import Sender as ssync_sender
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.obj.diskfile import DiskFileRouter, get_data_dir, \
    get_tmp_dir
from swift.common.storage_policy import POLICIES, EC_POLICY
from swift.common.exceptions import ConnectionTimeout

SYNC, REVERT = ('sync_only', 'sync_revert')


hubs.use_hub(get_hub())


class RebuildingECDiskFileStream(object):
    """
    This class wraps the the reconstructed fragment archive data and
    metadata in the DiskFile interface for ssync.
    """

    def __init__(self, metadata, fragment_index, rebuilt_fragment_iter):
        # start with metadata from a participating FA
        self.metadata = metadata

        # the new FA is going to have the same len as others in the set
        self._content_length = self.metadata['Content-Length']

        # update the FI and delete the ETag, the obj server will
        # recalc on the other side...
        self.metadata['X-Object-Sysmeta-Ec-Archive-Index'] = fragment_index
        del self.metadata['ETag']

        self.fragment_index = fragment_index
        self.rebuilt_fragment_iter = rebuilt_fragment_iter

    def get_metadata(self):
        return self.metadata

    @property
    def content_length(self):
        return self._content_length

    def reader(self):
        for chunk in self.rebuilt_fragment_iter:
            yield chunk


class ObjectReconstructor(Daemon):
    """
    Reconstruct objects using erasure code.  And also rebalance EC Fragment
    Archive objects off handoff nodes.

    Encapsulates most logic and data needed by the object reconstruction
    process. Each call to .reconstruct() performs one pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf, logger=None):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: logging object
        """
        self.conf = conf
        self.logger = logger or get_logger(
            conf, log_route='object-reconstructor')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.port = int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        # TODO:  related to cleanup of old files
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        # TODO:  review these chunk size defaults, probably want to go
        # with 1MB at least
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.headers = {
            'Content-Length': '0',
            'user-agent': 'obj-reconstructor %s' % os.getpid()}
        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))
        # TODO:  handoff_delete is not implemented
        self.handoff_delete = config_auto_int_value(
            conf.get('handoff_delete', 'auto'), 0)
        self._df_router = DiskFileRouter(conf, self.logger)

    def get_object_ring(self, policy_idx):
        """
        Get the ring object to use to handle a request based on its policy.

        :policy_idx: policy index as defined in swift.conf
        :returns: appropriate ring object
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

    def check_ring(self, object_ring):
        """
        Check to see if the ring has been updated

        :param object_ring: the ring to check
        :returns: boolean indicating whether or not the ring has changed
        """
        if time.time() > self.next_check:
            self.next_check = time.time() + self.ring_check_interval
            if object_ring.has_changed():
                return False
        return True

    def _full_path(self, node, part, path, policy):
        return '%(replication_ip)s:%(replication_port)s' \
            '/%(device)s/%(part)s%(path)s ' \
            'policy#%(policy)d frag#%(frag_index)d' % {
                'replication_ip': node['replication_ip'],
                'replication_port': node['replication_port'],
                'device': node['device'],
                'part': part, 'path': path,
                'policy': policy, 'frag_index': node['index'],
            }

    def _get_response(self, node, part, path, headers, policy):
        """
        Helper method for reconstruction that GETs a single EC fragment
        archive

        :param node: the node to GET from
        :param part: the partition
        :param path: full path of the desired EC archive
        :param headers: the headers to send
        :returns: response
        """
        resp = None
        headers['X-Backend-Node-Index'] = node['index']
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                                    part, 'GET', path, headers=headers)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
            if resp.status != HTTP_OK:
                self.logger.error(
                    _("Invalid response %(resp)s from %(full_path)s"),
                    {'resp': resp.status,
                     'full_path': self._full_path(node, part, path, policy)})
                resp = None
        except (Exception, Timeout):
            self.logger.exception(
                _("Trying to GET %(full_path)s"), {
                    'full_path': self._full_path(node, part, path, policy)})
        return resp

    def reconstruct_fa(self, job, node, policy, metadata):
        """
        Reconstructs a fragment archive - this method is called from ssync
        after a remote node responds that is missing this object - the local
        diskfile is opened to provide metadata - but to reconstruct the
        missing fragment archive we must connect to mutliple object servers.

        :param job: job from ssync_sender
        :param node: node that we're rebuilding to
        :param policy:  the relevant policy
        :param metadata:  the metadata to attach to the rebuilt archive
        :returns: a DiskFile like class for use by ssync
        """

        part_nodes = policy.object_ring.get_part_nodes(job['partition'])
        part_nodes.remove(node)

        # the fragment index we need to reconstruct is the position index
        # of the node we're rebuiling to within the primary part list
        fi_to_rebuild = node['index']

        # KISS send out connection requests to all nodes, see what sticks
        headers = {
            'X-Backend-Storage-Policy-Index': int(policy),
        }
        pile = GreenAsyncPile(len(part_nodes))
        for node in part_nodes:
            pile.spawn(self._get_response, node, job['partition'],
                       metadata['name'], headers, policy)
        responses = []
        etag = None
        for resp in pile:
            if not resp:
                continue
            resp.headers = HeaderKeyDict(resp.getheaders())
            responses.append(resp)
            etag = sorted(responses, reverse=True,
                          key=lambda r: Timestamp(
                              r.headers.get('X-Backend-Timestamp')
                          ))[0].headers.get('X-Object-Sysmeta-Ec-Etag')
            responses = [r for r in responses if
                         r.headers.get('X-Object-Sysmeta-Ec-Etag') == etag]

            if len(responses) >= policy.n_streams_for_decode:
                break
        else:
            self.logger.error(
                'Unable to get enough responses (%s/%s) '
                'to reconstruct %s with ETag %s' % (
                    len(responses), policy.n_streams_for_decode,
                    self._full_path(
                        node, job['partition'], metadata['name'], policy),
                    etag))
            return None

        rebuilt_fragment_iter = self.make_rebuilt_fragment_iter(
            responses[:policy.n_streams_for_decode], policy, fi_to_rebuild)
        return RebuildingECDiskFileStream(metadata, fi_to_rebuild,
                                          rebuilt_fragment_iter)

    def _reconstruct(self, policy, fragment_payload, fragment_index):
        # XXX with jerasure this doesn't work if we get an non-sequential list
        # of fragment indexes in the payload
        # segment = policy.pyeclib_driver.reconstruct(
        #     fragment_payload, [fragment_index])[0]

        # for safty until pyeclib 1.0.7 we'll just use decode and encode
        segment = policy.pyeclib_driver.decode(fragment_payload)
        return policy.pyeclib_driver.encode(segment)[fragment_index]

    def make_rebuilt_fragment_iter(self, responses, policy, fragment_index):
        """
        Turn a set of connections from backend object servers into a generator
        that yields up the rebuilt fragment archive for fragment_index.
        """

        def _get_one_fragment(resp):
            buff = ''
            remaining_bytes = policy.fragment_size
            while remaining_bytes:
                #XXX ChunkReadTimeout
                chunk = resp.read(remaining_bytes)
                if not chunk:
                    break
                remaining_bytes -= len(chunk)
                buff += chunk
            return buff

        def fragment_payload_iter():
            # We need a fragment from each connections, so best to
            # use a GreenPile to keep them ordered and in sync
            pile = GreenPile(len(responses))
            while True:
                for resp in responses:
                    pile.spawn(_get_one_fragment, resp)
                fragment_payload = [fragment for fragment in pile]
                if not all(fragment_payload):
                    break
                rebuilt_fragment = self._reconstruct(
                    policy, fragment_payload, fragment_index)
                yield rebuilt_fragment

        return fragment_payload_iter()

    def stats_line(self):
        """
        Logs various stats for the currently running reconstruction pass.
        """
        if self.reconstruction_count:
            elapsed = (time.time() - self.start) or 0.000001
            rate = self.reconstruction_count / elapsed
            self.logger.info(
                _("%(replicated)d/%(total)d (%(percentage).2f%%)"
                  " partitions reconstructed in %(time).2fs (%(rate).2f/sec, "
                  "%(remaining)s remaining)"),
                {'replicated': self.reconstruction_count,
                 'total': self.job_count,
                 'percentage':
                 self.reconstruction_count * 100.0 / self.job_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' % compute_eta(self.start,
                                                   self.reconstruction_count,
                                                   self.job_count)})
            if self.suffix_count:
                self.logger.info(
                    _("%(checked)d suffixes checked - "
                      "%(hashed).2f%% hashed, %(synced).2f%% synced"),
                    {'checked': self.suffix_count,
                     'hashed': (self.suffix_hash * 100.0) / self.suffix_count,
                     'synced': (self.suffix_sync * 100.0) / self.suffix_count})
                self.partition_times.sort()
                self.logger.info(
                    _("Partition times: max %(max).4fs, "
                      "min %(min).4fs, med %(med).4fs"),
                    {'max': self.partition_times[-1],
                     'min': self.partition_times[0],
                     'med': self.partition_times[
                         len(self.partition_times) // 2]})
        else:
            self.logger.info(
                _("Nothing reconstructed for %s seconds."),
                (time.time() - self.start))

    def kill_coros(self):
        """Utility function that kills all coroutines currently running."""
        for coro in list(self.run_pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

    def heartbeat(self):
        """
        Loop that runs in the background during reconstruction.  It
        periodically logs progress.
        """
        while True:
            eventlet.sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the reconstructor finishes its
        reconstruction pass in some eventuality.
        """
        while True:
            eventlet.sleep(self.lockup_timeout)
            if self.reconstruction_count == self.last_reconstruction_count:
                self.logger.error(_("Lockup detected.. killing live coros."))
                self.kill_coros()
            self.last_reconstruction_count = self.reconstruction_count

    def _get_partners(self, local_id, obj_ring, partition):
        """
        Returns the left and right parnters if this nodes is a primary,
        otherwise returns an empty list

        The return value takes two forms depending on if local_id is a primary
        node id in the obj_ring.

        If primary:

            :returns: [<node-to-left>, <node-to-right>]

        If handoff:

            :returns: []
        """
        partners = []
        part_nodes = obj_ring.get_part_nodes(int(partition))
        for node in part_nodes:
            if node['id'] == local_id:
                left = part_nodes[(node['index'] - 1) % len(part_nodes)]
                right = part_nodes[(node['index'] + 1) % len(part_nodes)]
                partners = [left, right]
                break
        return partners

    def tpool_get_info(self, policy, path, recalculate=None, do_listdir=False):
        df_mgr = self._df_router[policy]
        hashed, suffix_hashes = tpool_reraise(
            df_mgr._get_hashes, path, recalculate=recalculate,
            do_listdir=do_listdir, reclaim_age=self.reclaim_age)
        self.logger.update_stats('suffix.hashes', hashed)
        return suffix_hashes

    def get_suffix_delta(self, local_suff, local_index,
                         remote_suff, remote_index):
        suffixes = []
        for suffix, sub_dict_local in local_suff.iteritems():
            sub_dict_remote = remote_suff.get(suffix, {})
            if (local_index not in sub_dict_local or
                    sub_dict_local[local_index] !=
                    sub_dict_remote.get(remote_index)):
                suffixes.append(suffix)
        return suffixes

    def process_job(self, job):
        """
        the reconstructor doesn't have the notion of update() and
        update_deleted() like the replicator as it has to perform
        sort of a blend of both of those operations, sometimes its
        just sync'ing with its partners, sometimes its just
        reverting a partition and sometimes its reverting individual
        objects within a suffix dir to various primiries
        """
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        self.headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        begin = time.time()

        dest_nodes = itertools.chain(
            job['sync_to'],
            job['policy'].object_ring.get_more_nodes(int(job['partition'])))
        syncd_with = 0

        for node in dest_nodes:
            # if we've sync'd with enough nodes not counting insufficient
            # storage then we're done....
            if syncd_with == len(job['sync_to']):
                break

            # get hashes from the remote node
            try:
                with Timeout(self.http_timeout):
                    resp = http_connect(
                        node['replication_ip'], node['replication_port'],
                        node['device'], job['partition'], 'REPLICATE',
                        '', headers=self.headers).getresponse()
                    if resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.logger.error(
                            _('%s responded as unmounted'),
                            self._full_path(node, job['partition'], '',
                                            job['policy']))
                        continue
                    syncd_with += 1
                    if resp.status != HTTP_OK:
                        self.logger.error(
                            _("Invalid response %(resp)s "
                              "from %(full_path)s"), {
                                  'resp': resp.status,
                                  'full_path': self._full_path(
                                      node, job['partition'], '',
                                      job['policy'])
                              })
                        continue
                    remote_hash = pickle.loads(resp.read())
                    del resp

                # for regular sync we compare suffixes, for revert
                # we can't as another ECrecon may have already rebuilt the
                # the remote and udpated its hashes so instead we use
                # the suffix list provided for us in the job
                if job['sync_type'] == SYNC:
                    suffixes = self.get_suffix_delta(job['hashes'],
                                                     job['frag_index'],
                                                     remote_hash,
                                                     node['index'])
                    if not suffixes:
                        continue

                    # now recalculate local hashes for suffixes that don't
                    # match so we're comparing the latest
                    local_suff = self.tpool_get_info(job['policy'],
                                                     job['path'],
                                                     recalculate=suffixes)
                    self.suffix_count += len(local_suff)
                    suffixes = self.get_suffix_delta(local_suff,
                                                     job['frag_index'],
                                                     remote_hash,
                                                     node['index'])
                else:
                    suffixes = job['hashes'].keys()

                ssync_sender(self, node, job, suffixes)()
                with Timeout(self.http_timeout):
                    conn = http_connect(
                        node['replication_ip'], node['replication_port'],
                        node['device'], job['partition'], 'REPLICATE',
                        '/' + '-'.join(suffixes),
                        headers=self.headers)
                    conn.getresponse().read()
                self.suffix_sync += len(suffixes)
                self.logger.update_stats('suffix.syncs', len(suffixes))
            except (Exception, Timeout):
                self.logger.exception(
                    _("Trying to sync suffixes with %s") % self._full_path(
                        node, job['partition'], '', job['policy']))
        self.partition_times.append(time.time() - begin)
        self.logger.timing_since('partition.delete.timing', begin)

    def _get_job_info(self, local_dev, part_path, partition, policy):
        """
        Helper function for build_reconstruction_jobs(), handles
        common work that jobs will need to do so they don't
        duplicate the effort in each one

        :param local_dev:  the lcoal device
        :param obj_ring: the object ring
        :param part_path: full path to partition
        :param partition: partition number
        :param policy: the policy

        :returns: dict of job info for processing in a thread
        """

        jobs = []
        local_id = local_dev['id']

        # get a suffix hashes and frag indexes in this part
        hashes = self.tpool_get_info(policy, part_path, do_listdir=True)

        # dest_nodes represents all potential nodes we need to sync to
        part_nodes = policy.object_ring.get_part_nodes(int(partition))
        local_node = [n for n in part_nodes if n['id'] == local_id]

        # these are the partners we'll sync to when we're a primary
        partners = self._get_partners(local_id, policy.object_ring, partition)

        # decide what work needs to be done based on:
        # 1) part dir with all FI's matching the local node index
        #   this is the case where everything is where it belongs
        #   and we just need to compare hashes and sync if needed,
        #   here we sync with our partners
        # 2) part dir with one local and mix of others
        #   here we need to sync with our partners where FI matches
        #   the lcoal_id , all others are sync'd with their home
        #   nodes and then killed
        # 3) part dir with no local FI and just one or more others
        #   here we sync with just the FI that exists, nobody else
        #   and then all the local FAs are killed

        processed_frags = []
        # there will be one job per FI for each partition that includes
        # a list of suffix dirs that need to be sync'd
        for suffix in hashes:
            suffixes_per_fi = {}

            for frag_index in hashes[suffix]:
                job_info = {}

                # build up an FI key based dict of 'affected' suffixes
                suffixes_per_fi.setdefault(frag_index, []).append(suffix)

                # once we've created a job for an FI, we don't need another
                # one since they will all sync to the same place, we will
                # add the complete list of suffix dirs to the job after
                # we've processed all of the FIs and know what they are
                if frag_index in processed_frags:
                    continue
                processed_frags.append(frag_index)

                job_info['frag_index'] = frag_index
                job_info['sync_to'] = []

                # tombstones/durables have no FI encoded so when we build a
                # job for them, we can't know where to send them when we're
                # reverting.  So, in that case we talk to all part_nodes,
                # otherwise we talk to just the partners
                if frag_index is None:
                    other_nodes = \
                        [n for n in part_nodes if n['id'] != local_id]
                    revert = len(other_nodes) > len(part_nodes) - 1
                    if revert:
                        # TODO: optionally parameterize this so we can
                        # be smarter about who to send these to rather
                        # than everyone (safest)
                        job_info['sync_to'] = part_nodes
                        job_info['sync_type'] = REVERT
                    else:
                        job_info['sync_to'] = partners
                        job_info['sync_type'] = SYNC
                else:
                    # if the current FI belongs here, we sync this suffix dir
                    # with partners for all files matching the FI
                    if partners and int(frag_index) == local_node[0]['index']:
                        job_info['sync_to'] = partners
                        job_info['sync_type'] = SYNC
                    else:
                        # otherwise add in the coresponding node for this FI
                        # that of a FI
                        job_info['sync_type'] = REVERT
                        fi_node = [n for n in part_nodes if n['index'] ==
                                   int(frag_index)]
                        job_info['sync_to'] = fi_node

                # and now the rest of the job info needed to process...
                job_info['partition'] = partition
                job_info['path'] = part_path
                job_info['hashes'] = hashes
                job_info['local_dev'] = local_dev
                job_info['policy'] = policy
                job_info['device'] = local_dev['device']
                job_info['suffix_list'] = []
                if local_node:
                    job_info['local_index'] = local_node[0]['index']
                else:
                    job_info['local_index'] = '-1'
                jobs.append(job_info)

            # so we don't need the 'None' job unless there aren't any
            # "FI jobs" but we don't know that until now (after the
            # frag index loop) so lets rip it out if its in there.
            # note that the job itself is not a duplicate but the work
            # its doing is already covered if there are other jobs
            if len(jobs) > 1:
                remove_me = [j for j in jobs if j['frag_index'] is None]
                if remove_me:
                    jobs.remove(remove_me[0])

            # now we know all of the suffixes affected by this FI,
            # update the jobs with the suffixes they need to work on
            for fi in suffixes_per_fi:
                for job in jobs:
                    if job['frag_index'] == fi:
                        job['suffix_list'].extend(suffixes_per_fi[fi])

        # return a list of jobs for this part
        return jobs

    def collect_parts(self):
        """
        Helper for yielding partitions in the top level reconstructor
        """
        ips = whataremyips()
        for policy in POLICIES:
            if policy.policy_type != EC_POLICY:
                continue
            self._diskfile_mgr = self._df_router[policy]
            obj_ring = self.get_object_ring(int(policy))
            data_dir = get_data_dir(policy)
            local_devices = itertools.ifilter(
                lambda dev: dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port']),
                obj_ring.devs)
            for local_dev in local_devices:
                dev_path = join(self.devices_dir, local_dev['device'])
                obj_path = join(dev_path, data_dir)
                tmp_path = join(dev_path, get_tmp_dir(int(policy)))
                if self.mount_check and not ismount(dev_path):
                    self.logger.warn(_('%s is not mounted'),
                                     local_dev['device'])
                    continue
                unlink_older_than(tmp_path, time.time() -
                                  self.reclaim_age)
                if not os.path.exists(obj_path):
                    try:
                        mkdirs(obj_path)
                    except Exception:
                        self.logger.exception(
                            'Unable to create %s' % obj_path)
                    continue
                try:
                    partitions = os.listdir(obj_path)
                except OSError:
                    self.logger.exception(
                        'Unable to list partitions in %r' % obj_path)
                    continue
                for partition in partitions:
                    part_path = join(obj_path, partition)
                    if not (partition.isdigit() and
                            os.path.isdir(part_path)):
                        self.logger.warning(
                            'Unexpected entity in data dir: %r' % part_path)
                        remove_file(part_path)
                        continue
                    part_info = {
                        'local_dev': local_dev,
                        'policy': policy,
                        'partition': int(partition),
                        'part_path': part_path,
                    }
                    yield part_info

    def build_reconstruction_jobs(self, part_info):
        """
        Helper function for collect_jobs to build jobs for reconstruction
        using EC style storage policy
        """
        jobs = self._get_job_info(**part_info)

        random.shuffle(jobs)
        if self.handoffs_first:
            # Move the handoff revert jobs to the front of the list
            jobs.sort(key=lambda job: job['sync_type'], reverse=True)
        self.job_count = len(jobs)
        return jobs

    def _reset_stats(self):
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.reconstruction_count = 0
        self.last_reconstruction_count = -1

    def reconstruct(self, override_devices=None, override_partitions=None):
        """Run a reconstruction pass"""
        self._reset_stats()
        self.partition_times = []

        if override_devices is None:
            override_devices = []
        if override_partitions is None:
            override_partitions = []

        stats = eventlet.spawn(self.heartbeat)
        lockup_detector = eventlet.spawn(self.detect_lockups)
        eventlet.sleep()  # Give spawns a cycle

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            revert_list = []
            for part_info in self.collect_parts():
                if override_partitions and \
                        part_info['partition'] not in override_partitions:
                    continue
                jobs = self.build_reconstruction_jobs(part_info)
                for job in jobs:
                    if override_devices and \
                            job['device'] not in override_devices:
                        continue
                    dev_path = join(self.devices_dir, job['device'])
                    if self.mount_check and not ismount(dev_path):
                        self.logger.warn(_('%s is not mounted'),
                                         job['device'])
                        continue
                    if not self.check_ring(job['policy'].object_ring):
                        self.logger.info(_("Ring change detected. Aborting "
                                           "current reconstruction pass."))
                        return
                    if job['sync_type'] == REVERT:
                        part_revert = {}
                        part_revert['path'] = job['path']
                        part_revert['policy'] = job['policy']
                        part_revert['hashes'] = job['hashes']
                        revert_list.append(part_revert)
                    self.run_pool.spawn(self.process_job, job)

            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
                # now cleanup and recalc hashes.pkl before moving to
                # next partition.... if we reverted something here
                for revert in revert_list:
                    # recalc local hashes as we deleted something
                    # (either in senderor here or both)
                    if not os.path.isdir(revert['path']):
                        continue
                    suffixes = self.tpool_get_info(
                        revert['policy'], revert['path'],
                        recalculate=revert['hashes'].keys())
                    # the call above will cleanup empty suffix dirs
                    # so all that's left is to wipe out hashes.pkl
                    # and the lock file
                    if suffixes == {}:
                        try:
                            os.unlink(os.path.join(revert['path'],
                                                   'hashes.pkl'))
                            os.unlink(os.path.join(revert['path'],
                                                   '.lock'))
                        except OSError:
                            pass

                    # This is an attempt to remove the partition - it may or
                    # may not be empty and we don't care.  If not empty all
                    # that means is we had some other FI's in here for some
                    # reason.  If it is empty then we really were a handoff -
                    # also think about needing to recalc remote hashes
                    try:
                        os.rmdir(revert['path'])
                    except OSError:
                        pass
                    else:
                        self.logger.info(_("Removing partition: %s"),
                                         revert['path'])
        except (Exception, Timeout):
            self.logger.exception(_("Exception in top-level"
                                    "reconstruction loop"))
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()

    def run_once(self, *args, **kwargs):
        start = time.time()
        self.logger.info(_("Running object reconstructor in script mode."))
        override_devices = list_from_csv(kwargs.get('devices'))
        override_partitions = list_from_csv(kwargs.get('partitions'))
        self.reconstruct(
            override_devices=override_devices,
            override_partitions=override_partitions)
        total = (time.time() - start) / 60
        self.logger.info(
            _("Object reconstruction complete (once). (%.02f minutes)"), total)
        if not (override_partitions or override_devices):
            dump_recon_cache({'object_reconstruction_time': total,
                              'object_reconstruction_last': time.time()},
                             self.rcache, self.logger)

    def run_forever(self, *args, **kwargs):
        self.logger.info(_("Starting object reconstructor in daemon mode."))
        # Run the reconstructor continually
        while True:
            start = time.time()
            self.logger.info(_("Starting object reconstruction pass."))
            # Run the reconstructor
            self.reconstruct()
            total = (time.time() - start) / 60
            self.logger.info(
                _("Object reconstruction complete. (%.02f minutes)"), total)
            dump_recon_cache({'object_reconstruction_time': total,
                              'object_reconstruction_last': time.time()},
                             self.rcache, self.logger)
            self.logger.debug('reconstruction sleeping for %s seconds.',
                              self.run_pause)
            sleep(self.run_pause)
