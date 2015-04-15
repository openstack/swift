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
from os.path import join
import random
import time
import itertools
from collections import defaultdict
import cPickle as pickle
import shutil

from eventlet import (GreenPile, GreenPool, Timeout, sleep, hubs, tpool,
                      spawn)
from eventlet.support.greenlets import GreenletExit

from swift import gettext_ as _
from swift.common.utils import (
    whataremyips, unlink_older_than, compute_eta, get_logger,
    dump_recon_cache, ismount, mkdirs, config_true_value, list_from_csv,
    get_hub, tpool_reraise, GreenAsyncPile, Timestamp, remove_file)
from swift.common.swob import HeaderKeyDict
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device
from swift.obj.ssync_sender import Sender as ssync_sender
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.obj.diskfile import DiskFileRouter, get_data_dir, \
    get_tmp_dir
from swift.common.storage_policy import POLICIES, EC_POLICY
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    SuffixSyncError

SYNC, REVERT = ('sync_only', 'sync_revert')


hubs.use_hub(get_hub())


def _get_partners(frag_index, part_nodes):
    """
    Returns the left and right partners of the node whose index is
    equal to the given frag_index.

    :param frag_index: a fragment index
    :param part_nodes: a list of primary nodes
    :returns: [<node-to-left>, <node-to-right>]
    """
    return [
        part_nodes[(frag_index - 1) % len(part_nodes)],
        part_nodes[(frag_index + 1) % len(part_nodes)],
    ]


class RebuildingECDiskFileStream(object):
    """
    This class wraps the the reconstructed fragment archive data and
    metadata in the DiskFile interface for ssync.
    """

    def __init__(self, metadata, frag_index, rebuilt_fragment_iter):
        # start with metadata from a participating FA
        self.metadata = metadata

        # the new FA is going to have the same length as others in the set
        self._content_length = self.metadata['Content-Length']

        # update the FI and delete the ETag, the obj server will
        # recalc on the other side...
        self.metadata['X-Object-Sysmeta-Ec-Frag-Index'] = frag_index
        for etag_key in ('ETag', 'Etag'):
            self.metadata.pop(etag_key, None)

        self.frag_index = frag_index
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
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.port = int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        # defaults subject to change after beta
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.headers = {
            'Content-Length': '0',
            'user-agent': 'obj-reconstructor %s' % os.getpid()}
        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))
        self._df_router = DiskFileRouter(conf, self.logger)

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

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
            'policy#%(policy)d frag#%(frag_index)s' % {
                'replication_ip': node['replication_ip'],
                'replication_port': node['replication_port'],
                'device': node['device'],
                'part': part, 'path': path,
                'policy': policy,
                'frag_index': node.get('index', 'handoff'),
            }

    def _get_response(self, node, part, path, headers, policy):
        """
        Helper method for reconstruction that GETs a single EC fragment
        archive

        :param node: the node to GET from
        :param part: the partition
        :param path: full path of the desired EC archive
        :param headers: the headers to send
        :param policy: an instance of
                       :class:`~swift.common.storage_policy.BaseStoragePolicy`
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
                self.logger.warning(
                    _("Invalid response %(resp)s from %(full_path)s"),
                    {'resp': resp.status,
                     'full_path': self._full_path(node, part, path, policy)})
                resp = None
        except (Exception, Timeout):
            self.logger.exception(
                _("Trying to GET %(full_path)s"), {
                    'full_path': self._full_path(node, part, path, policy)})
        return resp

    def reconstruct_fa(self, job, node, metadata):
        """
        Reconstructs a fragment archive - this method is called from ssync
        after a remote node responds that is missing this object - the local
        diskfile is opened to provide metadata - but to reconstruct the
        missing fragment archive we must connect to multiple object servers.

        :param job: job from ssync_sender
        :param node: node that we're rebuilding to
        :param metadata:  the metadata to attach to the rebuilt archive
        :returns: a DiskFile like class for use by ssync
        :raises DiskFileError: if the fragment archive cannot be reconstructed
        """

        part_nodes = job['policy'].object_ring.get_part_nodes(
            job['partition'])
        part_nodes.remove(node)

        # the fragment index we need to reconstruct is the position index
        # of the node we're rebuilding to within the primary part list
        fi_to_rebuild = node['index']

        # KISS send out connection requests to all nodes, see what sticks
        headers = {
            'X-Backend-Storage-Policy-Index': int(job['policy']),
        }
        pile = GreenAsyncPile(len(part_nodes))
        path = metadata['name']
        for node in part_nodes:
            pile.spawn(self._get_response, node, job['partition'],
                       path, headers, job['policy'])
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

            if len(responses) >= job['policy'].ec_ndata:
                break
        else:
            self.logger.error(
                'Unable to get enough responses (%s/%s) '
                'to reconstruct %s with ETag %s' % (
                    len(responses), job['policy'].ec_ndata,
                    self._full_path(node, job['partition'],
                                    metadata['name'], job['policy']),
                    etag))
            raise DiskFileError('Unable to reconstruct EC archive')

        rebuilt_fragment_iter = self.make_rebuilt_fragment_iter(
            responses[:job['policy'].ec_ndata], path, job['policy'],
            fi_to_rebuild)
        return RebuildingECDiskFileStream(metadata, fi_to_rebuild,
                                          rebuilt_fragment_iter)

    def _reconstruct(self, policy, fragment_payload, frag_index):
        # XXX with jerasure this doesn't work if we need to rebuild a
        # parity fragment, and not all data fragments are available
        # segment = policy.pyeclib_driver.reconstruct(
        #     fragment_payload, [frag_index])[0]

        # for safety until pyeclib 1.0.7 we'll just use decode and encode
        segment = policy.pyeclib_driver.decode(fragment_payload)
        return policy.pyeclib_driver.encode(segment)[frag_index]

    def make_rebuilt_fragment_iter(self, responses, path, policy, frag_index):
        """
        Turn a set of connections from backend object servers into a generator
        that yields up the rebuilt fragment archive for frag_index.
        """

        def _get_one_fragment(resp):
            buff = ''
            remaining_bytes = policy.fragment_size
            while remaining_bytes:
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
                try:
                    with Timeout(self.node_timeout):
                        fragment_payload = [fragment for fragment in pile]
                except (Exception, Timeout):
                    self.logger.exception(
                        _("Error trying to rebuild %(path)s "
                          "policy#%(policy)d frag#%(frag_index)s"), {
                              'path': path,
                              'policy': policy,
                              'frag_index': frag_index,
                          })
                    break
                if not all(fragment_payload):
                    break
                rebuilt_fragment = self._reconstruct(
                    policy, fragment_payload, frag_index)
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
                _("%(reconstructed)d/%(total)d (%(percentage).2f%%)"
                  " partitions reconstructed in %(time).2fs (%(rate).2f/sec, "
                  "%(remaining)s remaining)"),
                {'reconstructed': self.reconstruction_count,
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
            sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the reconstructor finishes its
        reconstruction pass in some eventuality.
        """
        while True:
            sleep(self.lockup_timeout)
            if self.reconstruction_count == self.last_reconstruction_count:
                self.logger.error(_("Lockup detected.. killing live coros."))
                self.kill_coros()
            self.last_reconstruction_count = self.reconstruction_count

    def _get_hashes(self, policy, path, recalculate=None, do_listdir=False):
        df_mgr = self._df_router[policy]
        hashed, suffix_hashes = tpool_reraise(
            df_mgr._get_hashes, path, recalculate=recalculate,
            do_listdir=do_listdir, reclaim_age=self.reclaim_age)
        self.logger.update_stats('suffix.hashes', hashed)
        return suffix_hashes

    def get_suffix_delta(self, local_suff, local_index,
                         remote_suff, remote_index):
        """
        Compare the local suffix hashes with the remote suffix hashes
        for the given local and remote fragment indexes.  Return those
        suffixes which should be synced.

        :param local_suff: the local suffix hashes (from _get_hashes)
        :param local_index: the local fragment index for the job
        :param remote_suff: the remote suffix hashes (from remote
                            REPLICATE request)
        :param remote_index: the remote fragment index for the job

        :returns: a list of strings, the suffix dirs to sync
        """
        suffixes = []
        for suffix, sub_dict_local in local_suff.iteritems():
            sub_dict_remote = remote_suff.get(suffix, {})
            if (sub_dict_local.get(None) != sub_dict_remote.get(None) or
                    sub_dict_local.get(local_index) !=
                    sub_dict_remote.get(remote_index)):
                suffixes.append(suffix)
        return suffixes

    def rehash_remote(self, node, job, suffixes):
        try:
            with Timeout(self.http_timeout):
                conn = http_connect(
                    node['replication_ip'], node['replication_port'],
                    node['device'], job['partition'], 'REPLICATE',
                    '/' + '-'.join(sorted(suffixes)),
                    headers=self.headers)
                conn.getresponse().read()
        except (Exception, Timeout):
            self.logger.exception(
                _("Trying to sync suffixes with %s") % self._full_path(
                    node, job['partition'], '', job['policy']))

    def _get_suffixes_to_sync(self, job, node):
        """
        For SYNC jobs we need to make a remote REPLICATE request to get
        the remote node's current suffix's hashes and then compare to our
        local suffix's hashes to decide which suffixes (if any) are out
        of sync.

        :param: the job dict, with the keys defined in ``_get_part_jobs``
        :param node: the remote node dict
        :returns: a (possibly empty) list of strings, the suffixes to be
                  synced with the remote node.
        """
        # get hashes from the remote node
        remote_suffixes = None
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
            elif resp.status != HTTP_OK:
                self.logger.error(
                    _("Invalid response %(resp)s "
                      "from %(full_path)s"), {
                          'resp': resp.status,
                          'full_path': self._full_path(
                              node, job['partition'], '',
                              job['policy'])
                      })
            else:
                remote_suffixes = pickle.loads(resp.read())
        except (Exception, Timeout):
            # all exceptions are logged here so that our caller can
            # safely catch our exception and continue to the next node
            # without logging
            self.logger.exception('Unable to get remote suffix hashes '
                                  'from %r' % self._full_path(
                                      node, job['partition'], '',
                                      job['policy']))

        if remote_suffixes is None:
            raise SuffixSyncError('Unable to get remote suffix hashes')

        suffixes = self.get_suffix_delta(job['hashes'],
                                         job['frag_index'],
                                         remote_suffixes,
                                         node['index'])
        # now recalculate local hashes for suffixes that don't
        # match so we're comparing the latest
        local_suff = self._get_hashes(job['policy'], job['path'],
                                      recalculate=suffixes)

        suffixes = self.get_suffix_delta(local_suff,
                                         job['frag_index'],
                                         remote_suffixes,
                                         node['index'])

        self.suffix_count += len(suffixes)
        return suffixes

    def delete_reverted_objs(self, job, objects, frag_index):
        """
        For EC we can potentially revert only some of a partition
        so we'll delete reverted objects here. Note that we delete
        the fragment index of the file we sent to the remote node.

        :param job: the job being processed
        :param objects: a dict of objects to be deleted, each entry maps
                        hash=>timestamp
        :param frag_index: (int) the fragment index of data files to be deleted
        """
        df_mgr = self._df_router[job['policy']]
        for object_hash, timestamp in objects.items():
            try:
                df = df_mgr.get_diskfile_from_hash(
                    job['local_dev']['device'], job['partition'],
                    object_hash, job['policy'],
                    frag_index=frag_index)
                df.purge(Timestamp(timestamp), frag_index)
            except DiskFileError:
                continue

    def process_job(self, job):
        """
        Sync the local partition with the remote node(s) according to
        the parameters of the job.  For primary nodes, the SYNC job type
        will define both left and right hand sync_to nodes to ssync with
        as defined by this primary nodes index in the node list based on
        the fragment index found in the partition.  For non-primary
        nodes (either handoff revert, or rebalance) the REVERT job will
        define a single node in sync_to which is the proper/new home for
        the fragment index.

        N.B. ring rebalancing can be time consuming and handoff nodes'
        fragment indexes do not have a stable order, it's possible to
        have more than one REVERT job for a partition, and in some rare
        failure conditions there may even also be a SYNC job for the
        same partition - but each one will be processed separately
        because each job will define a separate list of node(s) to
        'sync_to'.

        :param: the job dict, with the keys defined in ``_get_job_info``
        """
        self.headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        begin = time.time()
        if job['job_type'] == REVERT:
            self._revert(job, begin)
        else:
            self._sync(job, begin)
        self.partition_times.append(time.time() - begin)
        self.reconstruction_count += 1

    def _sync(self, job, begin):
        """
        Process a SYNC job.
        """
        self.logger.increment(
            'partition.update.count.%s' % (job['local_dev']['device'],))
        # after our left and right partners, if there's some sort of
        # failure we'll continue onto the remaining primary nodes and
        # make sure they're in sync - or potentially rebuild missing
        # fragments we find
        dest_nodes = itertools.chain(
            job['sync_to'],
            # I think we could order these based on our index to better
            # protect against a broken chain
            itertools.ifilter(
                lambda n: n['id'] not in (n['id'] for n in job['sync_to']),
                job['policy'].object_ring.get_part_nodes(job['partition'])),
        )
        syncd_with = 0
        for node in dest_nodes:
            if syncd_with >= len(job['sync_to']):
                # success!
                break

            try:
                suffixes = self._get_suffixes_to_sync(job, node)
            except SuffixSyncError:
                continue

            if not suffixes:
                syncd_with += 1
                continue

            # ssync any out-of-sync suffixes with the remote node
            success, _ = ssync_sender(
                self, node, job, suffixes)()
            # let remote end know to rehash it's suffixes
            self.rehash_remote(node, job, suffixes)
            # update stats for this attempt
            self.suffix_sync += len(suffixes)
            self.logger.update_stats('suffix.syncs', len(suffixes))
            if success:
                syncd_with += 1
        self.logger.timing_since('partition.update.timing', begin)

    def _revert(self, job, begin):
        """
        Process a REVERT job.
        """
        self.logger.increment(
            'partition.delete.count.%s' % (job['local_dev']['device'],))
        # we'd desperately like to push this partition back to it's
        # primary location, but if that node is down, the next best thing
        # is one of the handoff locations - which *might* be us already!
        dest_nodes = itertools.chain(
            job['sync_to'],
            job['policy'].object_ring.get_more_nodes(job['partition']),
        )
        syncd_with = 0
        reverted_objs = {}
        for node in dest_nodes:
            if syncd_with >= len(job['sync_to']):
                break
            if node['id'] == job['local_dev']['id']:
                # this is as good a place as any for this data for now
                break
            success, in_sync_objs = ssync_sender(
                self, node, job, job['suffixes'])()
            self.rehash_remote(node, job, job['suffixes'])
            if success:
                syncd_with += 1
                reverted_objs.update(in_sync_objs)
        if syncd_with >= len(job['sync_to']):
            self.delete_reverted_objs(
                job, reverted_objs, job['frag_index'])
        self.logger.timing_since('partition.delete.timing', begin)

    def _get_part_jobs(self, local_dev, part_path, partition, policy):
        """
        Helper function to build jobs for a partition, this method will
        read the suffix hashes and create job dictionaries to describe
        the needed work.  There will be one job for each fragment index
        discovered in the partition.

        For a fragment index which corresponds to this node's ring
        index, a job with job_type SYNC will be created to ensure that
        the left and right hand primary ring nodes for the part have the
        corresponding left and right hand fragment archives.

        A fragment index (or entire partition) for which this node is
        not the primary corresponding node, will create job(s) with
        job_type REVERT to ensure that fragment archives are pushed to
        the correct node and removed from this one.

        A partition may result in multiple jobs.  Potentially many
        REVERT jobs, and zero or one SYNC job.

        :param local_dev:  the local device
        :param part_path: full path to partition
        :param partition: partition number
        :param policy: the policy

        :returns: a list of dicts of job info
        """
        # find all the fi's in the part, and which suffixes have them
        hashes = self._get_hashes(policy, part_path, do_listdir=True)
        non_data_fragment_suffixes = []
        data_fi_to_suffixes = defaultdict(list)
        for suffix, fi_hash in hashes.items():
            if not fi_hash:
                # this is for sanity and clarity, normally an empty
                # suffix would get del'd from the hashes dict, but an
                # OSError trying to re-hash the suffix could leave the
                # value empty - it will log the exception; but there's
                # no way to properly address this suffix at this time.
                continue
            data_frag_indexes = [f for f in fi_hash if f is not None]
            if not data_frag_indexes:
                non_data_fragment_suffixes.append(suffix)
            else:
                for fi in data_frag_indexes:
                    data_fi_to_suffixes[fi].append(suffix)

        # helper to ensure consistent structure of jobs
        def build_job(job_type, frag_index, suffixes, sync_to):
            return {
                'job_type': job_type,
                'frag_index': frag_index,
                'suffixes': suffixes,
                'sync_to': sync_to,
                'partition': partition,
                'path': part_path,
                'hashes': hashes,
                'policy': policy,
                'local_dev': local_dev,
                # ssync likes to have it handy
                'device': local_dev['device'],
            }

        # aggregate jobs for all the fragment index in this part
        jobs = []

        # check the primary nodes - to see if the part belongs here
        part_nodes = policy.object_ring.get_part_nodes(partition)
        for node in part_nodes:
            if node['id'] == local_dev['id']:
                # this partition belongs here, we'll need a sync job
                frag_index = node['index']
                try:
                    suffixes = data_fi_to_suffixes.pop(frag_index)
                except KeyError:
                    suffixes = []
                sync_job = build_job(
                    job_type=SYNC,
                    frag_index=frag_index,
                    suffixes=suffixes,
                    sync_to=_get_partners(frag_index, part_nodes),
                )
                # ssync callback to rebuild missing fragment_archives
                sync_job['sync_diskfile_builder'] = self.reconstruct_fa
                jobs.append(sync_job)
                break

        # assign remaining data fragment suffixes to revert jobs
        ordered_fis = sorted((len(suffixes), fi) for fi, suffixes
                             in data_fi_to_suffixes.items())
        for count, fi in ordered_fis:
            revert_job = build_job(
                job_type=REVERT,
                frag_index=fi,
                suffixes=data_fi_to_suffixes[fi],
                sync_to=[part_nodes[fi]],
            )
            jobs.append(revert_job)

        # now we need to assign suffixes that have no data fragments
        if non_data_fragment_suffixes:
            if jobs:
                # the first job will be either the sync_job, or the
                # revert_job for the fragment index that is most common
                # among the suffixes
                jobs[0]['suffixes'].extend(non_data_fragment_suffixes)
            else:
                # this is an unfortunate situation, we need a revert job to
                # push partitions off this node, but none of the suffixes
                # have any data fragments to hint at which node would be a
                # good candidate to receive the tombstones.
                jobs.append(build_job(
                    job_type=REVERT,
                    frag_index=None,
                    suffixes=non_data_fragment_suffixes,
                    # this is super safe
                    sync_to=part_nodes,
                    # something like this would be probably be better
                    # sync_to=random.sample(part_nodes, 3),
                ))
        # return a list of jobs for this part
        return jobs

    def collect_parts(self, override_devices=None,
                      override_partitions=None):
        """
        Helper for yielding partitions in the top level reconstructor
        """
        override_devices = override_devices or []
        override_partitions = override_partitions or []
        ips = whataremyips()
        for policy in POLICIES:
            if policy.policy_type != EC_POLICY:
                continue
            self._diskfile_mgr = self._df_router[policy]
            self.load_object_ring(policy)
            data_dir = get_data_dir(policy)
            local_devices = itertools.ifilter(
                lambda dev: dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port']),
                policy.object_ring.devs)
            for local_dev in local_devices:
                if override_devices and (local_dev['device'] not in
                                         override_devices):
                    continue
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
                    partition = int(partition)
                    if override_partitions and (partition not in
                                                override_partitions):
                        continue
                    part_info = {
                        'local_dev': local_dev,
                        'policy': policy,
                        'partition': partition,
                        'part_path': part_path,
                    }
                    yield part_info

    def build_reconstruction_jobs(self, part_info):
        """
        Helper function for collect_jobs to build jobs for reconstruction
        using EC style storage policy
        """
        jobs = self._get_part_jobs(**part_info)
        random.shuffle(jobs)
        if self.handoffs_first:
            # Move the handoff revert jobs to the front of the list
            jobs.sort(key=lambda job: job['job_type'], reverse=True)
        self.job_count += len(jobs)
        return jobs

    def _reset_stats(self):
        self.start = time.time()
        self.job_count = 0
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.reconstruction_count = 0
        self.last_reconstruction_count = -1

    def delete_partition(self, path):
        self.logger.info(_("Removing partition: %s"), path)
        tpool.execute(shutil.rmtree, path, ignore_errors=True)

    def reconstruct(self, **kwargs):
        """Run a reconstruction pass"""
        self._reset_stats()
        self.partition_times = []

        stats = spawn(self.heartbeat)
        lockup_detector = spawn(self.detect_lockups)
        sleep()  # Give spawns a cycle

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            for part_info in self.collect_parts(**kwargs):
                if not self.check_ring(part_info['policy'].object_ring):
                    self.logger.info(_("Ring change detected. Aborting "
                                       "current reconstruction pass."))
                    return
                jobs = self.build_reconstruction_jobs(part_info)
                if not jobs:
                    # If this part belongs on this node, _get_part_jobs
                    # will *always* build a sync_job - even if there's
                    # no suffixes in the partition that needs to sync.
                    # If there's any suffixes in the partition then our
                    # job list would have *at least* one revert job.
                    # Therefore we know this part a) doesn't belong on
                    # this node and b) doesn't have any suffixes in it.
                    self.run_pool.spawn(self.delete_partition,
                                        part_info['part_path'])
                for job in jobs:
                    self.run_pool.spawn(self.process_job, job)
            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
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
        override_partitions = [int(p) for p in
                               list_from_csv(kwargs.get('partitions'))]
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
