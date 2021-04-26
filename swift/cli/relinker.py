#!/usr/bin/env python
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


import argparse
import datetime
import errno
import fcntl
import json
import logging
import os
import time

from swift.common.storage_policy import POLICIES
from swift.common.utils import replace_partition_in_path, config_true_value, \
    audit_location_generator, get_logger, readconf, drop_privileges, \
    RateLimitedIterator, lock_path, PrefixLoggerAdapter, distribute_evenly, \
    non_negative_float, non_negative_int, config_auto_int_value
from swift.obj import diskfile


LOCK_FILE = '.relink.{datadir}.lock'
STATE_FILE = 'relink.{datadir}.json'
STATE_TMP_FILE = '.relink.{datadir}.json.tmp'
STEP_RELINK = 'relink'
STEP_CLEANUP = 'cleanup'
EXIT_SUCCESS = 0
EXIT_NO_APPLICABLE_POLICY = 2
EXIT_ERROR = 1


def policy(policy_name_or_index):
    value = POLICIES.get_by_name_or_index(policy_name_or_index)
    if value is None:
        raise ValueError
    return value


class Relinker(object):
    def __init__(self, conf, logger, device_list=None, do_cleanup=False):
        self.conf = conf
        self.logger = logger
        self.device_list = device_list or []
        self.do_cleanup = do_cleanup
        self.root = self.conf['devices']
        if len(self.device_list) == 1:
            self.root = os.path.join(self.root, list(self.device_list)[0])
        self.part_power = self.next_part_power = None
        self.diskfile_mgr = None
        self.dev_lock = None
        self.diskfile_router = diskfile.DiskFileRouter(self.conf, self.logger)
        self._zero_stats()

    def _zero_stats(self):
        self.stats = {
            'hash_dirs': 0,
            'files': 0,
            'linked': 0,
            'removed': 0,
            'errors': 0,
            'policies': 0,
        }

    def devices_filter(self, _, devices):
        if self.device_list:
            devices = [d for d in devices if d in self.device_list]

        return set(devices)

    def hook_pre_device(self, device_path):
        lock_file = os.path.join(device_path,
                                 LOCK_FILE.format(datadir=self.datadir))

        fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY)
        fcntl.flock(fd, fcntl.LOCK_EX)
        self.dev_lock = fd

        state_file = os.path.join(device_path,
                                  STATE_FILE.format(datadir=self.datadir))
        self.states["state"].clear()
        try:
            with open(state_file, 'rt') as f:
                state_from_disk = json.load(f)
                if state_from_disk["next_part_power"] != \
                        self.states["next_part_power"]:
                    raise ValueError
                on_disk_part_power = state_from_disk["part_power"]
                if on_disk_part_power != self.states["part_power"]:
                    self.states["prev_part_power"] = on_disk_part_power
                    raise ValueError
                self.states["state"].update(state_from_disk["state"])
        except (ValueError, TypeError, KeyError):
            # Bad state file: remove the file to restart from scratch
            os.unlink(state_file)
        except IOError as err:
            # Ignore file not found error
            if err.errno != errno.ENOENT:
                raise

    def hook_post_device(self, _):
        os.close(self.dev_lock)
        self.dev_lock = None

    def partitions_filter(self, datadir_path, partitions):
        # Remove all non partitions first (eg: auditor_status_ALL.json)
        partitions = [p for p in partitions if p.isdigit()]

        relinking = (self.part_power != self.next_part_power)
        if relinking:
            # All partitions in the upper half are new partitions and there is
            # nothing to relink there
            partitions = [part for part in partitions
                          if int(part) < 2 ** self.part_power]
        elif "prev_part_power" in self.states:
            # All partitions in the upper half are new partitions and there is
            # nothing to clean up there
            partitions = [part for part in partitions
                          if int(part) < 2 ** self.states["prev_part_power"]]

        # Format: { 'part': processed }
        if self.states["state"]:
            missing = list(set(partitions) - set(self.states["state"].keys()))
            if missing:
                # All missing partitions were created after the first run of
                # the relinker with this part_power/next_part_power pair. This
                # is expected when relinking, where new partitions appear that
                # are appropriate for the target part power. In such cases,
                # there's nothing to be done. Err on the side of caution
                # during cleanup, however.
                for part in missing:
                    self.states["state"][part] = relinking
            partitions = [
                str(part) for part, processed in self.states["state"].items()
                if not processed]
        else:
            self.states["state"].update({
                str(part): False for part in partitions})

        # Always scan the partitions in reverse order to minimize the amount
        # of IO (it actually only matters for relink, not for cleanup).
        #
        # Initial situation:
        #  objects/0/000/00000000...00000000/12345.data
        #  -> relinked to objects/1/000/10000000...00000000/12345.data
        #
        # If the relinker then scan partition 1, it will listdir that object
        # while it's unnecessary. By working in reverse order of partitions,
        # this is avoided.
        partitions = sorted(partitions, key=int, reverse=True)

        # do this last so that self.states, and thus the state file, has been
        # initiated with *all* partitions before partitions are restricted for
        # this particular run...
        conf_partitions = self.conf.get('partitions')
        if conf_partitions:
            partitions = [p for p in partitions if int(p) in conf_partitions]

        return partitions

    # Save states when a partition is done
    def hook_post_partition(self, partition_path):
        datadir_path, part = os.path.split(os.path.abspath(partition_path))
        device_path, datadir_name = os.path.split(datadir_path)
        device = os.path.basename(device_path)
        state_tmp_file = os.path.join(
            device_path, STATE_TMP_FILE.format(datadir=datadir_name))
        state_file = os.path.join(
            device_path, STATE_FILE.format(datadir=datadir_name))

        # We started with a partition space like
        #   |0              N|
        #   |ABCDEFGHIJKLMNOP|
        #
        # After relinking, it will be more like
        #   |0                             2N|
        #   |AABBCCDDEEFFGGHHIIJJKKLLMMNNOOPP|
        #
        # We want to hold off on rehashing until after cleanup, since that is
        # the point at which we've finished with filesystem manipulations. But
        # there's a slight complication: we know the upper half has nothing to
        # clean up, so the cleanup phase only looks at
        #   |0                             2N|
        #   |AABBCCDDEEFFGGHH                |
        #
        # To ensure that the upper half gets rehashed, too, do it as part of
        # relinking; as we finish
        #   |0              N|
        #   |        IJKLMNOP|
        # shift to the new partition space and rehash
        #   |0                             2N|
        #   |                IIJJKKLLMMNNOOPP|
        partition = int(part)
        if not self.do_cleanup and partition >= 2 ** (
                self.states['part_power'] - 1):
            for new_part in (2 * partition, 2 * partition + 1):
                self.diskfile_mgr.get_hashes(
                    device, new_part, [], self.policy)
        elif self.do_cleanup:
            hashes = self.diskfile_mgr.get_hashes(
                device, partition, [], self.policy)
            # In any reasonably-large cluster, we'd expect all old
            # partitions P to be empty after cleanup (i.e., it's unlikely
            # that there's another partition Q := P//2 that also has data
            # on this device).
            #
            # Try to clean up empty partitions now, so operators can use
            # existing rebalance-complete metrics to monitor relinking
            # progress (provided there are few/no handoffs when relinking
            # starts and little data is written to handoffs during the
            # increase).
            if not hashes:
                with lock_path(partition_path):
                    # Same lock used by invalidate_hashes, consolidate_hashes,
                    # get_hashes
                    try:
                        for f in ('hashes.pkl', 'hashes.invalid', '.lock'):
                            try:
                                os.unlink(os.path.join(partition_path, f))
                            except OSError as e:
                                if e.errno != errno.ENOENT:
                                    raise
                    except OSError:
                        pass
                try:
                    os.rmdir(partition_path)
                except OSError:
                    # Most likely, some data landed in here or we hit an error
                    # above. Let the replicator deal with things; it was worth
                    # a shot.
                    pass

        # Then mark this part as done, in case the process is interrupted and
        # needs to resume.
        self.states["state"][part] = True
        with open(state_tmp_file, 'wt') as f:
            json.dump(self.states, f)
            os.fsync(f.fileno())
        os.rename(state_tmp_file, state_file)
        num_parts_done = sum(
            1 for part in self.states["state"].values()
            if part)
        step = STEP_CLEANUP if self.do_cleanup else STEP_RELINK
        num_total_parts = len(self.states["state"])
        self.logger.info("Step: %s Device: %s Policy: %s Partitions: %d/%d" % (
            step, device, self.policy.name, num_parts_done, num_total_parts))

    def hashes_filter(self, suff_path, hashes):
        hashes = list(hashes)
        for hsh in hashes:
            fname = os.path.join(suff_path, hsh)
            if fname == replace_partition_in_path(
                    self.conf['devices'], fname, self.next_part_power):
                hashes.remove(hsh)
        return hashes

    def check_existing(self, new_file):
        existing_link = None
        link_created = False
        start = self.next_part_power - 1
        stop = max(start - self.conf['link_check_limit'], -1)
        for check_part_power in range(start, stop, -1):
            # Try to create the link from each of several previous part power
            # locations. If an attempt succeeds then either a link was made or
            # an existing link with the same inode as the next part power
            # location was found: either is acceptable. The part power location
            # that previously failed with EEXIST is included in the further
            # attempts here for simplicity.
            target_file = replace_partition_in_path(
                self.conf['devices'], new_file, check_part_power)
            try:
                link_created = diskfile.relink_paths(target_file, new_file,
                                                     ignore_missing=False)
                existing_link = target_file
                break
            except OSError:
                pass
        return existing_link, link_created

    def process_location(self, hash_path, new_hash_path):
        # Compare the contents of each hash dir with contents of same hash
        # dir in its new partition to verify that the new location has the
        # most up to date set of files. The new location may have newer
        # files if it has been updated since relinked.
        self.stats['hash_dirs'] += 1

        # Get on disk data for new and old locations, cleaning up any
        # reclaimable or obsolete files in each. The new location is
        # cleaned up *before* the old location to prevent false negatives
        # where the old still has a file that has been cleaned up in the
        # new; cleaning up the new location first ensures that the old will
        # always be 'cleaner' than the new.
        new_df_data = self.diskfile_mgr.cleanup_ondisk_files(new_hash_path)
        old_df_data = self.diskfile_mgr.cleanup_ondisk_files(hash_path)
        # Now determine the most up to date set of on disk files would be
        # given the content of old and new locations...
        new_files = set(new_df_data['files'])
        old_files = set(old_df_data['files'])
        union_files = new_files.union(old_files)
        union_data = self.diskfile_mgr.get_ondisk_files(
            union_files, '', verify=False)
        obsolete_files = set(info['filename']
                             for info in union_data.get('obsolete', []))
        # drop 'obsolete' files but retain 'unexpected' files which might
        # be misplaced diskfiles from another policy
        required_files = union_files.difference(obsolete_files)
        required_links = required_files.intersection(old_files)

        missing_links = 0
        created_links = 0
        unwanted_files = []
        for filename in required_links:
            # Before removing old files, be sure that the corresponding
            # required new files exist by calling relink_paths again. There
            # are several possible outcomes:
            #  - The common case is that the new file exists, in which case
            #    relink_paths checks that the new file has the same inode
            #    as the old file. An exception is raised if the inode of
            #    the new file is not the same as the old file.
            #  - The new file may not exist because the relinker failed to
            #    create the link to the old file and has erroneously moved
            #    on to cleanup. In this case the relink_paths will create
            #    the link now or raise an exception if that fails.
            #  - The new file may not exist because some other process,
            #    such as an object server handling a request, has cleaned
            #    it up since we called cleanup_ondisk_files(new_hash_path).
            #    In this case a new link will be created to the old file.
            #    This is unnecessary but simpler than repeating the
            #    evaluation of what links are now required and safer than
            #    assuming that a non-existent file that *was* required is
            #    no longer required. The new file will eventually be
            #    cleaned up again.
            self.stats['files'] += 1
            old_file = os.path.join(hash_path, filename)
            new_file = os.path.join(new_hash_path, filename)
            try:
                if diskfile.relink_paths(old_file, new_file):
                    self.logger.debug(
                        "Relinking%s created link: %s to %s",
                        ' (cleanup)' if self.do_cleanup else '',
                        old_file, new_file)
                    created_links += 1
                    self.stats['linked'] += 1
            except OSError as exc:
                existing_link = None
                link_created = False
                if exc.errno == errno.EEXIST and filename.endswith('.ts'):
                    # special case for duplicate tombstones in older partition
                    # power locations
                    # (https://bugs.launchpad.net/swift/+bug/1921718)
                    existing_link, link_created = self.check_existing(new_file)
                if existing_link:
                    self.logger.debug(
                        "Relinking%s: link not needed: %s to %s due to "
                        "existing %s", ' (cleanup)' if self.do_cleanup else '',
                        old_file, new_file, existing_link)
                    if link_created:
                        # uncommon case: the retry succeeded in creating a link
                        created_links += 1
                        self.stats['linked'] += 1
                    wanted_file = replace_partition_in_path(
                        self.conf['devices'], old_file, self.part_power)
                    if old_file not in (existing_link, wanted_file):
                        # A link exists to a different file and this file
                        # is not the current target for client requests. If
                        # this file is visited again it is possible that
                        # the existing_link will have been cleaned up and
                        # the check will fail, so clean it up now.
                        self.logger.info(
                            "Relinking%s: cleaning up unwanted file: %s",
                            ' (cleanup)' if self.do_cleanup else '', old_file)
                        unwanted_files.append(filename)
                else:
                    self.logger.warning(
                        "Error relinking%s: failed to relink %s to %s: %s",
                        ' (cleanup)' if self.do_cleanup else '',
                        old_file, new_file, exc)
                    self.stats['errors'] += 1
                    missing_links += 1
        if created_links:
            diskfile.invalidate_hash(os.path.dirname(new_hash_path))

        if self.do_cleanup and not missing_links:
            # use the sorted list to help unit testing
            unwanted_files = old_df_data['files']

        # the new partition hash dir has the most up to date set of on
        # disk files so it is safe to delete the old location...
        rehash = False
        for filename in unwanted_files:
            old_file = os.path.join(hash_path, filename)
            try:
                os.remove(old_file)
            except OSError as exc:
                self.logger.warning('Error cleaning up %s: %r', old_file, exc)
                self.stats['errors'] += 1
            else:
                rehash = True
                self.stats['removed'] += 1
                self.logger.debug("Removed %s", old_file)

        if rehash:
            try:
                diskfile.invalidate_hash(os.path.dirname(hash_path))
            except Exception as exc:
                # note: not counted as an error
                self.logger.warning(
                    'Error invalidating suffix for %s: %r',
                    hash_path, exc)

    def process_policy(self, policy):
        self.logger.info(
            'Processing files for policy %s under %s (cleanup=%s)',
            policy.name, self.root, self.do_cleanup)
        self.part_power = policy.object_ring.part_power
        self.next_part_power = policy.object_ring.next_part_power
        self.diskfile_mgr = self.diskfile_router[policy]
        self.datadir = diskfile.get_data_dir(policy)
        self.states = {
            "part_power": self.part_power,
            "next_part_power": self.next_part_power,
            "state": {},
        }

        locations = audit_location_generator(
            self.conf['devices'],
            self.datadir,
            mount_check=self.conf['mount_check'],
            devices_filter=self.devices_filter,
            hook_pre_device=self.hook_pre_device,
            hook_post_device=self.hook_post_device,
            partitions_filter=self.partitions_filter,
            hook_post_partition=self.hook_post_partition,
            hashes_filter=self.hashes_filter,
            logger=self.logger,
            error_counter=self.stats,
            yield_hash_dirs=True
        )
        if self.conf['files_per_second'] > 0:
            locations = RateLimitedIterator(
                locations, self.conf['files_per_second'])
        for hash_path, device, partition in locations:
            # note, in cleanup step next_part_power == part_power
            new_hash_path = replace_partition_in_path(
                self.conf['devices'], hash_path, self.next_part_power)
            if new_hash_path == hash_path:
                continue
            self.process_location(hash_path, new_hash_path)

    def run(self):
        self._zero_stats()
        for policy in self.conf['policies']:
            self.policy = policy
            policy.object_ring = None  # Ensure it will be reloaded
            policy.load_ring(self.conf['swift_dir'])
            ring = policy.object_ring
            if not ring.next_part_power:
                continue
            part_power_increased = ring.next_part_power == ring.part_power
            if self.do_cleanup != part_power_increased:
                continue

            self.stats['policies'] += 1
            self.process_policy(policy)

        policies = self.stats.pop('policies')
        if not policies:
            self.logger.warning(
                "No policy found to increase the partition power.")
            return EXIT_NO_APPLICABLE_POLICY

        hash_dirs = self.stats.pop('hash_dirs')
        files = self.stats.pop('files')
        linked = self.stats.pop('linked')
        removed = self.stats.pop('removed')
        action_errors = self.stats.pop('errors')
        unmounted = self.stats.pop('unmounted', 0)
        if unmounted:
            self.logger.warning('%d disks were unmounted', unmounted)
        listdir_errors = self.stats.pop('unlistable_partitions', 0)
        if listdir_errors:
            self.logger.warning(
                'There were %d errors listing partition directories',
                listdir_errors)
        if self.stats:
            self.logger.warning(
                'There were unexpected errors while enumerating disk '
                'files: %r', self.stats)

        if action_errors + listdir_errors + unmounted > 0:
            log_method = self.logger.warning
            # NB: audit_location_generator logs unmounted disks as warnings,
            # but we want to treat them as errors
            status = EXIT_ERROR
        else:
            log_method = self.logger.info
            status = EXIT_SUCCESS

        log_method(
            '%d hash dirs processed (cleanup=%s) (%d files, %d linked, '
            '%d removed, %d errors)', hash_dirs, self.do_cleanup, files,
            linked, removed, action_errors + listdir_errors)
        return status


def parallel_process(do_cleanup, conf, logger=None, device_list=None):
    logger = logger or logging.getLogger()
    device_list = sorted(set(device_list or os.listdir(conf['devices'])))
    workers = conf['workers']
    if workers == 'auto':
        workers = len(device_list)
    else:
        workers = min(workers, len(device_list))

    start = time.time()
    logger.info('Starting relinker (cleanup=%s) using %d workers: %s' %
                (do_cleanup, workers,
                 time.strftime('%X %x %Z', time.gmtime(start))))
    if workers == 0 or len(device_list) in (0, 1):
        ret = Relinker(
            conf, logger, device_list, do_cleanup=do_cleanup).run()
        logger.info('Finished relinker (cleanup=%s): %s (%s elapsed)' %
                    (do_cleanup, time.strftime('%X %x %Z', time.gmtime()),
                     datetime.timedelta(seconds=time.time() - start)))
        return ret

    children = {}
    for worker_devs in distribute_evenly(device_list, workers):
        pid = os.fork()
        if pid == 0:
            dev_logger = PrefixLoggerAdapter(logger, {})
            dev_logger.set_prefix('[pid=%s, devs=%s] ' % (
                os.getpid(), ','.join(worker_devs)))
            os._exit(Relinker(
                conf, dev_logger, worker_devs, do_cleanup=do_cleanup).run())
        else:
            children[pid] = worker_devs

    final_status = EXIT_SUCCESS
    final_messages = []
    while children:
        pid, status = os.wait()
        sig = status & 0xff
        status = status >> 8
        time_delta = time.time() - start
        devs = children.pop(pid, ['unknown device'])
        worker_desc = '(pid=%s, devs=%s)' % (pid, ','.join(devs))
        if sig != 0:
            final_status = EXIT_ERROR
            final_messages.append(
                'Worker %s exited in %.1fs after receiving signal: %s'
                % (worker_desc, time_delta, sig))
            continue

        if status == EXIT_SUCCESS:
            continue

        if status == EXIT_NO_APPLICABLE_POLICY:
            if final_status == EXIT_SUCCESS:
                final_status = status
            continue

        final_status = EXIT_ERROR
        if status == EXIT_ERROR:
            final_messages.append(
                'Worker %s completed in %.1fs with errors'
                % (worker_desc, time_delta))
        else:
            final_messages.append(
                'Worker %s exited in %.1fs with unexpected status %s'
                % (worker_desc, time_delta, status))

    for msg in final_messages:
        logger.warning(msg)
    logger.info('Finished relinker (cleanup=%s): %s (%s elapsed)' %
                (do_cleanup, time.strftime('%X %x %Z', time.gmtime()),
                 datetime.timedelta(seconds=time.time() - start)))
    return final_status


def main(args):
    parser = argparse.ArgumentParser(
        description='Relink and cleanup objects to increase partition power')
    parser.add_argument('action', choices=['relink', 'cleanup'])
    parser.add_argument('conf_file', nargs='?', help=(
        'Path to config file with [object-relinker] section'))
    parser.add_argument('--swift-dir', default=None,
                        dest='swift_dir', help='Path to swift directory')
    parser.add_argument(
        '--policy', default=[], dest='policies',
        action='append', type=policy,
        help='Policy to relink; may specify multiple (default: all)')
    parser.add_argument('--devices', default=None,
                        dest='devices', help='Path to swift device directory')
    parser.add_argument('--user', default=None, dest='user',
                        help='Drop privileges to this user before relinking')
    parser.add_argument('--device',
                        default=[], dest='device_list', action='append',
                        help='Device name to relink (default: all)')
    parser.add_argument('--partition', '-p', default=[], dest='partitions',
                        type=non_negative_int, action='append',
                        help='Partition to relink (default: all)')
    parser.add_argument('--skip-mount-check', default=False,
                        help='Don\'t test if disk is mounted',
                        action="store_true", dest='skip_mount_check')
    parser.add_argument('--files-per-second', default=None,
                        type=non_negative_float, dest='files_per_second',
                        help='Used to limit I/O. Zero implies no limit '
                             '(default: no limit).')
    parser.add_argument(
        '--workers', default=None, type=non_negative_int, help=(
            'Process devices across N workers '
            '(default: one worker per device)'))
    parser.add_argument('--logfile', default=None, dest='logfile',
                        help='Set log file name. Ignored if using conf_file.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Enable debug mode')
    parser.add_argument('--link-check-limit', type=non_negative_int,
                        default=None, dest='link_check_limit',
                        help='Maximum number of partition power locations to '
                             'check for a valid link target if relink '
                             'encounters an existing tombstone with different '
                             'inode in the next partition power location '
                             '(default: 2).')

    args = parser.parse_args(args)
    if args.conf_file:
        conf = readconf(args.conf_file, 'object-relinker')
        if args.debug:
            conf['log_level'] = 'DEBUG'
        user = args.user or conf.get('user')
        if user:
            drop_privileges(user)
        logger = get_logger(conf)
    else:
        conf = {'log_level': 'DEBUG' if args.debug else 'INFO'}
        if args.user:
            # Drop privs before creating log file
            drop_privileges(args.user)
            conf['user'] = args.user
        logging.basicConfig(
            format='%(message)s',
            level=logging.DEBUG if args.debug else logging.INFO,
            filename=args.logfile)
        logger = logging.getLogger()

    conf.update({
        'swift_dir': args.swift_dir or conf.get('swift_dir', '/etc/swift'),
        'devices': args.devices or conf.get('devices', '/srv/node'),
        'mount_check': (config_true_value(conf.get('mount_check', 'true'))
                        and not args.skip_mount_check),
        'files_per_second': (
            args.files_per_second if args.files_per_second is not None
            else non_negative_float(conf.get('files_per_second', '0'))),
        'policies': set(args.policies) or POLICIES,
        'partitions': set(args.partitions),
        'workers': config_auto_int_value(
            conf.get('workers') if args.workers is None else args.workers,
            'auto'),
        'link_check_limit': (
            args.link_check_limit if args.link_check_limit is not None
            else non_negative_int(conf.get('link_check_limit', 2))),
    })
    return parallel_process(
        args.action == 'cleanup', conf, logger, args.device_list)
