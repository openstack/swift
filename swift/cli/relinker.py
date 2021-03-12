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
import errno
import fcntl
import json
import logging
import os
from functools import partial
from swift.common.storage_policy import POLICIES
from swift.common.utils import replace_partition_in_path, config_true_value, \
    audit_location_generator, get_logger, readconf, drop_privileges, \
    RateLimitedIterator, lock_path
from swift.obj import diskfile


LOCK_FILE = '.relink.{datadir}.lock'
STATE_FILE = 'relink.{datadir}.json'
STATE_TMP_FILE = '.relink.{datadir}.json.tmp'
STEP_RELINK = 'relink'
STEP_CLEANUP = 'cleanup'
EXIT_SUCCESS = 0
EXIT_NO_APPLICABLE_POLICY = 2
EXIT_ERROR = 1


def non_negative_float(value):
    value = float(value)
    if value < 0:
        raise ValueError
    return value


def devices_filter(device, _, devices):
    if device:
        devices = [d for d in devices if d == device]

    return set(devices)


def hook_pre_device(locks, states, datadir, device_path):
    lock_file = os.path.join(device_path, LOCK_FILE.format(datadir=datadir))

    fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    locks[0] = fd

    state_file = os.path.join(device_path, STATE_FILE.format(datadir=datadir))
    states["state"].clear()
    try:
        with open(state_file, 'rt') as f:
            state_from_disk = json.load(f)
            if state_from_disk["next_part_power"] != states["next_part_power"]:
                raise ValueError
            if state_from_disk["part_power"] != states["part_power"]:
                states["prev_part_power"] = state_from_disk["part_power"]
                raise ValueError
            states["state"].update(state_from_disk["state"])
    except (ValueError, TypeError, KeyError):
        # Bad state file: remove the file to restart from scratch
        os.unlink(state_file)
    except IOError as err:
        # Ignore file not found error
        if err.errno != errno.ENOENT:
            raise


def hook_post_device(locks, _):
    os.close(locks[0])
    locks[0] = None


def partitions_filter(states, part_power, next_part_power,
                      datadir_path, partitions):
    # Remove all non partitions first (eg: auditor_status_ALL.json)
    partitions = [p for p in partitions if p.isdigit()]

    relinking = (part_power != next_part_power)
    if relinking:
        # All partitions in the upper half are new partitions and there is
        # nothing to relink there
        partitions = [part for part in partitions
                      if int(part) < 2 ** part_power]
    elif "prev_part_power" in states:
        # All partitions in the upper half are new partitions and there is
        # nothing to clean up there
        partitions = [part for part in partitions
                      if int(part) < 2 ** states["prev_part_power"]]

    # Format: { 'part': processed }
    if states["state"]:
        missing = list(set(partitions) - set(states["state"].keys()))
        if missing:
            # All missing partitions were created after the first run of the
            # relinker with this part_power/next_part_power pair. This is
            # expected when relinking, where new partitions appear that are
            # appropriate for the target part power. In such cases, there's
            # nothing to be done. Err on the side of caution during cleanup,
            # however.
            for part in missing:
                states["state"][part] = relinking
        partitions = [str(part) for part, processed in states["state"].items()
                      if not processed]
    else:
        states["state"].update({str(part): False for part in partitions})

    # Always scan the partitions in reverse order to minimize the amount of IO
    # (it actually only matters for relink, not for cleanup).
    #
    # Initial situation:
    #  objects/0/000/00000000000000000000000000000000/12345.data
    #  -> relinked to objects/1/000/10000000000000000000000000000000/12345.data
    #
    # If the relinker then scan partition 1, it will listdir that object while
    # it's unnecessary. By working in reverse order of partitions, this is
    # avoided.
    partitions = sorted(partitions, key=lambda part: int(part), reverse=True)

    return partitions


# Save states when a partition is done
def hook_post_partition(logger, states, step, policy, diskfile_manager,
                        partition_path):
    datadir_path, part = os.path.split(os.path.abspath(partition_path))
    device_path, datadir_name = os.path.split(datadir_path)
    device = os.path.basename(device_path)
    state_tmp_file = os.path.join(device_path,
                                  STATE_TMP_FILE.format(datadir=datadir_name))
    state_file = os.path.join(device_path,
                              STATE_FILE.format(datadir=datadir_name))

    # We started with a partition space like
    #   |0              N|
    #   |ABCDEFGHIJKLMNOP|
    #
    # After relinking, it will be more like
    #   |0                             2N|
    #   |AABBCCDDEEFFGGHHIIJJKKLLMMNNOOPP|
    #
    # We want to hold off on rehashing until after cleanup, since that is the
    # point at which we've finished with filesystem manipulations. But there's
    # a slight complication: we know the upper half has nothing to clean up,
    # so the cleanup phase only looks at
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
    if step == STEP_RELINK and partition >= 2 ** (states['part_power'] - 1):
        for new_part in (2 * partition, 2 * partition + 1):
            diskfile_manager.get_hashes(device, new_part, [], policy)
    elif step == STEP_CLEANUP:
        hashes = diskfile_manager.get_hashes(device, partition, [], policy)
        # In any reasonably-large cluster, we'd expect all old partitions P
        # to be empty after cleanup (i.e., it's unlikely that there's another
        # partition Q := P//2 that also has data on this device).
        #
        # Try to clean up empty partitions now, so operators can use existing
        # rebalance-complete metrics to monitor relinking progress (provided
        # there are few/no handoffs when relinking starts and little data is
        # written to handoffs during the increase).
        if not hashes:
            with lock_path(partition_path):
                # Same lock used by invalidate_hashes, consolidate_hashes,
                # get_hashes
                try:
                    os.unlink(os.path.join(partition_path, 'hashes.pkl'))
                    os.unlink(os.path.join(partition_path, 'hashes.invalid'))
                    os.unlink(os.path.join(partition_path, '.lock'))
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
    states["state"][part] = True
    with open(state_tmp_file, 'wt') as f:
        json.dump(states, f)
        os.fsync(f.fileno())
    os.rename(state_tmp_file, state_file)
    num_parts_done = sum(1 for part in states["state"].values() if part)
    logger.info("Device: %s Step: %s Partitions: %d/%d" % (
        device, step, num_parts_done, len(states["state"])))


def hashes_filter(next_part_power, suff_path, hashes):
    hashes = list(hashes)
    for hsh in hashes:
        fname = os.path.join(suff_path, hsh)
        if fname == replace_partition_in_path(
                fname, next_part_power, is_hash_dir=True):
            hashes.remove(hsh)
    return hashes


def determine_exit_code(logger, found_policy, processed, action, action_errors,
                        error_counter):
    if not found_policy:
        logger.warning("No policy found to increase the partition power.")
        return EXIT_NO_APPLICABLE_POLICY

    unmounted = error_counter.pop('unmounted', 0)
    if unmounted:
        logger.warning('%d disks were unmounted', unmounted)
    listdir_errors = error_counter.pop('unlistable_partitions', 0)
    if listdir_errors:
        logger.warning('There were %d errors listing partition directories',
                       listdir_errors)
    if error_counter:
        logger.warning(
            'There were unexpected errors while enumerating disk files: %r',
            error_counter)

    logger.info('%d diskfiles %s (%d errors)', processed, action,
                action_errors + listdir_errors)
    if action_errors + listdir_errors + unmounted > 0:
        # NB: audit_location_generator logs unmounted disks as warnings,
        # but we want to treat them as errors
        return EXIT_ERROR
    return EXIT_SUCCESS


def relink(conf, logger, device):
    diskfile_router = diskfile.DiskFileRouter(conf, logger)
    found_policy = False
    relinked = errors = 0
    error_counter = {}
    for policy in POLICIES:
        diskfile_mgr = diskfile_router[policy]
        policy.object_ring = None  # Ensure it will be reloaded
        policy.load_ring(conf['swift_dir'])
        part_power = policy.object_ring.part_power
        next_part_power = policy.object_ring.next_part_power
        if not next_part_power or next_part_power == part_power:
            continue
        logger.info('Relinking files for policy %s under %s',
                    policy.name, conf['devices'])
        found_policy = True
        datadir = diskfile.get_data_dir(policy)

        locks = [None]
        states = {
            "part_power": part_power,
            "next_part_power": next_part_power,
            "state": {},
        }
        relink_devices_filter = partial(devices_filter, device)
        relink_hook_pre_device = partial(hook_pre_device, locks, states,
                                         datadir)
        relink_hook_post_device = partial(hook_post_device, locks)
        relink_partition_filter = partial(partitions_filter,
                                          states, part_power, next_part_power)
        relink_hook_post_partition = partial(hook_post_partition, logger,
                                             states, STEP_RELINK, policy,
                                             diskfile_mgr)
        relink_hashes_filter = partial(hashes_filter, next_part_power)

        locations = audit_location_generator(
            conf['devices'],
            datadir,
            mount_check=conf['mount_check'],
            devices_filter=relink_devices_filter,
            hook_pre_device=relink_hook_pre_device,
            hook_post_device=relink_hook_post_device,
            partitions_filter=relink_partition_filter,
            hook_post_partition=relink_hook_post_partition,
            hashes_filter=relink_hashes_filter,
            logger=logger, error_counter=error_counter)
        if conf['files_per_second'] > 0:
            locations = RateLimitedIterator(
                locations, conf['files_per_second'])
        for fname, _, _ in locations:
            newfname = replace_partition_in_path(fname, next_part_power)
            try:
                logger.debug('Relinking %s to %s', fname, newfname)
                diskfile.relink_paths(fname, newfname)
                relinked += 1
                suffix_dir = os.path.dirname(os.path.dirname(newfname))
                diskfile.invalidate_hash(suffix_dir)
            except OSError as exc:
                errors += 1
                logger.warning("Relinking %s to %s failed: %s",
                               fname, newfname, exc)
    return determine_exit_code(
        logger=logger,
        found_policy=found_policy,
        processed=relinked, action='relinked',
        action_errors=errors,
        error_counter=error_counter,
    )


def cleanup(conf, logger, device):
    diskfile_router = diskfile.DiskFileRouter(conf, logger)
    errors = cleaned_up = 0
    error_counter = {}
    found_policy = False
    for policy in POLICIES:
        diskfile_mgr = diskfile_router[policy]
        policy.object_ring = None  # Ensure it will be reloaded
        policy.load_ring(conf['swift_dir'])
        part_power = policy.object_ring.part_power
        next_part_power = policy.object_ring.next_part_power
        if not next_part_power or next_part_power != part_power:
            continue
        logger.info('Cleaning up files for policy %s under %s',
                    policy.name, conf['devices'])
        found_policy = True
        datadir = diskfile.get_data_dir(policy)

        locks = [None]
        states = {
            "part_power": part_power,
            "next_part_power": next_part_power,
            "state": {},
        }
        cleanup_devices_filter = partial(devices_filter, device)
        cleanup_hook_pre_device = partial(hook_pre_device, locks, states,
                                          datadir)
        cleanup_hook_post_device = partial(hook_post_device, locks)
        cleanup_partition_filter = partial(partitions_filter,
                                           states, part_power, next_part_power)
        cleanup_hook_post_partition = partial(hook_post_partition, logger,
                                              states, STEP_CLEANUP, policy,
                                              diskfile_mgr)
        cleanup_hashes_filter = partial(hashes_filter, next_part_power)

        locations = audit_location_generator(
            conf['devices'],
            datadir,
            mount_check=conf['mount_check'],
            devices_filter=cleanup_devices_filter,
            hook_pre_device=cleanup_hook_pre_device,
            hook_post_device=cleanup_hook_post_device,
            partitions_filter=cleanup_partition_filter,
            hook_post_partition=cleanup_hook_post_partition,
            hashes_filter=cleanup_hashes_filter,
            logger=logger,
            error_counter=error_counter,
            yield_hash_dirs=True)
        if conf['files_per_second'] > 0:
            locations = RateLimitedIterator(
                locations, conf['files_per_second'])

        for hash_path, device, partition in locations:
            # Compare the contents of each hash dir with contents of same hash
            # dir in its new partition to verify that the new location has the
            # most up to date set of files. The new location may have newer
            # files if it has been updated since relinked.
            new_hash_path = replace_partition_in_path(
                hash_path, part_power, is_hash_dir=True)

            if new_hash_path == hash_path:
                continue

            # Get on disk data for new and old locations, cleaning up any
            # reclaimable or obsolete files in each. The new location is
            # cleaned up *before* the old location to prevent false negatives
            # where the old still has a file that has been cleaned up in the
            # new; cleaning up the new location first ensures that the old will
            # always be 'cleaner' than the new.
            new_df_data = diskfile_mgr.cleanup_ondisk_files(new_hash_path)
            old_df_data = diskfile_mgr.cleanup_ondisk_files(hash_path)
            # Now determine the most up to date set of on disk files would be
            # given the content of old and new locations...
            new_files = set(new_df_data['files'])
            old_files = set(old_df_data['files'])
            union_files = new_files.union(old_files)
            union_data = diskfile_mgr.get_ondisk_files(
                union_files, '', verify=False)
            obsolete_files = set(info['filename']
                                 for info in union_data.get('obsolete', []))
            # drop 'obsolete' files but retain 'unexpected' files which might
            # be misplaced diskfiles from another policy
            required_files = union_files.difference(obsolete_files)
            required_links = required_files.intersection(old_files)

            missing_links = 0
            created_links = 0
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
                old_file = os.path.join(hash_path, filename)
                new_file = os.path.join(new_hash_path, filename)
                try:
                    if diskfile.relink_paths(old_file, new_file):
                        logger.debug(
                            "Relinking (cleanup) created link: %s to %s",
                            old_file, new_file)
                        created_links += 1
                except OSError as exc:
                    logger.warning(
                        "Error relinking (cleanup): failed to relink %s to "
                        "%s: %s", old_file, new_file, exc)
                    errors += 1
                    missing_links += 1
            if created_links:
                diskfile.invalidate_hash(os.path.dirname(new_hash_path))
            if missing_links:
                continue

            # the new partition hash dir has the most up to date set of on
            # disk files so it is safe to delete the old location...
            rehash = False
            # use the sorted list to help unit testing
            for filename in old_df_data['files']:
                old_file = os.path.join(hash_path, filename)
                try:
                    os.remove(old_file)
                except OSError as exc:
                    logger.warning('Error cleaning up %s: %r', old_file, exc)
                    errors += 1
                else:
                    rehash = True
                    cleaned_up += 1
                    logger.debug("Removed %s", old_file)

            if rehash:
                try:
                    diskfile.invalidate_hash(os.path.dirname(hash_path))
                except Exception as exc:
                    # note: not counted as an error
                    logger.warning(
                        'Error invalidating suffix for %s: %r',
                        hash_path, exc)

    return determine_exit_code(
        logger=logger,
        found_policy=found_policy,
        processed=cleaned_up, action='cleaned up',
        action_errors=errors,
        error_counter=error_counter,
    )


def main(args):
    parser = argparse.ArgumentParser(
        description='Relink and cleanup objects to increase partition power')
    parser.add_argument('action', choices=['relink', 'cleanup'])
    parser.add_argument('conf_file', nargs='?', help=(
        'Path to config file with [object-relinker] section'))
    parser.add_argument('--swift-dir', default=None,
                        dest='swift_dir', help='Path to swift directory')
    parser.add_argument('--devices', default=None,
                        dest='devices', help='Path to swift device directory')
    parser.add_argument('--user', default=None, dest='user',
                        help='Drop privileges to this user before relinking')
    parser.add_argument('--device', default=None, dest='device',
                        help='Device name to relink (default: all)')
    parser.add_argument('--skip-mount-check', default=False,
                        help='Don\'t test if disk is mounted',
                        action="store_true", dest='skip_mount_check')
    parser.add_argument('--files-per-second', default=None,
                        type=non_negative_float, dest='files_per_second',
                        help='Used to limit I/O. Zero implies no limit '
                             '(default: no limit).')
    parser.add_argument('--logfile', default=None, dest='logfile',
                        help='Set log file name. Ignored if using conf_file.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Enable debug mode')

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
    })

    if args.action == 'relink':
        return relink(conf, logger, device=args.device)

    if args.action == 'cleanup':
        return cleanup(conf, logger, device=args.device)
