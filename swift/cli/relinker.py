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


import errno
import fcntl
import json
import logging
import os
from functools import partial
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import DiskFileDeleted, DiskFileNotExist, \
    DiskFileQuarantined
from swift.common.utils import replace_partition_in_path, \
    audit_location_generator, get_logger
from swift.obj import diskfile


LOCK_FILE = '.relink.{datadir}.lock'
STATE_FILE = 'relink.{datadir}.json'
STATE_TMP_FILE = '.relink.{datadir}.json.tmp'
STEP_RELINK = 'relink'
STEP_CLEANUP = 'cleanup'


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
    states.clear()
    try:
        with open(state_file, 'rt') as f:
            tmp = json.load(f)
            states.update(tmp)
    except ValueError:
        # Invalid JSON: remove the file to restart from scratch
        os.unlink(state_file)
    except IOError as err:
        # Ignore file not found error
        if err.errno != errno.ENOENT:
            raise


def hook_post_device(locks, _):
    os.close(locks[0])
    locks[0] = None


def partitions_filter(states, step, part_power, next_part_power,
                      datadir_path, partitions):
    # Remove all non partitions first (eg: auditor_status_ALL.json)
    partitions = [p for p in partitions if p.isdigit()]

    if not (step == STEP_CLEANUP and part_power == next_part_power):
        # This is not a cleanup after cancel, partitions in the upper half are
        # new partitions, there is nothing to relink/cleanup from there
        partitions = [p for p in partitions
                      if int(p) < 2 ** next_part_power / 2]

    # Format: { 'part': [relinked, cleaned] }
    if states:
        missing = list(set(partitions) - set(states.keys()))
        if missing:
            # All missing partitions was created after the first run of
            # relink, so after the new ring was distribued, so they already
            # are hardlinked in both partitions, but they will need to
            # cleaned.. Just update the state file.
            for part in missing:
                states[part] = [True, False]
        if step == STEP_RELINK:
            partitions = [str(p) for p, (r, c) in states.items() if not r]
        elif step == STEP_CLEANUP:
            partitions = [str(p) for p, (r, c) in states.items() if not c]
    else:
        states.update({str(p): [False, False] for p in partitions})

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
    partitions = sorted(partitions, key=lambda x: int(x), reverse=True)

    return partitions


# Save states when a partition is done
def hook_post_partition(states, step,
                        partition_path):
    part = os.path.basename(os.path.abspath(partition_path))
    datadir_path = os.path.dirname(os.path.abspath(partition_path))
    device_path = os.path.dirname(os.path.abspath(datadir_path))
    datadir_name = os.path.basename(os.path.abspath(datadir_path))
    state_tmp_file = os.path.join(device_path,
                                  STATE_TMP_FILE.format(datadir=datadir_name))
    state_file = os.path.join(device_path,
                              STATE_FILE.format(datadir=datadir_name))

    if step == STEP_RELINK:
        states[part][0] = True
    elif step == STEP_CLEANUP:
        states[part][1] = True
    with open(state_tmp_file, 'wt') as f:
        json.dump(states, f)
        os.fsync(f.fileno())
    os.rename(state_tmp_file, state_file)


def hashes_filter(next_part_power, suff_path, hashes):
    hashes = list(hashes)
    for hsh in hashes:
        fname = os.path.join(suff_path, hsh, 'fake-file-name')
        if replace_partition_in_path(fname, next_part_power) == fname:
            hashes.remove(hsh)
    return hashes


def relink(swift_dir='/etc/swift',
           devices='/srv/node',
           skip_mount_check=False,
           logger=logging.getLogger(),
           device=None):
    mount_check = not skip_mount_check
    run = False
    relinked = errors = 0
    for policy in POLICIES:
        policy.object_ring = None  # Ensure it will be reloaded
        policy.load_ring(swift_dir)
        part_power = policy.object_ring.part_power
        next_part_power = policy.object_ring.next_part_power
        if not next_part_power or next_part_power == part_power:
            continue
        logging.info('Relinking files for policy %s under %s',
                     policy.name, devices)
        run = True
        datadir = diskfile.get_data_dir(policy)

        locks = [None]
        states = {}
        relink_devices_filter = partial(devices_filter, device)
        relink_hook_pre_device = partial(hook_pre_device, locks, states,
                                         datadir)
        relink_hook_post_device = partial(hook_post_device, locks)
        relink_partition_filter = partial(partitions_filter,
                                          states, STEP_RELINK,
                                          part_power, next_part_power)
        relink_hook_post_partition = partial(hook_post_partition,
                                             states, STEP_RELINK)
        relink_hashes_filter = partial(hashes_filter, next_part_power)

        locations = audit_location_generator(
            devices,
            datadir,
            mount_check=mount_check,
            devices_filter=relink_devices_filter,
            hook_pre_device=relink_hook_pre_device,
            hook_post_device=relink_hook_post_device,
            partitions_filter=relink_partition_filter,
            hook_post_partition=relink_hook_post_partition,
            hashes_filter=relink_hashes_filter)
        for fname, _, _ in locations:
            newfname = replace_partition_in_path(fname, next_part_power)
            try:
                diskfile.relink_paths(fname, newfname, check_existing=True)
                relinked += 1
            except OSError as exc:
                errors += 1
                logger.warning("Relinking %s to %s failed: %s",
                               fname, newfname, exc)

    if not run:
        logger.warning("No policy found to increase the partition power.")
        return 2
    logging.info('Relinked %d diskfiles (%d errors)', relinked, errors)
    if errors > 0:
        return 1
    return 0


def cleanup(swift_dir='/etc/swift',
            devices='/srv/node',
            skip_mount_check=False,
            logger=logging.getLogger(),
            device=None):
    mount_check = not skip_mount_check
    conf = {'devices': devices, 'mount_check': mount_check}
    diskfile_router = diskfile.DiskFileRouter(conf, get_logger(conf))
    errors = cleaned_up = 0
    run = False
    for policy in POLICIES:
        policy.object_ring = None  # Ensure it will be reloaded
        policy.load_ring(swift_dir)
        part_power = policy.object_ring.part_power
        next_part_power = policy.object_ring.next_part_power
        if not next_part_power or next_part_power != part_power:
            continue
        logging.info('Cleaning up files for policy %s under %s',
                     policy.name, devices)
        run = True
        datadir = diskfile.get_data_dir(policy)

        locks = [None]
        states = {}
        cleanup_devices_filter = partial(devices_filter, device)
        cleanup_hook_pre_device = partial(hook_pre_device, locks, states,
                                          datadir)
        cleanup_hook_post_device = partial(hook_post_device, locks)
        cleanup_partition_filter = partial(partitions_filter,
                                           states, STEP_CLEANUP,
                                           part_power, next_part_power)
        cleanup_hook_post_partition = partial(hook_post_partition,
                                              states, STEP_CLEANUP)
        cleanup_hashes_filter = partial(hashes_filter, next_part_power)

        locations = audit_location_generator(
            devices,
            datadir,
            mount_check=mount_check,
            devices_filter=cleanup_devices_filter,
            hook_pre_device=cleanup_hook_pre_device,
            hook_post_device=cleanup_hook_post_device,
            partitions_filter=cleanup_partition_filter,
            hook_post_partition=cleanup_hook_post_partition,
            hashes_filter=cleanup_hashes_filter)
        for fname, device, partition in locations:
            expected_fname = replace_partition_in_path(fname, part_power)
            if fname == expected_fname:
                continue
            # Make sure there is a valid object file in the expected new
            # location. Note that this could be newer than the original one
            # (which happens if there is another PUT after partition power
            # has been increased, but cleanup did not yet run)
            loc = diskfile.AuditLocation(
                os.path.dirname(expected_fname), device, partition, policy)
            diskfile_mgr = diskfile_router[policy]
            df = diskfile_mgr.get_diskfile_from_audit_location(loc)
            try:
                with df.open():
                    pass
            except DiskFileQuarantined as exc:
                logger.warning('ERROR Object %(obj)s failed audit and was'
                               ' quarantined: %(err)r',
                               {'obj': loc, 'err': exc})
                errors += 1
                continue
            except DiskFileDeleted:
                pass
            except DiskFileNotExist as exc:
                err = False
                if policy.policy_type == 'erasure_coding':
                    # Might be a non-durable fragment - check that there is
                    # a fragment in the new path. Will be fixed by the
                    # reconstructor then
                    if not os.path.isfile(expected_fname):
                        err = True
                else:
                    err = True
                if err:
                    logger.warning(
                        'Error cleaning up %s: %r', fname, exc)
                    errors += 1
                    continue
            try:
                os.remove(fname)
                cleaned_up += 1
                logging.debug("Removed %s", fname)
            except OSError as exc:
                logger.warning('Error cleaning up %s: %r', fname, exc)
                errors += 1

    if not run:
        logger.warning("No policy found to increase the partition power.")
        return 2
    logging.info('Cleaned up %d diskfiles (%d errors)', cleaned_up, errors)
    if errors > 0:
        return 1
    return 0


def main(args):
    logging.basicConfig(
        format='%(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
        filename=args.logfile)

    logger = logging.getLogger()

    if args.action == 'relink':
        return relink(
            args.swift_dir, args.devices, args.skip_mount_check, logger,
            device=args.device)

    if args.action == 'cleanup':
        return cleanup(
            args.swift_dir, args.devices, args.skip_mount_check, logger,
            device=args.device)
