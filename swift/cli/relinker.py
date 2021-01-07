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

    if step in (STEP_RELINK, STEP_CLEANUP):
        states["state"][part] = True
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
        logger.info('Relinking files for policy %s under %s',
                    policy.name, devices)
        run = True
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
            hashes_filter=relink_hashes_filter,
            logger=logger)
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
    logger.info('Relinked %d diskfiles (%d errors)', relinked, errors)
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
        logger.info('Cleaning up files for policy %s under %s',
                    policy.name, devices)
        run = True
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
            hashes_filter=cleanup_hashes_filter,
            logger=logger)
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
                logger.debug("Removed %s", fname)
            except OSError as exc:
                logger.warning('Error cleaning up %s: %r', fname, exc)
                errors += 1

    if not run:
        logger.warning("No policy found to increase the partition power.")
        return 2
    logger.info('Cleaned up %d diskfiles (%d errors)', cleaned_up, errors)
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
