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


import logging
import os
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import DiskFileDeleted, DiskFileNotExist, \
    DiskFileQuarantined
from swift.common.utils import replace_partition_in_path, \
    audit_location_generator, get_logger
from swift.obj import diskfile


def relink(swift_dir='/etc/swift',
           devices='/srv/node',
           skip_mount_check=False,
           logger=logging.getLogger()):
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
        locations = audit_location_generator(
            devices,
            diskfile.get_data_dir(policy),
            mount_check=mount_check)
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
            logger=logging.getLogger()):
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
        locations = audit_location_generator(
            devices,
            diskfile.get_data_dir(policy),
            mount_check=mount_check)
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
            args.swift_dir, args.devices, args.skip_mount_check, logger)

    if args.action == 'cleanup':
        return cleanup(
            args.swift_dir, args.devices, args.skip_mount_check, logger)
