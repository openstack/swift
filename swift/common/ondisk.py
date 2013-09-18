# Copyright (c) 2010-2013 OpenStack, LLC.
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

"""Methods & Attributes for shared 'on-disk' data layouts."""

import os
import sys
import errno

from hashlib import md5
from random import shuffle
from ConfigParser import ConfigParser, NoSectionError, NoOptionError

from swift import gettext_ as _
from swift.common.utils import listdir, quote

# Used by hash_path to offer a bit more security when generating hashes for
# paths. It simply appends this value to all paths; guessing the hash a path
# will end up with would also require knowing this suffix.
_hash_conf = ConfigParser()
HASH_PATH_SUFFIX = ''
HASH_PATH_PREFIX = ''
if _hash_conf.read('/etc/swift/swift.conf'):
    try:
        HASH_PATH_SUFFIX = _hash_conf.get('swift-hash',
                                          'swift_hash_path_suffix')
    except (NoSectionError, NoOptionError):
        pass
    try:
        HASH_PATH_PREFIX = _hash_conf.get('swift-hash',
                                          'swift_hash_path_prefix')
    except (NoSectionError, NoOptionError):
        pass


def validate_configuration():
    if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
        sys.exit("Error: [swift-hash]: both swift_hash_path_suffix "
                 "and swift_hash_path_prefix are missing "
                 "from /etc/swift/swift.conf")


def hash_path(account, container=None, object=None, raw_digest=False):
    """
    Get the canonical hash for an account/container/object

    :param account: Account
    :param container: Container
    :param object: Object
    :param raw_digest: If True, return the raw version rather than a hex digest
    :returns: hash string
    """
    if object and not container:
        raise ValueError('container is required if object is provided')
    paths = [account]
    if container:
        paths.append(container)
    if object:
        paths.append(object)
    if raw_digest:
        return md5(HASH_PATH_PREFIX + '/' + '/'.join(paths)
                   + HASH_PATH_SUFFIX).digest()
    else:
        return md5(HASH_PATH_PREFIX + '/' + '/'.join(paths)
                   + HASH_PATH_SUFFIX).hexdigest()


def normalize_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps using values greater than or equal to November 20th,
    2286 at 17:46 UTC will use 11 digits to represent the number of
    seconds.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return "%016.05f" % (float(timestamp))


def validate_device_partition(device, partition):
    """
    Validate that a device and a partition are valid and won't lead to
    directory traversal when used.

    :param device: device to validate
    :param partition: partition to validate
    :raises: ValueError if given an invalid device or partition
    """
    invalid_device = False
    invalid_partition = False
    if not device or '/' in device or device in ['.', '..']:
        invalid_device = True
    if not partition or '/' in partition or partition in ['.', '..']:
        invalid_partition = True

    if invalid_device:
        raise ValueError('Invalid device: %s' % quote(device or ''))
    elif invalid_partition:
        raise ValueError('Invalid partition: %s' % quote(partition or ''))


def storage_directory(datadir, partition, name_hash):
    """
    Get the storage directory

    :param datadir: Base data directory
    :param partition: Partition
    :param name_hash: Account, container or object name hash
    :returns: Storage directory
    """
    return os.path.join(datadir, str(partition), name_hash[-3:], name_hash)


def audit_location_generator(devices, datadir, suffix='',
                             mount_check=True, logger=None):
    '''
    Given a devices path and a data directory, yield (path, device,
    partition) for all files in that directory

    :param devices: parent directory of the devices to be audited
    :param datadir: a directory located under self.devices. This should be
                    one of the DATADIR constants defined in the account,
                    container, and object servers.
    :param suffix: path name suffix required for all names returned
    :param mount_check: Flag to check if a mount check should be performed
                    on devices
    :param logger: a logger object
    '''
    device_dir = listdir(devices)
    # randomize devices in case of process restart before sweep completed
    shuffle(device_dir)
    for device in device_dir:
        if mount_check and not \
                os.path.ismount(os.path.join(devices, device)):
            if logger:
                logger.debug(
                    _('Skipping %s as it is not mounted'), device)
            continue
        datadir_path = os.path.join(devices, device, datadir)
        partitions = listdir(datadir_path)
        for partition in partitions:
            part_path = os.path.join(datadir_path, partition)
            try:
                suffixes = listdir(part_path)
            except OSError as e:
                if e.errno != errno.ENOTDIR:
                    raise
                continue
            for asuffix in suffixes:
                suff_path = os.path.join(part_path, asuffix)
                try:
                    hashes = listdir(suff_path)
                except OSError as e:
                    if e.errno != errno.ENOTDIR:
                        raise
                    continue
                for hsh in hashes:
                    hash_path = os.path.join(suff_path, hsh)
                    try:
                        files = sorted(listdir(hash_path), reverse=True)
                    except OSError as e:
                        if e.errno != errno.ENOTDIR:
                            raise
                        continue
                    for fname in files:
                        if suffix and not fname.endswith(suffix):
                            continue
                        path = os.path.join(hash_path, fname)
                        yield path, device, partition
