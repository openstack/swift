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
import os.path
import pwd
import re

from swift.common.storage_policy import split_policy_string
from swift.obj.fmgr_pb2 import VOLUME_DEFAULT, VOLUME_TOMBSTONE

# regex to extract policy from path (one KV per policy)
# TODO: use split_policy_string or similar, not re
policy_re = re.compile(r"^objects(-\d+)?$")
volume_name_re = re.compile(r"^v\d{7}$")
losf_name_re = re.compile(r"^losf(-\d+)?$")


class VIOError(IOError):
    """
    Exceptions are part of the interface, subclass IOError to make it easier
    to interface with diskfile.py
    """


class VOSError(OSError):
    """
    Exceptions are part of the interface, subclass OSError to make it easier
    to interface with diskfile.py
    """


class VFileException(Exception):
    pass


def get_volume_type(extension):
    ext_map = {
        ".ts": VOLUME_TOMBSTONE
    }

    return ext_map.get(extension, VOLUME_DEFAULT)


def valid_volume_name(name):
    """Returns True if name is a valid volume name, False otherwise"""
    if volume_name_re.match(name):
        return True
    else:
        return False


def valid_losf_name(name):
    """Returns True if name is a valid losf dir name, False otherwise"""
    if losf_name_re.match(name):
        return True
    else:
        return False


# used by "fsck" to get the socket path from the volume path
def get_socket_path_from_volume_path(volume_path):
    volume_path = os.path.normpath(volume_path)
    volume_dir_path, volume_name = os.path.split(volume_path)
    losf_path, volume_dir = os.path.split(volume_dir_path)
    mount_path, losf_dir = os.path.split(losf_path)
    if volume_dir != "volumes" or not valid_volume_name(volume_name) or \
            not valid_losf_name(losf_dir):
        raise ValueError("Invalid volume path")

    socket_path = os.path.join(losf_path, "rpc.socket")
    return socket_path


def get_mountpoint_from_volume_path(volume_path):
    volume_path = os.path.normpath(volume_path)
    volume_dir_path, volume_name = os.path.split(volume_path)
    losf_path, volume_dir = os.path.split(volume_dir_path)
    mount_path, losf_dir = os.path.split(losf_path)
    if volume_dir != "volumes" or not valid_volume_name(volume_name) or \
            not valid_losf_name(losf_dir):
        raise ValueError("Invalid volume path")
    return mount_path


class SwiftPathInfo(object):
    def __init__(self, type, socket_path=None, volume_dir=None,
                 policy_idx=None, partition=None, suffix=None, ohash=None,
                 filename=None):
        self.type = type
        self.socket_path = socket_path
        self.volume_dir = volume_dir
        self.policy_idx = policy_idx
        self.partition = partition
        self.suffix = suffix
        self.ohash = ohash
        self.filename = filename

    # parses a swift path, returns a SwiftPathInfo instance
    @classmethod
    def from_path(cls, path):
        count_to_type = {
            4: "file",
            3: "ohash",
            2: "suffix",
            1: "partition",
            0: "partitions"  # "objects" directory
        }

        clean_path = os.path.normpath(path)
        ldir = clean_path.split(os.sep)

        try:
            obj_idx = [i for i, elem in enumerate(ldir)
                       if elem.startswith("objects")][0]
        except IndexError:
            raise VOSError("cannot parse object directory")

        elements = ldir[(obj_idx + 1):]
        count = len(elements)

        if count > 4:
            raise VOSError("cannot parse swift file path")

        _, policy = split_policy_string(ldir[obj_idx])
        policy_idx = policy.idx

        prefix = os.path.join("/", *ldir[0:obj_idx])
        m = policy_re.match(ldir[obj_idx])
        if not m:
            raise VOSError(
                "cannot parse object element of directory")
        if m.group(1):
            sofsdir = "losf{}".format(m.group(1))
        else:
            sofsdir = "losf"
        socket_path = os.path.join(prefix, sofsdir, "rpc.socket")
        volume_dir = os.path.join(prefix, sofsdir, "volumes")

        type = count_to_type[count]
        return cls(type, socket_path, volume_dir, policy_idx, *elements)


class SwiftQuarantinedPathInfo(object):
    def __init__(self, type, socket_path=None, volume_dir=None,
                 policy_idx=None, ohash=None, filename=None):
        self.type = type
        self.socket_path = socket_path
        self.volume_dir = volume_dir
        self.policy_idx = policy_idx
        self.ohash = ohash
        self.filename = filename

    # parses a quarantined path (<device>/quarantined/objects-X or below),
    # returns a SwiftQuarantinedPathInfo instance
    @classmethod
    def from_path(cls, path):
        count_to_type = {
            3: "file",
            2: "ohash",
            1: "ohashes",
        }

        clean_path = os.path.normpath(path)
        ldir = clean_path.split(os.sep)

        try:
            quar_idx = ldir.index("quarantined")
        except ValueError:
            raise VOSError("cannot parse quarantined path %s" %
                           path)

        elements = ldir[(quar_idx + 1):]
        count = len(elements)

        if count < 1 or count > 3 or "objects" not in elements[0]:
            raise VOSError("cannot parse quarantined path %s" %
                           path)

        _, policy = split_policy_string(elements[0])
        policy_idx = policy.idx

        prefix = os.path.join("/", *ldir[:quar_idx])
        prefix = os.path.join(prefix, elements[0].replace("objects", "losf"))
        socket_path = os.path.join(prefix, "rpc.socket")
        volume_dir = os.path.join(prefix, "volumes")

        type = count_to_type[count]
        return cls(type, socket_path, volume_dir, policy_idx, *elements[1:])


def get_volume_index(volume_path):
    """
    returns the volume index, either from its basename, or full path
    """
    name = os.path.split(volume_path)[1]

    if not valid_volume_name(name):
        raise ValueError("Invalid volume name")

    index = int(name[1:8])
    return index


# given an offset and alignement, returns the next aligned offset
def next_aligned_offset(offset, alignment):
    if offset % alignment != 0:
        return (offset + (alignment - offset % alignment))
    else:
        return offset


def change_user(username):
    pw = pwd.getpwnam(username)
    uid = pw.pw_uid
    os.setuid(uid)
