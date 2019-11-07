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
import six
import os
import struct

from swift.common.utils import fdatasync

PICKLE_PROTOCOL = 2

# header version to use for new objects
OBJECT_HEADER_VERSION = 4
VOLUME_HEADER_VERSION = 1

# maximum serialized header length
MAX_OBJECT_HEADER_LEN = 512
MAX_VOLUME_HEADER_LEN = 128

OBJECT_START_MARKER = b"SWIFTOBJ"
VOLUME_START_MARKER = b"SWIFTVOL"

# object alignment within a volume.
# this is needed so that FALLOC_FL_PUNCH_HOLE can actually return space back
# to the filesystem (tested on XFS and ext4)
# we may not need to align files in volumes dedicated to short-lived files,
# such as tombstones (.ts extension),
# but currently we do align for all volume types.
ALIGNMENT = 4096

# constants
STATE_OBJ_FILE = 0
STATE_OBJ_QUARANTINED = 1


class HeaderException(IOError):
    def __init__(self, message):
        self.message = message
        super(HeaderException, self).__init__(message)


object_header_formats = {
    1: '8sBQQQ30sQQQQQ',
    2: '8sBQQQ64sQQQQQ',     # 64 characters for the filename
    3: '8sBQQQ64sQQQQQB',    # add state field
    4: '8sBQQQ64sQQQQQB32s'  # add metadata checksum
}


class ObjectHeader(object):
    """
    Version 1:
        Magic string (8 bytes)
        Header version (1 byte)
        Policy index (8 bytes)
        Object hash (16 bytes) (__init__)
        Filename (30 chars)
        Metadata offset (8 bytes)
        Metadata size (8 bytes)
        Data offset (8 bytes)
        Data size (8 bytes)
        Total object size (8 bytes)

    Version 2: similar but 64 chars for the filename
    Version 3: Adds a "state" field (unsigned char)
    """

    def __init__(self, version=OBJECT_HEADER_VERSION):
        if version not in object_header_formats.keys():
            raise HeaderException('Unsupported object header version')
        self.magic_string = OBJECT_START_MARKER
        self.version = version

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __len__(self):
        try:
            fmt = object_header_formats[self.version]
        except KeyError:
            raise HeaderException('Unsupported header version')
        return struct.calcsize(fmt)

    def pack(self):
        version_to_pack = {
            1: self.__pack_v1,
            2: self.__pack_v2,
            3: self.__pack_v3,
            4: self.__pack_v4
        }
        return version_to_pack[self.version]()

    def __pack_v1(self):
        fmt = object_header_formats[1]
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename).encode('ascii'),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size)

        return struct.pack(fmt, *args)

    def __pack_v2(self):
        fmt = object_header_formats[2]
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename).encode('ascii'),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size)

        return struct.pack(fmt, *args)

    def __pack_v3(self):
        fmt = object_header_formats[3]
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename).encode('ascii'),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size, self.state)

        return struct.pack(fmt, *args)

    def __pack_v4(self):
        fmt = object_header_formats[4]
        ohash_h = int(self.ohash, 16) >> 64
        ohash_l = int(self.ohash, 16) & 0x0000000000000000ffffffffffffffff

        args = (self.magic_string, self.version,
                self.policy_idx, ohash_h, ohash_l,
                str(self.filename).encode('ascii'),
                self.metadata_offset, self.metadata_size,
                self.data_offset, self.data_size, self.total_size, self.state,
                self.metastr_md5)

        return struct.pack(fmt, *args)

    @classmethod
    def unpack(cls, buf):
        version_to_unpack = {
            1: cls.__unpack_v1,
            2: cls.__unpack_v2,
            3: cls.__unpack_v3,
            4: cls.__unpack_v4
        }

        if buf[0:8] != OBJECT_START_MARKER:
            raise HeaderException('Not a header')
        version = struct.unpack('<B', buf[8:9])[0]
        if version not in object_header_formats.keys():
            raise HeaderException('Unsupported header version')

        return version_to_unpack[version](buf)

    @classmethod
    def __unpack_v1(cls, buf):
        fmt = object_header_formats[1]
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = cls()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        if six.PY2:
            header.filename = raw_header[5].rstrip(b'\0')
        else:
            header.filename = raw_header[5].rstrip(b'\0').decode('ascii')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]

        return header

    @classmethod
    def __unpack_v2(cls, buf):
        fmt = object_header_formats[2]
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = cls()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        if six.PY2:
            header.filename = raw_header[5].rstrip(b'\0')
        else:
            header.filename = raw_header[5].rstrip(b'\0').decode('ascii')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]

        return header

    @classmethod
    def __unpack_v3(cls, buf):
        fmt = object_header_formats[3]
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = cls()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        if six.PY2:
            header.filename = raw_header[5].rstrip(b'\0')
        else:
            header.filename = raw_header[5].rstrip(b'\0').decode('ascii')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]
        header.state = raw_header[11]

        return header

    @classmethod
    def __unpack_v4(cls, buf):
        fmt = object_header_formats[4]
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = cls()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.policy_idx = raw_header[2]
        header.ohash = "{:032x}".format((raw_header[3] << 64) + raw_header[4])
        if six.PY2:
            header.filename = raw_header[5].rstrip(b'\0')
        else:
            header.filename = raw_header[5].rstrip(b'\0').decode('ascii')
        header.metadata_offset = raw_header[6]
        header.metadata_size = raw_header[7]
        header.data_offset = raw_header[8]
        header.data_size = raw_header[9]
        # currently, total_size gets padded to the next 4k boundary, so that
        # fallocate can reclaim the block when hole punching.
        header.total_size = raw_header[10]
        header.state = raw_header[11]
        header.metastr_md5 = raw_header[12]

        return header


volume_header_formats = {
    1: '8sBQQQQLQ'
}


class VolumeHeader(object):
    """
    Version 1:
        Magic string (8 bytes)
        Header version (1 byte)
        Volume index (8 bytes)
        Partition index (8 bytes)
        Volume type (8 bytes)
        First object offset (8 bytes)
        Volume state (4 bytes) (enum from fmgr.proto)
        Volume compaction target (8 bytes)
            (only valid if state is STATE_COMPACTION_SRC)
    """
    def __init__(self, version=VOLUME_HEADER_VERSION):
        self.magic_string = VOLUME_START_MARKER
        self.version = version
        self.state = 0
        self.compaction_target = 0

    def __str__(self):
        prop_list = ['volume_idx', 'partition', 'type',
                     'state', 'compaction_target']
        h_str = ""
        for prop in prop_list:
            h_str += "{}: {}\n".format(prop, getattr(self, prop))
        return h_str[:-1]

    def __len__(self):
        try:
            fmt = volume_header_formats[self.version]
        except KeyError:
            raise HeaderException('Unsupported header version')
        return struct.calcsize(fmt)

    def pack(self):
        version_to_pack = {
            1: self.__pack_v1,
        }
        return version_to_pack[self.version]()

    def __pack_v1(self):
        fmt = volume_header_formats[1]

        args = (self.magic_string, self.version,
                self.volume_idx, self.partition, self.type,
                self.first_obj_offset, self.state,
                self.compaction_target)

        return struct.pack(fmt, *args)

    @classmethod
    def unpack(cls, buf):
        version_to_unpack = {
            1: cls.__unpack_v1
        }
        if buf[0:8] != VOLUME_START_MARKER:
            raise HeaderException('Not a header')
        version = struct.unpack('<B', buf[8:9])[0]
        if version not in volume_header_formats.keys():
            raise HeaderException('Unsupported header version')

        return version_to_unpack[version](buf)

    @classmethod
    def __unpack_v1(cls, buf):
        fmt = volume_header_formats[1]
        raw_header = struct.unpack(fmt, buf[0:struct.calcsize(fmt)])
        header = cls()
        header.magic_string = raw_header[0]
        header.version = raw_header[1]
        header.volume_idx = raw_header[2]
        header.partition = raw_header[3]
        header.type = raw_header[4]
        header.first_obj_offset = raw_header[5]
        header.state = raw_header[6]
        header.compaction_target = raw_header[7]

        return header


# Read volume header. Expects fp to be positionned at header offset
def read_volume_header(fp):
    buf = fp.read(MAX_VOLUME_HEADER_LEN)
    header = VolumeHeader.unpack(buf)
    return header


def write_volume_header(header, fd):
    os.write(fd, header.pack())


def read_object_header(fp):
    """
    Read object header
    :param fp: opened file, positioned at header start
    :return: an ObjectHeader
    """
    buf = fp.read(MAX_OBJECT_HEADER_LEN)
    header = ObjectHeader.unpack(buf)
    return header


def write_object_header(header, fp):
    """
    Rewrites header in open file
    :param header: header to write
    :param fp: opened volume
    """
    fp.write(header.pack())
    fdatasync(fp.fileno())


def erase_object_header(fd, offset):
    """
    Erase an object header by writing null bytes over it
    :param fd: volume file descriptor
    :param offset: absolute header offset
    """
    os.lseek(fd, offset, os.SEEK_SET)
    os.write(fd, b"\x00" * MAX_OBJECT_HEADER_LEN)
