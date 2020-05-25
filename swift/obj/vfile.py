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

"""
A "vfile" is a virtual file stored in a "volume".
A volume is an actual file where vfiles are stored.
vfile names and metadata (xattr) are also stored in the volume.
"""

import errno
import fcntl
import six
import hashlib
import re
from eventlet.green import os
from swift.obj.header import ObjectHeader, VolumeHeader, ALIGNMENT, \
    read_volume_header, HeaderException, STATE_OBJ_QUARANTINED, \
    STATE_OBJ_FILE, write_object_header, \
    read_object_header, OBJECT_HEADER_VERSION, write_volume_header, \
    erase_object_header, MAX_OBJECT_HEADER_LEN
from swift.common.exceptions import DiskFileNoSpace, \
    DiskFileBadMetadataChecksum
from swift.common.storage_policy import POLICIES
from swift.common.utils import fsync, fdatasync, fsync_dir, \
    fallocate
from swift.obj import rpc_http as rpc
from swift.obj.rpc_http import RpcError, StatusCode
from swift.obj.fmgr_pb2 import STATE_RW
from swift.obj.meta_pb2 import Metadata
from swift.obj.diskfile import _encode_metadata
from swift.common import utils
from swift.obj.vfile_utils import SwiftPathInfo, get_volume_index, \
    get_volume_type, next_aligned_offset, SwiftQuarantinedPathInfo, VOSError, \
    VIOError, VFileException

VCREATION_LOCK_NAME = "volume_creation.lock"

PICKLE_PROTOCOL = 2
METADATA_RESERVE = 500
VOL_AND_LOCKS_RE = re.compile(r'v\d{7}(.writelock)?')


def increment(logger, counter, count=1):
    if logger is not None:
        try:
            logger.update_stats(counter, count)
        except Exception:
            pass


class VFileReader(object):
    """
    Represents a vfile stored in a volume.
    """
    def __init__(self, fp, name, offset, header, metadata, logger):
        self.fp = fp
        self.name = name
        self.offset = offset
        self._header = header
        self.metadata = metadata
        self.logger = logger

    @property
    def data_size(self):
        return self._header.data_size

    @classmethod
    def get_vfile(cls, filepath, logger):
        """
        Returns a VFileReader instance from the path expected by swift
        :param filepath: full path to file
        :param logger: a logger object
        """
        si = SwiftPathInfo.from_path(filepath)
        if si.type != "file":
            err_msg = "Not a path to a swift file ({})".format(filepath)
            raise VIOError(errno.EINVAL, err_msg)

        full_name = si.ohash + si.filename
        return cls._get_vfile(full_name, si.volume_dir, si.socket_path, logger)

    @classmethod
    def get_quarantined_vfile(cls, filepath, logger):
        si = SwiftQuarantinedPathInfo.from_path(filepath)

        if si.type != "file":
            err_msg = "Not a path to a swift file ({})".format(filepath)
            raise VIOError(errno.EINVAL, err_msg)

        full_name = si.ohash + si.filename
        return cls._get_vfile(full_name, si.volume_dir, si.socket_path, logger,
                              is_quarantined=True)

    @classmethod
    def _get_vfile(cls, name, volume_dir, socket_path, logger,
                   is_quarantined=False, repair_tool=False):
        """
        Returns a VFileReader instance
        :param name: full name: object hash+filename
        :param volume_dir: directory where the volume is stored
        :param socket_path: full path to KV socket
        :param logger: logger object
        :param is_quarantined: object is quarantined
        :param repair_tool: True if requests comes from a repair tool
        """
        # get the object
        try:
            obj = rpc.get_object(socket_path, name,
                                 is_quarantined=is_quarantined,
                                 repair_tool=repair_tool)
        except RpcError as e:
            if e.code == StatusCode.NotFound:
                raise VIOError(errno.ENOENT,
                               "No such file or directory: {}".format(name))
            # May need to handle more cases ?
            raise (e)

        # get the volume file name from the object
        volume_filename = get_volume_name(obj.volume_index)
        volume_filepath = os.path.join(volume_dir, volume_filename)

        fp = open(volume_filepath, 'rb')
        fp.seek(obj.offset)
        try:
            header = read_object_header(fp)
        except HeaderException:
            fp.seek(obj.offset)
            data = fp.read(512)
            if all(c == '\x00' for c in data):
                # unregister the object here
                rpc.unregister_object(socket_path, name)
                msg = "Zeroed header found for {} at offset {} in volume\
                        {}".format(name, obj.offset, volume_filepath)
                increment(logger, 'vfile.already_punched')
                raise VFileException(msg)
            msg = "Failed to read header for {} at offset {} in volume\
                   {}".format(name, obj.offset, volume_filepath)
            raise VIOError(errno.EIO, msg)

        # check that we have the object we were expecting
        header_fullname = "{}{}".format(header.ohash, header.filename)
        if header_fullname != name:
            # until we journal the renames, after a crash we may not have the
            # rename in the KV. Handle this here for now
            non_durable_name = re.sub(r'(#\d+)#d.', r'\1.', header_fullname)
            if non_durable_name == name:
                increment(logger, 'vfile.already_renamed')
                rpc.rename_object(socket_path, name, header_fullname)
            else:
                increment(logger, 'vfile.wrong_object_header_name')
                raise VIOError(errno.EIO,
                               "Wrong object header name. Header: {} Expected:\
                                {}".format(header_fullname, name))

        metadata = read_metadata(fp, obj.offset, header)

        # seek to beginning of data
        fp.seek(obj.offset + header.data_offset)

        return cls(fp, obj.name, obj.offset, header, metadata, logger)

    def read(self, size=None):
        """
        Wraps read to prevent reading beyond the vfile content.
        """
        curpos = self.fp.tell()
        data_size = self._header.data_size
        data_start_offset = self.offset + self._header.data_offset
        data_end_offset = data_start_offset + data_size

        if curpos >= data_end_offset or size == 0:
            return ''
        if size:
            if size > data_end_offset - curpos:
                size = data_end_offset - curpos
        else:
            size = data_end_offset - curpos

        buf = self.fp.read(size)
        return buf

    def seek(self, pos):
        """
        Wraps seek to bind offset from the vfile start to its end.
        """
        real_data_offset = self.offset + self._header.data_offset
        real_new_pos = real_data_offset + pos
        if (real_new_pos < real_data_offset or
                real_new_pos > real_data_offset + self._header.data_size):
            raise VIOError(errno.EINVAL, "Invalid seek")
        self.fp.seek(real_new_pos)

    def tell(self):
        curpos = self.fp.tell()
        vpos = curpos - (self.offset + self._header.data_offset)
        return vpos

    def close(self):
        self.fp.close()


def _may_grow_volume(volume_fd, volume_offset, obj_size, conf, logger):
    """
    Grows a volume if needed.
    if free_space < obj_size + object header len, allocate obj_size padded to
    volume_alloc_chunk_size

    """
    volume_alloc_chunk_size = conf['volume_alloc_chunk_size']

    if obj_size is None:
        obj_size = 0

    volume_size = os.lseek(volume_fd, 0, os.SEEK_END)
    free_space = volume_size - volume_offset

    obj_header_len = len(ObjectHeader(version=OBJECT_HEADER_VERSION))
    required_space = obj_header_len + obj_size

    if free_space < required_space:
        _allocate_volume_space(volume_fd, volume_offset, required_space,
                               volume_alloc_chunk_size, logger)


class VFileWriter(object):
    def __init__(self, datadir, fd, lock_fd, volume_dir,
                 volume_index, header, offset, logger):
        si = SwiftPathInfo.from_path(datadir)

        self.fd = fd
        self.lock_fd = lock_fd
        self.volume_dir = volume_dir
        self.volume_index = volume_index
        self.header = header
        self.offset = offset
        self.socket_path = si.socket_path
        self.partition = si.partition
        # may be used for statsd. Do not use it to log or it will hang the
        # object-server process. (eventlet)
        self.logger = logger

    @classmethod
    def create(cls, datadir, obj_size, conf, logger, extension=None):
        # parse datadir
        si = SwiftPathInfo.from_path(datadir)

        if si.type != "ohash":
            raise VOSError("not a valid object hash path")

        if obj_size is not None:
            if obj_size < 0:
                raise VOSError("obj size may not be negative")

        socket_path = os.path.normpath(si.socket_path)
        volume_dir = os.path.normpath(si.volume_dir)

        # get a writable volume
        # TODO : check that we fallocate enough if obj_size > volume
        #        chunk alloc size
        volume_file, lock_file, volume_path = open_or_create_volume(
            socket_path, si.partition, extension, volume_dir,
            conf, logger, size=obj_size)
        volume_index = get_volume_index(volume_path)

        # create object header
        header = ObjectHeader(version=OBJECT_HEADER_VERSION)
        # TODO: this is unused, always set to zero.
        header.ohash = si.ohash
        header.policy_idx = 0
        header.data_offset = len(header) + conf['metadata_reserve']
        header.data_size = 0
        # requires header v3
        header.state = STATE_OBJ_FILE

        try:
            # get offset at which to start writing
            offset = rpc.get_next_offset(socket_path, volume_index)

            # pre-allocate space if needed
            _may_grow_volume(volume_file, offset, obj_size, conf, logger)

            # seek to absolute object offset + relative data offset
            # (we leave space for the header and some metadata)
            os.lseek(volume_file, offset + header.data_offset,
                     os.SEEK_SET)
        except Exception:
            os.close(volume_file)
            os.close(lock_file)
            raise

        return cls(datadir, volume_file, lock_file, volume_dir,
                   volume_index, header, offset, logger)

    def commit(self, filename, metadata):
        """
        Write the header, metadata, sync, and register vfile in KV.
        """
        if self.fd < 0:
            raise VIOError(errno.EBADF, "Bad file descriptor")

        if not filename:
            raise VIOError("filename cannot be empty")

        # how much data has been written ?
        # header.data_offset is relative to the object's offset
        data_offset = self.offset + self.header.data_offset
        data_end = os.lseek(self.fd, 0, os.SEEK_CUR)
        self.header.data_size = data_end - data_offset
        # FIXME: this is unused message, please fix as expected
        # txt = "commit: {} data_end {} data_offset: {}"

        self.header.filename = filename
        # metastr = pickle.dumps(self.metadata, PICKLE_PROTOCOL)
        # create and populate protobuf object
        meta = Metadata()
        enc_metadata = _encode_metadata(metadata)
        for k, v in enc_metadata.items():
            meta.attrs.add(key=k, value=v)

        metastr = meta.SerializeToString()
        metastr_md5 = hashlib.md5(metastr).hexdigest().encode('ascii')

        self.header.metadata_size = len(metastr)
        self.header.metadata_offset = len(self.header)
        self.header.metastr_md5 = metastr_md5

        # calculate the end object offset (this includes the padding, if any)
        # start from data_end, and add: metadata remainder if any, footer,
        # padding

        # how much reserved metadata space do we have ?
        # Should be equal to "metadata_reserve"
        metadata_available_space = (self.header.data_offset -
                                    self.header.metadata_offset)
        metadata_remainder = max(0, self.header.metadata_size -
                                 metadata_available_space)

        object_end = data_end + metadata_remainder

        object_end = next_aligned_offset(object_end, ALIGNMENT)

        self.header.total_size = object_end - self.offset

        # write header
        os.lseek(self.fd, self.offset, os.SEEK_SET)
        os.write(self.fd, self.header.pack())

        # write metadata, and footer
        metadata_offset = self.offset + self.header.metadata_offset
        if self.header.metadata_size > metadata_available_space:
            os.lseek(self.fd, metadata_offset, os.SEEK_SET)
            os.write(self.fd, metastr[:metadata_available_space])
            # metadata does not fit in reserved space,
            # write the remainder after the data
            os.lseek(self.fd, data_end, os.SEEK_SET)
            os.write(self.fd, metastr[metadata_available_space:])
        else:
            os.lseek(self.fd, metadata_offset, os.SEEK_SET)
            os.write(self.fd, metastr)

        # Sanity check, we should not go beyond object_end
        curpos = os.lseek(self.fd, 0, os.SEEK_CUR)
        if curpos > object_end:
            errtxt = "BUG: wrote past object_end! curpos: {} object_end: {}"
            raise Exception(errtxt.format(curpos, object_end))

        # sync data. fdatasync() is enough, if the volume was just created,
        # it has been fsync()'ed previously, along with its parent directory.
        fdatasync(self.fd)

        # register object
        full_name = "{}{}".format(self.header.ohash, filename)
        try:
            rpc.register_object(self.socket_path, full_name, self.volume_index,
                                self.offset, object_end)
        except RpcError:
            # If we failed to register the object, erase the header so that it
            # will not be picked up by the volume checker if there is a crash
            # or power failure before it gets overwritten by another object.
            erase_object_header(self.fd, self.offset)
            raise

        increment(self.logger, 'vfile.vfile_creation')
        increment(self.logger, 'vfile.total_space_used',
                  self.header.total_size)


def open_or_create_volume(socket_path, partition, extension, volume_dir,
                          conf, logger, size=0):
    """
    Tries to open or create a volume for writing. If a volume cannot be
    opened or created, a VOSError exception is raised.
    :return: volume file descriptor, lock file descriptor, absolute path
    to volume.
    """
    volume_file, lock_file, volume_path = open_writable_volume(socket_path,
                                                               partition,
                                                               extension,
                                                               volume_dir,
                                                               conf,
                                                               logger)
    if not volume_file:
        # attempt to create new volume for partition
        try:
            volume_file, lock_file, volume_path = create_writable_volume(
                socket_path, partition, extension, volume_dir,
                conf, logger, size=size)
        except Exception as err:
            error_msg = "Failed to open or create a volume for writing: "
            error_msg += getattr(err, "strerror", "Unknown error")
            raise VOSError(errno.ENOSPC, error_msg)

    return volume_file, lock_file, volume_path


def _create_new_lock_file(volume_dir, logger):
    creation_lock_path = os.path.join(volume_dir, VCREATION_LOCK_NAME)
    with open(creation_lock_path, 'w') as creation_lock_file:
        # this may block
        fcntl.flock(creation_lock_file, fcntl.LOCK_EX)

        index = get_next_volume_index(volume_dir)
        next_lock_name = get_lock_file_name(index)
        next_lock_path = os.path.join(volume_dir, next_lock_name)

        try:
            lock_file = os.open(next_lock_path,
                                os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except OSError:
            increment(logger, 'vfile.volume_creation.fail_other')
            raise

        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            increment(logger, 'vfile.volume_creation.fail_other')
            os.close(lock_file)
            os.unlink(next_lock_path)
            raise

    return index, next_lock_path, lock_file


# create a new volume
def create_writable_volume(socket_path, partition, extension, volume_dir,
                           conf, logger, state=STATE_RW, size=0):
    """
    Creates a new writable volume, and associated lock file.
    returns a volume_file, a lock_file, and the index of the volume that has
    been created.
    If the extension is specified, a specific volume may be used.
    (Currently, .ts files go to separate volumes as they are short-lived, in
    order to limit fragmentation)
    state can be STATE_RW (new RW volume) or STATE_COMPACTION_TARGET (new empty
    volume which will be used for compaction, and to which new objects cannot
    be written).
    size is the space that should be allocated to the volume (in addition to
    the volume header)
    """

    if size is None:
        size = 0

    # Check if we have exceeded the allowed volume count for this partition
    # Move this check below with the lock held ? (now, we may have
    # a few extra volumes)
    volume_type = get_volume_type(extension)
    volumes = rpc.list_volumes(socket_path, partition, volume_type)
    max_volume_count = conf['max_volume_count']
    if len(volumes) >= max_volume_count:
        err_txt = ("Maximum count of volumes reached for partition:"
                   " {} type: {}".format(partition, volume_type))
        increment(logger, 'vfile.volume_creation.fail_count_exceeded')
        raise VOSError(errno.EDQUOT, err_txt)

    try:
        os.makedirs(volume_dir)
    except OSError as err:
        if err.errno == errno.EEXIST:
            pass
        else:
            raise

    index, next_lock_path, lock_file = _create_new_lock_file(
        volume_dir, logger)

    # create the volume
    next_volume_name = get_volume_name(index)
    next_volume_path = os.path.join(volume_dir, next_volume_name)

    vol_header = VolumeHeader()
    vol_header.volume_idx = index
    vol_header.type = volume_type
    vol_header.partition = int(partition)
    # first object alignment
    vol_header.first_obj_offset = len(vol_header) + (
        ALIGNMENT - len(vol_header) % ALIGNMENT)
    vol_header.state = state

    # How much space is needed for the object ? (assuming metadata fits in the
    # reserved space, but we cannot know this in advance)
    alloc_size = vol_header.first_obj_offset + size
    volume_alloc_chunk_size = conf['volume_alloc_chunk_size']

    try:
        volume_file = os.open(next_volume_path,
                              os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        _allocate_volume_space(volume_file, 0, alloc_size,
                               volume_alloc_chunk_size, logger)

        # Write volume header
        write_volume_header(vol_header, volume_file)

        # If the uploader is slow to send data to the object server, a crash
        # may occur before the object is received and a call to fsync() is
        # issued. We end up with volumes without a header.
        # Issue a fsync() here, at the cost of performance early on. As
        # partitions get volumes we switch to open_writable_volume, avoiding
        # the fsync.
        fsync(volume_file)
        fsync_dir(volume_dir)

        # Register volume
        rpc.register_volume(socket_path, partition, vol_header.type, index,
                            vol_header.first_obj_offset, vol_header.state)
    except Exception:
        os.close(lock_file)
        os.close(volume_file)
        os.unlink(next_lock_path)
        os.unlink(next_volume_path)
        increment(logger, 'vfile.volume_creation.fail_other')
        raise

    increment(logger, 'vfile.volume_creation.ok')
    return volume_file, lock_file, next_volume_path


def _allocate_volume_space(volume_fd, offset, length, volume_alloc_chunk_size,
                           logger, ignore_error=False):
    """
    Will pre-allocate space for the volume given the offset and length,
    aligned to volume_alloc_chunk_size.
    May ignore an OSError
    :param volume_fd: file descriptor of the volume
    :param offset: offset from which to grow the volume
    :param length: length to grow, relative to offset
    :param volume_alloc_chunk_size: pad length to align to this chunk size
    :param ignore_error: ignore OSError
    :return:
    """
    try:
        alloc_size = next_aligned_offset(length, volume_alloc_chunk_size)
        fallocate(volume_fd, alloc_size, offset)
        increment(logger, 'vfile.volume_alloc_space',
                  alloc_size)
    except OSError as err:
        if not ignore_error:
            if err.errno in (errno.ENOSPC, errno.EDQUOT):
                raise DiskFileNoSpace()
            raise


def delete_volume(socket_path, volume_path, logger):
    """
    Deletes a volume from disk and removes entry in the KV
    """
    index = get_volume_index(volume_path)
    volume_lock_path = "{}.writelock".format(volume_path)

    # Remove KV entry
    rpc.unregister_volume(socket_path, index)

    # Remove volume and lock
    os.unlink(volume_path)
    os.unlink(volume_lock_path)


def open_writable_volume(socket_path, partition, extension, volume_dir, conf,
                         logger):
    """
    Opens a volume available for writing.
    returns a volume file, a lock_file, and the volume path
    :param socket_path: full path to KV socket
    :param partition: partition name
    :param extension: file extension
    """
    volume_type = get_volume_type(extension)
    volume_file = None
    lock_file = None
    volume_file_path = None
    # query the KV for available volumes given the partition and type
    volumes = rpc.list_volumes(socket_path, partition, volume_type)

    # writable candidates are volumes which are in RW state and not too large
    volumes = [vol for vol in volumes if vol.volume_state == STATE_RW and
               vol.next_offset < conf['max_volume_size']]
    volume_files = [get_volume_name(volume.volume_index) for volume in
                    volumes]

    for volume_file_name in volume_files:
        volume_file_path = os.path.join(volume_dir, volume_file_name)
        volume_file, lock_file = open_volume(volume_file_path)
        if volume_file:
            break

    return volume_file, lock_file, volume_file_path


def open_volume(volume_path):
    """Locks the volume, and returns a fd to the volume and a fd to its lock
    file. Returns None, None, if it cannot be locked. Raises for any other
    error.
    :param volume_path: full path to volume
    :return: (volume fd, lock fd)
    """
    lock_file_path = "{}.writelock".format(volume_path)

    try:
        lock_file = os.open(lock_file_path, os.O_WRONLY)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        # if the volume lock file as been removed, create it
        lock_file = os.open(lock_file_path,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)

    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as err:
        if err.errno in (errno.EACCES, errno.EAGAIN):
            # volume is locked
            os.close(lock_file)
            return None, None
        else:
            try:
                os.close(lock_file)
            except Exception:
                pass
            raise
    except Exception:
        try:
            os.close(lock_file)
        except Exception:
            pass
        raise

    volume_file = os.open(volume_path, os.O_WRONLY)
    return volume_file, lock_file


def change_volume_state(volume_file_path, state, compaction_target=None):
    """
    Changes the volumes state. caller locks the volume
    TODO: should this handle RPC as well ? (currently done by caller)
    TODO: take an optional size parameter so we can fallocate() the file
    :param volume_file_path: path of volume to modify
    :param state: new state
    :param compaction_target: ID of the volume compaction target, if applicable
    """
    volume_file = open(volume_file_path, "rb+")

    h = read_volume_header(volume_file)
    h.state = state
    if compaction_target:
        h.compaction_target = compaction_target
    volume_file.seek(0)
    volume_file.write(h.pack())


def get_next_volume_index(volume_dir):
    """
    Returns the next volume index to use for the given dir.
    Caller must hold the volume creation lock.
    :param volume_dir: volume directory
    :return: the next volume index to use
    """
    dir_entries = os.listdir(volume_dir)
    # Get all volumes and their lock: a volume should always have a lock,
    # but a fsck may have removed either. If we find such a case, skip the
    # index.
    volumes_and_locks_idxs = set([name[1:8] for name in dir_entries if
                                  VOL_AND_LOCKS_RE.match(name)])
    if len(volumes_and_locks_idxs) < 1:
        return 1

    # This is about 30% faster than calling int() in the list comprehension
    # above.
    idxs = sorted(int(i) for i in volumes_and_locks_idxs)

    # find the first "hole" in the indexes
    for pos, idx in enumerate(idxs, start=1):
        if pos != idx:
            return pos

    # no hole found
    return idx + 1


def get_lock_file_name(index):
    if index <= 0 or index > 9999999:
        raise VFileException("invalid lock file index")
    lock_file_name = "v{0:07d}.writelock".format(index)
    return lock_file_name


def get_volume_name(index):
    if index <= 0 or index > 9999999:
        raise VFileException("invalid volume file index")
    volume_file_name = "v{0:07d}".format(index)
    return volume_file_name


def listdir(path):
    type_to_func = {
        'ohash': _list_ohash,
        'suffix': _list_suffix,
        'partition': _list_partition,
        'partitions': _list_partitions
    }

    path = os.path.normpath(path)
    si = SwiftPathInfo.from_path(path)

    # get part power from the ring (needed as we generate "directories" based
    # on it.
    if not POLICIES[si.policy_idx].object_ring:
        POLICIES[si.policy_idx].load_ring('/etc/swift')
    part_power = 32 - POLICIES[si.policy_idx].object_ring._part_shift

    ret = type_to_func[si.type](si, part_power)
    return [str(e) for e in ret]


def exists(path):
    """
    Similar to os.path.exists
    LOSF manages files below the "objects" directory. if the query is about
    that directory, use os.path.exists, otherwise check in the KV.
    It does not really make sense in LOSF context as callers will then issue a
    "mkdir", which is a noop. But having this means we touch less of the
    existing code. (diskfile, reconstructor, replicator)
    :param path: full path to directory
    :return: True is path exists, False otherwise
    """
    si = SwiftPathInfo.from_path(path)
    if si.type == 'partitions':
        return os.path.exists(path)

    # if a directory is empty, it does not exist
    if listdir(path):
        return True
    else:
        return False

    # Does not handle "file"


def isdir(path):
    """
    Similar to os.path.isdir
    :param path: full path to directory
    :return:
    """
    si = SwiftPathInfo.from_path(path)
    if si.type == 'partitions':
        return os.path.isdir(path)
    if si.type == 'file':
        return False
    if listdir(path):
        return True
    else:
        return False


def isfile(path):
    """
    Similar to os.path.isfile
    :param path: full path to directory
    :return:
    """
    si = SwiftPathInfo.from_path(path)
    if si.type == 'partitions':
        return os.path.isfile(path)
    if si.type == 'file':
        return True
    return False


def mkdirs(path):
    """
    Similar to utils.mkdirs
    Noop, except if the directory is the "objects" directory
    :param path: full path to directory
    """
    si = SwiftPathInfo.from_path(path)
    if si.type == 'partitions':
        return utils.mkdirs(path)


def list_quarantine(quarantine_path):
    """
    Lists all quarantined object hashes for the disk/policy
    :param quarantine_path: quarantined path
    :return: a list of quarantined object hashes
    """
    si = SwiftQuarantinedPathInfo.from_path(quarantine_path)
    if si.type != "ohashes":
        err_msg = "Not a path to a quarantined file ({})".format(
            quarantine_path)
        raise VIOError(errno.EINVAL, err_msg)
    return rpc.list_quarantined_ohashes(si.socket_path)


def list_quarantined_ohash(quarantined_ohash_path):
    si = SwiftQuarantinedPathInfo.from_path(quarantined_ohash_path)
    if si.type != "ohash":
        err_msg = "Not a path to a quarantined file ({})".format(
            quarantined_ohash_path)
        raise VIOError(errno.EINVAL, err_msg)
    return rpc.list_quarantined_ohash(si.socket_path, si.ohash)


def _list_ohash(si, part_power):
    """
    :param si: SwiftPathInfo object
    :param part_power:
    :return: list of files within the object directory
    """
    return rpc.list_prefix(si.socket_path, si.ohash)


def _list_suffix(si, part_power):
    """
    :param si: SwiftPathInfo object
    :param part_power:
    :return: list of object hashes directory within the suffix directory
    """
    return rpc.list_suffix(si.socket_path, int(si.partition),
                           si.suffix, part_power)


def _list_partition(si, part_power):
    """
    :param si: SwiftPathInfo object
    :param part_power:
    :return: list of suffixes within the partition
    """
    return rpc.list_partition(si.socket_path, int(si.partition),
                              part_power)


def _list_partitions(si, part_power):
    """
    :param si: SwiftPathInfo object
    :param part_power:
    :return: list of partitions
    """
    return rpc.list_partitions(si.socket_path, part_power)


def set_header_state(socket_path, name, quarantine):
    """
    Set a vfile header state (quarantined or not)
    :param name: full name
    :param socket_path: socket path
    :param quarantine: True to quarantine, False to unquarantine
    :return:
    """
    try:
        obj = rpc.get_object(socket_path, name, is_quarantined=not quarantine,
                             repair_tool=False)
    except RpcError as e:
        if e.code == StatusCode.NotFound:
            raise IOError("No such file or directory: {}".format(name))
        raise (e)

    volume_filename = get_volume_name(obj.volume_index)
    volume_dir = socket_path.replace("rpc.socket", "volumes")
    volume_filepath = os.path.join(volume_dir, volume_filename)
    with open(volume_filepath, 'r+b') as fp:
        fp.seek(obj.offset)
        try:
            header = read_object_header(fp)
        except HeaderException:
            # until we journal the deletes, after a crash we may have an entry
            # for an object that has been "punched" from the volume.
            # if we find a hole instead of the header, remove entry from
            # kv and return.
            fp.seek(obj.offset)
            data = fp.read(MAX_OBJECT_HEADER_LEN)
            if all(c == '\x00' for c in data):
                # unregister the object here
                rpc.unregister_object(socket_path, name)
                return
            msg = "Failed to read header for {} at offset {} in volume\
                   {}".format(name, obj.offset, volume_filepath)
            raise VFileException(msg)
        if quarantine:
            header.state = STATE_OBJ_QUARANTINED
        else:
            header.state = STATE_OBJ_FILE
        fp.seek(obj.offset)
        write_object_header(header, fp)


def quarantine_ohash(dirpath, policy):
    """
    Quarantine the object (all files below the object hash directory)
    :param dirpath: path to object directory
    :param policy: policy
    :return:
    """
    si = SwiftPathInfo.from_path(dirpath)
    if si.type != 'ohash':
        raise VFileException("dirpath not an object dir: {}".format(dirpath))

    if policy.policy_type == 'erasure_coding':
        sort_f = lambda x: utils.Timestamp(x.split('#')[0])
    else:
        sort_f = lambda x: utils.Timestamp(os.path.splitext(x)[0])

    final = []
    vfiles = listdir(dirpath)

    try:
        for ext in ['.data', '.meta', '.ts']:
            partial = [v for v in vfiles if os.path.splitext(v)[1] == ext]
            partial.sort(key=sort_f)
            final.extend(partial)
    except Exception:
        final = vfiles

    for vfile in final:
        vfilepath = os.path.join(dirpath, vfile)
        sif = SwiftPathInfo.from_path(vfilepath)
        full_name = sif.ohash + sif.filename
        # update header
        set_header_state(sif.socket_path, full_name, quarantine=True)
        try:
            # update KV
            rpc.quarantine_object(si.socket_path, full_name)
        except RpcError as e:
            if e.code == StatusCode.NotFound:
                errmsg = "No such file or directory: '{}'"
                raise OSError(2, errmsg.format(vfilepath))
            raise(e)


def unquarantine_ohash(socket_path, ohash):
    """
    Unquarantine the object (all files below the object hash directory).
    Is this needed? Used for tests but currently not called from anywhere
    :param socket_path: path to KV socket
    :param ohash: object hash
    """
    for objname in rpc.list_quarantined_ohash(socket_path, ohash):
        full_name = "{}{}".format(ohash, objname)
        set_header_state(socket_path, full_name, quarantine=False)
        try:
            rpc.unquarantine_object(socket_path, full_name)
        except RpcError as e:
            if e.code == StatusCode.NotFound:
                errmsg = "No such file or directory: '{}'"
                raise OSError(2, errmsg.format(full_name))
            raise(e)


# def exists(path):
#     """
#     :param filepath: path to vfile
#     :return: True if file exists, False otherwise
#     """
#     si = SwiftPathInfo.from_path(path)
#     if si.type == 'partitions':
#         os.path.exists
#     try:
#         VFileReader.get_vfile(path, None)
#         return True
#     except RpcError as e:
#         if e.code == StatusCode.NotFound:
#             return False
#         raise (e)


def rmtree(path):
    """
    Delete a directory recursively. (Actually, it only delete objects, as
    directories do not exist)
    :param path:  path to the "directory" to remove
    :param logger:
    """
    type_to_func = {
        'ohash': _rmtree_ohash,
        'suffix': _rmtree_suffix,
        'partition': _rmtree_partition
        # 'partitions': _rmtree_partitions
    }

    path = os.path.normpath(path)
    si = SwiftPathInfo.from_path(path)
    type_to_func[si.type](path)


def _rmtree_ohash(path):
    files = listdir(path)
    for name in files:
        filepath = os.path.join(path, name)
        delete_vfile_from_path(filepath)


def _rmtree_suffix(path):
    ohashes = listdir(path)
    for ohash in ohashes:
        ohashpath = os.path.join(path, ohash)
        _rmtree_ohash(ohashpath)


def _rmtree_partition(path):
    suffixes = listdir(path)
    for suffix in suffixes:
        suffixpath = os.path.join(path, suffix)
        _rmtree_suffix(suffixpath)


def delete_vfile_from_path(filepath):
    si = SwiftPathInfo.from_path(filepath)
    full_name = si.ohash + si.filename

    def _unregister_object(socket_path, name, volume_index, offset, size):
        try:
            rpc.unregister_object(socket_path, name)
        except RpcError as e:
            if e.code == StatusCode.NotFound:
                raise VOSError(errno.ENOENT, "No such file or directory:\
                               '{}'".format(filepath))
            raise(e)

    try:
        obj = rpc.get_object(si.socket_path, full_name)
    except RpcError as e:
        if e.code == StatusCode.NotFound:
            raise VOSError(errno.ENOENT, "No such file or directory:\
                           '{}'".format(filepath))
    volume_filename = get_volume_name(obj.volume_index)
    volume_filepath = os.path.join(si.volume_dir, volume_filename)

    with open(volume_filepath, 'r+b') as fp:
        # get object length
        fp.seek(obj.offset)
        try:
            header = read_object_header(fp)
        except HeaderException:
            # until we journal the deletes, after a crash we may have an entry
            # for an object that has been "punched" from the volume.
            # if we find a hole instead of the header, remove entry from
            # kv and return.
            fp.seek(obj.offset)
            data = fp.read(MAX_OBJECT_HEADER_LEN)
            if all(c == '\x00' for c in data):
                # unregister the object here
                _unregister_object(si.socket_path, full_name,
                                   obj.volume_index, obj.offset, 0)
                return

            msg = "Failed to read header for {} at offset {} in volume\
                   {}".format(full_name, obj.offset, volume_filepath)
            raise VFileException(msg)

        # check that we have the object we were expecting
        header_fullname = "{}{}".format(header.ohash, header.filename)
        if header_fullname != full_name:
            # until we journal the renames, after a crash we may not have the
            # rename in the KV. If that's the case, continue.
            non_durable_name = re.sub(r'(#\d+)#d.', r'\1.', header_fullname)
            if non_durable_name != full_name:
                raise VFileException(
                    "Wrong header name. Header: {} Expected: {}".format(
                        header_fullname, full_name))
        utils.punch_hole(fp.fileno(), obj.offset, header.total_size)

    _unregister_object(si.socket_path, full_name, obj.volume_index,
                       obj.offset, header.total_size)


# delete an object hash directory
def rmtree_ohash(path):
    pass


def read_metadata(fp, offset, header):
    """
    Reads vfile metadata
    :param fp: opened file
    :param offset: absolute offset to the beginning of the vfile
    :param header: vfile header
    :return: metadata dict
    """
    metadata_offset = offset + header.metadata_offset
    metadata_size = header.metadata_size
    data_offset = offset + header.data_offset
    data_end = offset + header.data_offset + header.data_size
    metadata_available_space = data_offset - metadata_offset

    fp.seek(metadata_offset)
    if metadata_size > metadata_available_space:
        metastr = fp.read(metadata_available_space)
        fp.seek(data_end)
        metastr += fp.read(metadata_size - metadata_available_space)
    else:
        metastr = fp.read(metadata_size)

    # Verify checksum, if any
    if hasattr(header, 'metastr_md5'):
        metadata_checksum = header.metastr_md5
        computed_checksum = hashlib.md5(metastr).hexdigest().encode('ascii')
        if metadata_checksum != computed_checksum:
            raise DiskFileBadMetadataChecksum(
                "Metadata checksum mismatch for %s: "
                "stored checksum='%s', computed='%s'" % (
                    header.filename, metadata_checksum, computed_checksum))
    else:
        # we don't support updating from the older format for now
        pass

    meta = Metadata()
    meta.ParseFromString(metastr)
    metadata = {}
    for attr in meta.attrs:
        if attr.key:
            if six.PY2:
                metadata[attr.key] = attr.value
            else:
                metadata[attr.key.decode('utf8', 'surrogateescape')] = \
                    attr.value.decode('utf8', 'surrogateescape')

    return metadata


def rename_vfile(filepath, newfilepath, logger):
    """
    Renames a vfile. All writes to the KV are asynchronous. If we were to make
    a synchronous WriteBatch call, all previous writes would also be synced,
    killing performance. See:
    https://github.com/google/leveldb/blob/master/doc/index.md
    Currently :
        - update the header in place , synchronously
        - update the KV asynchronously (delete, put)

    A file can only be renamed within a KV
    This is currently only used by the erasure code diskfile manager
    """
    # Get current file info
    si = SwiftPathInfo.from_path(filepath)
    full_name = si.ohash + si.filename

    # Get new file info
    si_new = SwiftPathInfo.from_path(newfilepath)
    new_full_name = si_new.ohash + si_new.filename

    # Sanity check, same KV
    if si.socket_path != si_new.socket_path:
        raise VFileException("attempted to rename a file to a different KV")

    # rename file in place in the header
    vf_reader = VFileReader._get_vfile(full_name, si.volume_dir,
                                       si.socket_path, logger)
    vf_offset = vf_reader.offset
    header = vf_reader._header
    volume_path = vf_reader.fp.name
    vf_reader.close()

    header.filename = si_new.filename

    vol_fd = os.open(volume_path, os.O_WRONLY)
    os.lseek(vol_fd, vf_offset, os.SEEK_SET)
    os.write(vol_fd, header.pack())
    fdatasync(vol_fd)
    os.close(vol_fd)

    # Update the KV (async)
    try:
        rpc.rename_object(si.socket_path, full_name, new_full_name,
                          si.partition)
    except RpcError as e:
        if e.code == StatusCode.NotFound:
            raise VIOError(errno.ENOENT,
                           "No such file or directory: {}".format(full_name))
        else:
            raise
