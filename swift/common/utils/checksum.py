# Copyright (c) 2024 NVIDIA
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

try:
    import anycrc
except ImportError:
    anycrc = None
import binascii
import ctypes
import ctypes.util
import errno
import socket
import struct
import zlib


# See if anycrc is available...
if anycrc:
    crc32c_anycrc = anycrc.Model('CRC32C').calc
    crc64nvme_anycrc = anycrc.Model('CRC64-NVME').calc
else:
    crc32c_anycrc = None
    crc64nvme_anycrc = None


def find_isal():
    # If isal is available system-wide, great!
    isal_lib = ctypes.util.find_library('isal')
    if isal_lib is None:
        # py38+: Hopefully pyeclib was installed from a manylinux wheel
        # with isal baked in?
        try:
            import pyeclib  # noqa
            from importlib.metadata import \
                files as pkg_files, PackageNotFoundError  # py38+
        except ImportError:
            pass
        else:
            # Assume busted installs won't have it
            try:
                pyeclib_files = pkg_files('pyeclib')
                if pyeclib_files is None:
                    # Have a dist-info, but no RECORD file??
                    pyeclib_files = []
            except PackageNotFoundError:
                # Could import pyeclib, but no dist-info directory??
                pyeclib_files = []
            isal_libs = [f for f in pyeclib_files
                         if f.name.startswith("libisal")]
            if len(isal_libs) == 1:
                isal_lib = isal_libs[0].locate()
    return ctypes.CDLL(isal_lib) if isal_lib else None


isal = find_isal()

if hasattr(isal, 'crc32_iscsi'):  # isa-l >= 2.16
    isal.crc32_iscsi.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_uint]
    isal.crc32_iscsi.restype = ctypes.c_uint

    def crc32c_isal(data, value=0):
        result = isal.crc32_iscsi(
            data,
            len(data),
            value ^ 0xffff_ffff,
        )
        # for some reason, despite us specifying that restype is uint,
        # it can come back signed??
        return (result & 0xffff_ffff) ^ 0xffff_ffff
else:
    crc32c_isal = None

if hasattr(isal, 'crc64_rocksoft_refl'):  # isa-l >= 2.31.0
    isal.crc64_rocksoft_refl.argtypes = [
        ctypes.c_uint64, ctypes.c_char_p, ctypes.c_uint64]
    isal.crc64_rocksoft_refl.restype = ctypes.c_uint64

    def crc64nvme_isal(data, value=0):
        return isal.crc64_rocksoft_refl(
            value,
            data,
            len(data),
        )
else:
    crc64nvme_isal = None


# The kernel may also provide crc32c
AF_ALG = getattr(socket, 'AF_ALG', 38)
try:
    _sock = socket.socket(AF_ALG, socket.SOCK_SEQPACKET)
    _sock.bind(("hash", "crc32c"))
except OSError as e:
    if e.errno == errno.ENOENT:
        # could create socket, but crc32c is unknown
        _sock.close()
    elif e.errno != errno.EAFNOSUPPORT:
        raise
    crc32c_kern = None
else:
    def crc32c_kern(data, value=0):
        crc32c_sock = socket.socket(AF_ALG, socket.SOCK_SEQPACKET)
        try:
            crc32c_sock.bind(("hash", "crc32c"))
            crc32c_sock.setsockopt(
                socket.SOL_ALG,
                socket.ALG_SET_KEY,
                struct.pack("I", value ^ 0xffff_ffff))
            sock, _ = crc32c_sock.accept()
            try:
                sock.sendall(data)
                return struct.unpack("I", sock.recv(4))[0]
            finally:
                sock.close()
        finally:
            crc32c_sock.close()


def _select_crc32c_impl():
    # Use the best implementation available.
    # On various hardware we've seen
    #
    #  CPU           |   ISA-L   |  Kernel
    # ---------------+-----------+----------
    # Intel N100     |  ~9GB/s   | ~3.5GB/s
    # ARM Cortex-A55 |  ~2.5GB/s | ~0.4GB/s
    # Intel 11850H   |  ~7GB/s   | ~2.6GB/s
    # AMD 3900XT     | ~20GB/s   | ~5GB/s
    #
    # i.e., ISA-L is consistently 3-5x faster than kernel sockets
    selected = crc32c_isal or crc32c_kern or crc32c_anycrc or None
    if not selected:
        raise NotImplementedError(
            'no crc32c implementation, install isal or anycrc')
    return selected


def _select_crc64nvme_impl():
    selected = crc64nvme_isal or crc64nvme_anycrc or None
    if not selected:
        raise NotImplementedError(
            'no crc64nvme implementation, install isal or anycrc')
    return selected


class CRCHasher(object):
    """
    Helper that works like a hashlib hasher, but with a CRC.
    """
    def __init__(self, name, crc_func, data=None, initial_value=0, width=32):
        """
        Initialize the CRCHasher.

        :param name: Name of the hasher
        :param crc_func: Function to compute the CRC.
        :param data: Data to update the hasher.
        :param initial_value: Initial CRC value.
        :param width: Width (in bits) of CRC values.
        """
        self.name = name
        self.crc_func = crc_func
        self.crc = initial_value
        if width not in (32, 64):
            raise ValueError("CRCHasher only supports 32- or 64-bit CRCs")
        self.width = width
        if data is not None:
            self.update(data)

    @property
    def digest_size(self):
        return self.width / 8

    @property
    def digest_fmt(self):
        return "!I" if self.width == 32 else "!Q"

    def update(self, data):
        """
        Update the CRC with new data.

        :param data: Data to update the CRC with.
        """
        self.crc = self.crc_func(data, self.crc)

    def digest(self):
        """
        Return the current CRC value as a big-endian integer of length
        ``width / 8`` bytes.

        :returns: Packed CRC value. (bytes)
        """
        return struct.pack(self.digest_fmt, self.crc)

    def hexdigest(self):
        """
        Return the hexadecimal representation of the current CRC value.

        :returns: Hexadecimal CRC value. (str)
        """
        hex = binascii.hexlify(self.digest()).decode("ascii")
        return hex

    def copy(self):
        """
        Copy the current state of this CRCHasher to a new one.

        :returns:
        """
        return CRCHasher(self.name,
                         self.crc_func,
                         initial_value=self.crc,
                         width=self.width)


def crc32(data=None, initial_value=0):
    return CRCHasher('crc32',
                     zlib.crc32,
                     data=data,
                     initial_value=initial_value)


def crc32c(data=None, initial_value=0):
    return CRCHasher('crc32c',
                     _select_crc32c_impl(),
                     data=data,
                     initial_value=initial_value)


def crc64nvme(data=None, initial_value=0):
    return CRCHasher('crc64nvme',
                     _select_crc64nvme_impl(),
                     data=data,
                     initial_value=initial_value,
                     width=64)


def log_selected_implementation(logger):
    try:
        impl = _select_crc32c_impl()
    except NotImplementedError:
        logger.warning(
            'No implementation found for CRC32C; '
            'install ISA-L or anycrc for support.')
    else:
        logger.info('Using %s implementation for CRC32C.' % impl.__name__)

    try:
        impl = _select_crc64nvme_impl()
    except NotImplementedError:
        logger.warning(
            'No implementation found for CRC64NVME; '
            'install ISA-L or anycrc for support.')
    else:
        logger.info('Using %s implementation for CRC64NVME.' % impl.__name__)
