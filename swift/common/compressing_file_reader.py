# Copyright (c) 2010-2011 OpenStack, LLC.
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

import zlib
import struct


class CompressingFileReader(object):
    '''
    Wraps a file object and provides a read method that returns gzip'd data.

    One warning: if read is called with a small value, the data returned may
    be bigger than the value. In this case, the "compressed" data will be
    bigger than the original data. To solve this, use a bigger read buffer.

    An example use case:
    Given an uncompressed file on disk, provide a way to read compressed data
    without buffering the entire file data in memory. Using this class, an
    uncompressed log file could be uploaded as compressed data with chunked
    transfer encoding.

    gzip header and footer code taken from the python stdlib gzip module

    :param file_obj: File object to read from
    :param compresslevel: compression level
    '''

    def __init__(self, file_obj, compresslevel=9):
        self._f = file_obj
        self._compressor = zlib.compressobj(compresslevel,
                                            zlib.DEFLATED,
                                            -zlib.MAX_WBITS,
                                            zlib.DEF_MEM_LEVEL,
                                            0)
        self.done = False
        self.first = True
        self.crc32 = 0
        self.total_size = 0

    def read(self, *a, **kw):
        if self.done:
            return ''
        x = self._f.read(*a, **kw)
        if x:
            self.crc32 = zlib.crc32(x, self.crc32) & 0xffffffffL
            self.total_size += len(x)
            compressed = self._compressor.compress(x)
            if not compressed:
                compressed = self._compressor.flush(zlib.Z_SYNC_FLUSH)
        else:
            compressed = self._compressor.flush(zlib.Z_FINISH)
            crc32 = struct.pack("<L", self.crc32 & 0xffffffffL)
            size = struct.pack("<L", self.total_size & 0xffffffffL)
            footer = crc32 + size
            compressed += footer
            self.done = True
        if self.first:
            self.first = False
            header = '\037\213\010\000\000\000\000\000\002\377'
            compressed = header + compressed
        return compressed
