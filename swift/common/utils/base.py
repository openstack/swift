# Copyright (c) 2010-2024 OpenStack Foundation
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
Miscellaneous utility functions that may be used in other utils modules.

This module is imported by other utils modules.
This module should not import from other utils modules.
"""

import codecs
import hashlib

from urllib.parse import quote as _quote


try:
    _test_md5 = hashlib.md5(usedforsecurity=False)  # nosec

    def md5(string=b'', usedforsecurity=True):
        """Return an md5 hashlib object using usedforsecurity parameter

        For python distributions that support the usedforsecurity keyword
        parameter, this passes the parameter through as expected.
        See https://bugs.python.org/issue9216
        """
        return hashlib.md5(string, usedforsecurity=usedforsecurity)  # nosec
except TypeError:
    def md5(string=b'', usedforsecurity=True):
        """Return an md5 hashlib object without usedforsecurity parameter

        For python distributions that do not yet support this keyword
        parameter, we drop the parameter
        """
        return hashlib.md5(string)  # nosec


utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')
# Apparently under py3 we need to go to utf-16 to collapse surrogates?
utf16_decoder = codecs.getdecoder('utf-16')
utf16_encoder = codecs.getencoder('utf-16')


def get_valid_utf8_str(str_or_unicode):
    """
    Get valid parts of utf-8 str from str, unicode and even invalid utf-8 str

    :param str_or_unicode: a string or an unicode which can be invalid utf-8
    """
    if isinstance(str_or_unicode, bytes):
        try:
            (str_or_unicode, _len) = utf8_decoder(str_or_unicode,
                                                  'surrogatepass')
        except UnicodeDecodeError:
            (str_or_unicode, _len) = utf8_decoder(str_or_unicode,
                                                  'replace')
    (str_or_unicode, _len) = utf16_encoder(str_or_unicode, 'surrogatepass')
    (valid_unicode_str, _len) = utf16_decoder(str_or_unicode, 'replace')
    return valid_unicode_str.encode('utf-8')


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf-8 strings before quoting
    """
    quoted = _quote(get_valid_utf8_str(value), safe)
    if isinstance(value, bytes):
        quoted = quoted.encode('utf-8')
    return quoted


def split_path(path, minsegs=1, maxsegs=None, rest_with_last=False):
    """
    Validate and split the given HTTP request path.

    **Examples**::

        ['a'] = split_path('/a')
        ['a', None] = split_path('/a', 1, 2)
        ['a', 'c'] = split_path('/a/c', 1, 2)
        ['a', 'c', 'o/r'] = split_path('/a/c/o/r', 1, 3, True)

    :param path: HTTP Request path to be split
    :param minsegs: Minimum number of segments to be extracted
    :param maxsegs: Maximum number of segments to be extracted
    :param rest_with_last: If True, trailing data will be returned as part
                           of last segment.  If False, and there is
                           trailing data, raises ValueError.
    :returns: list of segments with a length of maxsegs (non-existent
              segments will return as None)
    :raises ValueError: if given an invalid path
    """
    if not maxsegs:
        maxsegs = minsegs
    if minsegs > maxsegs:
        raise ValueError('minsegs > maxsegs: %d > %d' % (minsegs, maxsegs))
    if rest_with_last:
        segs = path.split('/', maxsegs)
        minsegs += 1
        maxsegs += 1
        count = len(segs)
        if (segs[0] or count < minsegs or count > maxsegs or
                '' in segs[1:minsegs]):
            raise ValueError('Invalid path: %s' % quote(path))
    else:
        minsegs += 1
        maxsegs += 1
        segs = path.split('/', maxsegs)
        count = len(segs)
        if (segs[0] or count < minsegs or count > maxsegs + 1 or
                '' in segs[1:minsegs] or
                (count == maxsegs + 1 and segs[maxsegs])):
            raise ValueError('Invalid path: %s' % quote(path))
    segs = segs[1:maxsegs]
    if not all(segs[:-1]):
        raise ValueError('Invalid path: %s' % quote(path))
    segs.extend([None] * (maxsegs - 1 - len(segs)))
    return segs
