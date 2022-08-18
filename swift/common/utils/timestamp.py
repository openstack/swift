# Copyright (c) 2010-2023 OpenStack Foundation
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

"""Timestamp-related functions for use with Swift."""

import datetime
import functools
import math
import sys
import time


NORMAL_FORMAT = "%016.05f"
INTERNAL_FORMAT = NORMAL_FORMAT + '_%016x'
SHORT_FORMAT = NORMAL_FORMAT + '_%x'
MAX_OFFSET = (16 ** 16) - 1
PRECISION = 1e-5
# Setting this to True will cause the internal format to always display
# extended digits - even when the value is equivalent to the normalized form.
# This isn't ideal during an upgrade when some servers might not understand
# the new time format - but flipping it to True works great for testing.
FORCE_INTERNAL = False  # or True


@functools.total_ordering
class Timestamp(object):
    """
    Internal Representation of Swift Time.

    The normalized form of the X-Timestamp header looks like a float
    with a fixed width to ensure stable string sorting - normalized
    timestamps look like "1402464677.04188"

    To support overwrites of existing data without modifying the original
    timestamp but still maintain consistency a second internal offset vector
    is append to the normalized timestamp form which compares and sorts
    greater than the fixed width float format but less than a newer timestamp.
    The internalized format of timestamps looks like
    "1402464677.04188_0000000000000000" - the portion after the underscore is
    the offset and is a formatted hexadecimal integer.

    The internalized form is not exposed to clients in responses from
    Swift.  Normal client operations will not create a timestamp with an
    offset.

    The Timestamp class in common.utils supports internalized and
    normalized formatting of timestamps and also comparison of timestamp
    values.  When the offset value of a Timestamp is 0 - it's considered
    insignificant and need not be represented in the string format; to
    support backwards compatibility during a Swift upgrade the
    internalized and normalized form of a Timestamp with an
    insignificant offset are identical.  When a timestamp includes an
    offset it will always be represented in the internalized form, but
    is still excluded from the normalized form.  Timestamps with an
    equivalent timestamp portion (the float part) will compare and order
    by their offset.  Timestamps with a greater timestamp portion will
    always compare and order greater than a Timestamp with a lesser
    timestamp regardless of it's offset.  String comparison and ordering
    is guaranteed for the internalized string format, and is backwards
    compatible for normalized timestamps which do not include an offset.
    """

    def __init__(self, timestamp, offset=0, delta=0, check_bounds=True):
        """
        Create a new Timestamp.

        :param timestamp: time in seconds since the Epoch, may be any of:

            * a float or integer
            * normalized/internalized string
            * another instance of this class (offset is preserved)

        :param offset: the second internal offset vector, an int
        :param delta: deca-microsecond difference from the base timestamp
                      param, an int
        """
        if isinstance(timestamp, bytes):
            timestamp = timestamp.decode('ascii')
        if isinstance(timestamp, str):
            base, base_offset = timestamp.partition('_')[::2]
            self.timestamp = float(base)
            if '_' in base_offset:
                raise ValueError('invalid literal for int() with base 16: '
                                 '%r' % base_offset)
            if base_offset:
                self.offset = int(base_offset, 16)
            else:
                self.offset = 0
        else:
            self.timestamp = float(timestamp)
            self.offset = getattr(timestamp, 'offset', 0)
        # increment offset
        if offset >= 0:
            self.offset += offset
        else:
            raise ValueError('offset must be non-negative')
        if self.offset > MAX_OFFSET:
            raise ValueError('offset must be smaller than %d' % MAX_OFFSET)
        self.raw = int(round(self.timestamp / PRECISION))
        # add delta
        if delta:
            self.raw = self.raw + delta
            if self.raw <= 0:
                raise ValueError(
                    'delta must be greater than %d' % (-1 * self.raw))
            self.timestamp = float(self.raw * PRECISION)
        if check_bounds:
            if self.timestamp < 0:
                raise ValueError('timestamp cannot be negative')
            if self.timestamp >= 10000000000:
                raise ValueError('timestamp too large')

    @classmethod
    def now(cls, offset=0, delta=0):
        return cls(time.time(), offset=offset, delta=delta)

    def __repr__(self):
        return INTERNAL_FORMAT % (self.timestamp, self.offset)

    def __str__(self):
        raise TypeError('You must specify which string format is required')

    def __float__(self):
        return self.timestamp

    def __int__(self):
        return int(self.timestamp)

    def __bool__(self):
        return bool(self.timestamp or self.offset)

    @property
    def normal(self):
        return NORMAL_FORMAT % self.timestamp

    @property
    def internal(self):
        if self.offset or FORCE_INTERNAL:
            return INTERNAL_FORMAT % (self.timestamp, self.offset)
        else:
            return self.normal

    @property
    def short(self):
        if self.offset or FORCE_INTERNAL:
            return SHORT_FORMAT % (self.timestamp, self.offset)
        else:
            return self.normal

    @property
    def isoformat(self):
        """
        Get an isoformat string representation of the 'normal' part of the
        Timestamp with microsecond precision and no trailing timezone, for
        example::

            1970-01-01T00:00:00.000000

        :return: an isoformat string
        """
        t = float(self.normal)
        # On Python 3, round manually using ROUND_HALF_EVEN rounding
        # method, to use the same rounding method than Python 2. Python 3
        # used a different rounding method, but Python 3.4.4 and 3.5.1 use
        # again ROUND_HALF_EVEN as Python 2.
        # See https://bugs.python.org/issue23517
        frac, t = math.modf(t)
        us = round(frac * 1e6)
        if us >= 1000000:
            t += 1
            us -= 1000000
        elif us < 0:
            t -= 1
            us += 1000000
        dt = datetime.datetime.fromtimestamp(t, UTC)
        dt = dt.replace(microsecond=us)

        isoformat = dt.isoformat()
        # need to drop tzinfo
        isoformat = isoformat[:isoformat.index('+')]
        # python isoformat() doesn't include msecs when zero
        if len(isoformat) < len("1970-01-01T00:00:00.000000"):
            isoformat += ".000000"
        return isoformat

    @classmethod
    def from_isoformat(cls, date_string):
        """
        Parse an isoformat string representation of time to a Timestamp object.

        :param date_string: a string formatted as per an Timestamp.isoformat
            property.
        :return: an instance of  this class.
        """
        start = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        delta = start - EPOCH
        # This calculation is based on Python 2.7's Modules/datetimemodule.c,
        # function delta_to_microseconds(), but written in Python.
        return cls(delta.total_seconds())

    def ceil(self):
        """
        Return the 'normal' part of the timestamp rounded up to the nearest
        integer number of seconds.

        This value should be used whenever the second-precision Last-Modified
        time of a resource is required.

        :return: a float value with second precision.
        """
        return math.ceil(float(self))

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, Timestamp):
            try:
                other = Timestamp(other, check_bounds=False)
            except ValueError:
                return False
        return self.internal == other.internal

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        if other is None:
            return False
        if not isinstance(other, Timestamp):
            other = Timestamp(other, check_bounds=False)
        if other.timestamp < 0:
            return False
        if other.timestamp >= 10000000000:
            return True
        return self.internal < other.internal

    def __hash__(self):
        return hash(self.internal)

    def __invert__(self):
        if self.offset:
            raise ValueError('Cannot invert timestamps with offsets')
        return Timestamp((999999999999999 - self.raw) * PRECISION)


def encode_timestamps(t1, t2=None, t3=None, explicit=False):
    """
    Encode up to three timestamps into a string. Unlike a Timestamp object, the
    encoded string does NOT used fixed width fields and consequently no
    relative chronology of the timestamps can be inferred from lexicographic
    sorting of encoded timestamp strings.

    The format of the encoded string is:
        <t1>[<+/-><t2 - t1>[<+/-><t3 - t2>]]

    i.e. if t1 = t2 = t3 then just the string representation of t1 is returned,
    otherwise the time offsets for t2 and t3 are appended. If explicit is True
    then the offsets for t2 and t3 are always appended even if zero.

    Note: any offset value in t1 will be preserved, but offsets on t2 and t3
    are not preserved. In the anticipated use cases for this method (and the
    inverse decode_timestamps method) the timestamps passed as t2 and t3 are
    not expected to have offsets as they will be timestamps associated with a
    POST request. In the case where the encoding is used in a container objects
    table row, t1 could be the PUT or DELETE time but t2 and t3 represent the
    content type and metadata times (if different from the data file) i.e.
    correspond to POST timestamps. In the case where the encoded form is used
    in a .meta file name, t1 and t2 both correspond to POST timestamps.
    """
    form = '{0}'
    values = [t1.short]
    if t2 is not None:
        t2_t1_delta = t2.raw - t1.raw
        explicit = explicit or (t2_t1_delta != 0)
        values.append(t2_t1_delta)
        if t3 is not None:
            t3_t2_delta = t3.raw - t2.raw
            explicit = explicit or (t3_t2_delta != 0)
            values.append(t3_t2_delta)
        if explicit:
            form += '{1:+x}'
            if t3 is not None:
                form += '{2:+x}'
    return form.format(*values)


def decode_timestamps(encoded, explicit=False):
    """
    Parses a string of the form generated by encode_timestamps and returns
    a tuple of the three component timestamps. If explicit is False, component
    timestamps that are not explicitly encoded will be assumed to have zero
    delta from the previous component and therefore take the value of the
    previous component. If explicit is True, component timestamps that are
    not explicitly encoded will be returned with value None.
    """
    # TODO: some tests, e.g. in test_replicator, put float timestamps values
    # into container db's, hence this defensive check, but in real world
    # this may never happen.
    if not isinstance(encoded, str):
        ts = Timestamp(encoded)
        return ts, ts, ts

    parts = []
    signs = []
    pos_parts = encoded.split('+')
    for part in pos_parts:
        # parse time components and their signs
        # e.g. x-y+z --> parts = [x, y, z] and signs = [+1, -1, +1]
        neg_parts = part.split('-')
        parts = parts + neg_parts
        signs = signs + [1] + [-1] * (len(neg_parts) - 1)
    t1 = Timestamp(parts[0])
    t2 = t3 = None
    if len(parts) > 1:
        t2 = t1
        delta = signs[1] * int(parts[1], 16)
        # if delta = 0 we want t2 = t3 = t1 in order to
        # preserve any offset in t1 - only construct a distinct
        # timestamp if there is a non-zero delta.
        if delta:
            t2 = Timestamp((t1.raw + delta) * PRECISION)
    elif not explicit:
        t2 = t1
    if len(parts) > 2:
        t3 = t2
        delta = signs[2] * int(parts[2], 16)
        if delta:
            t3 = Timestamp((t2.raw + delta) * PRECISION)
    elif not explicit:
        t3 = t2
    return t1, t2, t3


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
    return Timestamp(timestamp).normal


EPOCH = datetime.datetime(1970, 1, 1)


def last_modified_date_to_timestamp(last_modified_date_str):
    """
    Convert a last modified date (like you'd get from a container listing,
    e.g. 2014-02-28T23:22:36.698390) to a float.
    """
    return Timestamp.from_isoformat(last_modified_date_str)


def normalize_delete_at_timestamp(timestamp, high_precision=False):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx (10) or xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps less than 0000000000 are raised to
    0000000000 and values greater than November 20th, 2286 at
    17:46:39 UTC will be capped at that date and time, resulting in
    no return value exceeding 9999999999.99999 (or 9999999999 if
    using low-precision).

    This cap is because the expirer is already working through a
    sorted list of strings that were all a length of 10. Adding
    another digit would mess up the sort and cause the expirer to
    break from processing early. By 2286, this problem will need to
    be fixed, probably by creating an additional .expiring_objects
    account to work from with 11 (or more) digit container names.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    fmt = '%016.5f' if high_precision else '%010d'
    return fmt % min(max(0, float(timestamp)), 9999999999.99999)


if sys.version_info < (3, 11):
    class _UTC(datetime.tzinfo):
        """
        A tzinfo class for datetimes that returns a 0 timedelta (UTC time)
        """

        def dst(self, dt):
            return datetime.timedelta(0)
        utcoffset = dst

        def tzname(self, dt):
            return 'UTC'

    UTC = _UTC()
else:
    from datetime import UTC
