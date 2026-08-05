# Copyright (c) 2025 NVIDIA
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
from swift.common.utils import Timestamp


class UploadId:
    """
    Encapsulate properties of an upload id.

    A serialized upload id has the form:

        <timestamp>&<shard_alignment>

    where:
      * <timestamp> is the data timestamp of the object.
      * <shard_alignment> is the character '$' which can be used to force shard
        bound alignment.

    For example:

        0000001234.5678&$

    This form has the following properties:
        * All UploadId variants of a user object will list contiguously.
        * UploadId's will list in ascending chronological order.
        * The shard alignment character occurs immediately *after* the
          timestamp. When sharding is aligned on this character, this ensures
          that all parts of an upload fall in the same shard.

    :param timestamp: the object creation timestamp.
    """
    _newest = None
    SHARD_ALIGNMENT_CHARACTER = '$'
    PARAM_SEPARATOR = '&'

    def __init__(self, timestamp):
        self.timestamp = Timestamp(timestamp)

    def __eq__(self, other):
        return str(self) == str(other)

    def __str__(self):
        return self.serialize()

    def serialize(self, **kwargs):
        return self.PARAM_SEPARATOR.join(
            (self.timestamp.internal, self.SHARD_ALIGNMENT_CHARACTER))

    @classmethod
    def _parse(cls, value):
        params = value.split(cls.PARAM_SEPARATOR)
        if len(params) != 2:
            raise ValueError()
        if params[1] != cls.SHARD_ALIGNMENT_CHARACTER:
            raise ValueError()
        return cls(params[0])

    @classmethod
    def parse(cls, value):
        try:
            return cls._parse(value)
        except (ValueError, AttributeError):
            raise ValueError('Invalid %s: %s' % (cls.__name__, value))

    @classmethod
    def newest(cls):
        """
        Returns an UploadId for the maximum possible timestamp. The serialized
        form of this UploadId will sort after any UploadId created at a
        realistic timestamp.
        """
        # used to form an UploadId that sorts after any other upload id
        if cls._newest is None:
            cls._newest = cls(Timestamp.max())
        return cls._newest


class ObjectRef:
    """
    Encapsulate properties of the internal name for a specific variant of an
    object.

    The internal name of an object instance has up to three components joined
    by a ``/`` delimiter:
      * <user_name> is the object name in the user namespace, common to all
        variants of the same user object. This is a native string (unquoted
        utf8).
      * an optional <object_id> that is a native string unique to each variant
        of an object, for example an upload id.
      * an optional <tail>, for example an upload part number

    The serialized form of an ObjectRef has the form:

        <user_name>[/<object_id>[/<tail>]]

    :param user_name: an unquoted utf8 object name.
    :param obj_id: a string variant id.
    :param tail: a string tail component.
    """
    DELIMITER = '/'

    def __init__(self, user_name, obj_id=None, tail=None):
        self.user_name = user_name
        self.obj_id = str(obj_id) if obj_id else None
        self.tail = tail

    def __eq__(self, other):
        return isinstance(other, ObjectRef) and str(other) == str(self)

    def clone(self):
        return ObjectRef(self.user_name, self.obj_id, self.tail)

    def serialize(self, drop_tail=False):
        val = self.user_name
        if self.obj_id:
            val += (self.DELIMITER + self.obj_id)
            if self.tail is not None and not drop_tail:
                val += (self.DELIMITER + self.tail)
        return val

    def __str__(self):
        return self.serialize()

    @classmethod
    def _parse(cls, name):
        name, _, rest = name.partition(cls.DELIMITER)
        uid_str, delimiter, tail = rest.partition(cls.DELIMITER)
        tail = tail if delimiter else None
        return cls(name, obj_id=uid_str, tail=tail)

    @classmethod
    def parse(cls, name):
        try:
            return cls._parse(name)
        except ValueError as err:
            raise ValueError('Invalid object reference: %s' % err)
