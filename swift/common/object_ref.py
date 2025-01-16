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
from swift.common.request_helpers import split_reserved_name
from swift.common.utils import RESERVED, Timestamp


class BaseObjectId:
    SHARD_ALIGNMENT_CHARACTER = '$'
    PARAM_SEPARATOR = '&'

    def __init__(self, timestamp):
        self.timestamp = Timestamp(timestamp)

    def __eq__(self, other):
        return str(self) == str(other)

    def serialize(self, **kwargs):
        raise NotImplementedError

    def __str__(self):
        return self.serialize()

    def _parse(cls, params):
        raise NotImplementedError

    @classmethod
    def parse(cls, value):
        try:
            return cls._parse(value)
        except (ValueError, AttributeError):
            raise ValueError('Invalid %s: %s' % (cls.__name__, value))


class UploadId(BaseObjectId):
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

    def serialize(self, **kwargs):
        return self.PARAM_SEPARATOR.join(
            (self.timestamp.normal, self.SHARD_ALIGNMENT_CHARACTER))

    @classmethod
    def _parse(cls, value):
        params = value.split(cls.PARAM_SEPARATOR)
        if len(params) != 2:
            raise ValueError()
        if params[1] != cls.SHARD_ALIGNMENT_CHARACTER:
            raise ValueError()
        return cls(params[0])

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


class HistoryId(BaseObjectId):
    """
    Encapsulate properties of a history id.

    A serialized history id for a null version object instance has the form:

        -null-&<shard_alignment>&<~timestamp>

    A serialized history id for a real version object instance has the form:

        <~timestamp>&<shard_alignment>&

    where:
      * <~timestamp> is the inverted data timestamp of the object.
      * <shard_alignment> is the character '$' which can be used to force shard
        bound alignment.

    For example, a null version history id:

        -null-&$&009999999876.99999

    For example, a real version history id:

        9999999876.99999&$&

    This form has the following properties:
        * All real version HistoryId's of a user object will list contiguously.
        * All null version HistoryId's of a user object will list contiguously.
        * Null version HistoryId's will list ahead of real version HistoryId's.
        * Null version HistoryId's will list in descending chronological order.
        * Real version HistoryId's will list in descending chronological order.
        * The shard alignment character in a null version HistoryId occurs
          immediately *before* the timestamp. When sharding is aligned on this
          character, this ensures that the complete history of all null
          versions falls in the same shard.
        * The shard alignment character in a real version HistoryId occurs
          immediately *after* the timestamp. When sharding is aligned on this
          character, this ensures that the complete history of a real version
          falls in the same shard.

    :param timestamp: the object creation timestamp.
    :param null: set True if the HistoryId is for a 'null'
        version created while object versioning is not enabled; set False if
        the HistoryId is for a retained version created while object versioning
        is enabled.
    """
    NULL_AFFIX = '-null-'

    def __init__(self, timestamp, null=False):
        super().__init__(timestamp)
        self.null = null

    def serialize(self, prefix_only=False):
        # TODO: the prefix_only arg isn't great: it doesn't really give you a
        #   "serialization" of the object in that the return value can't
        #   necessarily be parsed. But it'll do as a hack for now. Perhaps a
        #   separate prefix method would be better?
        """
        Serialize the object id to a string.

        :param time_ascending: If True then the serialized form includes the
            timestamp and sorts in chronological order. If False (default) then
            the serialized form includes the inverse of the timestamp and sorts
            in reverse chronological order.
        :param prefix_only: If True then the serialized form includes only
            the prefix that is common to all variants of the same version. For
            null versions, the timestamp is not part of the prefix. For
            non-null versions, the prefix includes the timestamp but no other
            parameters.
        """
        inv_timepart = (~Timestamp(self.timestamp.normal)).internal

        if self.null:
            prefix_parts = [self.NULL_AFFIX]
            other_parts = [self.SHARD_ALIGNMENT_CHARACTER, inv_timepart]
        else:
            prefix_parts = [inv_timepart]
            other_parts = [self.SHARD_ALIGNMENT_CHARACTER, '']
        if prefix_only:
            return self.PARAM_SEPARATOR.join(prefix_parts)
        else:
            return self.PARAM_SEPARATOR.join(prefix_parts + other_parts)

    @classmethod
    def _parse(cls, value):
        params = value.split(cls.PARAM_SEPARATOR)
        if len(params) != 3:
            raise ValueError()
        if params[0] == cls.NULL_AFFIX:
            if params[1] != cls.SHARD_ALIGNMENT_CHARACTER:
                raise ValueError()
            null = True
            inv_timestamp = params[2]
        else:
            if params[1:] != [cls.SHARD_ALIGNMENT_CHARACTER, '']:
                raise ValueError()
            null = False
            inv_timestamp = params[0]
        return cls(~Timestamp(inv_timestamp), null=null)


class ObjectRef:
    """
    Encapsulate properties of the internal name for a specific variant of an
    object.

    The internal name of an object instance has two components:
      * <user_name> is the object name in the user namespace, common to all
        variants of the same user object. This is a native string (unquoted
        utf8).
      * <object_id> is a native string that is unique to each variant of an
        object, for example a history id or upload id.

    The serialized form of an ObjectRef has the form:

        <R><user_name><R><object_id>

    where <R> is the reserved character.

    :param user_name: an unquoted utf8 object name.
    :param obj_id: a string variant id.
    """
    # TODO: the reserved 0x00 character doesn't play well with sqlite3 cli
    # https://www.sqlite.org/nulinstr.html
    # is there a better way to delimit the user name from the object id?
    DELIMITER = RESERVED
    TAIL_DELIMITER = '/'

    def __init__(self, user_name, obj_id=None, tail=None):
        self.user_name = user_name
        self.obj_id = str(obj_id) if obj_id else None
        self.tail = tail

    def __eq__(self, other):
        return isinstance(other, ObjectRef) and str(other) == str(self)

    def clone(self):
        return ObjectRef(self.user_name, self.obj_id, self.tail)

    def serialize(self, drop_tail=False):
        val = self.DELIMITER + self.user_name
        if self.obj_id:
            val = val + self.DELIMITER + self.obj_id
            if self.tail is not None and not drop_tail:
                val = val + self.TAIL_DELIMITER + self.tail
        return val

    def __str__(self):
        return self.serialize()

    @property
    def basename(self):
        return self.DELIMITER + self.user_name

    @classmethod
    def _parse(cls, name):
        name, rest = split_reserved_name(name, maxsplit=1)
        uid_str, delimiter, tail = rest.partition(cls.TAIL_DELIMITER)
        tail = tail if delimiter else None
        return cls(name, uid_str, tail)

    @classmethod
    def parse(cls, name):
        try:
            return cls._parse(name)
        except ValueError as err:
            raise ValueError('Invalid object reference: %s' % err)
