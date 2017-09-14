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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Pluggable Back-ends for Container Server
"""

import errno
import os
from uuid import uuid4
from contextlib import contextmanager

import six
import six.moves.cPickle as pickle
from six.moves import range
import sqlite3

from swift.common.utils import Timestamp, encode_timestamps, \
    decode_timestamps, extract_swift_bytes, PivotRange
from swift.common.db import DatabaseBroker, utf8encode, BROKER_TIMEOUT


SQLITE_ARG_LIMIT = 999

DATADIR = 'containers'

RECORD_TYPE_OBJECT = 0
RECORD_TYPE_PIVOT_NODE = 1

DB_STATE_NOTFOUND = 0
DB_STATE_UNSHARDED = 1
DB_STATE_SHARDING = 2
DB_STATE_SHARDED = 3
DB_STATE = [
    'notfound',
    'unsharded',
    'sharding',
    'sharded']

POLICY_STAT_TABLE_CREATE = '''
    CREATE TABLE policy_stat (
        storage_policy_index INTEGER PRIMARY KEY,
        object_count INTEGER DEFAULT 0,
        bytes_used INTEGER DEFAULT 0
    );
'''

POLICY_STAT_TRIGGER_SCRIPT = '''
    CREATE TRIGGER object_insert_policy_stat AFTER INSERT ON object
    BEGIN
        UPDATE policy_stat
        SET object_count = object_count + (1 - new.deleted),
            bytes_used = bytes_used + new.size
        WHERE storage_policy_index = new.storage_policy_index;
        INSERT INTO policy_stat (
            storage_policy_index, object_count, bytes_used)
        SELECT new.storage_policy_index,
               (1 - new.deleted),
               new.size
        WHERE NOT EXISTS(
            SELECT changes() as change
            FROM policy_stat
            WHERE change <> 0
        );
        UPDATE container_info
        SET hash = chexor(hash, new.name, new.created_at);
    END;

    CREATE TRIGGER object_delete_policy_stat AFTER DELETE ON object
    BEGIN
        UPDATE policy_stat
        SET object_count = object_count - (1 - old.deleted),
            bytes_used = bytes_used - old.size
        WHERE storage_policy_index = old.storage_policy_index;
        UPDATE container_info
        SET hash = chexor(hash, old.name, old.created_at);
    END;
'''

CONTAINER_INFO_TABLE_SCRIPT = '''
    CREATE TABLE container_info (
        account TEXT,
        container TEXT,
        created_at TEXT,
        put_timestamp TEXT DEFAULT '0',
        delete_timestamp TEXT DEFAULT '0',
        reported_put_timestamp TEXT DEFAULT '0',
        reported_delete_timestamp TEXT DEFAULT '0',
        reported_object_count INTEGER DEFAULT 0,
        reported_bytes_used INTEGER DEFAULT 0,
        hash TEXT default '00000000000000000000000000000000',
        id TEXT,
        status TEXT DEFAULT '',
        status_changed_at TEXT DEFAULT '0',
        metadata TEXT DEFAULT '',
        x_container_sync_point1 INTEGER DEFAULT -1,
        x_container_sync_point2 INTEGER DEFAULT -1,
        storage_policy_index INTEGER DEFAULT 0,
        reconciler_sync_point INTEGER DEFAULT -1
    );
'''

CONTAINER_STAT_VIEW_SCRIPT = '''
    CREATE VIEW container_stat
    AS SELECT ci.account, ci.container, ci.created_at,
        ci.put_timestamp, ci.delete_timestamp,
        ci.reported_put_timestamp, ci.reported_delete_timestamp,
        ci.reported_object_count, ci.reported_bytes_used, ci.hash,
        ci.id, ci.status, ci.status_changed_at, ci.metadata,
        ci.x_container_sync_point1, ci.x_container_sync_point2,
        ci.reconciler_sync_point,
        ci.storage_policy_index,
        coalesce(ps.object_count, 0) AS object_count,
        coalesce(ps.bytes_used, 0) AS bytes_used
    FROM container_info ci LEFT JOIN policy_stat ps
    ON ci.storage_policy_index = ps.storage_policy_index;

    CREATE TRIGGER container_stat_update
    INSTEAD OF UPDATE ON container_stat
    BEGIN
        UPDATE container_info
        SET account = NEW.account,
            container = NEW.container,
            created_at = NEW.created_at,
            put_timestamp = NEW.put_timestamp,
            delete_timestamp = NEW.delete_timestamp,
            reported_put_timestamp = NEW.reported_put_timestamp,
            reported_delete_timestamp = NEW.reported_delete_timestamp,
            reported_object_count = NEW.reported_object_count,
            reported_bytes_used = NEW.reported_bytes_used,
            hash = NEW.hash,
            id = NEW.id,
            status = NEW.status,
            status_changed_at = NEW.status_changed_at,
            metadata = NEW.metadata,
            x_container_sync_point1 = NEW.x_container_sync_point1,
            x_container_sync_point2 = NEW.x_container_sync_point2,
            storage_policy_index = NEW.storage_policy_index,
            reconciler_sync_point = NEW.reconciler_sync_point;
    END;
'''


def update_new_item_from_existing(new_item, existing):
    """
    Compare the data and meta related timestamps of a new object item with
    the timestamps of an existing object record, and update the new item
    with data and/or meta related attributes from the existing record if
    their timestamps are newer.

    The multiple timestamps are encoded into a single string for storing
    in the 'created_at' column of the objects db table.

    :param new_item: A dict of object update attributes
    :param existing: A dict of existing object attributes
    :return: True if any attributes of the new item dict were found to be
             newer than the existing and therefore not updated, otherwise
             False implying that the updated item is equal to the existing.
    """

    # item[created_at] may be updated so keep a copy of the original
    # value in case we process this item again
    new_item.setdefault('data_timestamp', new_item['created_at'])

    # content-type and metadata timestamps may be encoded in
    # item[created_at], or may be set explicitly.
    item_ts_data, item_ts_ctype, item_ts_meta = decode_timestamps(
        new_item['data_timestamp'])

    if new_item.get('ctype_timestamp'):
        item_ts_ctype = Timestamp(new_item.get('ctype_timestamp'))
        item_ts_meta = item_ts_ctype
    if new_item.get('meta_timestamp'):
        item_ts_meta = Timestamp(new_item.get('meta_timestamp'))

    if not existing:
        # encode new_item timestamps into one string for db record
        new_item['created_at'] = encode_timestamps(
            item_ts_data, item_ts_ctype, item_ts_meta)
        return True

    # decode existing timestamp into separate data, content-type and
    # metadata timestamps
    rec_ts_data, rec_ts_ctype, rec_ts_meta = decode_timestamps(
        existing['created_at'])

    # Extract any swift_bytes values from the content_type values. This is
    # necessary because the swift_bytes value to persist should be that at the
    # most recent data timestamp whereas the content-type value to persist is
    # that at the most recent content-type timestamp. The two values happen to
    # be stored in the same database column for historical reasons.
    for item in (new_item, existing):
        content_type, swift_bytes = extract_swift_bytes(item['content_type'])
        item['content_type'] = content_type
        item['swift_bytes'] = swift_bytes

    newer_than_existing = [True, True, True]
    if rec_ts_data >= item_ts_data:
        # apply data attributes from existing record
        new_item.update([(k, existing[k])
                         for k in ('size', 'etag', 'deleted', 'swift_bytes')])
        item_ts_data = rec_ts_data
        newer_than_existing[0] = False
    if rec_ts_ctype >= item_ts_ctype:
        # apply content-type attribute from existing record
        new_item['content_type'] = existing['content_type']
        item_ts_ctype = rec_ts_ctype
        newer_than_existing[1] = False
    if rec_ts_meta >= item_ts_meta:
        # apply metadata timestamp from existing record
        item_ts_meta = rec_ts_meta
        newer_than_existing[2] = False

    # encode updated timestamps into one string for db record
    new_item['created_at'] = encode_timestamps(
        item_ts_data, item_ts_ctype, item_ts_meta)

    # append the most recent swift_bytes onto the most recent content_type in
    # new_item and restore existing to its original state
    for item in (new_item, existing):
        if item['swift_bytes']:
            item['content_type'] += ';swift_bytes=%s' % item['swift_bytes']
        del item['swift_bytes']

    return any(newer_than_existing)


def merge_data(item, existing):
    # The Parameters match one of the following:
    #  1. Prefix + item, existing = existing + item
    #  2. Prefix + item, prefix + existing = prefix + exiting + item
    #  3. item, existing = item
    #  4. item, prefix + existing = item
    if item and existing:
        # Merge
        prefix = False
        prefix_str = ''
        if '+' not in item and '-' not in item:
            # no item prefix, short circuit to item (No. 3 or 4).
            return item, False

        if '+' in existing or '-' in existing:
            prefix = True

        item = int(existing) + int(item)
        if prefix:
            prefix_str = '-' if item < 0 else '+'
        else:
            # Can't be -'ve
            if item < 0:
                item = 0
        return ("%s%d" % (prefix_str, item), prefix)
    elif item:
        return item, None
    elif existing:
        return existing, None
    else:
        # Both missing
        return '', True


def merge_pivots(item, existing):
    if not existing:
        return True
    if existing['created_at'] < item['created_at']:
        return True
    elif existing['created_at'] > item['created_at']:
        return False

    # created_at must be the same, now we need to look for meta data updates
    if existing['meta_timestamp'] > item['meta_timestamp']:
        item, existing = existing, item
    if existing['meta_timestamp'] == item['meta_timestamp']:
        return False
    for col in ('object_count', 'bytes_used'):
        item[col] = str(item[col])
        existing[col] = str(existing[col])
        if '-' not in item[col] and '+' not in item[col]:
            # existing is a definite definition, so just use it.
            continue
        # It is an increment/decrement so we need to merge with item
        item[col], prefixed = merge_data(item[col], existing[col])
        if prefixed:
            item['prefixed'] = True
        else:
            item.pop('prefixed', None)
    return True


class ContainerBroker(DatabaseBroker):
    """
    Encapsulates working with a container database.

    Note that this may involve multiple on-disk DB files, if sharding.
    """
    db_type = 'container'
    db_contains_type = 'object'
    db_reclaim_timestamp = 'created_at'

    def __init__(self, db_file, timeout=BROKER_TIMEOUT, logger=None,
                 account=None, container=None, pending_timeout=None,
                 stale_reads_ok=False):
        super(ContainerBroker, self).__init__(
            db_file, timeout, logger, account, container, pending_timeout,
            stale_reads_ok)
        # The auditor will create a backend using the pivot_db as the db_file.
        if db_file == ':memory:' or db_file.endswith("_pivot.db"):
            self._pivot_db_file = db_file
        else:
            self._pivot_db_file = db_file[:-len('.db')] + "_pivot.db"

    def get_db_state(self):
        if self._db_file == ':memory:':
            return DB_STATE_UNSHARDED
        db_exists = os.path.exists(self._db_file) and \
            self._db_file != self._pivot_db_file
        pivot_exists = os.path.exists(self._pivot_db_file)
        if db_exists and not pivot_exists:
            return DB_STATE_UNSHARDED
        elif db_exists and pivot_exists:
            return DB_STATE_SHARDING
        elif not db_exists and pivot_exists:
            return DB_STATE_SHARDED
        else:
            # Neither db nor pivot db exists
            return DB_STATE_NOTFOUND

    @property
    def db_file(self):
        db_state = self.get_db_state()
        if db_state in (DB_STATE_NOTFOUND, DB_STATE_UNSHARDED):
            return self._db_file
        elif db_state in (DB_STATE_SHARDING, DB_STATE_SHARDED):
            return self._pivot_db_file

    def _db_exists(self):
        return self.get_db_state() != DB_STATE_NOTFOUND

    @property
    def storage_policy_index(self):
        if not hasattr(self, '_storage_policy_index'):
            self._storage_policy_index = \
                self.get_info()['storage_policy_index']
        return self._storage_policy_index

    def _initialize(self, conn, put_timestamp, storage_policy_index):
        """
        Create a brand new container database (tables, indices, triggers, etc.)
        """
        if not self.account:
            raise ValueError(
                'Attempting to create a new database with no account set')
        if not self.container:
            raise ValueError(
                'Attempting to create a new database with no container set')
        if storage_policy_index is None:
            storage_policy_index = 0
        self.create_object_table(conn)
        self.create_policy_stat_table(conn, storage_policy_index)
        self.create_container_info_table(conn, put_timestamp,
                                         storage_policy_index)
        self.create_pivot_ranges_table(conn)

    def create_object_table(self, conn):
        """
        Create the object table which is specific to the container DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

        :param conn: DB connection object
        """
        conn.executescript("""
            CREATE TABLE object (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                created_at TEXT,
                size INTEGER,
                content_type TEXT,
                etag TEXT,
                deleted INTEGER DEFAULT 0,
                storage_policy_index INTEGER DEFAULT 0
            );

            CREATE INDEX ix_object_deleted_name ON object (deleted, name);

            CREATE TRIGGER object_update BEFORE UPDATE ON object
            BEGIN
                SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
            END;

        """ + POLICY_STAT_TRIGGER_SCRIPT)

    def create_container_info_table(self, conn, put_timestamp,
                                    storage_policy_index):
        """
        Create the container_info table which is specific to the container DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.
        Also creates the container_stat view.

        :param conn: DB connection object
        :param put_timestamp: put timestamp
        :param storage_policy_index: storage policy index
        """
        if put_timestamp is None:
            put_timestamp = Timestamp(0).internal
        # The container_stat view is for compatibility; old versions of Swift
        # expected a container_stat table with columns "object_count" and
        # "bytes_used", but when that stuff became per-storage-policy and
        # moved to the policy_stat table, we stopped creating those columns in
        # container_stat.
        #
        # To retain compatibility, we create the container_stat view with some
        # triggers to make it behave like the old container_stat table. This
        # way, if an old version of Swift encounters a database with the new
        # schema, it can still work.
        #
        # Note that this can occur during a rolling Swift upgrade if a DB gets
        # rsynced from an old node to a new, so it's necessary for
        # availability during upgrades. The fact that it enables downgrades is
        # a nice bonus.
        conn.executescript(CONTAINER_INFO_TABLE_SCRIPT +
                           CONTAINER_STAT_VIEW_SCRIPT)
        conn.execute("""
            INSERT INTO container_info (account, container, created_at, id,
                put_timestamp, status_changed_at, storage_policy_index)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (self.account, self.container, Timestamp.now().internal,
              str(uuid4()), put_timestamp, put_timestamp,
              storage_policy_index))

    def create_policy_stat_table(self, conn, storage_policy_index=0):
        """
        Create policy_stat table.

        :param conn: DB connection object
        :param storage_policy_index: the policy_index the container is
                                     being created with
        """
        conn.executescript(POLICY_STAT_TABLE_CREATE)
        conn.execute("""
            INSERT INTO policy_stat (storage_policy_index)
            VALUES (?)
        """, (storage_policy_index,))

    def create_pivot_ranges_table(self, conn):
        """
        Create the pivot_ranges table which is specific to the container DB.

        :param conn: DB connection object
        """
        conn.executescript("""
            CREATE TABLE pivot_ranges (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                lower TEXT,
                upper TEXT,
                object_count INTEGER DEFAULT 0,
                bytes_used INTEGER DEFAULT 0,
                created_at TEXT,
                meta_timestamp TEXT,
                deleted INTEGER DEFAULT 0
            );
        """)

    def get_db_version(self, conn):
        if self._db_version == -1:
            self._db_version = 0
            for row in conn.execute('''
                    SELECT name FROM sqlite_master
                    WHERE name = 'ix_object_deleted_name' '''):
                self._db_version = 1
        return self._db_version

    def _newid(self, conn):
        conn.execute('''
            UPDATE container_stat
            SET reported_put_timestamp = 0, reported_delete_timestamp = 0,
                reported_object_count = 0, reported_bytes_used = 0''')

    def _delete_db(self, conn, timestamp):
        """
        Mark the DB as deleted

        :param conn: DB connection object
        :param timestamp: timestamp to mark as deleted
        """
        conn.execute("""
            UPDATE container_stat
            SET delete_timestamp = ?,
                status = 'DELETED',
                status_changed_at = ?
            WHERE delete_timestamp < ? """, (timestamp, timestamp, timestamp))

    def _commit_puts_load(self, item_list, entry):
        """See :func:`swift.common.db.DatabaseBroker._commit_puts_load`"""
        data = pickle.loads(entry.decode('base64'))
        record_type = data[9] if len(data) > 9 else RECORD_TYPE_OBJECT
        if record_type == RECORD_TYPE_PIVOT_NODE:
            (name, timestamp, lower, upper, object_count, bytes_used,
             meta_timestamp, deleted) = data[:8]
            item = {
                'name': name,
                'created_at': timestamp,
                'lower': lower,
                'upper': upper,
                'object_count': object_count,
                'bytes_used': bytes_used,
                'meta_timestamp': meta_timestamp,
                'deleted': deleted,
                'storage_policy_index': 0,
                'record_type': record_type}
        else:
            (name, timestamp, size, content_type, etag, deleted) = data[:6]
            storage_policy_index = data[6] if len(data) > 6 else 0
            content_type_timestamp = data[7] if len(data) > 7 else None
            meta_timestamp = data[8] if len(data) > 8 else None
            item = {
                'name': name,
                'created_at': timestamp,
                'size': size,
                'content_type': content_type,
                'etag': etag,
                'deleted': deleted,
                'storage_policy_index': storage_policy_index,
                'ctype_timestamp': content_type_timestamp,
                'meta_timestamp': meta_timestamp,
                'record_type': record_type}
        item_list.append(item)

    def empty(self):
        """
        Check if container DB is empty.

        :returns: True if the database has no active objects, False otherwise
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            try:
                if self.get_db_state() == DB_STATE_SHARDED:
                    row = conn.execute(
                        'SELECT max(object_count) from pivot_ranges where '
                        'deleted = 0').fetchone()
                else:
                    row = conn.execute(
                        'SELECT max(object_count) from policy_stat').fetchone()
            except sqlite3.OperationalError as err:
                if not any(msg in str(err) for msg in (
                        "no such column: storage_policy_index",
                        "no such table: policy_stat")):
                    raise
                row = conn.execute(
                    'SELECT object_count from container_stat').fetchone()
            return (row[0] == 0)

    def delete_object(self, name, timestamp, storage_policy_index=0):
        """
        Mark an object deleted.

        :param name: object name to be deleted
        :param timestamp: timestamp when the object was marked as deleted
        :param storage_policy_index: the storage policy index for the object
        """
        self.put_object(name, timestamp, 0, 'application/deleted', 'noetag',
                        deleted=1, storage_policy_index=storage_policy_index)

    def make_tuple_for_pickle(self, record):
        if record['record_type'] == RECORD_TYPE_PIVOT_NODE:
            # NB: there's some padding since record_type *MUST* be at index 9
            return (record['name'], record['created_at'], record['lower'],
                    record['upper'], record['object_count'],
                    record['bytes_used'], record['meta_timestamp'],
                    record['deleted'], 0, record['record_type'])
        return (record['name'], record['created_at'], record['size'],
                record['content_type'], record['etag'], record['deleted'],
                record['storage_policy_index'],
                record['ctype_timestamp'],
                record['meta_timestamp'],
                record['record_type'])

    def put_object(self, name, timestamp, size, content_type, etag, deleted=0,
                   storage_policy_index=0, ctype_timestamp=None,
                   meta_timestamp=None):
        """
        Creates an object in the DB with its metadata.

        :param name: object name to be created
        :param timestamp: timestamp of when the object was created
        :param size: object size
        :param content_type: object content-type
        :param etag: object etag
        :param deleted: if True, marks the object as deleted and sets the
                        deleted_at timestamp to timestamp
        :param storage_policy_index: the storage policy index for the object
        :param ctype_timestamp: timestamp of when content_type was last
                                updated
        :param meta_timestamp: timestamp of when metadata was last updated
        """
        record = {'name': name, 'created_at': timestamp, 'size': size,
                  'content_type': content_type, 'etag': etag,
                  'deleted': deleted,
                  'storage_policy_index': storage_policy_index,
                  'ctype_timestamp': ctype_timestamp,
                  'meta_timestamp': meta_timestamp,
                  'record_type': RECORD_TYPE_OBJECT}
        self.put_record(record)

    def delete_pivot(self, name, timestamp, meta_timestamp=None,
                     lower=None, upper=None):
        """
        Mark an pivot range deleted.

        :param name: pivot name to be deleted
        :param timestamp: timestamp when the object was marked as deleted
        :param meta_timestamp: timestamp of when metadata was last updated
        :param lower: pivots lower range
        :param upper: pivots upper range
        """
        self.put_pivot(name, timestamp, meta_timestamp, deleted=1, lower=lower,
                       upper=upper, object_count=0, bytes_used=0)

    def put_pivot(self, name, timestamp, meta_timestamp=None, deleted=0,
                  lower=None, upper=None, object_count=0, bytes_used=0):
        """
        Creates a pivot range in the DB with its metadata.

        :param name: pivot name to be created
        :param timestamp: timestamp of when the object was created
        :param meta_timestamp: timestamp of when metadata was last updated
        :param deleted: if True, marks the object as deleted and sets the
                        deleted_at timestamp to timestamp
        :param lower: pivots lower range
        :param upper: pivots upper range
        :param object_count: number of object stored in range
        :param bytes_used: number of bytes used in the range
        """
        record = {'name': name, 'created_at': timestamp, 'lower': lower,
                  'upper': upper, 'object_count': object_count,
                  'bytes_used': bytes_used, 'deleted': deleted,
                  'storage_policy_index': 0,
                  'meta_timestamp': meta_timestamp,
                  'record_type': RECORD_TYPE_PIVOT_NODE}
        self.put_record(record)

    def _is_deleted_info(self, object_count, put_timestamp, delete_timestamp,
                         **kwargs):
        """
        Apply delete logic to database info.

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        # The container is considered deleted if the delete_timestamp
        # value is greater than the put_timestamp, and there are no
        # objects in the container.
        return (object_count in (None, '', 0, '0')) and (
            Timestamp(delete_timestamp) > Timestamp(put_timestamp))

    def _is_deleted(self, conn):
        """
        Check container_stat view and evaluate info.

        :param conn: database conn

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        info = conn.execute('''
            SELECT put_timestamp, delete_timestamp, object_count
            FROM container_stat''').fetchone()
        return self._is_deleted_info(**info)

    def get_info_is_deleted(self):
        """
        Get the is_deleted status and info for the container.

        :returns: a tuple, in the form (info, is_deleted) info is a dict as
                  returned by get_info and is_deleted is a boolean.
        """
        if not self._db_exists():
            return {}, True
        info = self.get_info()
        return info, self._is_deleted_info(**info)

    def get_replication_info(self):
        info = super(ContainerBroker, self).get_replication_info()
        info['pivot_max_row'] = self.get_max_row('pivot_points')
        return info

    def _get_info(self):
        self._commit_puts_stale_ok()
        with self.get() as conn:
            data = None
            trailing_sync = 'x_container_sync_point1, x_container_sync_point2'
            trailing_pol = 'storage_policy_index'
            errors = set()
            while not data:
                try:
                    data = conn.execute(('''
                        SELECT account, container, created_at, put_timestamp,
                            delete_timestamp, status_changed_at,
                            object_count, bytes_used,
                            reported_put_timestamp, reported_delete_timestamp,
                            reported_object_count, reported_bytes_used, hash,
                            id, %s, %s
                            FROM container_stat
                    ''') % (trailing_sync, trailing_pol)).fetchone()
                except sqlite3.OperationalError as err:
                    err_msg = str(err)
                    if err_msg in errors:
                        # only attempt migration once
                        raise
                    errors.add(err_msg)
                    if 'no such column: storage_policy_index' in err_msg:
                        trailing_pol = '0 AS storage_policy_index'
                    elif 'no such column: x_container_sync_point' in err_msg:
                        trailing_sync = '-1 AS x_container_sync_point1, ' \
                                        '-1 AS x_container_sync_point2'
                    else:
                        raise
            data = dict(data)
            # populate instance cache
            self._storage_policy_index = data['storage_policy_index']
            self.account = data['account']
            self.container = data['container']

            data['db_state'] = self.get_db_state()

            return data

    def get_info(self):
        """
        Get global data for the container.

        :returns: dict with keys: account, container, created_at,
                  put_timestamp, delete_timestamp, status_changed_at,
                  object_count, bytes_used, reported_put_timestamp,
                  reported_delete_timestamp, reported_object_count,
                  reported_bytes_used, hash, id, x_container_sync_point1,
                  x_container_sync_point2, and storage_policy_index,
                  pivot_point.
        """
        data = self._get_info()

        if data['db_state'] == DB_STATE_SHARDING:
            # grab the obj_count, bytes used from locked DB. We need
            # obj_count for sharding.
            other_info = self.get_brokers()[0]._get_info()
            data.update({'object_count': other_info.get('object_count', 0),
                         'bytes_used': other_info.get('bytes_used', 0)})
        elif data['db_state'] == DB_STATE_SHARDED:
            data.update(self.get_pivot_usage())

        return data

    def set_x_container_sync_points(self, sync_point1, sync_point2):
        with self.get() as conn:
            try:
                self._set_x_container_sync_points(conn, sync_point1,
                                                  sync_point2)
            except sqlite3.OperationalError as err:
                if 'no such column: x_container_sync_point' not in \
                        str(err):
                    raise
                self._migrate_add_container_sync_points(conn)
                self._set_x_container_sync_points(conn, sync_point1,
                                                  sync_point2)
            conn.commit()

    def _set_x_container_sync_points(self, conn, sync_point1, sync_point2):
        if sync_point1 is not None and sync_point2 is not None:
            conn.execute('''
                UPDATE container_stat
                SET x_container_sync_point1 = ?,
                    x_container_sync_point2 = ?
            ''', (sync_point1, sync_point2))
        elif sync_point1 is not None:
            conn.execute('''
                UPDATE container_stat
                SET x_container_sync_point1 = ?
            ''', (sync_point1,))
        elif sync_point2 is not None:
            conn.execute('''
                UPDATE container_stat
                SET x_container_sync_point2 = ?
            ''', (sync_point2,))

    def get_policy_stats(self):
        with self.get() as conn:
            try:
                info = conn.execute('''
                    SELECT storage_policy_index, object_count, bytes_used
                    FROM policy_stat
                ''').fetchall()
            except sqlite3.OperationalError as err:
                if not any(msg in str(err) for msg in (
                        "no such column: storage_policy_index",
                        "no such table: policy_stat")):
                    raise
                info = conn.execute('''
                    SELECT 0 as storage_policy_index, object_count, bytes_used
                    FROM container_stat
                ''').fetchall()
        policy_stats = {}
        for row in info:
            stats = dict(row)
            key = stats.pop('storage_policy_index')
            policy_stats[key] = stats
        return policy_stats

    def has_multiple_policies(self):
        with self.get() as conn:
            try:
                curs = conn.execute('''
                    SELECT count(storage_policy_index)
                    FROM policy_stat
                    ''').fetchone()
            except sqlite3.OperationalError as err:
                if 'no such table: policy_stat' not in str(err):
                    raise
                # no policy_stat row
                return False
            if curs and curs[0] > 1:
                return True
            # only one policy_stat row
            return False

    def set_storage_policy_index(self, policy_index, timestamp=None):
        """
        Update the container_stat policy_index and status_changed_at.
        """
        if timestamp is None:
            timestamp = Timestamp.now().internal

        def _setit(conn):
            conn.execute('''
                INSERT OR IGNORE INTO policy_stat (storage_policy_index)
                VALUES (?)
             ''', (policy_index,))
            conn.execute('''
                UPDATE container_stat
                SET storage_policy_index = ?,
                    status_changed_at = MAX(?, status_changed_at)
                WHERE storage_policy_index <> ?
            ''', (policy_index, timestamp, policy_index))
            conn.commit()

        with self.get() as conn:
            try:
                _setit(conn)
            except sqlite3.OperationalError as err:
                if not any(msg in str(err) for msg in (
                        "no such column: storage_policy_index",
                        "no such table: policy_stat")):
                    raise
                self._migrate_add_storage_policy(conn)
                _setit(conn)

        self._storage_policy_index = policy_index

    def reported(self, put_timestamp, delete_timestamp, object_count,
                 bytes_used):
        """
        Update reported stats, available with container's `get_info`.

        :param put_timestamp: put_timestamp to update
        :param delete_timestamp: delete_timestamp to update
        :param object_count: object_count to update
        :param bytes_used: bytes_used to update
        """
        with self.get() as conn:
            conn.execute('''
                UPDATE container_stat
                SET reported_put_timestamp = ?, reported_delete_timestamp = ?,
                    reported_object_count = ?, reported_bytes_used = ?
            ''', (put_timestamp, delete_timestamp, object_count, bytes_used))
            conn.commit()

    def list_objects_iter(self, limit, marker, end_marker, prefix, delimiter,
                          path=None, storage_policy_index=0, reverse=False,
                          include_end_marker=False, include_deleted=False):
        """
        Get a list of objects sorted by name starting at marker onward, up
        to limit entries.  Entries will begin with the prefix and will not
        have the delimiter after the prefix.

        :param limit: maximum number of entries to get
        :param marker: marker query
        :param end_marker: end marker query
        :param prefix: prefix query
        :param delimiter: delimiter for query
        :param path: if defined, will set the prefix and delimiter based on
                     the path
        :param storage_policy_index: storage policy index for query
        :param reverse: reverse the result order.
        :param include_end_marker: Include the item at end_marker in results
        :param include_deleted: Include items that have the delete marker set

        :returns: list of tuples of (name, created_at, size, content_type,
                  etag, deleted)
        """
        delim_force_gte = False
        (marker, end_marker, prefix, delimiter, path) = utf8encode(
            marker, end_marker, prefix, delimiter, path)
        self._commit_puts_stale_ok()
        if reverse:
            # Reverse the markers if we are reversing the listing.
            marker, end_marker = end_marker, marker
        if path is not None:
            prefix = path
            if path:
                prefix = path = path.rstrip('/') + '/'
            delimiter = '/'
        elif delimiter and not prefix:
            prefix = ''
        if prefix:
            end_prefix = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        orig_marker = marker
        with self.get() as conn:
            results = []
            while len(results) < limit:
                query = 'SELECT name, created_at, size, content_type, etag'
                if self.get_db_version(conn) < 1:
                    query += ', +deleted '
                else:
                    query += ', deleted '
                query += 'FROM object '
                query_args = []
                query_conditions = []
                if end_marker and (not prefix or end_marker < end_prefix):
                    if include_end_marker:
                        query_conditions.append('name <= ?')
                    else:
                        query_conditions.append('name < ?')
                    query_args.append(end_marker)
                elif prefix:
                    query_conditions.append('name < ?')
                    query_args.append(end_prefix)

                if delim_force_gte:
                    query_conditions.append('name >= ?')
                    query_args.append(marker)
                    # Always set back to False
                    delim_force_gte = False
                elif marker and marker >= prefix:
                    query_conditions.append('name > ?')
                    query_args.append(marker)
                elif prefix:
                    query_conditions.append('name >= ?')
                    query_args.append(prefix)
                if not include_deleted:
                    if self.get_db_version(conn) < 1:
                        query_conditions.append('+deleted = 0')
                    else:
                        query_conditions.append('deleted = 0')
                # storage policy filter
                query_conditions.append('storage_policy_index = ?')
                query_args.append(storage_policy_index)
                full_query = query
                if query_conditions:
                    full_query += 'WHERE ' + ' AND '.join(query_conditions)
                tail_query = '''
                    ORDER BY name %s LIMIT ?
                ''' % ('DESC' if reverse else '')
                tail_args = [limit - len(results)]
                try:
                    curs = conn.execute(full_query + tail_query,
                                        tuple(query_args + tail_args))
                except sqlite3.OperationalError as err:
                    if 'no such column: storage_policy_index' not in str(err):
                        raise
                    query_conditions = query_conditions[:-1]
                    query_args = query_args[:-1]
                    full_query = query
                    if query_conditions:
                        full_query += 'WHERE ' + ' AND '.join(query_conditions)
                    curs = conn.execute(full_query + tail_query,
                                        tuple(query_args + tail_args))
                curs.row_factory = None

                # Delimiters without a prefix is ignored, further if there
                # is no delimiter then we can simply return the result as
                # prefixes are now handled in the SQL statement.
                if prefix is None or not delimiter:
                    return [self._transform_record(r) for r in curs]

                # We have a delimiter and a prefix (possibly empty string) to
                # handle
                rowcount = 0
                for row in curs:
                    rowcount += 1
                    name = row[0]
                    if reverse:
                        end_marker = name
                    else:
                        marker = name

                    if len(results) >= limit:
                        curs.close()
                        return results
                    end = name.find(delimiter, len(prefix))
                    if path is not None:
                        if name == path:
                            continue
                        if end >= 0 and len(name) > end + len(delimiter):
                            if reverse:
                                end_marker = name[:end + 1]
                            else:
                                marker = name[:end] + chr(ord(delimiter) + 1)
                            curs.close()
                            break
                    elif end >= 0:
                        if reverse:
                            end_marker = name[:end + 1]
                        else:
                            marker = name[:end] + chr(ord(delimiter) + 1)
                            # we want result to be inclusive of delim+1
                            delim_force_gte = True
                        dir_name = name[:end + 1]
                        if dir_name != orig_marker:
                            results.append([dir_name, '0', 0, None, ''])
                        curs.close()
                        break
                    results.append(self._transform_record(row))
                if not rowcount:
                    break
            return results

    def _transform_record(self, record):
        """
        Decode the created_at timestamp into separate data, content-type and
        meta timestamps and replace the created_at timestamp with the
        metadata timestamp i.e. the last-modified time.
        """
        t_data, t_ctype, t_meta = decode_timestamps(record[1])
        return (record[0], t_meta.internal) + record[2:]

    def _record_to_dict(self, rec, record_type=RECORD_TYPE_OBJECT):
        if rec:
            if record_type == RECORD_TYPE_OBJECT:
                keys = ('name', 'created_at', 'size', 'content_type', 'etag',
                        'deleted', 'storage_policy_index')
            else:
                keys = ('name', 'created_at', 'lower', 'upper', 'object_count',
                        'bytes_used', 'meta_timestamp', 'deleted')
            result = dict(zip(keys, rec))
            result.update({'record_type': record_type})
            return result
        return None

    def merge_items(self, item_list, source=None):
        """
        Merge items into the object or pivot_ranges tables.

        :param item_list: list of dictionaries of {'name', 'created_at',
                          'size', 'content_type', 'etag', 'deleted',
                          'storage_policy_index', 'ctype_timestamp',
                          'meta_timestamp', 'record_type'}
        :param source: if defined, update incoming_sync with the source
        """
        for item in item_list:
            item.setdefault('record_type', RECORD_TYPE_OBJECT)
            cols = ['name']
            if item['record_type'] == RECORD_TYPE_PIVOT_NODE:
                cols += ['lower', 'upper']
            for col in cols:
                if isinstance(item[col], six.text_type):
                    item[col] = item[col].encode('utf-8')

        pivot_range_list = [item for item in item_list
                            if item['record_type'] == RECORD_TYPE_PIVOT_NODE]

        item_list = [item for item in item_list
                     if item['record_type'] == RECORD_TYPE_OBJECT]

        def get_records_object(curs, chunk, query_mod):
            sql = (('SELECT name, created_at, size, content_type,'
                    'etag, deleted, storage_policy_index '
                    'FROM object '
                    'WHERE %s name IN (%s)')
                   % (query_mod, ','.join('?' * len(chunk))))
            return (((record[0], record[6]), record)
                    for record in curs.execute(sql, chunk))

        def get_records_pivot(curs, chunk, query_mod):
            sql = (('SELECT name, created_at, lower, upper, object_count, '
                    'bytes_used, meta_timestamp, deleted '
                    'FROM pivot_ranges '
                    'WHERE %s name IN (%s)')
                   % (query_mod,
                      ','.join('?' * len(chunk))))
            return (((rec[0],), rec)
                    for rec in curs.execute(sql, chunk))

        def _merge_items(curs, rec_list, is_object=True):
            query_mod = ''
            if self.get_db_version(conn) >= 1:
                query_mod = ' deleted IN (0, 1) AND '

            if is_object:
                get_records = get_records_object
                keys = ('name', 'storage_policy_index')
                del_keys = ('name', 'storage_policy_index')
                add_keys = ('name', 'created_at', 'size', 'content_type',
                            'etag', 'deleted', 'storage_policy_index')
                transform_item = update_new_item_from_existing
                table = 'object'
                record_type = RECORD_TYPE_OBJECT
            else:
                get_records = get_records_pivot
                keys = ('name',)
                del_keys = ('name',)
                add_keys = ('name', 'created_at', 'lower', 'upper',
                            'object_count', 'bytes_used', 'meta_timestamp',
                            'deleted')
                transform_item = merge_pivots
                table = 'pivot_ranges'
                record_type = RECORD_TYPE_PIVOT_NODE

            # Get created_at times for objects in rec_list that already exist.
            # We must chunk it up to avoid sqlite's limit of 999 args.
            records = {}
            for offset in range(0, len(rec_list), SQLITE_ARG_LIMIT):
                chunk = [record['name'] for record
                         in rec_list[offset:offset + SQLITE_ARG_LIMIT]]
                records.update(
                    get_records(curs, chunk, query_mod))

            # Sort item_list into things that need adding and deleting, based
            # on results of created_at query.
            to_delete = {}
            to_add = {}
            for item in rec_list:
                item.setdefault('storage_policy_index', 0)  # legacy
                item_ident = tuple(item[key] for key in keys)
                existing = self._record_to_dict(records.get(item_ident),
                                                record_type=record_type)
                if transform_item(item, existing):
                    # exists with older timestamp
                    if item_ident in records:
                        to_delete[item_ident] = item
                    # duplicate entries in item_list
                    if item_ident in to_add:
                        transform_item(item, to_add[item_ident])
                    to_add[item_ident] = item

            if not is_object:
                # Now that all the to_add items are merged, they are either in
                # the form of incremented '+|-<count>' or absolute
                # '<count>'. If the former we need to increment before
                # we delete the current values.
                items = [i for i in to_add.values() if i.get('prefixed')]
                for item in items:
                    self.update_pivot_usage(item)

            if to_delete:
                filters = ' AND '.join(['%s = ?' % key for key in del_keys])
                sql = 'DELETE FROM %s WHERE %s%s' % (table, query_mod, filters)
                del_generator = (tuple([record[key] for key in del_keys])
                                 for record in to_delete.values())
                curs.executemany(sql, del_generator)

            if to_add:
                vals = ','.join('?' * len(add_keys))
                sql = 'INSERT INTO %s %s VALUES (%s)' % (table, add_keys, vals)
                add_generator = (tuple([record[key] for key in add_keys])
                                 for record in to_add.values())
                curs.executemany(sql, add_generator)

        def _really_merge_items(conn):
            curs = conn.cursor()
            curs.execute('BEGIN IMMEDIATE')
            if item_list:
                _merge_items(curs, item_list)
            if pivot_range_list:
                _merge_items(curs, pivot_range_list, False)
            if source and item_list:
                # for replication we rely on the remote end sending merges in
                # order with no gaps to increment sync_points
                sync_point = item_list[-1]['ROWID']
                curs.execute('''
                    UPDATE incoming_sync SET
                    sync_point=max(?, sync_point) WHERE remote_id=?
                ''', (sync_point, source))
                if curs.rowcount < 1:
                    curs.execute('''
                        INSERT INTO incoming_sync (sync_point, remote_id)
                        VALUES (?, ?)
                    ''', (sync_point, source))
            conn.commit()

        with self.get() as conn:
            try:
                return _really_merge_items(conn)
            except sqlite3.OperationalError as err:
                if 'no such column: storage_policy_index' not in str(err):
                    raise
                self._migrate_add_storage_policy(conn)
                return _really_merge_items(conn)

    def get_reconciler_sync(self):
        with self.get() as conn:
            try:
                return conn.execute('''
                    SELECT reconciler_sync_point FROM container_stat
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                if "no such column: reconciler_sync_point" not in str(err):
                    raise
                return -1

    def update_reconciler_sync(self, point):
        query = '''
            UPDATE container_stat
            SET reconciler_sync_point = ?
        '''
        with self.get() as conn:
            try:
                conn.execute(query, (point,))
            except sqlite3.OperationalError as err:
                if "no such column: reconciler_sync_point" not in str(err):
                    raise
                self._migrate_add_storage_policy(conn)
                conn.execute(query, (point,))
            conn.commit()

    def get_misplaced_since(self, start, count):
        """
        Get a list of objects which are in a storage policy different
        from the container's storage policy.

        :param start: last reconciler sync point
        :param count: maximum number of entries to get

        :returns: list of dicts with keys: name, created_at, size,
                  content_type, etag, storage_policy_index
        """
        qry = '''
            SELECT ROWID, name, created_at, size, content_type, etag,
                   deleted, storage_policy_index
            FROM object
            WHERE ROWID > ?
            AND storage_policy_index != (
                SELECT storage_policy_index FROM container_stat LIMIT 1)
            ORDER BY ROWID ASC LIMIT ?
        '''
        self._commit_puts_stale_ok()
        with self.get() as conn:
            try:
                cur = conn.execute(qry, (start, count))
            except sqlite3.OperationalError as err:
                if "no such column: storage_policy_index" not in str(err):
                    raise
                return []
            return list(dict(row) for row in cur.fetchall())

    def _migrate_add_container_sync_points(self, conn):
        """
        Add the x_container_sync_point columns to the 'container_stat' table.
        """
        conn.executescript('''
            BEGIN;
            ALTER TABLE container_stat
            ADD COLUMN x_container_sync_point1 INTEGER DEFAULT -1;
            ALTER TABLE container_stat
            ADD COLUMN x_container_sync_point2 INTEGER DEFAULT -1;
            COMMIT;
        ''')

    def _migrate_add_storage_policy(self, conn):
        """
        Migrate the container schema to support tracking objects from
        multiple storage policies.  If the container_stat table has any
        pending migrations, they are applied now before copying into
        container_info.

         * create the 'policy_stat' table.
         * copy the current 'object_count' and 'bytes_used' columns to a
           row in the 'policy_stat' table.
         * add the storage_policy_index column to the 'object' table.
         * drop the 'object_insert' and 'object_delete' triggers.
         * add the 'object_insert_policy_stat' and
           'object_delete_policy_stat' triggers.
         * create container_info table for non-policy container info
         * insert values from container_stat into container_info
         * drop container_stat table
         * create container_stat view
        """

        # I tried just getting the list of column names in the current
        # container_stat table with a pragma table_info, but could never get
        # it inside the same transaction as the DDL (non-DML) statements:
        #     https://docs.python.org/2/library/sqlite3.html
        #         #controlling-transactions
        # So we just apply all pending migrations to container_stat and copy a
        # static known list of column names into container_info.
        try:
            self._migrate_add_container_sync_points(conn)
        except sqlite3.OperationalError as e:
            if 'duplicate column' in str(e):
                conn.execute('ROLLBACK;')
            else:
                raise

        try:
            conn.executescript("""
                ALTER TABLE container_stat
                ADD COLUMN metadata TEXT DEFAULT '';
            """)
        except sqlite3.OperationalError as e:
            if 'duplicate column' not in str(e):
                raise

        column_names = ', '.join((
            'account', 'container', 'created_at', 'put_timestamp',
            'delete_timestamp', 'reported_put_timestamp',
            'reported_object_count', 'reported_bytes_used', 'hash', 'id',
            'status', 'status_changed_at', 'metadata',
            'x_container_sync_point1', 'x_container_sync_point2'))

        conn.executescript(
            'BEGIN;' +
            POLICY_STAT_TABLE_CREATE +
            '''
                INSERT INTO policy_stat (
                    storage_policy_index, object_count, bytes_used)
                SELECT 0, object_count, bytes_used
                FROM container_stat;

                ALTER TABLE object
                ADD COLUMN storage_policy_index INTEGER DEFAULT 0;

                DROP TRIGGER object_insert;
                DROP TRIGGER object_delete;
            ''' +
            POLICY_STAT_TRIGGER_SCRIPT +
            CONTAINER_INFO_TABLE_SCRIPT +
            '''
                INSERT INTO container_info (%s)
                SELECT %s FROM container_stat;

                DROP TABLE IF EXISTS container_stat;
            ''' % (column_names, column_names) +
            CONTAINER_STAT_VIEW_SCRIPT +
            'COMMIT;')

    def get_pivot_ranges(self, connection=None):
        """
        :return:
        """

        def _get_pivot_ranges(conn):
            try:
                sql = '''
                SELECT name, created_at, lower, upper, object_count,
                    bytes_used, meta_timestamp
                FROM pivot_ranges
                WHERE deleted=0
                ORDER BY lower, upper;
                '''
                data = conn.execute(sql)
                data.row_factory = None
                return [row for row in data]
            except sqlite3.OperationalError as err:
                if 'no such table: pivot_ranges' not in str(err):
                    raise
                self.create_pivot_ranges_table(conn)
            return []

        self._commit_puts_stale_ok()
        if connection:
            return _get_pivot_ranges(connection)
        else:
            with self.get() as conn:
                return _get_pivot_ranges(conn)

    def update_pivot_usage(self, item):

        def update_usage(current, new_data):
            if not current:
                current = '0'

            if new_data.startswith('-') or new_data.startswith('+'):
                new_data = int(new_data)
                current = int(current)
                new_data += current
                if new_data < 0:
                    new_data = 0

            return int(new_data)

        with self.get() as conn:
            # we need the current value.
            try:
                sql = '''
                SELECT object_count, bytes_used
                FROM pivot_ranges
                WHERE deleted=0 AND
                created_at < ? AND
                name == ?;
                '''
                data = conn.execute(sql, item['created_at'], item['name'])
                row = data.fetchone()
                if not row:
                    row = (0, 0)
            except sqlite3.OperationalError:
                row = (0, 0)

            item['object_count'] = update_usage(row[0], item['object_count'])
            item['bytes_used'] = update_usage(row[1], item['bytes_used'])

    def get_pivot_usage(self):
        self._commit_puts_stale_ok()
        with self.get() as conn:
            try:
                sql = '''
                SELECT sum(object_count), sum(bytes_used)
                FROM pivot_ranges
                WHERE deleted=0;
                '''
                data = conn.execute(sql)
                data.row_factory = None
                row = data.fetchone()
                object_count = row[0]
                bytes_used = row[1]
                return {'bytes_used': bytes_used,
                        'object_count': object_count}
            except sqlite3.OperationalError as err:
                if 'no such table: pivot_ranges' not in str(err):
                    raise
                self.create_pivot_ranges_table(conn)
            return {'bytes_used': 0, 'object_count': 0}

    def pivot_nodes_to_items(self, nodes):
        # TODO: split this to separate helper method for each given type?
        result = list()
        for item in nodes:
            if isinstance(item, PivotRange):
                obj = dict(item, deleted=0)
            else:
                obj = {
                    'name': item[0],
                    'created_at': item[1],
                    'lower': item[2],
                    'upper': item[3],
                    'object_count': item[4],
                    'bytes_used': item[5],
                    'meta_timestamp': item[6],
                    'deleted': item[7] if len(item) > 7 else 0}

            obj.update({
                'storage_policy_index': 0,
                'record_type': RECORD_TYPE_PIVOT_NODE})
            result.append(obj)
        return result

    def build_pivot_ranges(self):
        ranges = list()

        for node in self.get_pivot_ranges():
            ranges.append(PivotRange(*node))
        return ranges

    def _get_next_pivot_point(self, shard_container_size, last_upper, conn):
        try:
            offset = int(shard_container_size) // 2

            sql = 'SELECT name FROM object WHERE deleted=0 '
            args = []
            if last_upper:
                sql += "AND name > ? "
                args.append(last_upper)
            sql += "ORDER BY name LIMIT 1 OFFSET %d" % offset
            data = conn.execute(sql, args)
            data = data.fetchone()
            if not data:
                return None
            return data['name']
        except Exception:
            return ''

    def get_next_pivot_point(self, shard_container_size, last_upper=None,
                             connection=None):
        """
        Finds the next entry in the objects table to use as a pivot point
        when sharding.

        If there is an error or no objects in the container it will return an
        emply string ('').

        :return: The middle object in the container.
        """

        self._commit_puts_stale_ok()
        if connection:
            return self._get_next_pivot_point(shard_container_size, last_upper,
                                              connection)
        else:
            try:
                if self.get_db_state() == DB_STATE_SHARDING:
                    self._create_connection(self._db_file)
                with self.get() as conn:
                    return self._get_next_pivot_point(shard_container_size,
                                                      last_upper, conn)
            finally:
                self._create_connection()

    def is_shrinking(self):
        return self.metadata.get('X-Container-Sysmeta-Shard-Merge') or \
            self.metadata.get('X-Container-Sysmeta-Shard-Shrink')

    def get_shrinking_containers(self):
        res = dict()
        if self.is_shrinking():
            shard_shrink = \
                self.metadata.get('X-Container-Sysmeta-Shard-Shrink')
            if shard_shrink:
                res['shrink'] = shard_shrink[0]
            shard_merge = \
                self.metadata.get('X-Container-Sysmeta-Shard-Merge')
            if shard_merge:
                res['merge'] = shard_merge[0]
        return res

    def is_root_container(self):
        """Is this container a root container

        Any container that doesn't have a root container associated with
        it is a root container. So strictly speaking an unsharded container is
        also a type of root container.
        """
        return not self.metadata.get('X-Container-Sysmeta-Shard-Container')

    @contextmanager
    def sharding_lock(self):
        lockpath = '%s/.sharding' % self.db_dir
        try:
            fd = os.open(lockpath, os.O_WRONLY | os.O_CREAT)
            yield fd
        finally:
            os.close(fd)
            os.unlink(lockpath)

    def has_sharding_lock(self):
        lockpath = '%s/.sharding' % self.db_dir
        return os.path.exists(lockpath)

    def set_sharding_state(self):
        db_state = self.get_db_state()
        if not db_state == DB_STATE_UNSHARDED:
            self.logger.warn("Container '%s/%s' cannot be set to sharding "
                             "state while in %s state", self.account,
                             self.container, DB_STATE[db_state])
            return False

        # firstly lets ensure we have a connection, otherwise we could start
        # playing with the wrong database while setting up the pivot database
        if not self.conn:
            with self.get():
                pass

        # For this initial version, we'll create a new container along side.
        # Later we will remove parts so the pivot DB only has what it really
        # needs
        sub_broker = ContainerBroker(self._pivot_db_file, self.timeout,
                                     self.logger, self.account, self.container,
                                     self.pending_timeout, self.stale_reads_ok)
        sub_broker.initialize(Timestamp.now().internal,
                              self.storage_policy_index)
        sub_broker.update_metadata(self.metadata)

        # If there are pivot_ranges defined.. which can happen when the scanner
        # node finds the first pivot then replicates out to the others who
        # are still in the UNSHARDED state.
        pivot_ranges = self.get_pivot_ranges()
        if pivot_ranges:
            pivot_ranges = \
                [self._record_to_dict(list(r) + [0], RECORD_TYPE_PIVOT_NODE)
                 for r in pivot_ranges]
            sub_broker.merge_items(pivot_ranges)

        # We also need to sync the sync tables as we have no idea how long
        # sharding will take and we want to be able to continue replication
        # (albeit we only ever usync in sharding state, but we need to keep
        # the sync point mutable)
        for incoming in (True, False):
            syncs = self.get_syncs(incoming)
            sub_broker.merge_syncs(syncs, incoming)

        # Initialise the rowid to continue from where the last one ended
        max_row = self.get_max_row()
        with sub_broker.get() as conn:
            try:
                sql = "INSERT into object " \
                      "(ROWID, name, created_at, size, content_type, etag) " \
                    "values (?, 'pivted_remove', ?, 0, '', ?)"
                conn.execute(sql, (max_row, Timestamp.now().internal,
                                   '68b329da9893e34099c7d8ad5cb9c940'))
                conn.execute('DELETE FROM object WHERE ROWID = %d' % max_row)
                conn.commit()
            except sqlite3.OperationalError as err:
                self.logger.error('Failed to set the ROWID of the pivot '
                                  'database for %s/%s: %s', self.account,
                                  self.container, err)

        # Now we need to reset the connection so next time the correct database
        # will be in use.
        self.conn = None

        return True

    def set_sharded_state(self):
        db_state = self.get_db_state()
        if not db_state == DB_STATE_SHARDING:
            self.logger.warn("Container '%s/%s' cannot be set to sharded "
                             "state while in %s state", self.account,
                             self.container, DB_STATE[db_state])
            return False

        # TODO add some checks to see if we are ready to unlink the old db
        try:
            os.unlink(self._db_file)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

        # now reset the connection so next time the correct database will
        # be used
        self.conn = None
        return True

    def get_brokers(self):
        brokers = []
        state = self.get_db_state()
        if state != DB_STATE_SHARDING:
            brokers.append(ContainerBroker(
                self.db_file, self.timeout, self.logger, self.account,
                self.container, self.pending_timeout, self.stale_reads_ok))
        else:
            brokers.append(ContainerBroker(
                self._db_file, self.timeout, self.logger, self.account,
                self.container, self.pending_timeout, self.stale_reads_ok))
            brokers[0]._pivot_db_file = self._db_file
            brokers.append(ContainerBroker(
                self._pivot_db_file, self.timeout, self.logger, self.account,
                self.container, self.pending_timeout, self.stale_reads_ok))
        return brokers

    def get_items_since(self, start, count):
        """
        Get a list of objects in the database between start and end.

        :param start: start ROWID
        :param count: number to get
        :returns: list of objects between start and end
        """
        if self.get_db_state() == DB_STATE_SHARDING:
            # When in sharding state the there are 2 databases that may
            # contain the items. So based on where the point and the max_row
            # of the old readonly database we can figure out where to read
            # from.
            self._create_connection(self._db_file)
            old_max_row = self.get_max_row()
            if old_max_row <= start:
                self._create_connection(self._pivot_db_file)
                return \
                    super(ContainerBroker, self).get_items_since(start, count)

            objs = super(ContainerBroker, self).get_items_since(start, count)
            if len(objs) == count:
                return objs

            self._create_connection(self._pivot_db_file)
            objs.extend(super(ContainerBroker, self).get_items_since(
                old_max_row, count - len(objs)))
            return objs
        else:
            return super(ContainerBroker, self).get_items_since(start, count)
