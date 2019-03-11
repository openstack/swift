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

import six
from six.moves import range
import sqlite3
from eventlet import tpool

from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.exceptions import LockTimeout
from swift.common.utils import Timestamp, encode_timestamps, \
    decode_timestamps, extract_swift_bytes, storage_directory, hash_path, \
    ShardRange, renamer, find_shard_range, MD5_OF_EMPTY_STRING, mkdirs, \
    get_db_files, parse_db_filename, make_db_file_path, split_path
from swift.common.db import DatabaseBroker, utf8encode, BROKER_TIMEOUT, \
    zero_like, DatabaseAlreadyExists

SQLITE_ARG_LIMIT = 999

DATADIR = 'containers'

RECORD_TYPE_OBJECT = 'object'
RECORD_TYPE_SHARD = 'shard'
SHARD_RANGE_TABLE = 'shard_range'

NOTFOUND = 'not_found'
UNSHARDED = 'unsharded'
SHARDING = 'sharding'
SHARDED = 'sharded'
COLLAPSED = 'collapsed'


SHARD_STATS_STATES = [ShardRange.ACTIVE, ShardRange.SHARDING,
                      ShardRange.SHRINKING]
SHARD_LISTING_STATES = SHARD_STATS_STATES + [ShardRange.CLEAVED]
SHARD_UPDATE_STATES = [ShardRange.CREATED, ShardRange.CLEAVED,
                       ShardRange.ACTIVE, ShardRange.SHARDING]


# attribute names in order used when transforming shard ranges from dicts to
# tuples and vice-versa
SHARD_RANGE_KEYS = ('name', 'timestamp', 'lower', 'upper', 'object_count',
                    'bytes_used', 'meta_timestamp', 'deleted', 'state',
                    'state_timestamp', 'epoch')

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


def merge_shards(shard_data, existing):
    """
    Compares ``shard_data`` with ``existing`` and updates ``shard_data`` with
    any items of ``existing`` that take precedence over the corresponding item
    in ``shard_data``.

    :param shard_data: a dict representation of shard range that may be
        modified by this method.
    :param existing: a dict representation of shard range.
    :returns: True if ``shard data`` has any item(s) that are considered to
        take precedence over the corresponding item in ``existing``
    """
    if not existing:
        return True
    if existing['timestamp'] < shard_data['timestamp']:
        # note that currently we do not roll forward any meta or state from
        # an item that was created at older time, newer created time trumps
        return True
    elif existing['timestamp'] > shard_data['timestamp']:
        return False

    new_content = False
    # timestamp must be the same, so preserve existing range bounds and deleted
    for k in ('lower', 'upper', 'deleted'):
        shard_data[k] = existing[k]

    # now we need to look for meta data updates
    if existing['meta_timestamp'] >= shard_data['meta_timestamp']:
        for k in ('object_count', 'bytes_used', 'meta_timestamp'):
            shard_data[k] = existing[k]
    else:
        new_content = True

    if (existing['state_timestamp'] == shard_data['state_timestamp']
            and shard_data['state'] > existing['state']):
        new_content = True
    elif existing['state_timestamp'] >= shard_data['state_timestamp']:
        for k in ('state', 'state_timestamp', 'epoch'):
            shard_data[k] = existing[k]
    else:
        new_content = True
    return new_content


class ContainerBroker(DatabaseBroker):
    """
    Encapsulates working with a container database.

    Note that this may involve multiple on-disk DB files if the container
    becomes sharded:

      * :attr:`_db_file` is the path to the legacy container DB name, i.e.
        ``<hash>.db``. This file should exist for an initialised broker that
        has never been sharded, but will not exist once a container has been
        sharded.
      * :attr:`db_files` is a list of existing db files for the broker. This
        list should have at least one entry for an initialised broker, and
        should have two entries while a broker is in SHARDING state.
      * :attr:`db_file` is the path to whichever db is currently authoritative
        for the container. Depending on the container's state, this may not be
        the same as the ``db_file`` argument given to :meth:`~__init__`, unless
        ``force_db_file`` is True in which case :attr:`db_file` is always equal
        to the ``db_file`` argument given to :meth:`~__init__`.
      * :attr:`pending_file` is always equal to :attr:`_db_file` extended with
        ``.pending``, i.e. ``<hash>.db.pending``.
    """
    db_type = 'container'
    db_contains_type = 'object'
    db_reclaim_timestamp = 'created_at'

    def __init__(self, db_file, timeout=BROKER_TIMEOUT, logger=None,
                 account=None, container=None, pending_timeout=None,
                 stale_reads_ok=False, skip_commits=False,
                 force_db_file=False):
        self._init_db_file = db_file
        if db_file == ':memory:':
            base_db_file = db_file
        else:
            base_db_file = make_db_file_path(db_file, None)
        super(ContainerBroker, self).__init__(
            base_db_file, timeout, logger, account, container, pending_timeout,
            stale_reads_ok, skip_commits=skip_commits)
        # the root account and container are populated on demand
        self._root_account = self._root_container = None
        self._force_db_file = force_db_file
        self._db_files = None

    @classmethod
    def create_broker(cls, device_path, part, account, container, logger=None,
                      epoch=None, put_timestamp=None,
                      storage_policy_index=None):
        """
        Create a ContainerBroker instance. If the db doesn't exist, initialize
        the db file.

        :param device_path: device path
        :param part: partition number
        :param account: account name string
        :param container: container name string
        :param logger: a logger instance
        :param epoch: a timestamp to include in the db filename
        :param put_timestamp: initial timestamp if broker needs to be
            initialized
        :param storage_policy_index: the storage policy index
        :return: a :class:`swift.container.backend.ContainerBroker` instance
        """
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = make_db_file_path(
            os.path.join(device_path, db_dir, hsh + '.db'), epoch)
        broker = ContainerBroker(db_path, account=account, container=container,
                                 logger=logger)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(put_timestamp, storage_policy_index)
            except DatabaseAlreadyExists:
                pass
        return broker

    def get_db_state(self):
        """
        Returns the current state of on disk db files.
        """
        if self._db_file == ':memory:':
            return UNSHARDED
        if not self.db_files:
            return NOTFOUND
        if len(self.db_files) > 1:
            return SHARDING
        if self.db_epoch is None:
            # never been sharded
            return UNSHARDED
        if self.db_epoch != self._own_shard_range().epoch:
            return UNSHARDED
        if not self.get_shard_ranges():
            return COLLAPSED
        return SHARDED

    def sharding_initiated(self):
        """
        Returns True if a broker has shard range state that would be necessary
        for sharding to have been initiated, False otherwise.
        """
        own_shard_range = self.get_own_shard_range()
        if own_shard_range.state in (ShardRange.SHARDING,
                                     ShardRange.SHRINKING,
                                     ShardRange.SHARDED):
            return bool(self.get_shard_ranges())
        return False

    def sharding_required(self):
        """
        Returns True if a broker has shard range state that would be necessary
        for sharding to have been initiated but has not yet completed sharding,
        False otherwise.
        """
        db_state = self.get_db_state()
        return (db_state == SHARDING or
                (db_state == UNSHARDED and self.sharding_initiated()))

    def is_sharded(self):
        return self.get_db_state() == SHARDED

    def reload_db_files(self):
        """
        Reloads the cached list of valid on disk db files for this broker.
        """
        if self._db_file == ':memory:':
            return
        # reset connection so the next access will use the correct DB file
        self.conn = None
        self._db_files = get_db_files(self._init_db_file)

    @property
    def db_files(self):
        """
        Gets the cached list of valid db files that exist on disk for this
        broker.

        The cached list may be refreshed by calling
            :meth:`~swift.container.backend.ContainerBroker.reload_db_files`.

        :return: A list of paths to db files ordered by ascending epoch;
            the list may be empty.
        """
        if not self._db_files:
            self.reload_db_files()
        return self._db_files

    @property
    def db_file(self):
        """
        Get the path to the primary db file for this broker. This is typically
        the db file for the most recent sharding epoch. However, if no db files
        exist on disk, or if ``force_db_file`` was True when the broker was
        constructed, then the primary db file is the file passed to the broker
        constructor.

        :return: A path to a db file; the file does not necessarily exist.
        """
        if self._force_db_file:
            return self._init_db_file
        if self.db_files:
            return self.db_files[-1]
        return self._init_db_file

    @property
    def db_epoch(self):
        hash_, epoch, ext = parse_db_filename(self.db_file)
        return epoch

    @property
    def storage_policy_index(self):
        if not hasattr(self, '_storage_policy_index'):
            self._storage_policy_index = \
                self.get_info()['storage_policy_index']
        return self._storage_policy_index

    @property
    def path(self):
        self._populate_instance_cache()
        return '%s/%s' % (self.account, self.container)

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
        self.create_shard_range_table(conn)
        self._db_files = None

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

    def create_shard_range_table(self, conn):
        """
        Create the shard_range table which is specific to the container DB.

        :param conn: DB connection object
        """
        # Use execute (not executescript) so we get the benefits of our
        # GreenDBConnection. Creating a table requires a whole-DB lock;
        # *any* in-progress cursor will otherwise trip a "database is locked"
        # error.
        conn.execute("""
            CREATE TABLE %s (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                timestamp TEXT,
                lower TEXT,
                upper TEXT,
                object_count INTEGER DEFAULT 0,
                bytes_used INTEGER DEFAULT 0,
                meta_timestamp TEXT,
                deleted INTEGER DEFAULT 0,
                state INTEGER,
                state_timestamp TEXT,
                epoch TEXT
            );
        """ % SHARD_RANGE_TABLE)

        conn.execute("""
            CREATE TRIGGER shard_range_update BEFORE UPDATE ON %s
            BEGIN
                SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
            END;
        """ % SHARD_RANGE_TABLE)

    def get_db_version(self, conn):
        if self._db_version == -1:
            self._db_version = 0
            for row in conn.execute('''
                    SELECT name FROM sqlite_master
                    WHERE name = 'ix_object_deleted_name' '''):
                self._db_version = 1
        return self._db_version

    def _get_deleted_key(self, connection):
        if self.get_db_version(connection) < 1:
            return '+deleted'
        return 'deleted'

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
        (name, timestamp, size, content_type, etag, deleted) = entry[:6]
        if len(entry) > 6:
            storage_policy_index = entry[6]
        else:
            storage_policy_index = 0
        content_type_timestamp = meta_timestamp = None
        if len(entry) > 7:
            content_type_timestamp = entry[7]
        if len(entry) > 8:
            meta_timestamp = entry[8]
        item_list.append({'name': name,
                          'created_at': timestamp,
                          'size': size,
                          'content_type': content_type,
                          'etag': etag,
                          'deleted': deleted,
                          'storage_policy_index': storage_policy_index,
                          'ctype_timestamp': content_type_timestamp,
                          'meta_timestamp': meta_timestamp})

    def _empty(self):
        self._commit_puts_stale_ok()
        with self.get() as conn:
            try:
                row = conn.execute(
                    'SELECT max(object_count) from policy_stat').fetchone()
            except sqlite3.OperationalError as err:
                if not any(msg in str(err) for msg in (
                        "no such column: storage_policy_index",
                        "no such table: policy_stat")):
                    raise
                row = conn.execute(
                    'SELECT object_count from container_stat').fetchone()
            return zero_like(row[0])

    def empty(self):
        """
        Check if container DB is empty.

        This method uses more stringent checks on object count than
        :meth:`is_deleted`: this method checks that there are no objects in any
        policy; if the container is in the process of sharding then both fresh
        and retiring databases are checked to be empty; if a root container has
        shard ranges then they are checked to be empty.

        :returns: True if the database has no active objects, False otherwise
        """
        if not all(broker._empty() for broker in self.get_brokers()):
            return False
        if self.is_root_container() and self.sharding_initiated():
            # sharded shards don't get updates from their shards so their shard
            # usage should not be relied upon
            return self.get_shard_usage()['object_count'] <= 0
        return True

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
        return (record['name'], record['created_at'], record['size'],
                record['content_type'], record['etag'], record['deleted'],
                record['storage_policy_index'],
                record['ctype_timestamp'],
                record['meta_timestamp'])

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
                  'meta_timestamp': meta_timestamp}
        self.put_record(record)

    def remove_objects(self, lower, upper, max_row=None):
        """
        Removes object records in the given namespace range from the object
        table.

        Note that objects are removed regardless of their storage_policy_index.

        :param lower: defines the lower bound of object names that will be
            removed; names greater than this value will be removed; names less
            than or equal to this value will not be removed.
        :param upper: defines the upper bound of object names that will be
            removed; names less than or equal to this value will be removed;
            names greater than this value will not be removed. The empty string
            is interpreted as there being no upper bound.
        :param max_row: if specified only rows less than or equal to max_row
            will be removed
        """
        query_conditions = []
        query_args = []
        if max_row is not None:
            query_conditions.append('ROWID <= ?')
            query_args.append(str(max_row))
        if lower:
            query_conditions.append('name > ?')
            query_args.append(lower)
        if upper:
            query_conditions.append('name <= ?')
            query_args.append(upper)

        query = 'DELETE FROM object WHERE deleted in (0, 1)'
        if query_conditions:
            query += ' AND ' + ' AND '.join(query_conditions)

        with self.get() as conn:
            conn.execute(query, query_args)
            conn.commit()

    def _is_deleted_info(self, object_count, put_timestamp, delete_timestamp,
                         **kwargs):
        """
        Apply delete logic to database info.

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        # The container is considered deleted if the delete_timestamp
        # value is greater than the put_timestamp, and there are no
        # objects in the container.
        return zero_like(object_count) and (
            Timestamp(delete_timestamp) > Timestamp(put_timestamp))

    def _is_deleted(self, conn):
        """
        Check if the DB is considered to be deleted.

        This object count used in this check is the same as the container
        object count that would be returned in the result of :meth:`get_info`
        and exposed to a client i.e. it is based on the container_stat view for
        the current storage policy index or relevant shard range usage.

        :param conn: database conn

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        info = conn.execute('''
            SELECT put_timestamp, delete_timestamp, object_count
            FROM container_stat''').fetchone()
        info = dict(info)
        info.update(self._get_alternate_object_stats()[1])
        return self._is_deleted_info(**info)

    def is_reclaimable(self, now, reclaim_age):
        with self.get() as conn:
            info = conn.execute('''
                SELECT put_timestamp, delete_timestamp
                FROM container_stat''').fetchone()
        if (Timestamp(now - reclaim_age) >
            Timestamp(info['delete_timestamp']) >
                Timestamp(info['put_timestamp'])):
            return self.empty()
        return False

    def get_info_is_deleted(self):
        """
        Get the is_deleted status and info for the container.

        :returns: a tuple, in the form (info, is_deleted) info is a dict as
                  returned by get_info and is_deleted is a boolean.
        """
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return {}, True
        info = self.get_info()
        return info, self._is_deleted_info(**info)

    def get_replication_info(self):
        info = super(ContainerBroker, self).get_replication_info()
        info['shard_max_row'] = self.get_max_row(SHARD_RANGE_TABLE)
        return info

    def _do_get_info_query(self, conn):
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
        return data

    def _get_info(self):
        self._commit_puts_stale_ok()
        with self.get() as conn:
            return self._do_get_info_query(conn)

    def _populate_instance_cache(self, conn=None):
        # load cached instance attributes from the database if necessary
        if self.container is None:
            with self.maybe_get(conn) as conn:
                self._do_get_info_query(conn)

    def _get_alternate_object_stats(self):
        state = self.get_db_state()
        if state == SHARDING:
            other_info = self.get_brokers()[0]._get_info()
            stats = {'object_count': other_info['object_count'],
                     'bytes_used': other_info['bytes_used']}
        elif state == SHARDED and self.is_root_container():
            stats = self.get_shard_usage()
        else:
            stats = {}
        return state, stats

    def get_info(self):
        """
        Get global data for the container.

        :returns: dict with keys: account, container, created_at,
                  put_timestamp, delete_timestamp, status_changed_at,
                  object_count, bytes_used, reported_put_timestamp,
                  reported_delete_timestamp, reported_object_count,
                  reported_bytes_used, hash, id, x_container_sync_point1,
                  x_container_sync_point2, and storage_policy_index,
                  db_state.
        """
        data = self._get_info()
        state, stats = self._get_alternate_object_stats()
        data.update(stats)
        data['db_state'] = state
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
                          include_deleted=False, since_row=None,
                          transform_func=None, all_policies=False):
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
        :param include_deleted: if True, include only deleted objects; if
            False (default), include only undeleted objects; otherwise, include
            both deleted and undeleted objects.
        :param since_row: include only items whose ROWID is greater than
            the given row id; by default all rows are included.
        :param transform_func: an optional function that if given will be
            called for each object to get a transformed version of the object
            to include in the listing; should have same signature as
            :meth:`~_transform_record`; defaults to :meth:`~_transform_record`.
        :param all_policies: if True, include objects for all storage policies
            ignoring any value given for ``storage_policy_index``
        :returns: list of tuples of (name, created_at, size, content_type,
                  etag, deleted)
        """
        if include_deleted is True:
            deleted_arg = ' = 1'
        elif include_deleted is False:
            deleted_arg = ' = 0'
        else:
            deleted_arg = ' in (0, 1)'

        if transform_func is None:
            transform_func = self._transform_record
        delim_force_gte = False
        if six.PY2:
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
            deleted_key = self._get_deleted_key(conn)
            query_keys = ['name', 'created_at', 'size', 'content_type',
                          'etag', deleted_key]
            while len(results) < limit:
                query_args = []
                query_conditions = []
                if end_marker and (not prefix or end_marker < end_prefix):
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
                elif marker and (not prefix or marker >= prefix):
                    query_conditions.append('name > ?')
                    query_args.append(marker)
                elif prefix:
                    query_conditions.append('name >= ?')
                    query_args.append(prefix)
                query_conditions.append(deleted_key + deleted_arg)
                if since_row:
                    query_conditions.append('ROWID > ?')
                    query_args.append(since_row)

                def build_query(keys, conditions, args):
                    query = 'SELECT ' + ', '.join(keys) + ' FROM object '
                    if conditions:
                        query += 'WHERE ' + ' AND '.join(conditions)
                    tail_query = '''
                        ORDER BY name %s LIMIT ?
                    ''' % ('DESC' if reverse else '')
                    return query + tail_query, args + [limit - len(results)]

                # storage policy filter
                if all_policies:
                    query, args = build_query(
                        query_keys + ['storage_policy_index'],
                        query_conditions,
                        query_args)
                else:
                    query, args = build_query(
                        query_keys + ['storage_policy_index'],
                        query_conditions + ['storage_policy_index = ?'],
                        query_args + [storage_policy_index])
                try:
                    curs = conn.execute(query, tuple(args))
                except sqlite3.OperationalError as err:
                    if 'no such column: storage_policy_index' not in str(err):
                        raise
                    query, args = build_query(
                        query_keys + ['0 as storage_policy_index'],
                        query_conditions, query_args)
                    curs = conn.execute(query, tuple(args))
                curs.row_factory = None

                # Delimiters without a prefix is ignored, further if there
                # is no delimiter then we can simply return the result as
                # prefixes are now handled in the SQL statement.
                if prefix is None or not delimiter:
                    return [transform_func(r) for r in curs]

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
                    results.append(transform_func(row))
                if not rowcount:
                    break
            return results

    def get_objects(self, limit=None, marker='', end_marker='',
                    include_deleted=None, since_row=None):
        """
        Returns a list of objects, including deleted objects, in all policies.
        Each object in the list is described by a dict with keys {'name',
        'created_at', 'size', 'content_type', 'etag', 'deleted',
        'storage_policy_index'}.

        :param limit: maximum number of entries to get
        :param marker: if set, objects with names less than or equal to this
            value will not be included in the list.
        :param end_marker: if set, objects with names greater than or equal to
            this value will not be included in the list.
        :param include_deleted: if True, include only deleted objects; if
            False, include only undeleted objects; otherwise (default), include
            both deleted and undeleted objects.
        :param since_row: include only items whose ROWID is greater than
            the given row id; by default all rows are included.
        :return: a list of dicts, each describing an object.
        """

        limit = CONTAINER_LISTING_LIMIT if limit is None else limit
        return self.list_objects_iter(
            limit, marker, end_marker, prefix=None, delimiter=None, path=None,
            reverse=False, include_deleted=include_deleted,
            transform_func=self._record_to_dict, since_row=since_row,
            all_policies=True
        )

    def _transform_record(self, record):
        """
        Returns a tuple of (name, last-modified time, size, content_type and
        etag) for the given record.

        The given record's created_at timestamp is decoded into separate data,
        content-type and meta timestamps and the metadata timestamp is used as
        the last-modified time value.
        """
        t_data, t_ctype, t_meta = decode_timestamps(record[1])
        return (record[0], t_meta.internal) + record[2:5]

    def _record_to_dict(self, rec):
        if rec:
            keys = ('name', 'created_at', 'size', 'content_type', 'etag',
                    'deleted', 'storage_policy_index')
            return dict(zip(keys, rec))
        return None

    def merge_items(self, item_list, source=None):
        """
        Merge items into the object table.

        :param item_list: list of dictionaries of {'name', 'created_at',
                          'size', 'content_type', 'etag', 'deleted',
                          'storage_policy_index', 'ctype_timestamp',
                          'meta_timestamp'}
        :param source: if defined, update incoming_sync with the source
        """
        for item in item_list:
            if six.PY2 and isinstance(item['name'], six.text_type):
                item['name'] = item['name'].encode('utf-8')
            elif not six.PY2 and isinstance(item['name'], six.binary_type):
                item['name'] = item['name'].decode('utf-8')

        def _really_really_merge_items(conn):
            curs = conn.cursor()
            if self.get_db_version(conn) >= 1:
                query_mod = ' deleted IN (0, 1) AND '
            else:
                query_mod = ''
            curs.execute('BEGIN IMMEDIATE')
            # Get sqlite records for objects in item_list that already exist.
            # We must chunk it up to avoid sqlite's limit of 999 args.
            records = {}
            for offset in range(0, len(item_list), SQLITE_ARG_LIMIT):
                chunk = [rec['name'] for rec in
                         item_list[offset:offset + SQLITE_ARG_LIMIT]]
                records.update(
                    ((rec[0], rec[6]), rec) for rec in curs.execute(
                        'SELECT name, created_at, size, content_type,'
                        'etag, deleted, storage_policy_index '
                        'FROM object WHERE ' + query_mod + ' name IN (%s)' %
                        ','.join('?' * len(chunk)), chunk))
            # Sort item_list into things that need adding and deleting, based
            # on results of created_at query.
            to_delete = set()
            to_add = {}
            for item in item_list:
                item.setdefault('storage_policy_index', 0)  # legacy
                item_ident = (item['name'], item['storage_policy_index'])
                existing = self._record_to_dict(records.get(item_ident))
                if update_new_item_from_existing(item, existing):
                    if item_ident in records:  # exists with older timestamp
                        to_delete.add(item_ident)
                    if item_ident in to_add:  # duplicate entries in item_list
                        update_new_item_from_existing(item, to_add[item_ident])
                    to_add[item_ident] = item
            if to_delete:
                curs.executemany(
                    'DELETE FROM object WHERE ' + query_mod +
                    'name=? AND storage_policy_index=?',
                    (item_ident for item_ident in to_delete))
            if to_add:
                curs.executemany(
                    'INSERT INTO object (name, created_at, size, content_type,'
                    'etag, deleted, storage_policy_index) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    ((rec['name'], rec['created_at'], rec['size'],
                      rec['content_type'], rec['etag'], rec['deleted'],
                      rec['storage_policy_index'])
                     for rec in to_add.values()))
            if source:
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

        def _really_merge_items(conn):
            return tpool.execute(_really_really_merge_items, conn)

        with self.get() as conn:
            try:
                return _really_merge_items(conn)
            except sqlite3.OperationalError as err:
                if 'no such column: storage_policy_index' not in str(err):
                    raise
                self._migrate_add_storage_policy(conn)
                return _really_merge_items(conn)

    def merge_shard_ranges(self, shard_ranges):
        """
        Merge shard ranges into the shard range table.

        :param shard_ranges: a shard range or a list of shard ranges; each
            shard range should be an instance of
            :class:`~swift.common.utils.ShardRange` or a dict representation of
            a shard range having ``SHARD_RANGE_KEYS``.
        """
        if not shard_ranges:
            return
        if not isinstance(shard_ranges, list):
            shard_ranges = [shard_ranges]

        item_list = []
        for item in shard_ranges:
            if isinstance(item, ShardRange):
                item = dict(item)
            for col in ('name', 'lower', 'upper'):
                if six.PY2 and isinstance(item[col], six.text_type):
                    item[col] = item[col].encode('utf-8')
                elif not six.PY2 and isinstance(item[col], six.binary_type):
                    item[col] = item[col].decode('utf-8')
            item_list.append(item)

        def _really_merge_items(conn):
            curs = conn.cursor()
            curs.execute('BEGIN IMMEDIATE')

            # Get rows for items that already exist.
            # We must chunk it up to avoid sqlite's limit of 999 args.
            records = {}
            for offset in range(0, len(item_list), SQLITE_ARG_LIMIT):
                chunk = [record['name'] for record
                         in item_list[offset:offset + SQLITE_ARG_LIMIT]]
                records.update(
                    (rec[0], rec) for rec in curs.execute(
                        'SELECT %s FROM %s '
                        'WHERE deleted IN (0, 1) AND name IN (%s)' %
                        (', '.join(SHARD_RANGE_KEYS), SHARD_RANGE_TABLE,
                         ','.join('?' * len(chunk))), chunk))

            # Sort item_list into things that need adding and deleting
            to_delete = set()
            to_add = {}
            for item in item_list:
                item_ident = item['name']
                existing = records.get(item_ident)
                if existing:
                    existing = dict(zip(SHARD_RANGE_KEYS, existing))
                if merge_shards(item, existing):
                    # exists with older timestamp
                    if item_ident in records:
                        to_delete.add(item_ident)
                    # duplicate entries in item_list
                    if (item_ident not in to_add or
                            merge_shards(item, to_add[item_ident])):
                        to_add[item_ident] = item

            if to_delete:
                curs.executemany(
                    'DELETE FROM %s WHERE deleted in (0, 1) '
                    'AND name = ?' % SHARD_RANGE_TABLE,
                    ((item_ident,) for item_ident in to_delete))
            if to_add:
                vals = ','.join('?' * len(SHARD_RANGE_KEYS))
                curs.executemany(
                    'INSERT INTO %s (%s) VALUES (%s)' %
                    (SHARD_RANGE_TABLE, ','.join(SHARD_RANGE_KEYS), vals),
                    tuple([item[k] for k in SHARD_RANGE_KEYS]
                          for item in to_add.values()))
            conn.commit()

        with self.get() as conn:
            try:
                return _really_merge_items(conn)
            except sqlite3.OperationalError as err:
                # Without the rollback, new enough (>= py37) python/sqlite3
                # will panic:
                #   sqlite3.OperationalError: cannot start a transaction
                #   within a transaction
                conn.rollback()
                if ('no such table: %s' % SHARD_RANGE_TABLE) not in str(err):
                    raise
                self.create_shard_range_table(conn)
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

    def _reclaim(self, conn, age_timestamp, sync_timestamp):
        super(ContainerBroker, self)._reclaim(conn, age_timestamp,
                                              sync_timestamp)
        # populate instance cache, but use existing conn to avoid deadlock
        # when it has a pending update
        self._populate_instance_cache(conn=conn)
        try:
            conn.execute('''
                DELETE FROM %s WHERE deleted = 1 AND timestamp < ?
                AND name != ?
            ''' % SHARD_RANGE_TABLE, (sync_timestamp, self.path))
        except sqlite3.OperationalError as err:
            if ('no such table: %s' % SHARD_RANGE_TABLE) not in str(err):
                raise

    def _get_shard_range_rows(self, connection=None, include_deleted=False,
                              states=None, include_own=False,
                              exclude_others=False):
        """
        Returns a list of shard range rows.

        To get all shard ranges use ``include_own=True``. To get only the
        broker's own shard range use ``include_own=True`` and
        ``exclude_others=True``.

        :param connection: db connection
        :param include_deleted: include rows marked as deleted
        :param states: include only rows matching the given state(s); can be an
            int or a list of ints.
        :param include_own: boolean that governs whether the row whose name
            matches the broker's path is included in the returned list. If
            True, that row is included, otherwise it is not included. Default
            is False.
        :param exclude_others: boolean that governs whether the rows whose
            names do not match the broker's path are included in the returned
            list. If True, those rows are not included, otherwise they are
            included. Default is False.
        :return: a list of tuples.
        """

        if exclude_others and not include_own:
            return []

        included_states = set()
        if isinstance(states, (list, tuple, set)):
            included_states.update(states)
        elif states is not None:
            included_states.add(states)

        def do_query(conn):
            condition = ''
            conditions = []
            params = []
            if not include_deleted:
                conditions.append('deleted=0')
            if included_states:
                conditions.append('state in (%s)' % ','.join(
                    '?' * len(included_states)))
                params.extend(included_states)
            if not include_own:
                conditions.append('name != ?')
                params.append(self.path)
            if exclude_others:
                conditions.append('name = ?')
                params.append(self.path)
            if conditions:
                condition = ' WHERE ' + ' AND '.join(conditions)
            sql = '''
            SELECT %s
            FROM %s%s;
            ''' % (', '.join(SHARD_RANGE_KEYS), SHARD_RANGE_TABLE, condition)
            data = conn.execute(sql, params)
            data.row_factory = None
            return [row for row in data]

        try:
            with self.maybe_get(connection) as conn:
                return do_query(conn)
        except sqlite3.OperationalError as err:
            if ('no such table: %s' % SHARD_RANGE_TABLE) not in str(err):
                raise
            return []

    @classmethod
    def resolve_shard_range_states(cls, states):
        """
        Given a list of values each of which may be the name of a state, the
        number of a state, or an alias, return the set of state numbers
        described by the list.

        The following alias values are supported: 'listing' maps to all states
        that are considered valid when listing objects; 'updating' maps to all
        states that are considered valid for redirecting an object update.

        :param states: a list of values each of which may be the name of a
            state, the number of a state, or an alias
        :return: a set of integer state numbers, or None if no states are given
        :raises ValueError: if any value in the given list is neither a valid
            state nor a valid alias
        """
        if states:
            resolved_states = set()
            for state in states:
                if state == 'listing':
                    resolved_states.update(SHARD_LISTING_STATES)
                elif state == 'updating':
                    resolved_states.update(SHARD_UPDATE_STATES)
                else:
                    resolved_states.add(ShardRange.resolve_state(state)[0])
            return resolved_states
        return None

    def get_shard_ranges(self, marker=None, end_marker=None, includes=None,
                         reverse=False, include_deleted=False, states=None,
                         include_own=False,
                         exclude_others=False, fill_gaps=False):
        """
        Returns a list of persisted shard ranges.

        :param marker: restricts the returned list to shard ranges whose
            namespace includes or is greater than the marker value.
        :param end_marker: restricts the returned list to shard ranges whose
            namespace includes or is less than the end_marker value.
        :param includes: restricts the returned list to the shard range that
            includes the given value; if ``includes`` is specified then
            ``marker`` and ``end_marker`` are ignored.
        :param reverse: reverse the result order.
        :param include_deleted: include items that have the delete marker set
        :param states: if specified, restricts the returned list to shard
            ranges that have the given state(s); can be a list of ints or a
            single int.
        :param include_own: boolean that governs whether the row whose name
            matches the broker's path is included in the returned list. If
            True, that row is included, otherwise it is not included. Default
            is False.
        :param exclude_others: boolean that governs whether the rows whose
            names do not match the broker's path are included in the returned
            list. If True, those rows are not included, otherwise they are
            included. Default is False.
        :param fill_gaps: if True, insert own shard range to fill any gaps in
            at the tail of other shard ranges.
        :return: a list of instances of :class:`swift.common.utils.ShardRange`
        """
        def shard_range_filter(sr):
            end = start = True
            if end_marker:
                end = end_marker > sr.lower
            if marker:
                start = marker < sr.upper
            return start and end

        if reverse:
            marker, end_marker = end_marker, marker
        if marker and end_marker and marker >= end_marker:
            return []

        shard_ranges = [
            ShardRange(*row)
            for row in self._get_shard_range_rows(
                include_deleted=include_deleted, states=states,
                include_own=include_own,
                exclude_others=exclude_others)]
        # note if this ever changes to *not* sort by upper first then it breaks
        # a key assumption for bisect, which is used by utils.find_shard_ranges
        shard_ranges.sort(key=lambda sr: (sr.upper, sr.state, sr.lower))
        if includes:
            shard_range = find_shard_range(includes, shard_ranges)
            return [shard_range] if shard_range else []

        if marker or end_marker:
            shard_ranges = list(filter(shard_range_filter, shard_ranges))
        if fill_gaps:
            if shard_ranges:
                last_upper = shard_ranges[-1].upper
            else:
                last_upper = marker or ShardRange.MIN
            required_upper = end_marker or ShardRange.MAX
            if required_upper > last_upper:
                filler_sr = self.get_own_shard_range()
                filler_sr.lower = last_upper
                filler_sr.upper = required_upper
                shard_ranges.append(filler_sr)

        if reverse:
            shard_ranges.reverse()

        return shard_ranges

    def _own_shard_range(self, no_default=False):
        shard_ranges = self.get_shard_ranges(include_own=True,
                                             include_deleted=True,
                                             exclude_others=True)
        if shard_ranges:
            own_shard_range = shard_ranges[0]
        elif no_default:
            return None
        else:
            own_shard_range = ShardRange(
                self.path, Timestamp.now(), ShardRange.MIN, ShardRange.MAX,
                state=ShardRange.ACTIVE)
        return own_shard_range

    def get_own_shard_range(self, no_default=False):
        """
        Returns a shard range representing this broker's own shard range. If no
        such range has been persisted in the broker's shard ranges table then a
        default shard range representing the entire namespace will be returned.

        The returned shard range will be updated with the current object stats
        for this broker and a meta timestamp set to the current time. For these
        values to be persisted the caller must merge the shard range.

        :param no_default: if True and the broker's own shard range is not
            found in the shard ranges table then None is returned, otherwise a
            default shard range is returned.
        :return: an instance of :class:`~swift.common.utils.ShardRange`
        """
        own_shard_range = self._own_shard_range(no_default=no_default)
        if own_shard_range:
            info = self.get_info()
            own_shard_range.update_meta(
                info['object_count'], info['bytes_used'])
        return own_shard_range

    def is_own_shard_range(self, shard_range):
        return shard_range.name == self.path

    def enable_sharding(self, epoch):
        """
        Updates this broker's own shard range with the given epoch, sets its
        state to SHARDING and persists it in the DB.

        :param epoch: a :class:`~swift.utils.common.Timestamp`
        :return: the broker's updated own shard range.
        """
        own_shard_range = self._own_shard_range()
        own_shard_range.update_state(ShardRange.SHARDING, epoch)
        own_shard_range.epoch = epoch
        self.merge_shard_ranges(own_shard_range)
        return own_shard_range

    def get_shard_usage(self):
        """
        Get the aggregate object stats for all shard ranges in states ACTIVE,
        SHARDING or SHRINKING.

        :return: a dict with keys {bytes_used, object_count}
        """
        shard_ranges = self.get_shard_ranges(states=SHARD_STATS_STATES)
        return {'bytes_used': sum(sr.bytes_used for sr in shard_ranges),
                'object_count': sum(sr.object_count for sr in shard_ranges)}

    def get_all_shard_range_data(self):
        """
        Returns a list of all shard range data, including own shard range and
        deleted shard ranges.

        :return: A list of dict representations of a ShardRange.
        """
        shard_ranges = self.get_shard_ranges(include_deleted=True,
                                             include_own=True)
        return [dict(sr) for sr in shard_ranges]

    def set_sharding_state(self):
        """
        Creates and initializes a fresh DB file in preparation for sharding a
        retiring DB. The broker's own shard range must have an epoch timestamp
        for this method to succeed.

        :return: True if the fresh DB was successfully created, False
            otherwise.
        """
        epoch = self.get_own_shard_range().epoch
        if not epoch:
            self.logger.warning("Container '%s' cannot be set to sharding "
                                "state: missing epoch", self.path)
            return False
        state = self.get_db_state()
        if not state == UNSHARDED:
            self.logger.warning("Container '%s' cannot be set to sharding "
                                "state while in %s state", self.path, state)
            return False

        info = self.get_info()
        # The tmp_dir is cleaned up by the replicators after reclaim_age, so if
        # we initially create the fresh DB there, we will already have cleanup
        # covered if there is an error.
        tmp_dir = os.path.join(self.get_device_path(), 'tmp')
        if not os.path.exists(tmp_dir):
            mkdirs(tmp_dir)
        tmp_db_file = os.path.join(tmp_dir, "fresh%s.db" % str(uuid4()))
        fresh_broker = ContainerBroker(tmp_db_file, self.timeout, self.logger,
                                       self.account, self.container)
        fresh_broker.initialize(info['put_timestamp'],
                                info['storage_policy_index'])
        # copy relevant data from the retiring db to the fresh db
        fresh_broker.update_metadata(self.metadata)
        fresh_broker.merge_shard_ranges(self.get_all_shard_range_data())
        # copy sync points so that any peer in sync with retiring db will
        # appear to be in sync with the fresh db, although the peer shouldn't
        # attempt to replicate objects to a db with shard ranges.
        for incoming in (True, False):
            syncs = self.get_syncs(incoming)
            fresh_broker.merge_syncs(syncs, incoming)

        max_row = self.get_max_row()
        with fresh_broker.get() as fresh_broker_conn:
            # Initialise the rowid to continue from where the retiring db ended
            try:
                sql = "INSERT into object " \
                      "(ROWID, name, created_at, size, content_type, etag) " \
                    "values (?, 'tmp_sharding', ?, 0, '', ?)"
                fresh_broker_conn.execute(
                    sql, (max_row, Timestamp.now().internal,
                          MD5_OF_EMPTY_STRING))
                fresh_broker_conn.execute(
                    'DELETE FROM object WHERE ROWID = ?', (max_row,))
                fresh_broker_conn.commit()
            except sqlite3.OperationalError as err:
                self.logger.error(
                    'Failed to set the ROWID of the fresh database for %s: %s',
                    self.path, err)
                return False

            # Set the created_at and hash in the container_info table the same
            # in both brokers
            try:
                fresh_broker_conn.execute(
                    'UPDATE container_stat SET created_at=?',
                    (info['created_at'],))
                fresh_broker_conn.commit()
            except sqlite3.OperationalError as err:
                self.logger.error('Failed to set matching created_at time in '
                                  'the fresh database for %s: %s',
                                  self.path, err)
                return False

        # Rename to the new database
        fresh_db_filename = make_db_file_path(self._db_file, epoch)
        renamer(tmp_db_file, fresh_db_filename)
        self.reload_db_files()
        return True

    def set_sharded_state(self):
        """
        Unlink's the broker's retiring DB file.

        :return: True if the retiring DB was successfully unlinked, False
            otherwise.
        """
        state = self.get_db_state()
        if not state == SHARDING:
            self.logger.warning("Container %r cannot be set to sharded "
                                "state while in %s state",
                                self.path, state)
            return False

        self.reload_db_files()
        if len(self.db_files) < 2:
            self.logger.warning(
                'Refusing to delete db file for %r: no fresher db file found '
                'in %r.', self.path, self.db_files)
            return False

        retiring_file = self.db_files[-2]
        try:
            os.unlink(retiring_file)
            self.logger.debug('Unlinked retiring db %r', retiring_file)
        except OSError as err:
            if err.errno != errno.ENOENT:
                self.logger.exception('Failed to unlink %r' % self._db_file)
            return False

        self.reload_db_files()
        if len(self.db_files) >= 2:
            self.logger.warning(
                'Still have multiple db files after unlinking %r: %r',
                retiring_file, self.db_files)
            return False

        return True

    def get_brokers(self):
        """
        Return a list of brokers for component dbs. The list has two entries
        while the db state is sharding: the first entry is a broker for the
        retiring db with ``skip_commits`` set to ``True``; the second entry is
        a broker for the fresh db  with ``skip_commits`` set to ``False``. For
        any other db state the list has one entry.

        :return: a list of :class:`~swift.container.backend.ContainerBroker`
        """
        if len(self.db_files) > 2:
            self.logger.warning('Unexpected db files will be ignored: %s' %
                                self.db_files[:-2])
        brokers = []
        db_files = self.db_files[-2:]
        while db_files:
            db_file = db_files.pop(0)
            sub_broker = ContainerBroker(
                db_file, self.timeout, self.logger, self.account,
                self.container, self.pending_timeout, self.stale_reads_ok,
                force_db_file=True, skip_commits=bool(db_files))
            brokers.append(sub_broker)
        return brokers

    def set_sharding_sysmeta(self, key, value):
        """
        Updates the broker's metadata metadata stored under the given key
        prefixed with a sharding specific namespace.

        :param key: metadata key in the sharding metadata namespace.
        :param value: metadata value
        """
        self.update_metadata({'X-Container-Sysmeta-Shard-' + key:
                              (value, Timestamp.now().internal)})

    def get_sharding_sysmeta(self, key=None):
        """
        Returns sharding specific info from the broker's metadata.

        :param key: if given the value stored under ``key`` in the sharding
            info will be returned.
        :return: either a dict of sharding info or the value stored under
            ``key`` in that dict.
        """
        prefix = 'X-Container-Sysmeta-Shard-'
        metadata = self.metadata
        info = dict((k[len(prefix):], v[0]) for
                    k, v in metadata.items() if k.startswith(prefix))
        if key:
            return info.get(key)
        return info

    def _load_root_info(self):
        """
        Load the root container name and account for the container represented
        by this broker.

        The root container path, if set, is stored in sysmeta under the key
        ``X-Container-Sysmeta-Shard-Root``. If this sysmeta is not set then the
        container is considered to be a root container and ``_root_account``
        and ``_root_container`` are set equal to the broker ``account`` and
        ``container`` attributes respectively.

        """
        path = self.get_sharding_sysmeta('Root')
        if not path:
            # Ensure account/container get populated
            self._populate_instance_cache()
            self._root_account = self.account
            self._root_container = self.container
            return

        try:
            self._root_account, self._root_container = split_path(
                '/' + path, 2, 2)
        except ValueError:
            raise ValueError("Expected X-Container-Sysmeta-Shard-Root to be "
                             "of the form 'account/container', got %r" % path)

    @property
    def root_account(self):
        if not self._root_account:
            self._load_root_info()
        return self._root_account

    @property
    def root_container(self):
        if not self._root_container:
            self._load_root_info()
        return self._root_container

    @property
    def root_path(self):
        return '%s/%s' % (self.root_account, self.root_container)

    def is_root_container(self):
        """
        Returns True if this container is a root container, False otherwise.

        A root container is a container that is not a shard of another
        container.
        """
        self._populate_instance_cache()
        return (self.root_account == self.account and
                self.root_container == self.container)

    def _get_next_shard_range_upper(self, shard_size, last_upper=None):
        """
        Returns the name of the object that is ``shard_size`` rows beyond
        ``last_upper`` in the object table ordered by name. If ``last_upper``
        is not given then it defaults to the start of object table ordered by
        name.

        :param last_upper: the upper bound of the last found shard range.
        :return: an object name, or None if the number of rows beyond
            ``last_upper`` is less than ``shard_size``.
        """
        self._commit_puts_stale_ok()
        with self.get() as connection:
            sql = ('SELECT name FROM object WHERE %s=0 ' %
                   self._get_deleted_key(connection))
            args = []
            if last_upper:
                sql += "AND name > ? "
                args.append(str(last_upper))
            sql += "ORDER BY name LIMIT 1 OFFSET %d" % (shard_size - 1)
            row = connection.execute(sql, args).fetchone()
            return row['name'] if row else None

    def find_shard_ranges(self, shard_size, limit=-1, existing_ranges=None):
        """
        Scans the container db for shard ranges. Scanning will start at the
        upper bound of the any ``existing_ranges`` that are given, otherwise
        at ``ShardRange.MIN``. Scanning will stop when ``limit`` shard ranges
        have been found or when no more shard ranges can be found. In the
        latter case, the upper bound of the final shard range will be equal to
        the upper bound of the container namespace.

        This method does not modify the state of the db; callers are
        responsible for persisting any shard range data in the db.

        :param shard_size: the size of each shard range
        :param limit: the maximum number of shard points to be found; a
            negative value (default) implies no limit.
        :param existing_ranges: an optional list of existing ShardRanges; if
            given, this list should be sorted in order of upper bounds; the
            scan for new shard ranges will start at the upper bound of the last
            existing ShardRange.
        :return:  a tuple; the first value in the tuple is a list of
            dicts each having keys {'index', 'lower', 'upper', 'object_count'}
            in order of ascending 'upper'; the second value in the tuple is a
            boolean which is True if the last shard range has been found, False
            otherwise.
        """
        existing_ranges = existing_ranges or []
        object_count = self.get_info().get('object_count', 0)
        if shard_size >= object_count:
            # container not big enough to shard
            return [], False

        own_shard_range = self.get_own_shard_range()
        progress = 0
        progress_reliable = True
        # update initial state to account for any existing shard ranges
        if existing_ranges:
            if all([sr.state == ShardRange.FOUND
                    for sr in existing_ranges]):
                progress = sum([sr.object_count for sr in existing_ranges])
            else:
                # else: object count in existing shard ranges may have changed
                # since they were found so progress cannot be reliably
                # calculated; use default progress of zero - that's ok,
                # progress is used for optimisation not correctness
                progress_reliable = False
            last_shard_upper = existing_ranges[-1].upper
            if last_shard_upper >= own_shard_range.upper:
                # == implies all ranges were previously found
                # > implies an acceptor range has been set into which this
                # shard should cleave itself
                return [], True
        else:
            last_shard_upper = own_shard_range.lower

        found_ranges = []
        sub_broker = self.get_brokers()[0]
        index = len(existing_ranges)
        while limit is None or limit < 0 or len(found_ranges) < limit:
            if progress + shard_size >= object_count:
                # next shard point is at or beyond final object name so don't
                # bother with db query
                next_shard_upper = None
            else:
                try:
                    next_shard_upper = sub_broker._get_next_shard_range_upper(
                        shard_size, last_shard_upper)
                except (sqlite3.OperationalError, LockTimeout):
                    self.logger.exception(
                        "Problem finding shard upper in %r: " % self.db_file)
                    break

            if (next_shard_upper is None or
                    next_shard_upper > own_shard_range.upper):
                # We reached the end of the container namespace, or possibly
                # beyond if the container has misplaced objects. In either case
                # limit the final shard range to own_shard_range.upper.
                next_shard_upper = own_shard_range.upper
                if progress_reliable:
                    # object count may include misplaced objects so the final
                    # shard size may not be accurate until cleaved, but at
                    # least the sum of shard sizes will equal the unsharded
                    # object_count
                    shard_size = object_count - progress

            # NB shard ranges are created with a non-zero object count so that
            # the apparent container object count remains constant, and the
            # container is non-deletable while shards have been found but not
            # yet cleaved
            found_ranges.append(
                {'index': index,
                 'lower': str(last_shard_upper),
                 'upper': str(next_shard_upper),
                 'object_count': shard_size})

            if next_shard_upper == own_shard_range.upper:
                return found_ranges, True

            progress += shard_size
            last_shard_upper = next_shard_upper
            index += 1

        return found_ranges, False
