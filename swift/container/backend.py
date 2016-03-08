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

import os
from uuid import uuid4
import time

import six
import six.moves.cPickle as pickle
from six.moves import range
import sqlite3

from swift.common.utils import Timestamp, encode_timestamps, decode_timestamps, \
    extract_swift_bytes
from swift.common.db import DatabaseBroker, utf8encode


SQLITE_ARG_LIMIT = 999

DATADIR = 'containers'

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
    in the 'created_at' column of the the objects db table.

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


class ContainerBroker(DatabaseBroker):
    """Encapsulates working with a container database."""
    db_type = 'container'
    db_contains_type = 'object'
    db_reclaim_timestamp = 'created_at'

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
        """, (self.account, self.container, Timestamp(time.time()).internal,
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
        (name, timestamp, size, content_type, etag, deleted) = data[:6]
        if len(data) > 6:
            storage_policy_index = data[6]
        else:
            storage_policy_index = 0
        content_type_timestamp = meta_timestamp = None
        if len(data) > 7:
            content_type_timestamp = data[7]
        if len(data) > 8:
            meta_timestamp = data[8]
        item_list.append({'name': name,
                          'created_at': timestamp,
                          'size': size,
                          'content_type': content_type,
                          'etag': etag,
                          'deleted': deleted,
                          'storage_policy_index': storage_policy_index,
                          'ctype_timestamp': content_type_timestamp,
                          'meta_timestamp': meta_timestamp})

    def empty(self):
        """
        Check if container DB is empty.

        :returns: True if the database has no active objects, False otherwise
        """
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
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return {}, True
        info = self.get_info()
        return info, self._is_deleted_info(**info)

    def get_info(self):
        """
        Get global data for the container.

        :returns: dict with keys: account, container, created_at,
                  put_timestamp, delete_timestamp, status_changed_at,
                  object_count, bytes_used, reported_put_timestamp,
                  reported_delete_timestamp, reported_object_count,
                  reported_bytes_used, hash, id, x_container_sync_point1,
                  x_container_sync_point2, and storage_policy_index.
        """
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
            timestamp = Timestamp(time.time()).internal

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
                          path=None, storage_policy_index=0, reverse=False):
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

        :returns: list of tuples of (name, created_at, size, content_type,
                  etag)
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
                query = '''SELECT name, created_at, size, content_type, etag
                           FROM object WHERE'''
                query_args = []
                if end_marker and (not prefix or end_marker < end_prefix):
                    query += ' name < ? AND'
                    query_args.append(end_marker)
                elif prefix:
                    query += ' name < ? AND'
                    query_args.append(end_prefix)

                if delim_force_gte:
                    query += ' name >= ? AND'
                    query_args.append(marker)
                    # Always set back to False
                    delim_force_gte = False
                elif marker and marker >= prefix:
                    query += ' name > ? AND'
                    query_args.append(marker)
                elif prefix:
                    query += ' name >= ? AND'
                    query_args.append(prefix)
                if self.get_db_version(conn) < 1:
                    query += ' +deleted = 0'
                else:
                    query += ' deleted = 0'
                orig_tail_query = '''
                    ORDER BY name %s LIMIT ?
                ''' % ('DESC' if reverse else '')
                orig_tail_args = [limit - len(results)]
                # storage policy filter
                policy_tail_query = '''
                    AND storage_policy_index = ?
                ''' + orig_tail_query
                policy_tail_args = [storage_policy_index] + orig_tail_args
                tail_query, tail_args = \
                    policy_tail_query, policy_tail_args
                try:
                    curs = conn.execute(query + tail_query,
                                        tuple(query_args + tail_args))
                except sqlite3.OperationalError as err:
                    if 'no such column: storage_policy_index' not in str(err):
                        raise
                    tail_query, tail_args = \
                        orig_tail_query, orig_tail_args
                    curs = conn.execute(query + tail_query,
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
                    elif end > 0:
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
            if isinstance(item['name'], six.text_type):
                item['name'] = item['name'].encode('utf-8')

        def _really_merge_items(conn):
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
            to_delete = {}
            to_add = {}
            for item in item_list:
                item.setdefault('storage_policy_index', 0)  # legacy
                item_ident = (item['name'], item['storage_policy_index'])
                existing = self._record_to_dict(records.get(item_ident))
                if update_new_item_from_existing(item, existing):
                    if item_ident in records:  # exists with older timestamp
                        to_delete[item_ident] = item
                    if item_ident in to_add:  # duplicate entries in item_list
                        update_new_item_from_existing(item, to_add[item_ident])
                    to_add[item_ident] = item
            if to_delete:
                curs.executemany(
                    'DELETE FROM object WHERE ' + query_mod +
                    'name=? AND storage_policy_index=?',
                    ((rec['name'], rec['storage_policy_index'])
                     for rec in to_delete.itervalues()))
            if to_add:
                curs.executemany(
                    'INSERT INTO object (name, created_at, size, content_type,'
                    'etag, deleted, storage_policy_index)'
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    ((rec['name'], rec['created_at'], rec['size'],
                      rec['content_type'], rec['etag'], rec['deleted'],
                      rec['storage_policy_index'])
                     for rec in to_add.itervalues()))
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
