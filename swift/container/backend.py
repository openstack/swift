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

from __future__ import with_statement
import os
from uuid import uuid4
import time
import cPickle as pickle
import errno

import sqlite3

from swift.common.utils import normalize_timestamp, lock_parent_directory
from swift.common.db import DatabaseBroker, DatabaseConnectionError, \
    PENDING_CAP, PICKLE_PROTOCOL, utf8encode


class ContainerBroker(DatabaseBroker):
    """Encapsulates working with a container database."""
    db_type = 'container'
    db_contains_type = 'object'
    db_reclaim_timestamp = 'created_at'

    def _initialize(self, conn, put_timestamp):
        """
        Create a brand new container database (tables, indices, triggers, etc.)
        """
        if not self.account:
            raise ValueError(
                'Attempting to create a new database with no account set')
        if not self.container:
            raise ValueError(
                'Attempting to create a new database with no container set')
        self.create_object_table(conn)
        self.create_container_stat_table(conn, put_timestamp)

    def create_object_table(self, conn):
        """
        Create the object table which is specifc to the container DB.
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
                deleted INTEGER DEFAULT 0
            );

            CREATE INDEX ix_object_deleted_name ON object (deleted, name);

            CREATE TRIGGER object_insert AFTER INSERT ON object
            BEGIN
                UPDATE container_stat
                SET object_count = object_count + (1 - new.deleted),
                    bytes_used = bytes_used + new.size,
                    hash = chexor(hash, new.name, new.created_at);
            END;

            CREATE TRIGGER object_update BEFORE UPDATE ON object
            BEGIN
                SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
            END;

            CREATE TRIGGER object_delete AFTER DELETE ON object
            BEGIN
                UPDATE container_stat
                SET object_count = object_count - (1 - old.deleted),
                    bytes_used = bytes_used - old.size,
                    hash = chexor(hash, old.name, old.created_at);
            END;
        """)

    def create_container_stat_table(self, conn, put_timestamp=None):
        """
        Create the container_stat table which is specific to the container DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

        :param conn: DB connection object
        :param put_timestamp: put timestamp
        """
        if put_timestamp is None:
            put_timestamp = normalize_timestamp(0)
        conn.executescript("""
            CREATE TABLE container_stat (
                account TEXT,
                container TEXT,
                created_at TEXT,
                put_timestamp TEXT DEFAULT '0',
                delete_timestamp TEXT DEFAULT '0',
                object_count INTEGER,
                bytes_used INTEGER,
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
                x_container_sync_point2 INTEGER DEFAULT -1
            );

            INSERT INTO container_stat (object_count, bytes_used)
                VALUES (0, 0);
        """)
        conn.execute('''
            UPDATE container_stat
            SET account = ?, container = ?, created_at = ?, id = ?,
                put_timestamp = ?
        ''', (self.account, self.container, normalize_timestamp(time.time()),
              str(uuid4()), put_timestamp))

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
        (name, timestamp, size, content_type, etag, deleted) = \
            pickle.loads(entry.decode('base64'))
        item_list.append({'name': name,
                          'created_at': timestamp,
                          'size': size,
                          'content_type': content_type,
                          'etag': etag,
                          'deleted': deleted})

    def empty(self):
        """
        Check if container DB is empty.

        :returns: True if the database has no active objects, False otherwise
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            row = conn.execute(
                'SELECT object_count from container_stat').fetchone()
            return (row[0] == 0)

    def delete_object(self, name, timestamp):
        """
        Mark an object deleted.

        :param name: object name to be deleted
        :param timestamp: timestamp when the object was marked as deleted
        """
        self.put_object(name, timestamp, 0, 'application/deleted', 'noetag', 1)

    def put_object(self, name, timestamp, size, content_type, etag, deleted=0):
        """
        Creates an object in the DB with its metadata.

        :param name: object name to be created
        :param timestamp: timestamp of when the object was created
        :param size: object size
        :param content_type: object content-type
        :param etag: object etag
        :param deleted: if True, marks the object as deleted and sets the
                        deteleted_at timestamp to timestamp
        """
        record = {'name': name, 'created_at': timestamp, 'size': size,
                  'content_type': content_type, 'etag': etag,
                  'deleted': deleted}
        if self.db_file == ':memory:':
            self.merge_items([record])
            return
        if not os.path.exists(self.db_file):
            raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        pending_size = 0
        try:
            pending_size = os.path.getsize(self.pending_file)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
        if pending_size > PENDING_CAP:
            self._commit_puts([record])
        else:
            with lock_parent_directory(self.pending_file,
                                       self.pending_timeout):
                with open(self.pending_file, 'a+b') as fp:
                    # Colons aren't used in base64 encoding; so they are our
                    # delimiter
                    fp.write(':')
                    fp.write(pickle.dumps(
                        (name, timestamp, size, content_type, etag, deleted),
                        protocol=PICKLE_PROTOCOL).encode('base64'))
                    fp.flush()

    def is_deleted(self, timestamp=None):
        """
        Check if the DB is considered to be deleted.

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return True
        self._commit_puts_stale_ok()
        with self.get() as conn:
            row = conn.execute('''
                SELECT put_timestamp, delete_timestamp, object_count
                FROM container_stat''').fetchone()
            # leave this db as a tombstone for a consistency window
            if timestamp and row['delete_timestamp'] > timestamp:
                return False
            # The container is considered deleted if the delete_timestamp
            # value is greater than the put_timestamp, and there are no
            # objects in the container.
            return (row['object_count'] in (None, '', 0, '0')) and \
                (float(row['delete_timestamp']) > float(row['put_timestamp']))

    def get_info(self):
        """
        Get global data for the container.

        :returns: dict with keys: account, container, created_at,
                  put_timestamp, delete_timestamp, object_count, bytes_used,
                  reported_put_timestamp, reported_delete_timestamp,
                  reported_object_count, reported_bytes_used, hash, id,
                  x_container_sync_point1, and x_container_sync_point2.
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            data = None
            trailing = 'x_container_sync_point1, x_container_sync_point2'
            while not data:
                try:
                    data = conn.execute('''
                        SELECT account, container, created_at, put_timestamp,
                            delete_timestamp, object_count, bytes_used,
                            reported_put_timestamp, reported_delete_timestamp,
                            reported_object_count, reported_bytes_used, hash,
                            id, %s
                        FROM container_stat
                    ''' % (trailing,)).fetchone()
                except sqlite3.OperationalError as err:
                    if 'no such column: x_container_sync_point' in str(err):
                        trailing = '-1 AS x_container_sync_point1, ' \
                                   '-1 AS x_container_sync_point2'
                    else:
                        raise
            data = dict(data)
            return data

    def set_x_container_sync_points(self, sync_point1, sync_point2):
        with self.get() as conn:
            orig_isolation_level = conn.isolation_level
            try:
                # We turn off auto-transactions to ensure the alter table
                # commands are part of the transaction.
                conn.isolation_level = None
                conn.execute('BEGIN')
                try:
                    self._set_x_container_sync_points(conn, sync_point1,
                                                      sync_point2)
                except sqlite3.OperationalError as err:
                    if 'no such column: x_container_sync_point' not in \
                            str(err):
                        raise
                    conn.execute('''
                        ALTER TABLE container_stat
                        ADD COLUMN x_container_sync_point1 INTEGER DEFAULT -1
                    ''')
                    conn.execute('''
                        ALTER TABLE container_stat
                        ADD COLUMN x_container_sync_point2 INTEGER DEFAULT -1
                    ''')
                    self._set_x_container_sync_points(conn, sync_point1,
                                                      sync_point2)
                conn.execute('COMMIT')
            finally:
                conn.isolation_level = orig_isolation_level

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
                          path=None):
        """
        Get a list of objects sorted by name starting at marker onward, up
        to limit entries.  Entries will begin with the prefix and will not
        have the delimiter after the prefix.

        :param limit: maximum number of entries to get
        :param marker: marker query
        :param end_marker: end marker query
        :param prefix: prefix query
        :param delimiter: delimiter for query
        :param path: if defined, will set the prefix and delimter based on
                     the path

        :returns: list of tuples of (name, created_at, size, content_type,
                  etag)
        """
        delim_force_gte = False
        (marker, end_marker, prefix, delimiter, path) = utf8encode(
            marker, end_marker, prefix, delimiter, path)
        self._commit_puts_stale_ok()
        if path is not None:
            prefix = path
            if path:
                prefix = path = path.rstrip('/') + '/'
            delimiter = '/'
        elif delimiter and not prefix:
            prefix = ''
        orig_marker = marker
        with self.get() as conn:
            results = []
            while len(results) < limit:
                query = '''SELECT name, created_at, size, content_type, etag
                           FROM object WHERE'''
                query_args = []
                if end_marker:
                    query += ' name < ? AND'
                    query_args.append(end_marker)
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
                query += ' ORDER BY name LIMIT ?'
                query_args.append(limit - len(results))
                curs = conn.execute(query, query_args)
                curs.row_factory = None

                if prefix is None:
                    # A delimiter without a specified prefix is ignored
                    return [r for r in curs]
                if not delimiter:
                    if not prefix:
                        # It is possible to have a delimiter but no prefix
                        # specified. As above, the prefix will be set to the
                        # empty string, so avoid performing the extra work to
                        # check against an empty prefix.
                        return [r for r in curs]
                    else:
                        return [r for r in curs if r[0].startswith(prefix)]

                # We have a delimiter and a prefix (possibly empty string) to
                # handle
                rowcount = 0
                for row in curs:
                    rowcount += 1
                    marker = name = row[0]
                    if len(results) >= limit or not name.startswith(prefix):
                        curs.close()
                        return results
                    end = name.find(delimiter, len(prefix))
                    if path is not None:
                        if name == path:
                            continue
                        if end >= 0 and len(name) > end + len(delimiter):
                            marker = name[:end] + chr(ord(delimiter) + 1)
                            curs.close()
                            break
                    elif end > 0:
                        marker = name[:end] + chr(ord(delimiter) + 1)
                        # we want result to be inclusinve of delim+1
                        delim_force_gte = True
                        dir_name = name[:end + 1]
                        if dir_name != orig_marker:
                            results.append([dir_name, '0', 0, None, ''])
                        curs.close()
                        break
                    results.append(row)
                if not rowcount:
                    break
            return results

    def merge_items(self, item_list, source=None):
        """
        Merge items into the object table.

        :param item_list: list of dictionaries of {'name', 'created_at',
                          'size', 'content_type', 'etag', 'deleted'}
        :param source: if defined, update incoming_sync with the source
        """
        with self.get() as conn:
            max_rowid = -1
            for rec in item_list:
                query = '''
                    DELETE FROM object
                    WHERE name = ? AND (created_at < ?)
                '''
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                conn.execute(query, (rec['name'], rec['created_at']))
                query = 'SELECT 1 FROM object WHERE name = ?'
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                if not conn.execute(query, (rec['name'],)).fetchall():
                    conn.execute('''
                        INSERT INTO object (name, created_at, size,
                            content_type, etag, deleted)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', ([rec['name'], rec['created_at'], rec['size'],
                          rec['content_type'], rec['etag'], rec['deleted']]))
                if source:
                    max_rowid = max(max_rowid, rec['ROWID'])
            if source:
                try:
                    conn.execute('''
                        INSERT INTO incoming_sync (sync_point, remote_id)
                        VALUES (?, ?)
                    ''', (max_rowid, source))
                except sqlite3.IntegrityError:
                    conn.execute('''
                        UPDATE incoming_sync SET sync_point=max(?, sync_point)
                        WHERE remote_id=?
                    ''', (max_rowid, source))
            conn.commit()
