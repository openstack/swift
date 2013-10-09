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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Database code for Swift """

from __future__ import with_statement
from contextlib import contextmanager, closing
import hashlib
import logging
import os
from uuid import uuid4
import sys
import time
import errno
from swift import gettext_ as _
from tempfile import mkstemp

from eventlet import sleep, Timeout
import sqlite3

from swift.common.utils import json, normalize_timestamp, renamer, \
    mkdirs, lock_parent_directory, fallocate
from swift.common.exceptions import LockTimeout


#: Whether calls will be made to preallocate disk space for database files.
DB_PREALLOCATION = True
#: Timeout for trying to connect to a DB
BROKER_TIMEOUT = 25
#: Pickle protocol to use
PICKLE_PROTOCOL = 2
#: Max number of pending entries
PENDING_CAP = 131072


def utf8encode(*args):
    return [(s.encode('utf8') if isinstance(s, unicode) else s) for s in args]


def utf8encodekeys(metadata):
    uni_keys = [k for k in metadata if isinstance(k, unicode)]
    for k in uni_keys:
        sv = metadata[k]
        del metadata[k]
        metadata[k.encode('utf-8')] = sv


class DatabaseConnectionError(sqlite3.DatabaseError):
    """More friendly error messages for DB Errors."""

    def __init__(self, path, msg, timeout=0):
        self.path = path
        self.timeout = timeout
        self.msg = msg

    def __str__(self):
        return 'DB connection error (%s, %s):\n%s' % (
            self.path, self.timeout, self.msg)


class DatabaseAlreadyExists(sqlite3.DatabaseError):
    """More friendly error messages for DB Errors."""

    def __init__(self, path):
        self.path = path

    def __str__(self):
        return 'DB %s already exists' % self.path


class GreenDBConnection(sqlite3.Connection):
    """SQLite DB Connection handler that plays well with eventlet."""

    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.get('timeout', BROKER_TIMEOUT)
        kwargs['timeout'] = 0
        self.db_file = args[0] if args else'-'
        sqlite3.Connection.__init__(self, *args, **kwargs)

    def _timeout(self, call):
        with LockTimeout(self.timeout, self.db_file):
            while True:
                try:
                    return call()
                except sqlite3.OperationalError as e:
                    if 'locked' not in str(e):
                        raise
                sleep(0.05)

    def execute(self, *args, **kwargs):
        return self._timeout(lambda: sqlite3.Connection.execute(
            self, *args, **kwargs))

    def commit(self):
        return self._timeout(lambda: sqlite3.Connection.commit(self))


def dict_factory(crs, row):
    """
    This should only be used when you need a real dict,
    i.e. when you're going to serialize the results.
    """
    return dict(
        ((col[0], row[idx]) for idx, col in enumerate(crs.description)))


def chexor(old, name, timestamp):
    """
    Each entry in the account and container databases is XORed by the 128-bit
    hash on insert or delete.  This serves as a rolling, order-independent hash
    of the contents. (check + XOR)

    :param old: hex representation of the current DB hash
    :param name: name of the object or container being inserted
    :param timestamp: timestamp of the new record
    :returns: a hex representation of the new hash value
    """
    if name is None:
        raise Exception('name is None!')
    new = hashlib.md5(('%s-%s' % (name, timestamp)).encode('utf8')).hexdigest()
    return '%032x' % (int(old, 16) ^ int(new, 16))


def get_db_connection(path, timeout=30, okay_to_create=False):
    """
    Returns a properly configured SQLite database connection.

    :param path: path to DB
    :param timeout: timeout for connection
    :param okay_to_create: if True, create the DB if it doesn't exist
    :returns: DB connection object
    """
    try:
        connect_time = time.time()
        conn = sqlite3.connect(path, check_same_thread=False,
                               factory=GreenDBConnection, timeout=timeout)
        if path != ':memory:' and not okay_to_create:
            # attempt to detect and fail when connect creates the db file
            stat = os.stat(path)
            if stat.st_size == 0 and stat.st_ctime >= connect_time:
                os.unlink(path)
                raise DatabaseConnectionError(path,
                                              'DB file created by connect?')
        conn.row_factory = sqlite3.Row
        conn.text_factory = str
        with closing(conn.cursor()) as cur:
            cur.execute('PRAGMA synchronous = NORMAL')
            cur.execute('PRAGMA count_changes = OFF')
            cur.execute('PRAGMA temp_store = MEMORY')
            cur.execute('PRAGMA journal_mode = DELETE')
        conn.create_function('chexor', 3, chexor)
    except sqlite3.DatabaseError:
        import traceback
        raise DatabaseConnectionError(path, traceback.format_exc(),
                                      timeout=timeout)
    return conn


class DatabaseBroker(object):
    """Encapsulates working with a database."""

    def __init__(self, db_file, timeout=BROKER_TIMEOUT, logger=None,
                 account=None, container=None, pending_timeout=None,
                 stale_reads_ok=False):
        """Encapsulates working with a database."""
        self.conn = None
        self.db_file = db_file
        self.pending_file = self.db_file + '.pending'
        self.pending_timeout = pending_timeout or 10
        self.stale_reads_ok = stale_reads_ok
        self.db_dir = os.path.dirname(db_file)
        self.timeout = timeout
        self.logger = logger or logging.getLogger()
        self.account = account
        self.container = container
        self._db_version = -1

    def initialize(self, put_timestamp=None):
        """
        Create the DB

        :param put_timestamp: timestamp of initial PUT request
        """
        if self.db_file == ':memory:':
            tmp_db_file = None
            conn = get_db_connection(self.db_file, self.timeout)
        else:
            mkdirs(self.db_dir)
            fd, tmp_db_file = mkstemp(suffix='.tmp', dir=self.db_dir)
            os.close(fd)
            conn = sqlite3.connect(tmp_db_file, check_same_thread=False,
                                   factory=GreenDBConnection, timeout=0)
        # creating dbs implicitly does a lot of transactions, so we
        # pick fast, unsafe options here and do a big fsync at the end.
        with closing(conn.cursor()) as cur:
            cur.execute('PRAGMA synchronous = OFF')
            cur.execute('PRAGMA temp_store = MEMORY')
            cur.execute('PRAGMA journal_mode = MEMORY')
        conn.create_function('chexor', 3, chexor)
        conn.row_factory = sqlite3.Row
        conn.text_factory = str
        conn.executescript("""
            CREATE TABLE outgoing_sync (
                remote_id TEXT UNIQUE,
                sync_point INTEGER,
                updated_at TEXT DEFAULT 0
            );
            CREATE TABLE incoming_sync (
                remote_id TEXT UNIQUE,
                sync_point INTEGER,
                updated_at TEXT DEFAULT 0
            );
            CREATE TRIGGER outgoing_sync_insert AFTER INSERT ON outgoing_sync
            BEGIN
                UPDATE outgoing_sync
                SET updated_at = STRFTIME('%s', 'NOW')
                WHERE ROWID = new.ROWID;
            END;
            CREATE TRIGGER outgoing_sync_update AFTER UPDATE ON outgoing_sync
            BEGIN
                UPDATE outgoing_sync
                SET updated_at = STRFTIME('%s', 'NOW')
                WHERE ROWID = new.ROWID;
            END;
            CREATE TRIGGER incoming_sync_insert AFTER INSERT ON incoming_sync
            BEGIN
                UPDATE incoming_sync
                SET updated_at = STRFTIME('%s', 'NOW')
                WHERE ROWID = new.ROWID;
            END;
            CREATE TRIGGER incoming_sync_update AFTER UPDATE ON incoming_sync
            BEGIN
                UPDATE incoming_sync
                SET updated_at = STRFTIME('%s', 'NOW')
                WHERE ROWID = new.ROWID;
            END;
        """)
        if not put_timestamp:
            put_timestamp = normalize_timestamp(0)
        self._initialize(conn, put_timestamp)
        conn.commit()
        if tmp_db_file:
            conn.close()
            with open(tmp_db_file, 'r+b') as fp:
                os.fsync(fp.fileno())
            with lock_parent_directory(self.db_file, self.pending_timeout):
                if os.path.exists(self.db_file):
                    # It's as if there was a "condition" where different parts
                    # of the system were "racing" each other.
                    raise DatabaseAlreadyExists(self.db_file)
                renamer(tmp_db_file, self.db_file)
            self.conn = get_db_connection(self.db_file, self.timeout)
        else:
            self.conn = conn

    def delete_db(self, timestamp):
        """
        Mark the DB as deleted

        :param timestamp: delete timestamp
        """
        timestamp = normalize_timestamp(timestamp)
        # first, clear the metadata
        cleared_meta = {}
        for k in self.metadata:
            cleared_meta[k] = ('', timestamp)
        self.update_metadata(cleared_meta)
        # then mark the db as deleted
        with self.get() as conn:
            self._delete_db(conn, timestamp)
            conn.commit()

    def possibly_quarantine(self, exc_type, exc_value, exc_traceback):
        """
        Checks the exception info to see if it indicates a quarantine situation
        (malformed or corrupted database). If not, the original exception will
        be reraised. If so, the database will be quarantined and a new
        sqlite3.DatabaseError will be raised indicating the action taken.
        """
        if 'database disk image is malformed' in str(exc_value):
            exc_hint = 'malformed'
        elif 'file is encrypted or is not a database' in str(exc_value):
            exc_hint = 'corrupted'
        else:
            raise exc_type, exc_value, exc_traceback
        prefix_path = os.path.dirname(self.db_dir)
        partition_path = os.path.dirname(prefix_path)
        dbs_path = os.path.dirname(partition_path)
        device_path = os.path.dirname(dbs_path)
        quar_path = os.path.join(device_path, 'quarantined',
                                 self.db_type + 's',
                                 os.path.basename(self.db_dir))
        try:
            renamer(self.db_dir, quar_path)
        except OSError as e:
            if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
            quar_path = "%s-%s" % (quar_path, uuid4().hex)
            renamer(self.db_dir, quar_path)
        detail = _('Quarantined %s to %s due to %s database') % \
                  (self.db_dir, quar_path, exc_hint)
        self.logger.error(detail)
        raise sqlite3.DatabaseError(detail)

    @contextmanager
    def get(self):
        """Use with the "with" statement; returns a database connection."""
        if not self.conn:
            if self.db_file != ':memory:' and os.path.exists(self.db_file):
                try:
                    self.conn = get_db_connection(self.db_file, self.timeout)
                except (sqlite3.DatabaseError, DatabaseConnectionError):
                    self.possibly_quarantine(*sys.exc_info())
            else:
                raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        conn = self.conn
        self.conn = None
        try:
            yield conn
            conn.rollback()
            self.conn = conn
        except sqlite3.DatabaseError:
            try:
                conn.close()
            except Exception:
                pass
            self.possibly_quarantine(*sys.exc_info())
        except (Exception, Timeout):
            conn.close()
            raise

    @contextmanager
    def lock(self):
        """Use with the "with" statement; locks a database."""
        if not self.conn:
            if self.db_file != ':memory:' and os.path.exists(self.db_file):
                self.conn = get_db_connection(self.db_file, self.timeout)
            else:
                raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        conn = self.conn
        self.conn = None
        orig_isolation_level = conn.isolation_level
        conn.isolation_level = None
        conn.execute('BEGIN IMMEDIATE')
        try:
            yield True
        except (Exception, Timeout):
            pass
        try:
            conn.execute('ROLLBACK')
            conn.isolation_level = orig_isolation_level
            self.conn = conn
        except (Exception, Timeout):
            logging.exception(
                _('Broker error trying to rollback locked connection'))
            conn.close()

    def newid(self, remote_id):
        """
        Re-id the database.  This should be called after an rsync.

        :param remote_id: the ID of the remote database being rsynced in
        """
        with self.get() as conn:
            row = conn.execute('''
                UPDATE %s_stat SET id=?
            ''' % self.db_type, (str(uuid4()),))
            row = conn.execute('''
                SELECT ROWID FROM %s ORDER BY ROWID DESC LIMIT 1
            ''' % self.db_contains_type).fetchone()
            sync_point = row['ROWID'] if row else -1
            conn.execute('''
                INSERT OR REPLACE INTO incoming_sync (sync_point, remote_id)
                VALUES (?, ?)
            ''', (sync_point, remote_id))
            self._newid(conn)
            conn.commit()

    def _newid(self, conn):
        # Override for additional work when receiving an rsynced db.
        pass

    def merge_timestamps(self, created_at, put_timestamp, delete_timestamp):
        """
        Used in replication to handle updating timestamps.

        :param created_at: create timestamp
        :param put_timestamp: put timestamp
        :param delete_timestamp: delete timestamp
        """
        with self.get() as conn:
            conn.execute('''
                UPDATE %s_stat SET created_at=MIN(?, created_at),
                                   put_timestamp=MAX(?, put_timestamp),
                                   delete_timestamp=MAX(?, delete_timestamp)
            ''' % self.db_type, (created_at, put_timestamp, delete_timestamp))
            conn.commit()

    def get_items_since(self, start, count):
        """
        Get a list of objects in the database between start and end.

        :param start: start ROWID
        :param count: number to get
        :returns: list of objects between start and end
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            curs = conn.execute('''
                SELECT * FROM %s WHERE ROWID > ? ORDER BY ROWID ASC LIMIT ?
            ''' % self.db_contains_type, (start, count))
            curs.row_factory = dict_factory
            return [r for r in curs]

    def get_sync(self, id, incoming=True):
        """
        Gets the most recent sync point for a server from the sync table.

        :param id: remote ID to get the sync_point for
        :param incoming: if True, get the last incoming sync, otherwise get
                         the last outgoing sync
        :returns: the sync point, or -1 if the id doesn't exist.
        """
        with self.get() as conn:
            row = conn.execute(
                "SELECT sync_point FROM %s_sync WHERE remote_id=?"
                % ('incoming' if incoming else 'outgoing'), (id,)).fetchone()
            if not row:
                return -1
            return row['sync_point']

    def get_syncs(self, incoming=True):
        """
        Get a serialized copy of the sync table.

        :param incoming: if True, get the last incoming sync, otherwise get
                         the last outgoing sync
        :returns: list of {'remote_id', 'sync_point'}
        """
        with self.get() as conn:
            curs = conn.execute('''
                SELECT remote_id, sync_point FROM %s_sync
            ''' % 'incoming' if incoming else 'outgoing')
            result = []
            for row in curs:
                result.append({'remote_id': row[0], 'sync_point': row[1]})
            return result

    def get_replication_info(self):
        """
        Get information about the DB required for replication.

        :returns: dict containing keys: hash, id, created_at, put_timestamp,
            delete_timestamp, count, max_row, and metadata
        """
        self._commit_puts_stale_ok()
        query_part1 = '''
            SELECT hash, id, created_at, put_timestamp, delete_timestamp,
                %s_count AS count,
                CASE WHEN SQLITE_SEQUENCE.seq IS NOT NULL
                    THEN SQLITE_SEQUENCE.seq ELSE -1 END AS max_row, ''' % \
            self.db_contains_type
        query_part2 = '''
            FROM (%s_stat LEFT JOIN SQLITE_SEQUENCE
                  ON SQLITE_SEQUENCE.name == '%s') LIMIT 1
        ''' % (self.db_type, self.db_contains_type)
        with self.get() as conn:
            try:
                curs = conn.execute(query_part1 + 'metadata' + query_part2)
            except sqlite3.OperationalError as err:
                if 'no such column: metadata' not in str(err):
                    raise
                curs = conn.execute(query_part1 + "'' as metadata" +
                                    query_part2)
            curs.row_factory = dict_factory
            return curs.fetchone()

    def _commit_puts(self, item_list=None):
        """
        Scan for .pending files and commit the found records by feeding them
        to merge_items().

        :param item_list: A list of items to commit in addition to .pending
        """
        if self.db_file == ':memory:' or not os.path.exists(self.pending_file):
            return
        if item_list is None:
            item_list = []
        with lock_parent_directory(self.pending_file, self.pending_timeout):
            self._preallocate()
            if not os.path.getsize(self.pending_file):
                if item_list:
                    self.merge_items(item_list)
                return
            with open(self.pending_file, 'r+b') as fp:
                for entry in fp.read().split(':'):
                    if entry:
                        try:
                            self._commit_puts_load(item_list, entry)
                        except Exception:
                            self.logger.exception(
                                _('Invalid pending entry %(file)s: %(entry)s'),
                                {'file': self.pending_file, 'entry': entry})
                if item_list:
                    self.merge_items(item_list)
                try:
                    os.ftruncate(fp.fileno(), 0)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise

    def _commit_puts_stale_ok(self):
        """
        Catch failures of _commit_puts() if broker is intended for
        reading of stats, and thus does not care for pending updates.
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise

    def _commit_puts_load(self, item_list, entry):
        """
        Unmarshall the :param:entry and append it to :param:item_list.
        This is implemented by a particular broker to be compatible
        with its :func:`merge_items`.
        """
        raise NotImplementedError

    def merge_syncs(self, sync_points, incoming=True):
        """
        Merge a list of sync points with the incoming sync table.

        :param sync_points: list of sync points where a sync point is a dict of
                            {'sync_point', 'remote_id'}
        :param incoming: if True, get the last incoming sync, otherwise get
                         the last outgoing sync
        """
        with self.get() as conn:
            for rec in sync_points:
                try:
                    conn.execute('''
                        INSERT INTO %s_sync (sync_point, remote_id)
                        VALUES (?, ?)
                    ''' % ('incoming' if incoming else 'outgoing'),
                        (rec['sync_point'], rec['remote_id']))
                except sqlite3.IntegrityError:
                    conn.execute('''
                        UPDATE %s_sync SET sync_point=max(?, sync_point)
                        WHERE remote_id=?
                    ''' % ('incoming' if incoming else 'outgoing'),
                        (rec['sync_point'], rec['remote_id']))
            conn.commit()

    def _preallocate(self):
        """
        The idea is to allocate space in front of an expanding db.  If it gets
        within 512k of a boundary, it allocates to the next boundary.
        Boundaries are 2m, 5m, 10m, 25m, 50m, then every 50m after.
        """
        if not DB_PREALLOCATION or self.db_file == ':memory:':
            return
        MB = (1024 * 1024)

        def prealloc_points():
            for pm in (1, 2, 5, 10, 25, 50):
                yield pm * MB
            while True:
                pm += 50
                yield pm * MB

        stat = os.stat(self.db_file)
        file_size = stat.st_size
        allocated_size = stat.st_blocks * 512
        for point in prealloc_points():
            if file_size <= point - MB / 2:
                prealloc_size = point
                break
        if allocated_size < prealloc_size:
            with open(self.db_file, 'rb+') as fp:
                fallocate(fp.fileno(), int(prealloc_size))

    @property
    def metadata(self):
        """
        Returns the metadata dict for the database. The metadata dict values
        are tuples of (value, timestamp) where the timestamp indicates when
        that key was set to that value.
        """
        with self.get() as conn:
            try:
                metadata = conn.execute('SELECT metadata FROM %s_stat' %
                                        self.db_type).fetchone()[0]
            except sqlite3.OperationalError as err:
                if 'no such column: metadata' not in str(err):
                    raise
                metadata = ''
        if metadata:
            metadata = json.loads(metadata)
            utf8encodekeys(metadata)
        else:
            metadata = {}
        return metadata

    def update_metadata(self, metadata_updates):
        """
        Updates the metadata dict for the database. The metadata dict values
        are tuples of (value, timestamp) where the timestamp indicates when
        that key was set to that value. Key/values will only be overwritten if
        the timestamp is newer. To delete a key, set its value to ('',
        timestamp). These empty keys will eventually be removed by
        :func:`reclaim`
        """
        old_metadata = self.metadata
        if set(metadata_updates).issubset(set(old_metadata)):
            for key, (value, timestamp) in metadata_updates.iteritems():
                if timestamp > old_metadata[key][1]:
                    break
            else:
                return
        with self.get() as conn:
            try:
                md = conn.execute('SELECT metadata FROM %s_stat' %
                                  self.db_type).fetchone()[0]
                md = json.loads(md) if md else {}
                utf8encodekeys(md)
            except sqlite3.OperationalError as err:
                if 'no such column: metadata' not in str(err):
                    raise
                conn.execute("""
                    ALTER TABLE %s_stat
                    ADD COLUMN metadata TEXT DEFAULT '' """ % self.db_type)
                md = {}
            for key, value_timestamp in metadata_updates.iteritems():
                value, timestamp = value_timestamp
                if key not in md or timestamp > md[key][1]:
                    md[key] = value_timestamp
            conn.execute('UPDATE %s_stat SET metadata = ?' % self.db_type,
                         (json.dumps(md),))
            conn.commit()

    def reclaim(self, age_timestamp, sync_timestamp):
        """
        Delete rows from the db_contains_type table that are marked deleted
        and whose created_at timestamp is < age_timestamp.  Also deletes rows
        from incoming_sync and outgoing_sync where the updated_at timestamp is
        < sync_timestamp.

        In addition, this calls the DatabaseBroker's :func:`_reclaim` method.

        :param age_timestamp: max created_at timestamp of object rows to delete
        :param sync_timestamp: max update_at timestamp of sync rows to delete
        """
        self._commit_puts()
        with self.get() as conn:
            conn.execute('''
                DELETE FROM %s WHERE deleted = 1 AND %s < ?
            ''' % (self.db_contains_type, self.db_reclaim_timestamp),
                (age_timestamp,))
            try:
                conn.execute('''
                    DELETE FROM outgoing_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
                conn.execute('''
                    DELETE FROM incoming_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
            except sqlite3.OperationalError as err:
                # Old dbs didn't have updated_at in the _sync tables.
                if 'no such column: updated_at' not in str(err):
                    raise
            DatabaseBroker._reclaim(self, conn, age_timestamp)
            conn.commit()

    def _reclaim(self, conn, timestamp):
        """
        Removes any empty metadata values older than the timestamp using the
        given database connection. This function will not call commit on the
        conn, but will instead return True if the database needs committing.
        This function was created as a worker to limit transactions and commits
        from other related functions.

        :param conn: Database connection to reclaim metadata within.
        :param timestamp: Empty metadata items last updated before this
                          timestamp will be removed.
        :returns: True if conn.commit() should be called
        """
        try:
            md = conn.execute('SELECT metadata FROM %s_stat' %
                              self.db_type).fetchone()[0]
            if md:
                md = json.loads(md)
                keys_to_delete = []
                for key, (value, value_timestamp) in md.iteritems():
                    if value == '' and value_timestamp < timestamp:
                        keys_to_delete.append(key)
                if keys_to_delete:
                    for key in keys_to_delete:
                        del md[key]
                    conn.execute('UPDATE %s_stat SET metadata = ?' %
                                 self.db_type, (json.dumps(md),))
                    return True
        except sqlite3.OperationalError as err:
            if 'no such column: metadata' not in str(err):
                raise
        return False

    def update_put_timestamp(self, timestamp):
        """
        Update the put_timestamp.  Only modifies it if it is greater than
        the current timestamp.

        :param timestamp: put timestamp
        """
        with self.get() as conn:
            conn.execute(
                'UPDATE %s_stat SET put_timestamp = ?'
                ' WHERE put_timestamp < ?' % self.db_type,
                (timestamp, timestamp))
            conn.commit()
