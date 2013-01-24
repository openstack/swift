# Copyright (c) 2010-2012 OpenStack, LLC.
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
from contextlib import contextmanager
import hashlib
import logging
import operator
import os
from uuid import uuid4
import sys
import time
import cPickle as pickle
import errno
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


class DatabaseConnectionError(sqlite3.DatabaseError):
    """More friendly error messages for DB Errors."""

    def __init__(self, path, msg, timeout=0):
        self.path = path
        self.timeout = timeout
        self.msg = msg

    def __str__(self):
        return 'DB connection error (%s, %s):\n%s' % (
            self.path, self.timeout, self.msg)


class GreenDBConnection(sqlite3.Connection):
    """SQLite DB Connection handler that plays well with eventlet."""

    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.get('timeout', BROKER_TIMEOUT)
        kwargs['timeout'] = 0
        self.db_file = args and args[0] or '-'
        sqlite3.Connection.__init__(self, *args, **kwargs)

    def _timeout(self, call):
        with LockTimeout(self.timeout, self.db_file):
            while True:
                try:
                    return call()
                except sqlite3.OperationalError, e:
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
    old = old.decode('hex')
    new = hashlib.md5(('%s-%s' % (name, timestamp)).encode('utf_8')).digest()
    response = ''.join(
        map(chr, map(operator.xor, map(ord, old), map(ord, new))))
    return response.encode('hex')


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
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA count_changes = OFF')
        conn.execute('PRAGMA temp_store = MEMORY')
        conn.execute('PRAGMA journal_mode = DELETE')
        conn.create_function('chexor', 3, chexor)
    except sqlite3.DatabaseError:
        import traceback
        raise DatabaseConnectionError(path, traceback.format_exc(),
                                      timeout=timeout)
    return conn


class DatabaseBroker(object):
    """Encapsulates working with a database."""

    def __init__(self, db_file, timeout=BROKER_TIMEOUT, logger=None,
                 account=None, container=None, pending_timeout=10,
                 stale_reads_ok=False):
        """ Encapsulates working with a database. """
        self.conn = None
        self.db_file = db_file
        self.pending_file = self.db_file + '.pending'
        self.pending_timeout = pending_timeout
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
        conn.execute('PRAGMA synchronous = OFF')
        conn.execute('PRAGMA temp_store = MEMORY')
        conn.execute('PRAGMA journal_mode = MEMORY')
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
                    raise DatabaseConnectionError(
                        self.db_file,
                        'DB created by someone else while working?')
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
        for k in self.metadata.iterkeys():
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
            raise exc_type(*exc_value.args), None, exc_traceback
        prefix_path = os.path.dirname(self.db_dir)
        partition_path = os.path.dirname(prefix_path)
        dbs_path = os.path.dirname(partition_path)
        device_path = os.path.dirname(dbs_path)
        quar_path = os.path.join(device_path, 'quarantined',
                                 self.db_type + 's',
                                 os.path.basename(self.db_dir))
        try:
            renamer(self.db_dir, quar_path)
        except OSError, e:
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
            except:
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
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
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
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
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
            except sqlite3.OperationalError, err:
                if 'no such column: metadata' not in str(err):
                    raise
                curs = conn.execute(query_part1 + "'' as metadata" +
                                    query_part2)
            curs.row_factory = dict_factory
            return curs.fetchone()

    def _commit_puts(self):
        pass    # stub to be overridden if need be

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
            except sqlite3.OperationalError, err:
                if 'no such column: metadata' not in str(err):
                    raise
                metadata = ''
        if metadata:
            metadata = json.loads(metadata)
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
        :func:reclaim
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
                md = md and json.loads(md) or {}
            except sqlite3.OperationalError, err:
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

    def reclaim(self, timestamp):
        """Removes any empty metadata values older than the timestamp"""
        if not self.metadata:
            return
        with self.get() as conn:
            if self._reclaim(conn, timestamp):
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
        except sqlite3.OperationalError, err:
            if 'no such column: metadata' not in str(err):
                raise
        return False


class ContainerBroker(DatabaseBroker):
    """Encapsulates working with a container database."""
    db_type = 'container'
    db_contains_type = 'object'

    def _initialize(self, conn, put_timestamp):
        """Creates a brand new database (tables, indices, triggers, etc.)"""
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

    def update_put_timestamp(self, timestamp):
        """
        Update the put_timestamp.  Only modifies it if it is greater than
        the current timestamp.

        :param timestamp: put timestamp
        """
        with self.get() as conn:
            conn.execute('''
                UPDATE container_stat SET put_timestamp = ?
                WHERE put_timestamp < ? ''', (timestamp, timestamp))
            conn.commit()

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

    def empty(self):
        """
        Check if the DB is empty.

        :returns: True if the database has no active objects, False otherwise
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            row = conn.execute(
                'SELECT object_count from container_stat').fetchone()
            return (row[0] == 0)

    def _commit_puts(self, item_list=None):
        """Handles commiting rows in .pending files."""
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
                            (name, timestamp, size, content_type, etag,
                                deleted) = pickle.loads(entry.decode('base64'))
                            item_list.append({'name': name,
                                              'created_at': timestamp,
                                              'size': size,
                                              'content_type': content_type,
                                              'etag': etag,
                                              'deleted': deleted})
                        except Exception:
                            self.logger.exception(
                                _('Invalid pending entry %(file)s: %(entry)s'),
                                {'file': self.pending_file, 'entry': entry})
                if item_list:
                    self.merge_items(item_list)
                try:
                    os.ftruncate(fp.fileno(), 0)
                except OSError, err:
                    if err.errno != errno.ENOENT:
                        raise

    def reclaim(self, object_timestamp, sync_timestamp):
        """
        Delete rows from the object table that are marked deleted and
        whose created_at timestamp is < object_timestamp.  Also deletes rows
        from incoming_sync and outgoing_sync where the updated_at timestamp is
        < sync_timestamp.

        In addition, this calls the DatabaseBroker's :func:_reclaim method.

        :param object_timestamp: max created_at timestamp of object rows to
                                 delete
        :param sync_timestamp: max update_at timestamp of sync rows to delete
        """
        self._commit_puts()
        with self.get() as conn:
            conn.execute("""
                    DELETE FROM object
                    WHERE deleted = 1
                    AND created_at < ?""", (object_timestamp,))
            try:
                conn.execute('''
                    DELETE FROM outgoing_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
                conn.execute('''
                    DELETE FROM incoming_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
            except sqlite3.OperationalError, err:
                # Old dbs didn't have updated_at in the _sync tables.
                if 'no such column: updated_at' not in str(err):
                    raise
            DatabaseBroker._reclaim(self, conn, object_timestamp)
            conn.commit()

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
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        if pending_size > PENDING_CAP:
            self._commit_puts([record])
        else:
            with lock_parent_directory(
                    self.pending_file, self.pending_timeout):
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
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
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

    def get_info(self, include_metadata=False):
        """
        Get global data for the container.

        :returns: dict with keys: account, container, created_at,
                  put_timestamp, delete_timestamp, object_count, bytes_used,
                  reported_put_timestamp, reported_delete_timestamp,
                  reported_object_count, reported_bytes_used, hash, id,
                  x_container_sync_point1, and x_container_sync_point2.
                  If include_metadata is set, metadata is included as a key
                  pointing to a dict of tuples of the metadata
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            data = None
            trailing1 = 'metadata'
            trailing2 = 'x_container_sync_point1, x_container_sync_point2'
            while not data:
                try:
                    data = conn.execute('''
                        SELECT account, container, created_at, put_timestamp,
                            delete_timestamp, object_count, bytes_used,
                            reported_put_timestamp, reported_delete_timestamp,
                            reported_object_count, reported_bytes_used, hash,
                            id, %s, %s
                        FROM container_stat
                    ''' % (trailing1, trailing2)).fetchone()
                except sqlite3.OperationalError, err:
                    if 'no such column: metadata' in str(err):
                        trailing1 = "'' as metadata"
                    elif 'no such column: x_container_sync_point' in str(err):
                        trailing2 = '-1 AS x_container_sync_point1, ' \
                                    '-1 AS x_container_sync_point2'
                    else:
                        raise
            data = dict(data)
            if include_metadata:
                try:
                    data['metadata'] = json.loads(data.get('metadata', ''))
                except ValueError:
                    data['metadata'] = {}
            elif 'metadata' in data:
                del data['metadata']
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
                except sqlite3.OperationalError, err:
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
        Update reported stats.

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
        (marker, end_marker, prefix, delimiter, path) = utf8encode(
            marker, end_marker, prefix, delimiter, path)
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
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
                if marker and marker >= prefix:
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
                    return [r for r in curs]
                if not delimiter:
                    return [r for r in curs if r[0].startswith(prefix)]
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


class AccountBroker(DatabaseBroker):
    """Encapsulates working with a account database."""
    db_type = 'account'
    db_contains_type = 'container'

    def _initialize(self, conn, put_timestamp):
        """
        Create a brand new database (tables, indices, triggers, etc.)

        :param conn: DB connection object
        :param put_timestamp: put timestamp
        """
        if not self.account:
            raise ValueError(
                'Attempting to create a new database with no account set')
        self.create_container_table(conn)
        self.create_account_stat_table(conn, put_timestamp)

    def create_container_table(self, conn):
        """
        Create container table which is specific to the account DB.

        :param conn: DB connection object
        """
        conn.executescript("""
            CREATE TABLE container (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                put_timestamp TEXT,
                delete_timestamp TEXT,
                object_count INTEGER,
                bytes_used INTEGER,
                deleted INTEGER DEFAULT 0
            );

            CREATE INDEX ix_container_deleted_name ON
                container (deleted, name);

            CREATE TRIGGER container_insert AFTER INSERT ON container
            BEGIN
                UPDATE account_stat
                SET container_count = container_count + (1 - new.deleted),
                    object_count = object_count + new.object_count,
                    bytes_used = bytes_used + new.bytes_used,
                    hash = chexor(hash, new.name,
                                  new.put_timestamp || '-' ||
                                    new.delete_timestamp || '-' ||
                                    new.object_count || '-' || new.bytes_used);
            END;

            CREATE TRIGGER container_update BEFORE UPDATE ON container
            BEGIN
                SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
            END;


            CREATE TRIGGER container_delete AFTER DELETE ON container
            BEGIN
                UPDATE account_stat
                SET container_count = container_count - (1 - old.deleted),
                    object_count = object_count - old.object_count,
                    bytes_used = bytes_used - old.bytes_used,
                    hash = chexor(hash, old.name,
                                  old.put_timestamp || '-' ||
                                    old.delete_timestamp || '-' ||
                                    old.object_count || '-' || old.bytes_used);
            END;
        """)

    def create_account_stat_table(self, conn, put_timestamp):
        """
        Create account_stat table which is specific to the account DB.

        :param conn: DB connection object
        :param put_timestamp: put timestamp
        """
        conn.executescript("""
            CREATE TABLE account_stat (
                account TEXT,
                created_at TEXT,
                put_timestamp TEXT DEFAULT '0',
                delete_timestamp TEXT DEFAULT '0',
                container_count INTEGER,
                object_count INTEGER DEFAULT 0,
                bytes_used INTEGER DEFAULT 0,
                hash TEXT default '00000000000000000000000000000000',
                id TEXT,
                status TEXT DEFAULT '',
                status_changed_at TEXT DEFAULT '0',
                metadata TEXT DEFAULT ''
            );

            INSERT INTO account_stat (container_count) VALUES (0);
        """)

        conn.execute('''
            UPDATE account_stat SET account = ?, created_at = ?, id = ?,
                   put_timestamp = ?
            ''', (self.account, normalize_timestamp(time.time()), str(uuid4()),
                  put_timestamp))

    def get_db_version(self, conn):
        if self._db_version == -1:
            self._db_version = 0
            for row in conn.execute('''
                    SELECT name FROM sqlite_master
                    WHERE name = 'ix_container_deleted_name' '''):
                self._db_version = 1
        return self._db_version

    def update_put_timestamp(self, timestamp):
        """
        Update the put_timestamp.  Only modifies it if it is greater than
        the current timestamp.

        :param timestamp: put timestamp
        """
        with self.get() as conn:
            conn.execute('''
                UPDATE account_stat SET put_timestamp = ?
                WHERE put_timestamp < ? ''', (timestamp, timestamp))
            conn.commit()

    def _delete_db(self, conn, timestamp, force=False):
        """
        Mark the DB as deleted.

        :param conn: DB connection object
        :param timestamp: timestamp to mark as deleted
        """
        conn.execute("""
            UPDATE account_stat
            SET delete_timestamp = ?,
                status = 'DELETED',
                status_changed_at = ?
            WHERE delete_timestamp < ? """, (timestamp, timestamp, timestamp))

    def _commit_puts(self, item_list=None):
        """Handles commiting rows in .pending files."""
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
                            (name, put_timestamp, delete_timestamp,
                             object_count, bytes_used, deleted) = \
                                pickle.loads(entry.decode('base64'))
                            item_list.append(
                                {'name': name,
                                 'put_timestamp': put_timestamp,
                                 'delete_timestamp': delete_timestamp,
                                 'object_count': object_count,
                                 'bytes_used': bytes_used,
                                 'deleted': deleted})
                        except Exception:
                            self.logger.exception(
                                _('Invalid pending entry %(file)s: %(entry)s'),
                                {'file': self.pending_file, 'entry': entry})
                if item_list:
                    self.merge_items(item_list)
                try:
                    os.ftruncate(fp.fileno(), 0)
                except OSError, err:
                    if err.errno != errno.ENOENT:
                        raise

    def empty(self):
        """
        Check if the account DB is empty.

        :returns: True if the database has no active containers.
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            row = conn.execute(
                'SELECT container_count from account_stat').fetchone()
            return (row[0] == 0)

    def reclaim(self, container_timestamp, sync_timestamp):
        """
        Delete rows from the container table that are marked deleted and
        whose created_at timestamp is < container_timestamp.  Also deletes rows
        from incoming_sync and outgoing_sync where the updated_at timestamp is
        < sync_timestamp.

        In addition, this calls the DatabaseBroker's :func:_reclaim method.

        :param container_timestamp: max created_at timestamp of container rows
                                    to delete
        :param sync_timestamp: max update_at timestamp of sync rows to delete
        """

        self._commit_puts()
        with self.get() as conn:
            conn.execute('''
                DELETE FROM container WHERE
                deleted = 1 AND delete_timestamp < ?
            ''', (container_timestamp,))
            try:
                conn.execute('''
                    DELETE FROM outgoing_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
                conn.execute('''
                    DELETE FROM incoming_sync WHERE updated_at < ?
                ''', (sync_timestamp,))
            except sqlite3.OperationalError, err:
                # Old dbs didn't have updated_at in the _sync tables.
                if 'no such column: updated_at' not in str(err):
                    raise
            DatabaseBroker._reclaim(self, conn, container_timestamp)
            conn.commit()

    def get_container_timestamp(self, container_name):
        """
        Get the put_timestamp of a container.

        :param container_name: container name

        :returns: put_timestamp of the container
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            ret = conn.execute('''
                SELECT put_timestamp FROM container
                WHERE name = ? AND deleted != 1''',
                              (container_name,)).fetchone()
            if ret:
                ret = ret[0]
            return ret

    def put_container(self, name, put_timestamp, delete_timestamp,
                      object_count, bytes_used):
        """
        Create a container with the given attributes.

        :param name: name of the container to create
        :param put_timestamp: put_timestamp of the container to create
        :param delete_timestamp: delete_timestamp of the container to create
        :param object_count: number of objects in the container
        :param bytes_used: number of bytes used by the container
        """
        if delete_timestamp > put_timestamp and \
                object_count in (None, '', 0, '0'):
            deleted = 1
        else:
            deleted = 0
        record = {'name': name, 'put_timestamp': put_timestamp,
                  'delete_timestamp': delete_timestamp,
                  'object_count': object_count,
                  'bytes_used': bytes_used,
                  'deleted': deleted}
        if self.db_file == ':memory:':
            self.merge_items([record])
            return
        if not os.path.exists(self.db_file):
            raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        pending_size = 0
        try:
            pending_size = os.path.getsize(self.pending_file)
        except OSError, err:
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
                        (name, put_timestamp, delete_timestamp, object_count,
                         bytes_used, deleted),
                        protocol=PICKLE_PROTOCOL).encode('base64'))
                    fp.flush()

    def can_delete_db(self, cutoff):
        """
        Check if the accont DB can be deleted.

        :returns: True if the account can be deleted, False otherwise
        """
        self._commit_puts()
        with self.get() as conn:
            row = conn.execute('''
                SELECT status, put_timestamp, delete_timestamp, container_count
                FROM account_stat''').fetchone()
            # The account is considered deleted if its status is marked
            # as 'DELETED" and the delete_timestamp is older than the supplied
            # cutoff date; or if the delete_timestamp value is greater than
            # the put_timestamp, and there are no containers for the account
            status_del = (row['status'] == 'DELETED')
            deltime = float(row['delete_timestamp'])
            past_cutoff = (deltime < cutoff)
            time_later = (row['delete_timestamp'] > row['put_timestamp'])
            no_containers = (row['container_count'] in (None, '', 0, '0'))
            return (
                (status_del and past_cutoff) or (time_later and no_containers))

    def is_deleted(self):
        """
        Check if the account DB is considered to be deleted.

        :returns: True if the account DB is considered to be deleted, False
                  otherwise
        """
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return True
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            row = conn.execute('''
                SELECT put_timestamp, delete_timestamp, container_count, status
                FROM account_stat''').fetchone()
            return row['status'] == 'DELETED' or (
                row['container_count'] in (None, '', 0, '0') and
                row['delete_timestamp'] > row['put_timestamp'])

    def is_status_deleted(self):
        """Only returns true if the status field is set to DELETED."""
        with self.get() as conn:
            row = conn.execute('''
                SELECT status
                FROM account_stat''').fetchone()
            return (row['status'] == "DELETED")

    def get_info(self):
        """
        Get global data for the account.

        :returns: dict with keys: account, created_at, put_timestamp,
                  delete_timestamp, container_count, object_count,
                  bytes_used, hash, id
        """
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        with self.get() as conn:
            return dict(conn.execute('''
                SELECT account, created_at,  put_timestamp, delete_timestamp,
                       container_count, object_count, bytes_used, hash, id
                FROM account_stat
            ''').fetchone())

    def list_containers_iter(self, limit, marker, end_marker, prefix,
                             delimiter):
        """
        Get a list of containerss sorted by name starting at marker onward, up
        to limit entries.  Entries will begin with the prefix and will not
        have the delimiter after the prefix.

        :param limit: maximum number of entries to get
        :param marker: marker query
        :param end_marker: end marker query
        :param prefix: prefix query
        :param delimiter: delimiter for query

        :returns: list of tuples of (name, object_count, bytes_used, 0)
        """
        (marker, end_marker, prefix, delimiter) = utf8encode(
            marker, end_marker, prefix, delimiter)
        try:
            self._commit_puts()
        except LockTimeout:
            if not self.stale_reads_ok:
                raise
        if delimiter and not prefix:
            prefix = ''
        orig_marker = marker
        with self.get() as conn:
            results = []
            while len(results) < limit:
                query = """
                    SELECT name, object_count, bytes_used, 0
                    FROM container
                    WHERE deleted = 0 AND """
                query_args = []
                if end_marker:
                    query += ' name < ? AND'
                    query_args.append(end_marker)
                if marker and marker >= prefix:
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
                    return [r for r in curs]
                if not delimiter:
                    return [r for r in curs if r[0].startswith(prefix)]
                rowcount = 0
                for row in curs:
                    rowcount += 1
                    marker = name = row[0]
                    if len(results) >= limit or not name.startswith(prefix):
                        curs.close()
                        return results
                    end = name.find(delimiter, len(prefix))
                    if end > 0:
                        marker = name[:end] + chr(ord(delimiter) + 1)
                        dir_name = name[:end + 1]
                        if dir_name != orig_marker:
                            results.append([dir_name, 0, 0, 1])
                        curs.close()
                        break
                    results.append(row)
                if not rowcount:
                    break
            return results

    def merge_items(self, item_list, source=None):
        """
        Merge items into the container table.

        :param item_list: list of dictionaries of {'name', 'put_timestamp',
                          'delete_timestamp', 'object_count', 'bytes_used',
                          'deleted'}
        :param source: if defined, update incoming_sync with the source
        """
        with self.get() as conn:
            max_rowid = -1
            for rec in item_list:
                record = [rec['name'], rec['put_timestamp'],
                          rec['delete_timestamp'], rec['object_count'],
                          rec['bytes_used'], rec['deleted']]
                query = '''
                    SELECT name, put_timestamp, delete_timestamp,
                           object_count, bytes_used, deleted
                    FROM container WHERE name = ?
                '''
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                curs = conn.execute(query, (rec['name'],))
                curs.row_factory = None
                row = curs.fetchone()
                if row:
                    row = list(row)
                    for i in xrange(5):
                        if record[i] is None and row[i] is not None:
                            record[i] = row[i]
                    if row[1] > record[1]:  # Keep newest put_timestamp
                        record[1] = row[1]
                    if row[2] > record[2]:  # Keep newest delete_timestamp
                        record[2] = row[2]
                    # If deleted, mark as such
                    if record[2] > record[1] and \
                            record[3] in (None, '', 0, '0'):
                        record[5] = 1
                    else:
                        record[5] = 0
                conn.execute('''
                    DELETE FROM container WHERE name = ? AND
                                                deleted IN (0, 1)
                ''', (record[0],))
                conn.execute('''
                    INSERT INTO container (name, put_timestamp,
                        delete_timestamp, object_count, bytes_used,
                        deleted)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', record)
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
