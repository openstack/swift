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

from contextlib import contextmanager, closing
import base64
import hashlib
import json
import logging
import os
from uuid import uuid4
import sys
import time
import errno
import six
import six.moves.cPickle as pickle
from swift import gettext_ as _
from tempfile import mkstemp

from eventlet import sleep, Timeout
import sqlite3

from swift.common.constraints import MAX_META_COUNT, MAX_META_OVERALL_SIZE, \
    check_utf8
from swift.common.utils import Timestamp, renamer, \
    mkdirs, lock_parent_directory, fallocate
from swift.common.exceptions import LockTimeout
from swift.common.swob import HTTPBadRequest


#: Whether calls will be made to preallocate disk space for database files.
DB_PREALLOCATION = False
#: Timeout for trying to connect to a DB
BROKER_TIMEOUT = 25
#: Pickle protocol to use
PICKLE_PROTOCOL = 2
#: Max size of .pending file in bytes. When this is exceeded, the pending
# records will be merged.
PENDING_CAP = 131072


def utf8encode(*args):
    return [(s.encode('utf8') if isinstance(s, six.text_type) else s)
            for s in args]


def native_str_keys(metadata):
    if six.PY2:
        uni_keys = [k for k in metadata if isinstance(k, six.text_type)]
        for k in uni_keys:
            sv = metadata[k]
            del metadata[k]
            metadata[k.encode('utf-8')] = sv
    else:
        bin_keys = [k for k in metadata if isinstance(k, six.binary_type)]
        for k in bin_keys:
            sv = metadata[k]
            del metadata[k]
            metadata[k.decode('utf-8')] = sv


ZERO_LIKE_VALUES = {None, '', 0, '0'}


def zero_like(count):
    """
    We've cargo culted our consumers to be tolerant of various expressions of
    zero in our databases for backwards compatibility with less disciplined
    producers.
    """
    return count in ZERO_LIKE_VALUES


def _db_timeout(timeout, db_file, call):
    with LockTimeout(timeout, db_file):
        retry_wait = 0.001
        while True:
            try:
                return call()
            except sqlite3.OperationalError as e:
                if 'locked' not in str(e):
                    raise
            sleep(retry_wait)
            retry_wait = min(retry_wait * 2, 0.05)


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

    def __init__(self, database, timeout=None, *args, **kwargs):
        if timeout is None:
            timeout = BROKER_TIMEOUT
        self.timeout = timeout
        self.db_file = database
        super(GreenDBConnection, self).__init__(database, 0, *args, **kwargs)

    def cursor(self, cls=None):
        if cls is None:
            cls = GreenDBCursor
        return sqlite3.Connection.cursor(self, cls)

    def commit(self):
        return _db_timeout(
            self.timeout, self.db_file,
            lambda: sqlite3.Connection.commit(self))


class GreenDBCursor(sqlite3.Cursor):
    """SQLite Cursor handler that plays well with eventlet."""

    def __init__(self, *args, **kwargs):
        self.timeout = args[0].timeout
        self.db_file = args[0].db_file
        super(GreenDBCursor, self).__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return _db_timeout(
            self.timeout, self.db_file, lambda: sqlite3.Cursor.execute(
                self, *args, **kwargs))


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
    :param timestamp: internalized timestamp of the new record
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
                 stale_reads_ok=False, skip_commits=False):
        """Encapsulates working with a database.

        :param db_file: path to a database file.
        :param timeout: timeout used for database operations.
        :param logger: a logger instance.
        :param account: name of account.
        :param container: name of container.
        :param pending_timeout: timeout used when attempting to take a lock to
            write to pending file.
        :param stale_reads_ok: if True then no error is raised if pending
            commits cannot be committed before the database is read, otherwise
            an error is raised.
        :param skip_commits: if True then this broker instance will never
            commit records from the pending file to the database;
            :meth:`~swift.common.db.DatabaseBroker.put_record` should not
            called on brokers with skip_commits True.
        """
        self.conn = None
        self._db_file = db_file
        self.pending_file = self._db_file + '.pending'
        self.pending_timeout = pending_timeout or 10
        self.stale_reads_ok = stale_reads_ok
        self.db_dir = os.path.dirname(db_file)
        self.timeout = timeout
        self.logger = logger or logging.getLogger()
        self.account = account
        self.container = container
        self._db_version = -1
        self.skip_commits = skip_commits

    def __str__(self):
        """
        Returns a string identifying the entity under broker to a human.
        The baseline implementation returns a full pathname to a database.
        This is vital for useful diagnostics.
        """
        return self.db_file

    def initialize(self, put_timestamp=None, storage_policy_index=None):
        """
        Create the DB

        The storage_policy_index is passed through to the subclass's
        ``_initialize`` method.  It is ignored by ``AccountBroker``.

        :param put_timestamp: internalized timestamp of initial PUT request
        :param storage_policy_index: only required for containers
        """
        if self._db_file == ':memory:':
            tmp_db_file = None
            conn = get_db_connection(self._db_file, self.timeout)
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
            put_timestamp = Timestamp(0).internal
        self._initialize(conn, put_timestamp,
                         storage_policy_index=storage_policy_index)
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

        :param timestamp: internalized delete timestamp
        """
        # first, clear the metadata
        cleared_meta = {}
        for k in self.metadata:
            cleared_meta[k] = ('', timestamp)
        self.update_metadata(cleared_meta)
        # then mark the db as deleted
        with self.get() as conn:
            self._delete_db(conn, timestamp)
            conn.commit()

    @property
    def db_file(self):
        return self._db_file

    def get_device_path(self):
        suffix_path = os.path.dirname(self.db_dir)
        partition_path = os.path.dirname(suffix_path)
        dbs_path = os.path.dirname(partition_path)
        return os.path.dirname(dbs_path)

    def quarantine(self, reason):
        """
        The database will be quarantined and a
        sqlite3.DatabaseError will be raised indicating the action taken.
        """
        device_path = self.get_device_path()
        quar_path = os.path.join(device_path, 'quarantined',
                                 self.db_type + 's',
                                 os.path.basename(self.db_dir))
        try:
            renamer(self.db_dir, quar_path, fsync=False)
        except OSError as e:
            if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
            quar_path = "%s-%s" % (quar_path, uuid4().hex)
            renamer(self.db_dir, quar_path, fsync=False)
        detail = _('Quarantined %(db_dir)s to %(quar_path)s due to '
                   '%(reason)s') % {'db_dir': self.db_dir,
                                    'quar_path': quar_path,
                                    'reason': reason}
        self.logger.error(detail)
        raise sqlite3.DatabaseError(detail)

    def possibly_quarantine(self, exc_type, exc_value, exc_traceback):
        """
        Checks the exception info to see if it indicates a quarantine situation
        (malformed or corrupted database). If not, the original exception will
        be reraised. If so, the database will be quarantined and a new
        sqlite3.DatabaseError will be raised indicating the action taken.
        """
        if 'database disk image is malformed' in str(exc_value):
            exc_hint = 'malformed database'
        elif 'malformed database schema' in str(exc_value):
            exc_hint = 'malformed database'
        elif ' is not a database' in str(exc_value):
            # older versions said 'file is not a database'
            # now 'file is encrypted or is not a database'
            exc_hint = 'corrupted database'
        elif 'disk I/O error' in str(exc_value):
            exc_hint = 'disk error while accessing database'
        else:
            six.reraise(exc_type, exc_value, exc_traceback)

        self.quarantine(exc_hint)

    @contextmanager
    def updated_timeout(self, new_timeout):
        """Use with "with" statement; updates ``timeout`` within the block."""
        old_timeout = self.timeout
        try:
            self.timeout = new_timeout
            if self.conn:
                self.conn.timeout = new_timeout
            yield old_timeout
        finally:
            self.timeout = old_timeout
            if self.conn:
                self.conn.timeout = old_timeout

    @contextmanager
    def maybe_get(self, conn):
        if conn:
            yield conn
        else:
            with self.get() as conn:
                yield conn

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

    def _is_deleted(self, conn):
        """
        Check if the database is considered deleted

        :param conn: database conn

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        raise NotImplementedError()

    def is_deleted(self):
        """
        Check if the DB is considered to be deleted.

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        if self.db_file != ':memory:' and not os.path.exists(self.db_file):
            return True
        self._commit_puts_stale_ok()
        with self.get() as conn:
            return self._is_deleted(conn)

    def empty(self):
        """
        Check if the broker abstraction contains any undeleted records.
        """
        raise NotImplementedError()

    def is_reclaimable(self, now, reclaim_age):
        """
        Check if the broker abstraction is empty, and has been marked deleted
        for at least a reclaim age.
        """
        info = self.get_replication_info()
        return (zero_like(info['count']) and
                (Timestamp(now - reclaim_age) >
                 Timestamp(info['delete_timestamp']) >
                 Timestamp(info['put_timestamp'])))

    def merge_timestamps(self, created_at, put_timestamp, delete_timestamp):
        """
        Used in replication to handle updating timestamps.

        :param created_at: create timestamp
        :param put_timestamp: put timestamp
        :param delete_timestamp: delete timestamp
        """
        with self.get() as conn:
            old_status = self._is_deleted(conn)
            conn.execute('''
                UPDATE %s_stat SET created_at=MIN(?, created_at),
                                   put_timestamp=MAX(?, put_timestamp),
                                   delete_timestamp=MAX(?, delete_timestamp)
            ''' % self.db_type, (created_at, put_timestamp, delete_timestamp))
            if old_status != self._is_deleted(conn):
                timestamp = Timestamp.now()
                self._update_status_changed_at(conn, timestamp.internal)

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
            ''' % ('incoming' if incoming else 'outgoing'))
            result = []
            for row in curs:
                result.append({'remote_id': row[0], 'sync_point': row[1]})
            return result

    def get_max_row(self, table=None):
        if not table:
            table = self.db_contains_type
        query = '''
            SELECT SQLITE_SEQUENCE.seq
            FROM SQLITE_SEQUENCE
            WHERE SQLITE_SEQUENCE.name == '%s'
            LIMIT 1
        ''' % (table, )
        with self.get() as conn:
            row = conn.execute(query).fetchone()
        return row[0] if row else -1

    def get_replication_info(self):
        """
        Get information about the DB required for replication.

        :returns: dict containing keys from get_info plus max_row and metadata

        Note:: get_info's <db_contains_type>_count is translated to just
               "count" and metadata is the raw string.
        """
        info = self.get_info()
        info['count'] = info.pop('%s_count' % self.db_contains_type)
        info['metadata'] = self.get_raw_metadata()
        info['max_row'] = self.get_max_row()
        return info

    def get_info(self):
        self._commit_puts_stale_ok()
        with self.get() as conn:
            curs = conn.execute('SELECT * from %s_stat' % self.db_type)
            curs.row_factory = dict_factory
            return curs.fetchone()

    def put_record(self, record):
        """
        Put a record into the DB. If the DB has an associated pending file with
        space then the record is appended to that file and a commit to the DB
        is deferred. If the DB is in-memory or its pending file is full then
        the record will be committed immediately.

        :param record: a record to be added to the DB.
        :raises DatabaseConnectionError: if the DB file does not exist or if
            ``skip_commits`` is True.
        :raises LockTimeout: if a timeout occurs while waiting to take a lock
            to write to the pending file.
        """
        if self._db_file == ':memory:':
            self.merge_items([record])
            return
        if not os.path.exists(self.db_file):
            raise DatabaseConnectionError(self.db_file, "DB doesn't exist")
        if self.skip_commits:
            raise DatabaseConnectionError(self.db_file,
                                          'commits not accepted')
        with lock_parent_directory(self.pending_file, self.pending_timeout):
            pending_size = 0
            try:
                pending_size = os.path.getsize(self.pending_file)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    raise
            if pending_size > PENDING_CAP:
                self._commit_puts([record])
            else:
                with open(self.pending_file, 'a+b') as fp:
                    # Colons aren't used in base64 encoding; so they are our
                    # delimiter
                    fp.write(b':')
                    fp.write(base64.b64encode(pickle.dumps(
                        self.make_tuple_for_pickle(record),
                        protocol=PICKLE_PROTOCOL)))
                    fp.flush()

    def _skip_commit_puts(self):
        return (self._db_file == ':memory:' or self.skip_commits or not
                os.path.exists(self.pending_file))

    def _commit_puts(self, item_list=None):
        """
        Scan for .pending files and commit the found records by feeding them
        to merge_items(). Assume that lock_parent_directory has already been
        called.

        :param item_list: A list of items to commit in addition to .pending
        """
        if self._skip_commit_puts():
            if item_list:
                # this broker instance should not be used to commit records,
                # but if it is then raise an error rather than quietly
                # discarding the records in item_list.
                raise DatabaseConnectionError(self.db_file,
                                              'commits not accepted')
            return
        if item_list is None:
            item_list = []
        self._preallocate()
        if not os.path.getsize(self.pending_file):
            if item_list:
                self.merge_items(item_list)
            return
        with open(self.pending_file, 'r+b') as fp:
            for entry in fp.read().split(b':'):
                if entry:
                    try:
                        data = pickle.loads(base64.b64decode(entry))
                        self._commit_puts_load(item_list, data)
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
        if self._skip_commit_puts():
            return
        try:
            with lock_parent_directory(self.pending_file,
                                       self.pending_timeout):
                self._commit_puts()
        except (LockTimeout, sqlite3.OperationalError):
            if not self.stale_reads_ok:
                raise

    def _commit_puts_load(self, item_list, entry):
        """
        Unmarshall the :param:entry tuple and append it to :param:item_list.
        This is implemented by a particular broker to be compatible
        with its :func:`merge_items`.
        """
        raise NotImplementedError

    def merge_items(self, item_list, source=None):
        """
        Save :param:item_list to the database.
        """
        raise NotImplementedError

    def make_tuple_for_pickle(self, record):
        """
        Turn this db record dict into the format this service uses for
        pending pickles.
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
        if not DB_PREALLOCATION or self._db_file == ':memory:':
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

    def get_raw_metadata(self):
        with self.get() as conn:
            try:
                row = conn.execute('SELECT metadata FROM %s_stat' %
                                   self.db_type).fetchone()
                if not row:
                    self.quarantine("missing row in %s_stat table" %
                                    self.db_type)
                metadata = row[0]
            except sqlite3.OperationalError as err:
                if 'no such column: metadata' not in str(err):
                    raise
                metadata = ''
        return metadata

    @property
    def metadata(self):
        """
        Returns the metadata dict for the database. The metadata dict values
        are tuples of (value, timestamp) where the timestamp indicates when
        that key was set to that value.
        """
        metadata = self.get_raw_metadata()
        if metadata:
            metadata = json.loads(metadata)
            native_str_keys(metadata)
        else:
            metadata = {}
        return metadata

    @staticmethod
    def validate_metadata(metadata):
        """
        Validates that metadata falls within acceptable limits.

        :param metadata: to be validated
        :raises HTTPBadRequest: if MAX_META_COUNT or MAX_META_OVERALL_SIZE
                 is exceeded, or if metadata contains non-UTF-8 data
        """
        meta_count = 0
        meta_size = 0
        for key, (value, timestamp) in metadata.items():
            if key and not check_utf8(key):
                raise HTTPBadRequest('Metadata must be valid UTF-8')
            if value and not check_utf8(value):
                raise HTTPBadRequest('Metadata must be valid UTF-8')
            key = key.lower()
            if value and key.startswith(('x-account-meta-',
                                         'x-container-meta-')):
                prefix = 'x-account-meta-'
                if key.startswith('x-container-meta-'):
                    prefix = 'x-container-meta-'
                key = key[len(prefix):]
                meta_count = meta_count + 1
                meta_size = meta_size + len(key) + len(value)
        if meta_count > MAX_META_COUNT:
            raise HTTPBadRequest('Too many metadata items; max %d'
                                 % MAX_META_COUNT)
        if meta_size > MAX_META_OVERALL_SIZE:
            raise HTTPBadRequest('Total metadata too large; max %d'
                                 % MAX_META_OVERALL_SIZE)

    def update_metadata(self, metadata_updates, validate_metadata=False):
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
            for key, (value, timestamp) in metadata_updates.items():
                if timestamp > old_metadata[key][1]:
                    break
            else:
                return
        with self.get() as conn:
            try:
                row = conn.execute('SELECT metadata FROM %s_stat' %
                                   self.db_type).fetchone()
                if not row:
                    self.quarantine("missing row in %s_stat table" %
                                    self.db_type)
                md = row[0]
                md = json.loads(md) if md else {}
                native_str_keys(md)
            except sqlite3.OperationalError as err:
                if 'no such column: metadata' not in str(err):
                    raise
                conn.execute("""
                    ALTER TABLE %s_stat
                    ADD COLUMN metadata TEXT DEFAULT '' """ % self.db_type)
                md = {}
            for key, value_timestamp in metadata_updates.items():
                value, timestamp = value_timestamp
                if key not in md or timestamp > md[key][1]:
                    md[key] = value_timestamp
            if validate_metadata:
                DatabaseBroker.validate_metadata(md)
            conn.execute('UPDATE %s_stat SET metadata = ?' % self.db_type,
                         (json.dumps(md),))
            conn.commit()

    def reclaim(self, age_timestamp, sync_timestamp):
        """
        Delete reclaimable rows and metadata from the db.

        By default this method will delete rows from the db_contains_type table
        that are marked deleted and whose created_at timestamp is <
        age_timestamp, and deletes rows from incoming_sync and outgoing_sync
        where the updated_at timestamp is < sync_timestamp. In addition, this
        calls the :meth:`_reclaim_metadata` method.

        Subclasses may reclaim other items by overriding :meth:`_reclaim`.

        :param age_timestamp: max created_at timestamp of object rows to delete
        :param sync_timestamp: max update_at timestamp of sync rows to delete
        """
        if not self._skip_commit_puts():
            with lock_parent_directory(self.pending_file,
                                       self.pending_timeout):
                self._commit_puts()
        with self.get() as conn:
            self._reclaim(conn, age_timestamp, sync_timestamp)
            self._reclaim_metadata(conn, age_timestamp)
            conn.commit()

    def _reclaim(self, conn, age_timestamp, sync_timestamp):
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

    def _reclaim_metadata(self, conn, timestamp):
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
        timestamp = Timestamp(timestamp)
        try:
            row = conn.execute('SELECT metadata FROM %s_stat' %
                               self.db_type).fetchone()
            if not row:
                self.quarantine("missing row in %s_stat table" %
                                self.db_type)
            md = row[0]
            if md:
                md = json.loads(md)
                keys_to_delete = []
                for key, (value, value_timestamp) in md.items():
                    if value == '' and Timestamp(value_timestamp) < timestamp:
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

        :param timestamp: internalized put timestamp
        """
        with self.get() as conn:
            conn.execute(
                'UPDATE %s_stat SET put_timestamp = ?'
                ' WHERE put_timestamp < ?' % self.db_type,
                (timestamp, timestamp))
            conn.commit()

    def update_status_changed_at(self, timestamp):
        """
        Update the status_changed_at field in the stat table.  Only
        modifies status_changed_at if the timestamp is greater than the
        current status_changed_at timestamp.

        :param timestamp: internalized timestamp
        """
        with self.get() as conn:
            self._update_status_changed_at(conn, timestamp)
            conn.commit()

    def _update_status_changed_at(self, conn, timestamp):
        conn.execute(
            'UPDATE %s_stat SET status_changed_at = ?'
            ' WHERE status_changed_at < ?' % self.db_type,
            (timestamp, timestamp))
