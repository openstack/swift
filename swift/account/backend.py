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
Pluggable Back-end for Account Server
"""


import sqlite3

from swift.common.utils import RESERVED_BYTE
from swift.common.utils.timestamp import Timestamp, NormalTimestamp
from swift.common.db import DatabaseBroker, zero_like

DATADIR = 'accounts'


def _hash_value(row):
    # The SQL expression for the value that a container_* trigger passes to
    # chexor() for the named trigger row ('new' or 'old'). The mpu byte counts
    # are appended only when at least one is non-zero, so that a row without
    # mpu bytes hashes exactly as it did before those columns existed.
    # chexor() cancels a row's contribution by hashing the row's value
    # again, so a row that was hashed in before a db was migrated would
    # otherwise never be cancelled out once its value had changed format.
    # It also keeps the hash of a db that has been migrated equal to that of
    # an unmigrated replica holding the same rows without mpu bytes.
    parts = 'coalesce(%s.bytes_in_parts, 0)' % row
    manifests = 'coalesce(%s.bytes_in_manifests, 0)' % row
    return (
        "%(r)s.put_timestamp || '-' || %(r)s.delete_timestamp || '-' || "
        "%(r)s.object_count || '-' || %(r)s.bytes_used || "
        "CASE WHEN %(p)s = 0 AND %(m)s = 0 THEN '' "
        "ELSE '-' || %(p)s || '-' || %(m)s END"
        % {'r': row, 'p': parts, 'm': manifests})


# note: bytes_in_parts and bytes_in_manifests are accumulated ungated by
# deleted, exactly like bytes_used, so that the invariant
# bytes_in_parts <= bytes_used holds for tombstoned container rows too. Both
# sides of the arithmetic are coalesced because a single NULL would otherwise
# poison the accumulator permanently - in sqlite, n + NULL is NULL, and a
# column DEFAULT does not apply to an explicitly bound NULL.
CONTAINER_TRIGGER_SCRIPT = """
    CREATE TRIGGER container_insert AFTER INSERT ON container
    BEGIN
        UPDATE account_stat
        SET container_count = container_count + (1 - new.deleted),
            object_count = object_count + new.object_count,
            bytes_used = bytes_used + new.bytes_used,
            bytes_in_parts = coalesce(bytes_in_parts, 0)
                + coalesce(new.bytes_in_parts, 0),
            bytes_in_manifests = coalesce(bytes_in_manifests, 0)
                + coalesce(new.bytes_in_manifests, 0),
            hash = chexor(hash, new.name, """ + _hash_value('new') + """);
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
            bytes_in_parts = coalesce(bytes_in_parts, 0)
                - coalesce(old.bytes_in_parts, 0),
            bytes_in_manifests = coalesce(bytes_in_manifests, 0)
                - coalesce(old.bytes_in_manifests, 0),
            hash = chexor(hash, old.name, """ + _hash_value('old') + """);
    END;

"""


POLICY_STAT_TRIGGER_SCRIPT = """
    CREATE TRIGGER container_insert_ps AFTER INSERT ON container
    BEGIN
        INSERT OR IGNORE INTO policy_stat
            (storage_policy_index, container_count, object_count, bytes_used,
             bytes_in_parts, bytes_in_manifests)
            VALUES (new.storage_policy_index, 0, 0, 0, 0, 0);
        UPDATE policy_stat
        SET container_count = container_count + (1 - new.deleted),
            object_count = object_count + new.object_count,
            bytes_used = bytes_used + new.bytes_used,
            bytes_in_parts = coalesce(bytes_in_parts, 0)
                + coalesce(new.bytes_in_parts, 0),
            bytes_in_manifests = coalesce(bytes_in_manifests, 0)
                + coalesce(new.bytes_in_manifests, 0)
        WHERE storage_policy_index = new.storage_policy_index;
    END;
    CREATE TRIGGER container_delete_ps AFTER DELETE ON container
    BEGIN
        UPDATE policy_stat
        SET container_count = container_count - (1 - old.deleted),
            object_count = object_count - old.object_count,
            bytes_used = bytes_used - old.bytes_used,
            bytes_in_parts = coalesce(bytes_in_parts, 0)
                - coalesce(old.bytes_in_parts, 0),
            bytes_in_manifests = coalesce(bytes_in_manifests, 0)
                - coalesce(old.bytes_in_manifests, 0)
        WHERE storage_policy_index = old.storage_policy_index;
    END;

"""


class AccountBroker(DatabaseBroker):
    """Encapsulates working with an account database."""
    db_type = 'account'
    db_contains_type = 'container'
    db_reclaim_timestamp = 'delete_timestamp'

    def _initialize(self, conn, put_timestamp, **kwargs):
        """
        Create a brand new account database (tables, indices, triggers, etc.)

        :param conn: DB connection object
        :param put_timestamp: put timestamp
        """
        if not self.account:
            raise ValueError(
                'Attempting to create a new database with no account set')
        self.create_container_table(conn)
        self.create_account_stat_table(conn, put_timestamp)
        self.create_policy_stat_table(conn)

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
                deleted INTEGER DEFAULT 0,
                storage_policy_index INTEGER DEFAULT 0,
                bytes_in_parts INTEGER DEFAULT 0,
                bytes_in_manifests INTEGER DEFAULT 0
            );

            CREATE INDEX ix_container_deleted_name ON
                container (deleted, name);
        """ + CONTAINER_TRIGGER_SCRIPT + POLICY_STAT_TRIGGER_SCRIPT)

    def create_account_stat_table(self, conn, put_timestamp):
        """
        Create account_stat table which is specific to the account DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

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
                bytes_in_parts INTEGER DEFAULT 0,
                bytes_in_manifests INTEGER DEFAULT 0,
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
                   put_timestamp = ?, status_changed_at = ?
            ''', (self.account, NormalTimestamp.now().internal,
                  self._new_db_id(), put_timestamp, put_timestamp))

    def create_policy_stat_table(self, conn):
        """
        Create policy_stat table which is specific to the account DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

        The seed assumes that account_stat has the current schema. That holds
        for a new db, which creates account_stat first, and for a legacy db,
        which only ever reaches here via :meth:`_migrate_container_table`,
        after that has brought account_stat up to date.

        :param conn: DB connection object
        """
        # the create and the seed must stay in one script so that a failure
        # part way through leaves nothing committed
        conn.executescript("""
            CREATE TABLE policy_stat (
                storage_policy_index INTEGER PRIMARY KEY,
                container_count INTEGER DEFAULT 0,
                object_count INTEGER DEFAULT 0,
                bytes_used INTEGER DEFAULT 0,
                bytes_in_parts INTEGER DEFAULT 0,
                bytes_in_manifests INTEGER DEFAULT 0
            );
            INSERT OR IGNORE INTO policy_stat (
                storage_policy_index, container_count, object_count,
                bytes_used, bytes_in_parts, bytes_in_manifests)
            SELECT 0, container_count, object_count, bytes_used,
                   bytes_in_parts, bytes_in_manifests
            FROM account_stat
            WHERE container_count > 0;
        """)

    @staticmethod
    def _add_column_if_missing(curs, table, column):
        """
        Add an INTEGER column with a DEFAULT of 0 to a table, unless the table
        already has that column.

        :param curs: DB cursor object
        :param table: name of the table to alter
        :param column: name of the column to add
        :returns: True if the column was added, False if it was already there.
        """
        try:
            curs.execute('ALTER TABLE %s ADD COLUMN %s INTEGER DEFAULT 0'
                         % (table, column))
        except sqlite3.OperationalError as err:
            # match on the column name, so that any other error - a missing
            # table, a locked db - still propagates
            if 'duplicate column name: %s' % column not in str(err):
                raise
            return False
        return True

    def get_db_version(self, conn):
        if self._db_version == -1:
            self._db_version = 0
            for row in conn.execute('''
                    SELECT name FROM sqlite_master
                    WHERE name = 'ix_container_deleted_name' '''):
                self._db_version = 1
        return self._db_version

    def _commit_puts_load(self, item_list, entry):
        """See :func:`swift.common.db.DatabaseBroker._commit_puts_load`"""
        # check to see if the update includes policy_index or not
        (name, put_timestamp, delete_timestamp, object_count, bytes_used,
         deleted) = entry[:6]
        if len(entry) > 6:
            storage_policy_index = entry[6]
        else:
            # legacy support during upgrade until first non legacy storage
            # policy is defined
            storage_policy_index = 0
        item = {'name': name,
                'put_timestamp': put_timestamp,
                'delete_timestamp': delete_timestamp,
                'object_count': object_count,
                'bytes_used': bytes_used,
                'deleted': deleted,
                'storage_policy_index': storage_policy_index}
        if len(entry) > 8:
            item['bytes_in_parts'] = entry[7]
            item['bytes_in_manifests'] = entry[8]
        item_list.append(item)

    def empty(self):
        """
        Check if the account DB is empty.

        :returns: True if the database has no active containers.
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            row = conn.execute(
                'SELECT container_count from account_stat').fetchone()
            return zero_like(row[0])

    def make_tuple_for_pickle(self, record):
        return (record['name'], record['put_timestamp'],
                record['delete_timestamp'], record['object_count'],
                record['bytes_used'], record['deleted'],
                record['storage_policy_index'],
                record['bytes_in_parts'], record['bytes_in_manifests'])

    def put_container(self, name, put_timestamp, delete_timestamp,
                      object_count, bytes_used, storage_policy_index,
                      bytes_in_parts=0, bytes_in_manifests=0):
        """
        Create a container with the given attributes.

        :param name: name of the container to create (a native string)
        :param put_timestamp: put_timestamp of the container to create
        :param delete_timestamp: delete_timestamp of the container to create
        :param object_count: number of objects in the container
        :param bytes_used: number of bytes used by the container
        :param storage_policy_index:  the storage policy for this container
        :param bytes_in_parts: number of bytes stored in mpu parts in the
            container
        :param bytes_in_manifests: number of bytes stored in mpu manifests in
            the container
        """
        if Timestamp(delete_timestamp) > Timestamp(put_timestamp) and \
                zero_like(object_count):
            deleted = 1
        else:
            deleted = 0
        record = {'name': name, 'put_timestamp': put_timestamp,
                  'delete_timestamp': delete_timestamp,
                  'object_count': object_count,
                  'bytes_used': bytes_used,
                  'deleted': deleted,
                  'storage_policy_index': storage_policy_index,
                  'bytes_in_parts': bytes_in_parts,
                  'bytes_in_manifests': bytes_in_manifests}
        self.put_record(record)

    def _is_deleted_info(self, status, container_count, delete_timestamp,
                         put_timestamp):
        """
        Apply delete logic to database info.

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        return status == 'DELETED' or zero_like(container_count) and (
            Timestamp(delete_timestamp) > Timestamp(put_timestamp))

    def _is_deleted(self, conn):
        """
        Check account_stat table and evaluate info.

        :param conn: database conn

        :returns: True if the DB is considered to be deleted, False otherwise
        """
        info = conn.execute('''
            SELECT put_timestamp, delete_timestamp, container_count, status
            FROM account_stat''').fetchone()
        return self._is_deleted_info(**info)

    def is_status_deleted(self):
        """Only returns true if the status field is set to DELETED."""
        with self.get() as conn:
            row = conn.execute('''
                SELECT put_timestamp, delete_timestamp, status
                FROM account_stat''').fetchone()
            return row['status'] == "DELETED" or (
                row['delete_timestamp'] > row['put_timestamp'])

    def get_policy_stats(self, do_migrations=False):
        """
        Get global policy stats for the account.

        :param do_migrations: boolean, if True the policy stat dicts will
                              always include the 'container_count',
                              'bytes_in_parts' and 'bytes_in_manifests' keys;
                              otherwise they may be omitted on legacy
                              databases until they are migrated.

        :returns: dict of policy stats where the key is the policy index and
                  the value is a dictionary like {'object_count': M,
                  'bytes_used': N, 'container_count': L,
                  'bytes_in_parts': P, 'bytes_in_manifests': Q}
        """
        def run_query(optional_selects):
            columns = ('storage_policy_index', 'object_count',
                       'bytes_used') + optional_selects
            return (conn.execute('''
                SELECT %s FROM policy_stat
                ''' % ', '.join(columns)).fetchall())

        # columns of policy_stat that may not exist in an older db;
        # these are dropped from a query rather than given a fallback
        # value, so that callers can tell that the db doesn't have them
        column_fallbacks = {('container_count',): None,
                            ('bytes_in_parts', 'bytes_in_manifests'): None}
        # for migration, start with a flattened list of column_fallbacks keys
        migration_columns = tuple(column for columns in column_fallbacks.keys()
                                  for column in columns)

        self._commit_puts_stale_ok()
        with self.get() as conn:
            if do_migrations:
                info = self._execute_with_container_table_migrations(
                    conn, run_query, migration_columns)
            else:
                # a legacy db may be missing more than one column, and sqlite
                # only reports the first, so the helper keeps retrying until
                # every distinct error has been dealt with once
                try:
                    info = self._execute_with_column_fallbacks(
                        conn, run_query, column_fallbacks)
                except sqlite3.OperationalError as err:
                    # a db so old that it has no policy_stat table at all is
                    # not worth a write lock either; report nothing
                    if 'no such table: policy_stat' not in str(err):
                        raise
                    info = []

        policy_stats = {}
        for row in info:
            stats = dict(row)
            key = stats.pop('storage_policy_index')
            policy_stats[key] = stats
        return policy_stats

    def get_info(self):
        """
        Get global data for the account.

        An account database that has not yet been migrated to add the
        ``bytes_in_parts`` and ``bytes_in_manifests`` columns reports zero for
        those keys.

        :returns: dict with keys: account, created_at, put_timestamp,
                  delete_timestamp, status_changed_at, container_count,
                  object_count, bytes_used, bytes_in_parts,
                  bytes_in_manifests, hash, id
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            def do_query(optional_selects):
                columns = ('account', 'created_at', 'put_timestamp',
                           'delete_timestamp', 'status_changed_at',
                           'container_count', 'object_count',
                           'bytes_used', 'hash', 'id') + optional_selects
                return conn.execute('''
                    SELECT %s
                    FROM account_stat
                ''' % ', '.join(columns)).fetchone()

            column_fallbacks = {
                ('bytes_in_parts', 'bytes_in_manifests'): 0,
            }
            data = dict(self._execute_with_column_fallbacks(
                conn, do_query, column_fallbacks))
        self.account = data['account']
        return data

    def _populate_instance_cache(self):
        """
        Lazily hydrate instance attributes used for logging and other
        read-mostly flows. Use `self.account is None` as the only
        indicator that we haven't populated yet.
        """
        if self.account is None:
            self.get_info()

    @property
    def path(self):
        """
        Logical namespace path used for logging.

        For AccountBroker we return just "<account>".
        """
        self._populate_instance_cache()
        return self.account

    def list_containers_iter(self, limit, marker, end_marker, prefix,
                             delimiter, reverse=False, allow_reserved=False):
        """
        Get a list of containers sorted by name starting at marker onward, up
        to limit entries. Entries will begin with the prefix and will not have
        the delimiter after the prefix.

        :param limit: maximum number of entries to get
        :param marker: marker query
        :param end_marker: end marker query
        :param prefix: prefix query
        :param delimiter: delimiter for query
        :param reverse: reverse the result order.
        :param allow_reserved: exclude names with reserved-byte by default

        :returns: list of tuples of (name, object_count, bytes_used,
                  put_timestamp, storage_policy_index, is_subdir)
        """
        delim_force_gte = False
        if reverse:
            # Reverse the markers if we are reversing the listing.
            marker, end_marker = end_marker, marker
        self._commit_puts_stale_ok()
        if delimiter and not prefix:
            prefix = ''
        if prefix:
            end_prefix = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        orig_marker = marker
        with self.get() as conn:
            results = []
            while len(results) < limit:
                query = """
                    SELECT name, object_count, bytes_used, put_timestamp,
                    {storage_policy_index}, 0
                    FROM container
                    WHERE """
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
                elif marker and (not prefix or marker >= prefix):
                    query += ' name > ? AND'
                    query_args.append(marker)
                elif prefix:
                    query += ' name >= ? AND'
                    query_args.append(prefix)
                if not allow_reserved:
                    query += ' name >= ? AND'
                    query_args.append(chr(ord(RESERVED_BYTE) + 1))
                if self.get_db_version(conn) < 1:
                    query += ' +deleted = 0'
                else:
                    query += ' deleted = 0'
                query += ' ORDER BY name %s LIMIT ?' % \
                         ('DESC' if reverse else '')
                query_args.append(limit - len(results))
                try:
                    # First, try querying with the storage policy index.
                    curs = conn.execute(
                        query.format(
                            storage_policy_index="storage_policy_index"),
                        query_args)
                except sqlite3.OperationalError as err:
                    # If the storage policy column is not available,
                    # the database has not been migrated to the new schema
                    # with storage_policy_index. Re-run the query with
                    # storage_policy_index set to 0, which is what
                    # would be set once the database is migrated.
                    # TODO(callumdickinson): If support for migrating
                    # pre-storage policy versions of Swift is dropped,
                    # then this special handling can be removed.
                    if "no such column: storage_policy_index" in str(err):
                        curs = conn.execute(
                            query.format(storage_policy_index="0"),
                            query_args)
                    else:
                        raise
                curs.row_factory = None

                # Delimiters without a prefix is ignored, further if there
                # is no delimiter then we can simply return the result as
                # prefixes are now handled in the SQL statement.
                if prefix is None or not delimiter:
                    return [r for r in curs]

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
                    if end >= 0:
                        if reverse:
                            end_marker = name[:end + len(delimiter)]
                        else:
                            marker = ''.join([
                                name[:end],
                                delimiter[:-1],
                                chr(ord(delimiter[-1:]) + 1),
                            ])
                            # we want result to be inclusive of delim+1
                            delim_force_gte = True
                        dir_name = name[:end + len(delimiter)]
                        if dir_name != orig_marker:
                            results.append([dir_name, 0, 0, '0', -1, 1])
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
                          'deleted', 'storage_policy_index',
                          'bytes_in_parts', 'bytes_in_manifests'}
        :param source: if defined, update incoming_sync with the source
        """
        def _really_merge_items(conn):
            max_rowid = -1
            curs = conn.cursor()
            for rec in item_list:
                rec.setdefault('storage_policy_index', 0)  # legacy
                # Items replicated from an older peer have no mpu byte counts.
                # Allow them to be set to zero so that bytes_used,
                # bytes_in_parts and bytes_in_manifests are always mutually
                # consistent. A subsequent update from an upgraded peer will
                # replace them all.
                record = [rec['name'], rec['put_timestamp'],
                          rec['delete_timestamp'], rec['object_count'],
                          rec['bytes_used'], rec['deleted'],
                          rec['storage_policy_index'],
                          # apply defaults in case update came from a legacy db
                          rec.get('bytes_in_parts') or 0,
                          rec.get('bytes_in_manifests') or 0]
                # note: this SELECT will trigger migration of older db's and
                # must therefore remain ahead of the later INSERT that would
                # not trigger migration
                query = '''
                    SELECT name, put_timestamp, delete_timestamp,
                           object_count, bytes_used, deleted,
                           storage_policy_index,
                           bytes_in_parts, bytes_in_manifests
                    FROM container WHERE name = ?
                '''
                if self.get_db_version(conn) >= 1:
                    query += ' AND deleted IN (0, 1)'
                curs_row = curs.execute(query, (rec['name'],))
                curs_row.row_factory = None
                row = curs_row.fetchone()
                if row:
                    row = list(row)
                    for i in range(5):
                        if record[i] is None and row[i] is not None:
                            record[i] = row[i]
                    if Timestamp(row[1]) > \
                       Timestamp(record[1]):  # Keep newest put_timestamp
                        record[1] = row[1]
                    if Timestamp(row[2]) > \
                       Timestamp(record[2]):  # Keep newest delete_timestamp
                        record[2] = row[2]
                    # If deleted, mark as such
                    if Timestamp(record[2]) > Timestamp(record[1]) and \
                            zero_like(record[3]):
                        record[5] = 1
                    else:
                        record[5] = 0
                curs.execute('''
                    DELETE FROM container WHERE name = ? AND
                                                deleted IN (0, 1)
                ''', (record[0],))
                curs.execute('''
                    INSERT INTO container (name, put_timestamp,
                        delete_timestamp, object_count, bytes_used,
                        deleted, storage_policy_index,
                        bytes_in_parts, bytes_in_manifests)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', record)
                if source:
                    max_rowid = max(max_rowid, rec['ROWID'])
            if source:
                try:
                    curs.execute('''
                        INSERT INTO incoming_sync (sync_point, remote_id)
                        VALUES (?, ?)
                    ''', (max_rowid, source))
                except sqlite3.IntegrityError:
                    curs.execute('''
                        UPDATE incoming_sync
                        SET sync_point=max(?, sync_point)
                        WHERE remote_id=?
                    ''', (max_rowid, source))
            conn.commit()

        with self.get() as conn:
            self._execute_with_container_table_migrations(
                conn, _really_merge_items, conn)

    def _migrate_add_container_count(self, curs):
        """
        Add the container_count column to the 'policy_stat' table, if it is
        missing. The caller is responsible for backfilling the column.

        :param curs: DB cursor object
        :returns: True if the column was added, False if it was already there.
        """
        return self._add_column_if_missing(
            curs, 'policy_stat', 'container_count')

    def _migrate_backfill_container_count(self, curs):
        """
        Ensure that the policy_state table container_count column is populated.

        :param curs: DB cursor object
        """
        # keep the simple case simple, if there's only one entry in the
        # policy_stat table we just copy the total container count from the
        # account_stat table

        # if that triggers an update then the where changes <> 0 *would* exist
        # and the insert or replace from the count subqueries won't execute

        curs.executescript("""
        UPDATE policy_stat
        SET container_count = (
            SELECT container_count
            FROM account_stat)
        WHERE (
            SELECT COUNT(storage_policy_index)
            FROM policy_stat
        ) <= 1;

        INSERT OR REPLACE INTO policy_stat (
            storage_policy_index,
            container_count,
            object_count,
            bytes_used,
            bytes_in_parts,
            bytes_in_manifests
        )
        SELECT p.storage_policy_index,
               c.count,
               p.object_count,
               p.bytes_used,
               p.bytes_in_parts,
               p.bytes_in_manifests
        FROM (
            SELECT storage_policy_index,
                   COUNT(*) as count
            FROM container
            WHERE deleted = 0
            GROUP BY storage_policy_index
        ) c
        JOIN policy_stat p
        ON p.storage_policy_index = c.storage_policy_index
        WHERE NOT EXISTS(
            SELECT changes() as change
            FROM policy_stat
            WHERE change <> 0
        );
        """)

    def _migrate_add_mpu_byte_counts(self, curs, table):
        """
        Add the mpu byte count columns to a table, if they are missing.

        :param curs: DB cursor object
        :param table: name of the table to alter
        """
        for column in ('bytes_in_parts', 'bytes_in_manifests'):
            self._add_column_if_missing(curs, table, column)

    def _migrate_container_table(self, conn):
        """
        Bring an older db up to the current schema: add the bytes_in_parts and
        bytes_in_manifests columns to the 'account_stat' and 'policy_stat'
        tables; create the 'policy_stat' table if it is missing or add its
        container_count column if only that is missing; add the
        storage_policy_index, bytes_in_parts and bytes_in_manifests columns to
        the 'container' table, and reinstall the triggers that reference them
        all.

        The whole schema is migrated whenever any part of it is found to be
        out of date, so a later step may assume that the earlier steps have
        been applied - most importantly, the triggers installed at the end can
        never reference a column that is missing. (sqlite resolves trigger
        column references at fire time rather than at CREATE TRIGGER time, so a
        trigger that referenced a missing column would be accepted silently and
        then fail on the next container PUT.)

        Each step is nonetheless guarded individually, because an older version
        of this code may already have applied some of them: for example, a db
        may already have the policy_stat container_count column but not the mpu
        byte counts.

        The mpu columns need no backfill: ALTER TABLE ... DEFAULT 0 gives
        every existing container row a zero, so the trigger-maintained
        account_stat and policy_stat sums remain consistent (0 == SUM(0)). An
        account db that pre-dates these columns cannot have been told about
        any mpu bytes. container_count does need a backfill, since existing
        container rows do count towards it.

        :param conn: DB connection object
        """
        curs = conn.cursor()
        self._migrate_add_mpu_byte_counts(curs, 'account_stat')

        if self._get_table_columns(conn, 'policy_stat'):
            # container_count is added ahead of the mpu columns so that a db
            # migrated in one step here ends up with the same column order as
            # one that had container_count added by an older version
            added_container_count = self._migrate_add_container_count(curs)
            self._migrate_add_mpu_byte_counts(curs, 'policy_stat')
            if added_container_count:
                # note: this rewrites whole policy_stat rows, so it must
                # follow the mpu columns being added, or they would be reset
                # to zero. It groups container rows by storage_policy_index,
                # which is safe: a db that has a policy_stat table always has
                # that column too.
                self._migrate_backfill_container_count(curs)
        else:
            # account_stat has just been brought up to date, so the new table
            # is seeded from it with every column, container_count included,
            # and needs no backfill
            try:
                self.create_policy_stat_table(conn)
            except sqlite3.OperationalError as err:
                # another process may have created the table since we looked
                if 'table policy_stat already exists' not in str(err):
                    raise

        # The migration adds columns to the table and replaces the triggers in
        # a single explicit transaction to ensure atomicity. This is important
        # because the migration is prompted by a missing column being detected,
        # so migration would not be repeated if the columns were added but the
        # triggers were not replaced. However, the triggers must not be
        # replaced unless the columns have been added, because they reference
        # the new columns.
        # Outside an explicit transaction, a column-adding ALTER would be
        # committed immediately. To avoid that it is necessary to construct a
        # single script that combines adding columns and replacing triggers,
        # all enclosed in a single transaction. If any of the columns already
        # exists, which is possible if a db has previously been partially
        # migrated (for example to add storage_policy_index), the entire script
        # will fail. The script therefore only adds columns that are not
        # already in the table. If the script still detects a duplicate column
        # then that implies another process has successfully completed the
        # migration.
        container_columns = self._get_table_columns(conn, 'container')
        alter_statements = ''.join(
            'ALTER TABLE container ADD COLUMN %s INTEGER DEFAULT 0;\n' % col
            for col in ('storage_policy_index', 'bytes_in_parts',
                        'bytes_in_manifests')
            if col not in container_columns)
        try:
            # executescript first commits any open transaction, then runs the
            # script as given; BEGIN IMMEDIATE takes the write lock
            curs.executescript(
                'BEGIN IMMEDIATE;\n' + alter_statements + '''
                DROP TRIGGER IF EXISTS container_insert;
                DROP TRIGGER IF EXISTS container_update;
                DROP TRIGGER IF EXISTS container_delete;
                DROP TRIGGER IF EXISTS container_insert_ps;
                DROP TRIGGER IF EXISTS container_delete_ps;
                ''' + CONTAINER_TRIGGER_SCRIPT + POLICY_STAT_TRIGGER_SCRIPT
                + 'COMMIT;')
        except sqlite3.OperationalError as err:
            # executescript leaves the transaction open on failure
            conn.rollback()
            # another process may have added the columns since we looked;
            # either the other migration completed everything, or it was an
            # older implementation that only added some of the columns, in
            # which case this migration will be retried in a subsequent call.
            if 'duplicate column name' not in str(err):
                raise

    def _execute_with_container_table_migrations(
            self, conn, func, *args, **kwargs):
        """
        Call ``func``, migrating the db and retrying ``func`` if it raises an
        ``sqlite3.OperationalError`` that reports that a table or column of the
        current schema is missing.

        :param conn: a DB connection.
        :param func: the callable to execute.
        :param args: positional args for ``func``.
        :param kwargs: keyword args for ``func``.
        :returns: the value returned by ``func``.
        """
        # Any of the following error messages indicate that the db pre-dates
        # some part of the current schema. All errors are fixed by the same
        # migration, which brings every table up to date in a single pass.
        # There is therefore no need to work out which part of the schema is
        # missing, and a later part of the migration may assume that the
        # earlier parts have been applied.
        migrations = dict.fromkeys(
            ('no such column: storage_policy_index',
             'no such column: container_count',
             'no such column: bytes_in_parts',
             'no such column: bytes_in_manifests',
             'no such table: policy_stat'),
            self._migrate_container_table)
        return self._execute_with_migrations(
            conn, func, migrations, *args, **kwargs)
