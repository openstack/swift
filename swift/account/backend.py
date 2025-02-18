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

from swift.common.utils import Timestamp, RESERVED_BYTE
from swift.common.db import DatabaseBroker, zero_like

DATADIR = 'accounts'


POLICY_STAT_TRIGGER_SCRIPT = """
    CREATE TRIGGER container_insert_ps AFTER INSERT ON container
    BEGIN
        INSERT OR IGNORE INTO policy_stat
            (storage_policy_index, container_count, object_count, bytes_used)
            VALUES (new.storage_policy_index, 0, 0, 0);
        UPDATE policy_stat
        SET container_count = container_count + (1 - new.deleted),
            object_count = object_count + new.object_count,
            bytes_used = bytes_used + new.bytes_used
        WHERE storage_policy_index = new.storage_policy_index;
    END;
    CREATE TRIGGER container_delete_ps AFTER DELETE ON container
    BEGIN
        UPDATE policy_stat
        SET container_count = container_count - (1 - old.deleted),
            object_count = object_count - old.object_count,
            bytes_used = bytes_used - old.bytes_used
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
                storage_policy_index INTEGER DEFAULT 0
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
        """ + POLICY_STAT_TRIGGER_SCRIPT)

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
            ''', (self.account, Timestamp.now().internal, self._new_db_id(),
                  put_timestamp, put_timestamp))

    def create_policy_stat_table(self, conn):
        """
        Create policy_stat table which is specific to the account DB.
        Not a part of Pluggable Back-ends, internal to the baseline code.

        :param conn: DB connection object
        """
        conn.executescript("""
            CREATE TABLE policy_stat (
                storage_policy_index INTEGER PRIMARY KEY,
                container_count INTEGER DEFAULT 0,
                object_count INTEGER DEFAULT 0,
                bytes_used INTEGER DEFAULT 0
            );
            INSERT OR IGNORE INTO policy_stat (
                storage_policy_index, container_count, object_count,
                bytes_used
            )
            SELECT 0, container_count, object_count, bytes_used
            FROM account_stat
            WHERE container_count > 0;
        """)

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
        item_list.append(
            {'name': name,
             'put_timestamp': put_timestamp,
             'delete_timestamp': delete_timestamp,
             'object_count': object_count,
             'bytes_used': bytes_used,
             'deleted': deleted,
             'storage_policy_index': storage_policy_index})

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
                record['storage_policy_index'])

    def put_container(self, name, put_timestamp, delete_timestamp,
                      object_count, bytes_used, storage_policy_index):
        """
        Create a container with the given attributes.

        :param name: name of the container to create (a native string)
        :param put_timestamp: put_timestamp of the container to create
        :param delete_timestamp: delete_timestamp of the container to create
        :param object_count: number of objects in the container
        :param bytes_used: number of bytes used by the container
        :param storage_policy_index:  the storage policy for this container
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
                  'storage_policy_index': storage_policy_index}
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
                              always include the 'container_count' key;
                              otherwise it may be omitted on legacy databases
                              until they are migrated.

        :returns: dict of policy stats where the key is the policy index and
                  the value is a dictionary like {'object_count': M,
                  'bytes_used': N, 'container_count': L}
        """
        columns = [
            'storage_policy_index',
            'container_count',
            'object_count',
            'bytes_used',
        ]

        def run_query():
            return (conn.execute('''
                SELECT %s
                FROM policy_stat
                ''' % ', '.join(columns)).fetchall())

        self._commit_puts_stale_ok()
        info = []
        with self.get() as conn:
            try:
                info = run_query()
            except sqlite3.OperationalError as err:
                if "no such column: container_count" in str(err):
                    if do_migrations:
                        self._migrate_add_container_count(conn)
                    else:
                        columns.remove('container_count')
                    info = run_query()
                elif "no such table: policy_stat" in str(err):
                    if do_migrations:
                        self.create_policy_stat_table(conn)
                        info = run_query()
                    # else, pass and let the results be empty
                else:
                    raise

        policy_stats = {}
        for row in info:
            stats = dict(row)
            key = stats.pop('storage_policy_index')
            policy_stats[key] = stats
        return policy_stats

    def get_info(self):
        """
        Get global data for the account.

        :returns: dict with keys: account, created_at, put_timestamp,
                  delete_timestamp, status_changed_at, container_count,
                  object_count, bytes_used, hash, id
        """
        self._commit_puts_stale_ok()
        with self.get() as conn:
            return dict(conn.execute('''
                SELECT account, created_at,  put_timestamp, delete_timestamp,
                       status_changed_at, container_count, object_count,
                       bytes_used, hash, id
                FROM account_stat
            ''').fetchone())

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
                          'deleted', 'storage_policy_index'}
        :param source: if defined, update incoming_sync with the source
        """
        def _really_merge_items(conn):
            max_rowid = -1
            curs = conn.cursor()
            for rec in item_list:
                rec.setdefault('storage_policy_index', 0)  # legacy
                record = [rec['name'], rec['put_timestamp'],
                          rec['delete_timestamp'], rec['object_count'],
                          rec['bytes_used'], rec['deleted'],
                          rec['storage_policy_index']]
                query = '''
                    SELECT name, put_timestamp, delete_timestamp,
                           object_count, bytes_used, deleted,
                           storage_policy_index
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
                        deleted, storage_policy_index)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
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
            # create the policy stat table if needed and add spi to container
            try:
                _really_merge_items(conn)
            except sqlite3.OperationalError as err:
                if 'no such column: storage_policy_index' not in str(err):
                    raise
                self._migrate_add_storage_policy_index(conn)
                _really_merge_items(conn)

    def _migrate_add_container_count(self, conn):
        """
        Add the container_count column to the 'policy_stat' table and
        update it

        :param conn: DB connection object
        """
        # add the container_count column
        curs = conn.cursor()
        curs.executescript('''
            DROP TRIGGER container_delete_ps;
            DROP TRIGGER container_insert_ps;
            ALTER TABLE policy_stat
            ADD COLUMN container_count INTEGER DEFAULT 0;
        ''' + POLICY_STAT_TRIGGER_SCRIPT)

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
            bytes_used
        )
        SELECT p.storage_policy_index,
               c.count,
               p.object_count,
               p.bytes_used
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
        conn.commit()

    def _migrate_add_storage_policy_index(self, conn):
        """
        Add the storage_policy_index column to the 'container' table and
        set up triggers, creating the policy_stat table if needed.

        :param conn: DB connection object
        """
        try:
            self.create_policy_stat_table(conn)
        except sqlite3.OperationalError as err:
            if 'table policy_stat already exists' not in str(err):
                raise
        conn.executescript('''
            ALTER TABLE container
            ADD COLUMN storage_policy_index INTEGER DEFAULT 0;
        ''' + POLICY_STAT_TRIGGER_SCRIPT)
