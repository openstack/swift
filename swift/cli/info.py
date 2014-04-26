# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import sqlite3
from datetime import datetime

from swift.common.utils import hash_path, storage_directory
from swift.common.ring import Ring
from swift.common.request_helpers import is_sys_meta, is_user_meta, \
    strip_sys_meta_prefix, strip_user_meta_prefix
from swift.account.backend import AccountBroker, DATADIR as ABDATADIR
from swift.container.backend import ContainerBroker, DATADIR as CBDATADIR


class InfoSystemExit(Exception):
    """
    Indicates to the caller that a sys.exit(1) should be performed.
    """
    pass


def print_ring_locations(ring, datadir, account, container=None):
    """
    print out ring locations of specified type

    :param ring: ring instance
    :param datadir: high level directory to store account/container/objects
    :param account: account name
    :param container: container name
    """
    if ring is None or datadir is None or account is None:
        raise ValueError('None type')
    storage_type = 'account'
    if container:
        storage_type = 'container'
    try:
        part, nodes = ring.get_nodes(account, container, None)
    except (ValueError, AttributeError):
        raise ValueError('Ring error')
    else:
        path_hash = hash_path(account, container, None)
        print '\nRing locations:'
        for node in nodes:
            print ('  %s:%s - /srv/node/%s/%s/%s.db' %
                   (node['ip'], node['port'], node['device'],
                    storage_directory(datadir, part, path_hash),
                    path_hash))
        print '\nnote: /srv/node is used as default value of `devices`, the ' \
            'real value is set in the %s config file on each storage node.' % \
            storage_type


def print_db_info_metadata(db_type, info, metadata):
    """
    print out data base info/metadata based on its type

    :param db_type: database type, account or container
    :param info: dict of data base info
    :param metadata: dict of data base metadata
    """
    if info is None:
        raise ValueError('DB info is None')

    if db_type not in ['container', 'account']:
        raise ValueError('Wrong DB type')

    try:
        account = info['account']
        container = None

        if db_type == 'container':
            container = info['container']
            path = '/%s/%s' % (account, container)
        else:
            path = '/%s' % account

        print 'Path: %s' % path
        print '  Account: %s' % account

        if db_type == 'container':
            print '  Container: %s' % container

        path_hash = hash_path(account, container)
        if db_type == 'container':
            print '  Container Hash: %s' % path_hash
        else:
            print '  Account Hash: %s' % path_hash

        print 'Metadata:'
        print ('  Created at: %s (%s)' %
               (datetime.utcfromtimestamp(float(info['created_at'])),
                info['created_at']))
        print ('  Put Timestamp: %s (%s)' %
               (datetime.utcfromtimestamp(float(info['put_timestamp'])),
                info['put_timestamp']))
        print ('  Delete Timestamp: %s (%s)' %
               (datetime.utcfromtimestamp(float(info['delete_timestamp'])),
                info['delete_timestamp']))
        if db_type == 'account':
            print '  Container Count: %s' % info['container_count']
        print '  Object Count: %s' % info['object_count']
        print '  Bytes Used: %s' % info['bytes_used']
        if db_type == 'container':
            print ('  Reported Put Timestamp: %s (%s)' %
                   (datetime.utcfromtimestamp(
                    float(info['reported_put_timestamp'])),
                    info['reported_put_timestamp']))
            print ('  Reported Delete Timestamp: %s (%s)' %
                   (datetime.utcfromtimestamp
                    (float(info['reported_delete_timestamp'])),
                    info['reported_delete_timestamp']))
            print '  Reported Object Count: %s' % info['reported_object_count']
            print '  Reported Bytes Used: %s' % info['reported_bytes_used']
        print '  Chexor: %s' % info['hash']
        print '  UUID: %s' % info['id']
    except KeyError:
        raise ValueError('Info is incomplete')

    meta_prefix = 'x_' + db_type + '_'
    for key, value in info.iteritems():
        if key.lower().startswith(meta_prefix):
            title = key.replace('_', '-').title()
            print '  %s: %s' % (title, value)
    user_metadata = {}
    sys_metadata = {}
    for key, (value, timestamp) in metadata.iteritems():
        if is_user_meta(db_type, key):
            user_metadata[strip_user_meta_prefix(db_type, key)] = value
        elif is_sys_meta(db_type, key):
            sys_metadata[strip_sys_meta_prefix(db_type, key)] = value
        else:
            title = key.replace('_', '-').title()
            print '  %s: %s' % (title, value)
    if sys_metadata:
        print '  System Metadata: %s' % sys_metadata
    else:
        print 'No system metadata found in db file'

    if user_metadata:
        print '  User Metadata: %s' % user_metadata
    else:
        print 'No user metadata found in db file'


def print_info(db_type, db_file, swift_dir='/etc/swift'):
    if db_type not in ('account', 'container'):
        print "Unrecognized DB type: internal error"
        raise InfoSystemExit()
    if not os.path.exists(db_file) or not db_file.endswith('.db'):
        print "DB file doesn't exist"
        raise InfoSystemExit()
    if not db_file.startswith(('/', './')):
        db_file = './' + db_file  # don't break if the bare db file is given
    if db_type == 'account':
        broker = AccountBroker(db_file)
        datadir = ABDATADIR
    else:
        broker = ContainerBroker(db_file)
        datadir = CBDATADIR
    try:
        info = broker.get_info()
    except sqlite3.OperationalError as err:
        if 'no such table' in str(err):
            print "Does not appear to be a DB of type \"%s\": %s" % (
                db_type, db_file)
            raise InfoSystemExit()
        raise
    account = info['account']
    container = info['container'] if db_type == 'container' else None
    print_db_info_metadata(db_type, info, broker.metadata)
    try:
        ring = Ring(swift_dir, ring_name=db_type)
    except Exception:
        ring = None
    else:
        print_ring_locations(ring, datadir, account, container)
