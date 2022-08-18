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

import time
from collections import defaultdict
import functools
import socket
import itertools
import logging

from eventlet import GreenPile, GreenPool, Timeout

from swift.common import constraints
from swift.common.daemon import Daemon, run_daemon
from swift.common.direct_client import (
    direct_head_container, direct_delete_container_object,
    direct_put_container_object, ClientException)
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.request_helpers import MISPLACED_OBJECTS_ACCOUNT, \
    USE_REPLICATION_NETWORK_HEADER
from swift.common.utils import get_logger, split_path, majority_size, \
    FileLikeIter, Timestamp, last_modified_date_to_timestamp, \
    LRUCache, decode_timestamps, hash_path, parse_options
from swift.common.storage_policy import POLICIES

MISPLACED_OBJECTS_CONTAINER_DIVISOR = 3600  # 1 hour
CONTAINER_POLICY_TTL = 30


def cmp_policy_info(info, remote_info):
    """
    You have to squint to see it, but the general strategy is just:

    if either has been recreated:
        return the newest (of the recreated)
    else
        return the oldest

    I tried cleaning it up for awhile, but settled on just writing a bunch of
    tests instead.  Once you get an intuitive sense for the nuance here you
    can try and see there's a better way to spell the boolean logic but it all
    ends up looking sorta hairy.

    :returns: -1 if info is correct, 1 if remote_info is better
    """
    def is_deleted(info):
        return (info['delete_timestamp'] > info['put_timestamp'] and
                info.get('count', info.get('object_count', 0)) == 0)

    def cmp(a, b):
        if a < b:
            return -1
        elif b < a:
            return 1
        else:
            return 0

    deleted = is_deleted(info)
    remote_deleted = is_deleted(remote_info)
    if any([deleted, remote_deleted]):
        if not deleted:
            return -1
        elif not remote_deleted:
            return 1
        return cmp(remote_info['status_changed_at'],
                   info['status_changed_at'])

    def has_been_recreated(info):
        return (info['put_timestamp'] > info['delete_timestamp'] >
                Timestamp(0))

    remote_recreated = has_been_recreated(remote_info)
    recreated = has_been_recreated(info)
    if any([remote_recreated, recreated]):
        if not recreated:
            return 1
        elif not remote_recreated:
            return -1
        # both have been recreated, everything devoles to here eventually
        most_recent_successful_delete = max(info['delete_timestamp'],
                                            remote_info['delete_timestamp'])
        if info['put_timestamp'] < most_recent_successful_delete:
            return 1
        elif remote_info['put_timestamp'] < most_recent_successful_delete:
            return -1
    return cmp(info['status_changed_at'], remote_info['status_changed_at'])


def incorrect_policy_index(info, remote_info):
    """
    Compare remote_info to info and decide if the remote storage policy index
    should be used instead of ours.
    """
    if 'storage_policy_index' not in remote_info:
        return False
    if remote_info['storage_policy_index'] == info['storage_policy_index']:
        return False

    # Only return True if remote_info has the better data;
    # see the docstring for cmp_policy_info
    return cmp_policy_info(info, remote_info) > 0


def translate_container_headers_to_info(headers):
    default_timestamp = Timestamp(0).internal
    return {
        'storage_policy_index': int(headers['X-Backend-Storage-Policy-Index']),
        'put_timestamp': headers.get('x-backend-put-timestamp',
                                     default_timestamp),
        'delete_timestamp': headers.get('x-backend-delete-timestamp',
                                        default_timestamp),
        'status_changed_at': headers.get('x-backend-status-changed-at',
                                         default_timestamp),
    }


def best_policy_index(headers):
    container_info = [translate_container_headers_to_info(header_set)
                      for header_set in headers]
    container_info.sort(key=functools.cmp_to_key(cmp_policy_info))
    return container_info[0]['storage_policy_index']


def get_reconciler_container_name(obj_timestamp):
    """
    Get the name of a container into which a misplaced object should be
    enqueued. The name is the object's last modified time rounded down to the
    nearest hour.

    :param obj_timestamp: a string representation of the object's 'created_at'
                          time from it's container db row.
    :return: a container name
    """
    # Use last modified time of object to determine reconciler container name
    _junk, _junk, ts_meta = decode_timestamps(obj_timestamp)
    return str(int(ts_meta) //
               MISPLACED_OBJECTS_CONTAINER_DIVISOR *
               MISPLACED_OBJECTS_CONTAINER_DIVISOR)


def get_reconciler_obj_name(policy_index, account, container, obj):
    return "%(policy_index)d:/%(acc)s/%(con)s/%(obj)s" % {
        'policy_index': policy_index, 'acc': account,
        'con': container, 'obj': obj}


def get_reconciler_content_type(op):
    try:
        return {
            'put': 'application/x-put',
            'delete': 'application/x-delete',
        }[op.lower()]
    except KeyError:
        raise ValueError('invalid operation type %r' % op)


def get_row_to_q_entry_translator(broker):
    account = broker.root_account
    container = broker.root_container
    op_type = {
        0: get_reconciler_content_type('put'),
        1: get_reconciler_content_type('delete'),
    }

    def translator(obj_info):
        name = get_reconciler_obj_name(obj_info['storage_policy_index'],
                                       account, container,
                                       obj_info['name'])
        return {
            'name': name,
            'deleted': 0,
            'created_at': obj_info['created_at'],
            'etag': obj_info['created_at'],
            'content_type': op_type[obj_info['deleted']],
            'size': 0,
        }
    return translator


def add_to_reconciler_queue(container_ring, account, container, obj,
                            obj_policy_index, obj_timestamp, op,
                            force=False, conn_timeout=5, response_timeout=15):
    """
    Add an object to the container reconciler's queue. This will cause the
    container reconciler to move it from its current storage policy index to
    the correct storage policy index.

    :param container_ring: container ring
    :param account: the misplaced object's account
    :param container: the misplaced object's container
    :param obj: the misplaced object
    :param obj_policy_index: the policy index where the misplaced object
                             currently is
    :param obj_timestamp: the misplaced object's X-Timestamp. We need this to
                          ensure that the reconciler doesn't overwrite a newer
                          object with an older one.
    :param op: the method of the operation (DELETE or PUT)
    :param force: over-write queue entries newer than obj_timestamp
    :param conn_timeout: max time to wait for connection to container server
    :param response_timeout: max time to wait for response from container
                             server

    :returns: .misplaced_object container name, False on failure. "Success"
              means a majority of containers got the update.
    """
    container_name = get_reconciler_container_name(obj_timestamp)
    object_name = get_reconciler_obj_name(obj_policy_index, account,
                                          container, obj)
    if force:
        # this allows an operator to re-enqueue an object that has
        # already been popped from the queue to be reprocessed, but
        # could potentially prevent out of order updates from making it
        # into the queue
        x_timestamp = Timestamp.now().internal
    else:
        x_timestamp = obj_timestamp
    q_op_type = get_reconciler_content_type(op)
    headers = {
        'X-Size': 0,
        'X-Etag': obj_timestamp,
        'X-Timestamp': x_timestamp,
        'X-Content-Type': q_op_type,
        USE_REPLICATION_NETWORK_HEADER: 'true',
    }

    def _check_success(*args, **kwargs):
        try:
            direct_put_container_object(*args, **kwargs)
            return 1
        except (ClientException, Timeout, socket.error):
            return 0

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(MISPLACED_OBJECTS_ACCOUNT,
                                           container_name)
    for node in nodes:
        pile.spawn(_check_success, node, part, MISPLACED_OBJECTS_ACCOUNT,
                   container_name, object_name, headers=headers,
                   conn_timeout=conn_timeout,
                   response_timeout=response_timeout)

    successes = sum(pile)
    if successes >= majority_size(len(nodes)):
        return container_name
    else:
        return False


def slightly_later_timestamp(ts, offset=1):
    return Timestamp(ts, offset=offset).internal


def parse_raw_obj(obj_info):
    """
    Translate a reconciler container listing entry to a dictionary
    containing the parts of the misplaced object queue entry.

    :param obj_info: an entry in an a container listing with the
                     required keys: name, content_type, and hash

    :returns: a queue entry dict with the keys: q_policy_index, account,
              container, obj, q_op, q_ts, q_record, and path
    """
    raw_obj_name = obj_info['name']
    policy_index, obj_name = raw_obj_name.split(':', 1)
    q_policy_index = int(policy_index)
    account, container, obj = split_path(obj_name, 3, 3, rest_with_last=True)
    try:
        q_op = {
            'application/x-put': 'PUT',
            'application/x-delete': 'DELETE',
        }[obj_info['content_type']]
    except KeyError:
        raise ValueError('invalid operation type %r' %
                         obj_info.get('content_type', None))
    return {
        'q_policy_index': q_policy_index,
        'account': account,
        'container': container,
        'obj': obj,
        'q_op': q_op,
        'q_ts': decode_timestamps((obj_info['hash']))[0],
        'q_record': last_modified_date_to_timestamp(
            obj_info['last_modified']),
        'path': '/%s/%s/%s' % (account, container, obj)
    }


@LRUCache(maxtime=CONTAINER_POLICY_TTL)
def direct_get_container_policy_index(container_ring, account_name,
                                      container_name):
    """
    Talk directly to the primary container servers to figure out the storage
    policy index for a given container.

    :param container_ring: ring in which to look up the container locations
    :param account_name: name of the container's account
    :param container_name: name of the container
    :returns: storage policy index, or None if it couldn't get a majority
    """
    def _eat_client_exception(*args):
        try:
            return direct_head_container(*args, headers={
                USE_REPLICATION_NETWORK_HEADER: 'true'})
        except ClientException as err:
            if err.http_status == 404:
                return err.http_headers
        except (Timeout, socket.error):
            pass

    pile = GreenPile()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pile.spawn(_eat_client_exception, node, part, account_name,
                   container_name)

    headers = [x for x in pile if x is not None]
    if len(headers) < majority_size(len(nodes)):
        return
    return best_policy_index(headers)


def direct_delete_container_entry(container_ring, account_name, container_name,
                                  object_name, headers=None):
    """
    Talk directly to the primary container servers to delete a particular
    object listing. Does not talk to object servers; use this only when a
    container entry does not actually have a corresponding object.
    """
    if headers is None:
        headers = {}
    headers[USE_REPLICATION_NETWORK_HEADER] = 'true'

    pool = GreenPool()
    part, nodes = container_ring.get_nodes(account_name, container_name)
    for node in nodes:
        pool.spawn_n(direct_delete_container_object, node, part, account_name,
                     container_name, object_name, headers=headers)

    # This either worked or it didn't; if it didn't, we'll retry on the next
    # reconciler loop when we see the queue entry again.
    pool.waitall()


class ContainerReconciler(Daemon):
    """
    Move objects that are in the wrong storage policy.
    """
    log_route = 'container-reconciler'

    def __init__(self, conf, logger=None, swift=None):
        self.conf = conf
        # This option defines how long an un-processable misplaced object
        # marker will be retried before it is abandoned.  It is not coupled
        # with the tombstone reclaim age in the consistency engine.
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.interval = float(conf.get('interval', 30))
        conf_path = conf.get('__file__') or \
            '/etc/swift/container-reconciler.conf'
        self.logger = logger or get_logger(
            conf, log_route=self.log_route)
        request_tries = int(conf.get('request_tries') or 3)
        self.swift = swift or InternalClient(
            conf_path,
            'Swift Container Reconciler',
            request_tries,
            use_replication_network=True,
            global_conf={'log_name': '%s-ic' % conf.get(
                'log_name', self.log_route)})
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.stats = defaultdict(int)
        self.last_stat_time = time.time()
        self.ring_check_interval = float(conf.get('ring_check_interval', 15))
        self.concurrency = int(conf.get('concurrency', 1))
        if self.concurrency < 1:
            raise ValueError("concurrency must be set to at least 1")
        self.processes = int(self.conf.get('processes', 0))
        if self.processes < 0:
            raise ValueError(
                'processes must be an integer greater than or equal to 0')
        self.process = int(self.conf.get('process', 0))
        if self.process < 0:
            raise ValueError(
                'process must be an integer greater than or equal to 0')
        if self.processes and self.process >= self.processes:
            raise ValueError(
                'process must be less than processes')

    def stats_log(self, metric, msg, *args, **kwargs):
        """
        Update stats tracking for metric and emit log message.
        """
        level = kwargs.pop('level', logging.DEBUG)
        log_message = '%s: ' % metric + msg
        self.logger.log(level, log_message, *args, **kwargs)
        self.stats[metric] += 1

    def log_stats(self, force=False):
        """
        Dump stats to logger, noop when stats have been already been
        logged in the last minute.
        """
        now = time.time()
        should_log = force or (now - self.last_stat_time > 60)
        if should_log:
            self.last_stat_time = now
            self.logger.info('Reconciler Stats: %r', dict(**self.stats))

    def pop_queue(self, container, obj, q_ts, q_record):
        """
        Issue a delete object request to the container for the misplaced
        object queue entry.

        :param container: the misplaced objects container
        :param obj: the name of the misplaced object
        :param q_ts: the timestamp of the misplaced object
        :param q_record: the timestamp of the queue entry

        N.B. q_ts will normally be the same time as q_record except when
        an object was manually re-enqued.
        """
        q_path = '/%s/%s/%s' % (MISPLACED_OBJECTS_ACCOUNT, container, obj)
        x_timestamp = slightly_later_timestamp(max(q_record, q_ts), offset=2)
        self.stats_log('pop_queue', 'remove %r (%f) from the queue (%s)',
                       q_path, q_ts, x_timestamp)
        headers = {'X-Timestamp': x_timestamp}
        direct_delete_container_entry(
            self.swift.container_ring, MISPLACED_OBJECTS_ACCOUNT,
            container, obj, headers=headers)

    def can_reconcile_policy(self, policy_index):
        pol = POLICIES.get_by_index(policy_index)
        if pol:
            pol.load_ring(self.swift_dir, reload_time=self.ring_check_interval)
            return pol.object_ring.next_part_power is None
        return False

    def throw_tombstones(self, account, container, obj, timestamp,
                         policy_index, path):
        """
        Issue a delete object request to the given storage_policy.

        :param account: the account name
        :param container: the container name
        :param obj: the object name
        :param timestamp: the timestamp of the object to delete
        :param policy_index: the policy index to direct the request
        :param path: the path to be used for logging
        """
        x_timestamp = slightly_later_timestamp(timestamp)
        self.stats_log('cleanup_attempt', '%r (%f) from policy_index '
                       '%s (%s) will be deleted',
                       path, timestamp, policy_index, x_timestamp)
        headers = {
            'X-Timestamp': x_timestamp,
            'X-Backend-Storage-Policy-Index': policy_index,
        }
        success = False
        try:
            self.swift.delete_object(account, container, obj,
                                     acceptable_statuses=(2, 404),
                                     headers=headers)
        except UnexpectedResponse as err:
            self.stats_log('cleanup_failed', '%r (%f) was not cleaned up '
                           'in storage_policy %s (%s)', path, timestamp,
                           policy_index, err)
        else:
            success = True
            self.stats_log('cleanup_success', '%r (%f) was successfully '
                           'removed from policy_index %s', path, timestamp,
                           policy_index)
        return success

    def _reconcile_object(self, account, container, obj, q_policy_index, q_ts,
                          q_op, path, **kwargs):
        """
        Perform object reconciliation.

        :param account: the account name of the misplaced object
        :param container: the container name of the misplaced object
        :param obj: the object name
        :param q_policy_index: the policy index of the source indicated by the
                               queue entry.
        :param q_ts: the timestamp of the misplaced object
        :param q_op: the operation of the misplaced request
        :param path: the full path of the misplaced object for logging

        :returns: True to indicate the request is fully processed
                  successfully, otherwise False.
        """
        container_policy_index = direct_get_container_policy_index(
            self.swift.container_ring, account, container)
        if container_policy_index is None:
            self.stats_log('unavailable_container', '%r (%f) unable to '
                           'determine the destination policy_index',
                           path, q_ts)
            return False
        if container_policy_index == q_policy_index:
            self.stats_log('noop_object', '%r (%f) container policy_index '
                           '%s matches queue policy index %s', path, q_ts,
                           container_policy_index, q_policy_index)
            return True

        # don't reconcile if the source or container policy_index is in the
        # middle of a PPI
        if not self.can_reconcile_policy(q_policy_index):
            self.stats_log('ppi_skip', 'Source policy (%r) in the middle of '
                           'a part power increase (PPI)', q_policy_index)
            return False
        if not self.can_reconcile_policy(container_policy_index):
            self.stats_log('ppi_skip', 'Container policy (%r) in the middle '
                           'of a part power increase (PPI)',
                           container_policy_index)
            return False

        # check if object exists in the destination already
        self.logger.debug('checking for %r (%f) in destination '
                          'policy_index %s', path, q_ts,
                          container_policy_index)
        headers = {
            'X-Backend-Storage-Policy-Index': container_policy_index}
        try:
            dest_obj = self.swift.get_object_metadata(
                account, container, obj, headers=headers,
                acceptable_statuses=(2, 4))
        except UnexpectedResponse:
            self.stats_log('unavailable_destination', '%r (%f) unable to '
                           'determine the destination timestamp, if any',
                           path, q_ts)
            return False
        dest_ts = Timestamp(dest_obj.get('x-backend-timestamp', 0))
        if dest_ts >= q_ts:
            self.stats_log('found_object', '%r (%f) in policy_index %s '
                           'is newer than queue (%f)', path, dest_ts,
                           container_policy_index, q_ts)
            return self.throw_tombstones(account, container, obj, q_ts,
                                         q_policy_index, path)

        # object is misplaced
        self.stats_log('misplaced_object', '%r (%f) in policy_index %s '
                       'should be in policy_index %s', path, q_ts,
                       q_policy_index, container_policy_index)

        # fetch object from the source location
        self.logger.debug('fetching %r (%f) from storage policy %s', path,
                          q_ts, q_policy_index)
        headers = {
            'X-Backend-Storage-Policy-Index': q_policy_index}
        try:
            source_obj_status, source_obj_info, source_obj_iter = \
                self.swift.get_object(account, container, obj,
                                      headers=headers,
                                      acceptable_statuses=(2, 4))
        except UnexpectedResponse as err:
            source_obj_status = err.resp.status_int
            source_obj_info = {}
            source_obj_iter = None

        source_ts = Timestamp(source_obj_info.get('x-backend-timestamp', 0))
        if source_obj_status == 404 and q_op == 'DELETE':
            return self.ensure_tombstone_in_right_location(
                q_policy_index, account, container, obj, q_ts, path,
                container_policy_index, source_ts)
        else:
            return self.ensure_object_in_right_location(
                q_policy_index, account, container, obj, q_ts, path,
                container_policy_index, source_ts, source_obj_status,
                source_obj_info, source_obj_iter)

    def ensure_object_in_right_location(self, q_policy_index, account,
                                        container, obj, q_ts, path,
                                        container_policy_index, source_ts,
                                        source_obj_status, source_obj_info,
                                        source_obj_iter, **kwargs):
        """
        Validate source object will satisfy the misplaced object queue entry
        and move to destination.

        :param q_policy_index: the policy_index for the source object
        :param account: the account name of the misplaced object
        :param container: the container name of the misplaced object
        :param obj: the name of the misplaced object
        :param q_ts: the timestamp of the misplaced object
        :param path: the full path of the misplaced object for logging
        :param container_policy_index: the policy_index of the destination
        :param source_ts: the timestamp of the source object
        :param source_obj_status: the HTTP status source object request
        :param source_obj_info: the HTTP headers of the source object request
        :param source_obj_iter: the body iter of the source object request
        """
        if source_obj_status // 100 != 2 or source_ts < q_ts:
            if q_ts < time.time() - self.reclaim_age:
                # it's old and there are no tombstones or anything; give up
                self.stats_log('lost_source', '%r (%s) was not available in '
                               'policy_index %s and has expired', path,
                               q_ts.internal, q_policy_index,
                               level=logging.CRITICAL)
                return True
            # the source object is unavailable or older than the queue
            # entry; a version that will satisfy the queue entry hopefully
            # exists somewhere in the cluster, so wait and try again
            self.stats_log('unavailable_source', '%r (%s) in '
                           'policy_index %s responded %s (%s)', path,
                           q_ts.internal, q_policy_index, source_obj_status,
                           source_ts.internal, level=logging.WARNING)
            return False

        # optimistically move any source with a timestamp >= q_ts
        ts = max(Timestamp(source_ts), q_ts)
        # move the object
        put_timestamp = slightly_later_timestamp(ts, offset=3)
        self.stats_log('copy_attempt', '%r (%f) in policy_index %s will be '
                       'moved to policy_index %s (%s)', path, source_ts,
                       q_policy_index, container_policy_index, put_timestamp)
        headers = source_obj_info.copy()
        headers['X-Backend-Storage-Policy-Index'] = container_policy_index
        headers['X-Timestamp'] = put_timestamp

        try:
            self.swift.upload_object(
                FileLikeIter(source_obj_iter), account, container, obj,
                headers=headers)
        except UnexpectedResponse as err:
            self.stats_log('copy_failed', 'upload %r (%f) from '
                           'policy_index %s to policy_index %s '
                           'returned %s', path, source_ts, q_policy_index,
                           container_policy_index, err, level=logging.WARNING)
            return False
        except:  # noqa
            self.stats_log('unhandled_error', 'unable to upload %r (%f) '
                           'from policy_index %s to policy_index %s ', path,
                           source_ts, q_policy_index, container_policy_index,
                           level=logging.ERROR, exc_info=True)
            return False

        self.stats_log('copy_success', '%r (%f) moved from policy_index %s '
                       'to policy_index %s (%s)', path, source_ts,
                       q_policy_index, container_policy_index, put_timestamp)

        return self.throw_tombstones(account, container, obj, q_ts,
                                     q_policy_index, path)

    def ensure_tombstone_in_right_location(self, q_policy_index, account,
                                           container, obj, q_ts, path,
                                           container_policy_index, source_ts,
                                           **kwargs):
        """
        Issue a DELETE request against the destination to match the
        misplaced DELETE against the source.
        """
        delete_timestamp = slightly_later_timestamp(q_ts, offset=3)
        self.stats_log('delete_attempt', '%r (%f) in policy_index %s '
                       'will be deleted from policy_index %s (%s)', path,
                       source_ts, q_policy_index, container_policy_index,
                       delete_timestamp)
        headers = {
            'X-Backend-Storage-Policy-Index': container_policy_index,
            'X-Timestamp': delete_timestamp,
        }
        try:
            self.swift.delete_object(account, container, obj,
                                     headers=headers)
        except UnexpectedResponse as err:
            self.stats_log('delete_failed', 'delete %r (%f) from '
                           'policy_index %s (%s) returned %s', path,
                           source_ts, container_policy_index,
                           delete_timestamp, err, level=logging.WARNING)
            return False
        except:  # noqa
            self.stats_log('unhandled_error', 'unable to delete %r (%f) '
                           'from policy_index %s (%s)', path, source_ts,
                           container_policy_index, delete_timestamp,
                           level=logging.ERROR, exc_info=True)
            return False

        self.stats_log('delete_success', '%r (%f) deleted from '
                       'policy_index %s (%s)', path, source_ts,
                       container_policy_index, delete_timestamp,
                       level=logging.INFO)

        return self.throw_tombstones(account, container, obj, q_ts,
                                     q_policy_index, path)

    def reconcile_object(self, info):
        """
        Process a possibly misplaced object write request.  Determine correct
        destination storage policy by checking with primary containers.  Check
        source and destination, copying or deleting into destination and
        cleaning up the source as needed.

        This method wraps _reconcile_object for exception handling.

        :param info: a queue entry dict

        :returns: True to indicate the request is fully processed
                  successfully, otherwise False.
        """
        self.logger.debug('checking placement for %r (%f) '
                          'in policy_index %s', info['path'],
                          info['q_ts'], info['q_policy_index'])
        success = False
        try:
            success = self._reconcile_object(**info)
        except:  # noqa
            self.logger.exception('Unhandled Exception trying to '
                                  'reconcile %r (%f) in policy_index %s',
                                  info['path'], info['q_ts'],
                                  info['q_policy_index'])
        if success:
            metric = 'success'
            msg = 'was handled successfully'
        else:
            metric = 'retry'
            msg = 'must be retried'
        msg = '%(path)r (%(q_ts)f) in policy_index %(q_policy_index)s ' + msg
        self.stats_log(metric, msg, info, level=logging.INFO)
        self.log_stats()
        return success

    def _iter_containers(self):
        """
        Generate a list of containers to process.
        """
        # hit most recent container first instead of waiting on the updaters
        current_container = get_reconciler_container_name(time.time())
        yield current_container
        self.logger.debug('looking for containers in %s',
                          MISPLACED_OBJECTS_ACCOUNT)
        container_gen = self.swift.iter_containers(MISPLACED_OBJECTS_ACCOUNT)
        while True:
            one_page = None
            try:
                one_page = list(itertools.islice(
                    container_gen, constraints.CONTAINER_LISTING_LIMIT))
            except UnexpectedResponse as err:
                self.logger.error('Error listing containers in '
                                  'account %s (%s)',
                                  MISPLACED_OBJECTS_ACCOUNT, err)

            if not one_page:
                # don't generally expect more than one page
                break
            # reversed order since we expect older containers to be empty
            for c in reversed(one_page):
                container = c['name']
                if container == current_container:
                    continue  # we've already hit this one this pass
                yield container

    def _iter_objects(self, container):
        """
        Generate a list of objects to process.

        :param container: the name of the container to process

        If the given container is empty and older than reclaim_age this
        processor will attempt to reap it.
        """
        self.logger.debug('looking for objects in %s', container)
        found_obj = False
        try:
            for raw_obj in self.swift.iter_objects(
                    MISPLACED_OBJECTS_ACCOUNT, container):
                found_obj = True
                yield raw_obj
        except UnexpectedResponse as err:
            self.logger.error('Error listing objects in container %s (%s)',
                              container, err)
        if float(container) < time.time() - self.reclaim_age and \
                not found_obj:
            # Try to delete old empty containers so the queue doesn't
            # grow without bound. It's ok if there's a conflict.
            self.swift.delete_container(
                MISPLACED_OBJECTS_ACCOUNT, container,
                acceptable_statuses=(2, 404, 409, 412))

    def should_process(self, queue_item):
        """
        Check if a given entry should be handled by this process.

        :param container: the queue container
        :param queue_item: an entry from the queue
        """
        if not self.processes:
            return True
        hexdigest = hash_path(
            queue_item['account'], queue_item['container'], queue_item['obj'])
        return int(hexdigest, 16) % self.processes == self.process

    def process_queue_item(self, q_container, q_entry, queue_item):
        """
        Process an entry and remove from queue on success.

        :param q_container: the queue container
        :param q_entry: the raw_obj name from the q_container
        :param queue_item: a parsed entry from the queue
        """
        finished = self.reconcile_object(queue_item)
        if finished:
            self.pop_queue(q_container, q_entry,
                           queue_item['q_ts'],
                           queue_item['q_record'])

    def reconcile(self):
        """
        Main entry point for concurrent processing of misplaced objects.

        Iterate over all queue entries and delegate processing to spawned
        workers in the pool.
        """
        self.logger.debug('pulling items from the queue')
        pool = GreenPool(self.concurrency)
        for container in self._iter_containers():
            self.logger.debug('checking container %s', container)
            for raw_obj in self._iter_objects(container):
                try:
                    queue_item = parse_raw_obj(raw_obj)
                except Exception:
                    self.stats_log('invalid_record',
                                   'invalid queue record: %r', raw_obj,
                                   level=logging.ERROR, exc_info=True)
                    continue
                if self.should_process(queue_item):
                    pool.spawn_n(self.process_queue_item,
                                 container, raw_obj['name'], queue_item)
            self.log_stats()
        pool.waitall()

    def run_once(self, *args, **kwargs):
        """
        Process every entry in the queue.
        """
        try:
            self.reconcile()
        except:  # noqa
            self.logger.exception('Unhandled Exception trying to reconcile')
        self.log_stats(force=True)

    def run_forever(self, *args, **kwargs):
        while True:
            self.run_once(*args, **kwargs)
            self.stats = defaultdict(int)
            self.logger.info('sleeping between intervals (%ss)', self.interval)
            time.sleep(self.interval)


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerReconciler, conf_file, **options)


if __name__ == '__main__':
    main()
