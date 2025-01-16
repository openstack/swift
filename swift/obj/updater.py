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
import queue

import pickle  # nosec: B403
import errno
import os
import signal
import sys
import time
import uuid
from random import random, shuffle
from bisect import insort
from collections import deque

from eventlet import spawn, Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_drive
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, renamer, write_pickle, \
    dump_recon_cache, config_true_value, RateLimitedIterator, split_path, \
    eventlet_monkey_patch, get_redirect_data, ContextPool, hash_path, \
    non_negative_float, config_positive_int_value, non_negative_int, \
    EventletRateLimiter, node_to_string, parse_options, load_recon_cache
from swift.common.daemon import Daemon, run_daemon
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.storage_policy import split_policy_string, PolicyError
from swift.common.recon import RECON_OBJECT_FILE, DEFAULT_RECON_CACHE_PATH
from swift.obj.diskfile import get_tmp_dir, ASYNCDIR_BASE
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR, \
    HTTP_MOVED_PERMANENTLY


class RateLimiterBucket(EventletRateLimiter):
    """
    Extends EventletRateLimiter to also maintain a deque of items that have
    been deferred due to rate-limiting, and to provide a comparator for sorting
    instanced by readiness.
    """
    def __init__(self, max_updates_per_second):
        super(RateLimiterBucket, self).__init__(max_updates_per_second,
                                                rate_buffer=0)
        self.deque = deque()

    def __len__(self):
        return len(self.deque)

    def __bool__(self):
        return bool(self.deque)

    def __lt__(self, other):
        # used to sort RateLimiterBuckets by readiness
        if isinstance(other, RateLimiterBucket):
            return self.running_time < other.running_time
        return self.running_time < other


class BucketizedUpdateSkippingLimiter(object):
    """
    Wrap an iterator to rate-limit updates on a per-bucket basis, where updates
    are mapped to buckets by hashing their destination path. If an update is
    rate-limited then it is placed on a deferral queue and may be sent later if
    the wrapped iterator is exhausted before the ``drain_until`` time is
    reached.

    The deferral queue has constrained size and once the queue is full updates
    are evicted using a first-in-first-out policy. This policy is used because
    updates on the queue may have been made obsolete by newer updates written
    to disk, and this is more likely for updates that have been on the queue
    longest.

    The iterator increments stats as follows:

    * The `deferrals` stat is incremented for each update that is
      rate-limited. Note that a individual update is rate-limited at most
      once.
    * The `skips` stat is incremented for each rate-limited update that is
      not eventually yielded. This includes updates that are evicted from the
      deferral queue and all updates that remain in the deferral queue when
      ``drain_until`` time is reached and the iterator terminates.
    * The `drains` stat is incremented for each rate-limited update that is
      eventually yielded.

    Consequently, when this iterator terminates, the sum of `skips` and
    `drains` is equal to the number of `deferrals`.

    :param update_iterable: an async_pending update iterable
    :param logger: a logger instance
    :param stats: a SweepStats instance
    :param num_buckets: number of buckets to divide container hashes into, the
                        more buckets total the less containers to a bucket
                        (once a busy container slows down a bucket the whole
                        bucket starts deferring)
    :param max_elements_per_group_per_second: tunable, when deferring kicks in
    :param max_deferred_elements: maximum number of deferred elements before
        skipping starts. Each bucket may defer updates, but once the total
        number of deferred updates summed across all buckets reaches this
        value then all buckets will skip subsequent updates.
    :param drain_until: time at which any remaining deferred elements must be
        skipped and the iterator stops. Once the wrapped iterator has been
        exhausted, this iterator will drain deferred elements from its buckets
        until either all buckets have drained or this time is reached.
    """

    def __init__(self, update_iterable, logger, stats, num_buckets=1000,
                 max_elements_per_group_per_second=50,
                 max_deferred_elements=0,
                 drain_until=0):
        self.iterator = iter(update_iterable)
        self.logger = logger
        self.stats = stats
        # if we want a smaller "blast radius" we could make this number bigger
        self.num_buckets = max(num_buckets, 1)
        self.max_deferred_elements = max_deferred_elements
        self.deferred_buckets = deque()
        self.drain_until = drain_until
        self.salt = str(uuid.uuid4())
        self.buckets = [RateLimiterBucket(max_elements_per_group_per_second)
                        for _ in range(self.num_buckets)]
        self.buckets_ordered_by_readiness = None

    def __iter__(self):
        return self

    def _bucket_key(self, update):
        acct, cont = split_update_path(update)
        return int(hash_path(acct, cont, self.salt), 16) % self.num_buckets

    def _get_time(self):
        return time.time()

    def __next__(self):
        # first iterate over the wrapped iterator...
        for update_ctx in self.iterator:
            bucket = self.buckets[self._bucket_key(update_ctx['update'])]
            now = self._get_time()
            if bucket.is_allowed(now=now):
                # no need to ratelimit, just return next update
                return update_ctx

            self.stats.deferrals += 1
            self.logger.increment("deferrals")
            if self.max_deferred_elements > 0:
                if len(self.deferred_buckets) >= self.max_deferred_elements:
                    # create space to defer this update by popping the least
                    # recent deferral from the least recently deferred bucket;
                    # updates read from disk recently are preferred over those
                    # read from disk less recently.
                    oldest_deferred_bucket = self.deferred_buckets.popleft()
                    oldest_deferred_bucket.deque.popleft()
                    self.stats.skips += 1
                    self.logger.increment("skips")
                # append the update to the bucket's queue and append the bucket
                # to the queue of deferred buckets
                # note: buckets may have multiple entries in deferred_buckets,
                # one for each deferred update in that particular bucket
                bucket.deque.append(update_ctx)
                self.deferred_buckets.append(bucket)
            else:
                self.stats.skips += 1
                self.logger.increment("skips")

        if self.buckets_ordered_by_readiness is None:
            # initialise a queue of those buckets with deferred elements;
            # buckets are queued in the chronological order in which they are
            # ready to serve an element
            self.buckets_ordered_by_readiness = queue.PriorityQueue()
            for bucket in self.buckets:
                if bucket:
                    self.buckets_ordered_by_readiness.put(bucket)

        # now drain the buckets...
        undrained_elements = []
        while not self.buckets_ordered_by_readiness.empty():
            now = self._get_time()
            bucket = self.buckets_ordered_by_readiness.get_nowait()
            if now < self.drain_until:
                # wait for next element to be ready
                bucket.wait(now=now)
                # drain the most recently deferred element
                item = bucket.deque.pop()
                if bucket:
                    # bucket has more deferred elements, re-insert in queue in
                    # correct chronological position
                    self.buckets_ordered_by_readiness.put(bucket)
                self.stats.drains += 1
                self.logger.increment("drains")
                return item
            else:
                # time to stop iterating: gather all un-drained elements
                undrained_elements.extend(bucket.deque)

        if undrained_elements:
            # report final batch of skipped elements
            self.stats.skips += len(undrained_elements)
            self.logger.update_stats("skips", len(undrained_elements))

        raise StopIteration()


class OldestAsyncPendingTracker:
    """
    Manages the tracking of the oldest async pending updates for each
    account-container pair using a sorted list for timestamps. Evicts the
    newest pairs when t max_entries is reached. Supports retrieving the N
    oldest async pending updates or calculating the age of the oldest pending
    update.
    """
    def __init__(
        self,
        max_entries,
    ):
        self.max_entries = max_entries
        self.sorted_entries = []
        self.ac_to_timestamp = {}

    def add_update(self, account, container, timestamp):
        """
        Add or update a timestamp for a given account and container.

        :param account: (str) The account name.
        :param container: (str) The container name.
        :param timestamp: (float) The timestamp to add or update.
        """
        # Ensure the timestamp is a float
        timestamp = float(timestamp)

        ac = (account, container)

        if ac in self.ac_to_timestamp:
            old_timestamp = self.ac_to_timestamp[ac]
            # Only replace the existing timestamp if the new one is older
            if timestamp < old_timestamp:
                # Remove the old (timestamp, ac) from the
                # sorted list
                self.sorted_entries.remove((old_timestamp, ac))
                # Insert the new (timestamp, ac) in the sorted order
                insort(self.sorted_entries, (timestamp, ac))
                # Update the ac_to_timestamp dictionary
                self.ac_to_timestamp[ac] = timestamp
        else:
            # Insert the new (timestamp, ac) in the sorted order
            insort(self.sorted_entries, (timestamp, ac))
            self.ac_to_timestamp[ac] = timestamp

        # Check size and evict the newest ac(s) if necessary
        if (len(self.ac_to_timestamp) > self.max_entries):
            # Pop the newest entry (largest timestamp)
            _, newest_ac = (self.sorted_entries.pop())
            del self.ac_to_timestamp[newest_ac]

    def get_n_oldest_timestamp_acs(self, n):
        oldest_entries = self.sorted_entries[:n]
        return {
            'oldest_count': len(oldest_entries),
            'oldest_entries': [
                {
                    'timestamp': entry[0],
                    'account': entry[1][0],
                    'container': entry[1][1],
                }
                for entry in oldest_entries
            ],
        }

    def get_oldest_timestamp(self):
        if self.sorted_entries:
            return self.sorted_entries[0][0]
        return None

    def get_oldest_timestamp_age(self):
        current_time = time.time()
        oldest_timestamp = self.get_oldest_timestamp()
        if oldest_timestamp is not None:
            return current_time - oldest_timestamp
        return None

    def reset(self):
        self.sorted_entries = []
        self.ac_to_timestamp = {}

    def get_memory_usage(self):
        return self._get_size(self)

    def _get_size(self, obj, seen=None):
        if seen is None:
            seen = set()

        obj_id = id(obj)
        if obj_id in seen:
            return 0
        seen.add(obj_id)

        size = sys.getsizeof(obj)

        if isinstance(obj, dict):
            size += sum(
                self._get_size(k, seen) + self._get_size(v, seen)
                for k, v in obj.items()
            )
        elif hasattr(obj, '__dict__'):
            size += self._get_size(obj.__dict__, seen)
        elif (
            hasattr(obj, '__iter__')
            and not isinstance(obj, (str, bytes, bytearray))
        ):
            size += sum(self._get_size(i, seen) for i in obj)

        return size


class SweepStats(object):
    """
    Stats bucket for an update sweep

    A measure of the rate at which updates are being rate-limited is::

        deferrals / (deferrals + successes + failures - drains)

    A measure of the rate at which updates are not being sent during a sweep
    is::

        skips / (skips + successes + failures)
    """
    def __init__(self, errors=0, failures=0, quarantines=0, successes=0,
                 unlinks=0, outdated_unlinks=0, redirects=0, skips=0,
                 deferrals=0, drains=0):
        self.errors = errors
        self.failures = failures
        self.quarantines = quarantines
        self.successes = successes
        self.unlinks = unlinks
        self.outdated_unlinks = outdated_unlinks
        self.redirects = redirects
        self.skips = skips
        self.deferrals = deferrals
        self.drains = drains

    def copy(self):
        return type(self)(self.errors, self.failures, self.quarantines,
                          self.successes, self.unlinks, self.outdated_unlinks,
                          self.redirects, self.skips, self.deferrals,
                          self.drains)

    def since(self, other):
        return type(self)(self.errors - other.errors,
                          self.failures - other.failures,
                          self.quarantines - other.quarantines,
                          self.successes - other.successes,
                          self.unlinks - other.unlinks,
                          self.outdated_unlinks - other.outdated_unlinks,
                          self.redirects - other.redirects,
                          self.skips - other.skips,
                          self.deferrals - other.deferrals,
                          self.drains - other.drains)

    def reset(self):
        self.errors = 0
        self.failures = 0
        self.quarantines = 0
        self.successes = 0
        self.unlinks = 0
        self.outdated_unlinks = 0
        self.redirects = 0
        self.skips = 0
        self.deferrals = 0
        self.drains = 0

    def __str__(self):
        keys = (
            (self.successes, 'successes'),
            (self.failures, 'failures'),
            (self.quarantines, 'quarantines'),
            (self.unlinks, 'unlinks'),
            (self.outdated_unlinks, 'outdated_unlinks'),
            (self.errors, 'errors'),
            (self.redirects, 'redirects'),
            (self.skips, 'skips'),
            (self.deferrals, 'deferrals'),
            (self.drains, 'drains'),
        )
        return ', '.join('%d %s' % pair for pair in keys)


def split_update_path(update):
    """
    Split the account and container parts out of the async update data.

    N.B. updates to shards set the container_path key while the account and
    container keys are always the root.
    """
    container_path = update.get('container_path')
    if container_path:
        acct, cont = split_path('/' + container_path, minsegs=2)
    else:
        acct, cont = update['account'], update['container']
    return acct, cont


class ObjectUpdater(Daemon):
    """Update object information in container listings."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = float(conf.get('interval', 300))
        self.container_ring = None
        self.concurrency = int(conf.get('concurrency', 8))
        self.updater_workers = config_positive_int_value(
            conf.get('updater_workers', 1))
        if 'slowdown' in conf:
            self.logger.warning(
                'The slowdown option is deprecated in favor of '
                'objects_per_second. This option may be ignored in a '
                'future release.')
            objects_per_second = 1 / (
                float(conf.get('slowdown', '0.01')) + 0.01)
        else:
            objects_per_second = 50
        self.objects_running_time = 0
        self.max_objects_per_second = \
            float(conf.get('objects_per_second',
                           objects_per_second))
        self.max_objects_per_container_per_second = non_negative_float(
            conf.get('max_objects_per_container_per_second', 0))
        self.per_container_ratelimit_buckets = config_positive_int_value(
            conf.get('per_container_ratelimit_buckets', 1000))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.report_interval = float(conf.get('report_interval', 300))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.rcache = os.path.join(self.recon_cache_path, RECON_OBJECT_FILE)
        max_entries = config_positive_int_value(
            conf.get('async_tracker_max_entries', 100)
        )
        self.dump_count = config_positive_int_value(
            conf.get('async_tracker_dump_count', 5)
        )
        self.stats = SweepStats()
        self.oldest_async_pendings = OldestAsyncPendingTracker(max_entries)
        self.max_deferred_updates = non_negative_int(
            conf.get('max_deferred_updates', 10000))
        self.begin = time.time()

    def _listdir(self, path):
        try:
            return os.listdir(path)
        except OSError as e:
            self.stats.errors += 1
            self.logger.increment('errors')
            self.logger.error('ERROR: Unable to access %(path)s: '
                              '%(error)s',
                              {'path': path, 'error': e})
            return []

    def get_container_ring(self):
        """Get the container ring.  Load it, if it hasn't been yet."""
        if not self.container_ring:
            self.container_ring = Ring(self.swift_dir, ring_name='container')
        return self.container_ring

    def _process_device_in_child(self, dev_path, device):
        """Process a single device in a forked child process."""
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.environ.pop('NOTIFY_SOCKET', None)
        eventlet_monkey_patch()
        self.stats.reset()
        self.oldest_async_pendings.reset()
        forkbegin = time.time()
        self.object_sweep(dev_path)
        elapsed = time.time() - forkbegin
        self.logger.info(
            ('Object update sweep of %(device)s '
                'completed: %(elapsed).02fs, %(stats)s'),
            {'device': device, 'elapsed': elapsed,
                'stats': self.stats})
        self.dump_device_recon(device)

    def _process_devices(self, devices):
        """Process devices, handling both single and multi-threaded modes."""
        pids = []
        # read from container ring to ensure it's fresh
        self.get_container_ring().get_nodes('')
        for device in devices:
            try:
                dev_path = check_drive(self.devices, device,
                                       self.mount_check)
            except ValueError as err:
                # We don't count this as an error. The occasional
                # unmounted drive is part of normal cluster operations,
                # so a simple warning is sufficient.
                self.logger.warning('Skipping: %s', err)
                continue
            while len(pids) >= self.updater_workers:
                pids.remove(os.wait()[0])
            pid = os.fork()
            if pid:
                pids.append(pid)
            else:
                self._process_device_in_child(dev_path, device)
                sys.exit()

        while pids:
            pids.remove(os.wait()[0])

    def run_forever(self, *args, **kwargs):
        """Run the updater continuously."""
        time.sleep(random() * self.interval)
        while True:
            elapsed = self.run_once(*args, **kwargs)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the updater once."""
        self.logger.info('Begin object update sweep of all devices')
        self.begin = time.time()
        devices = self._listdir(self.devices)
        self._process_devices(devices)
        now = time.time()
        elapsed = now - self.begin
        self.logger.info(
            ('Object update sweep of all devices completed: '
             '%(elapsed).02fs'),
            {'elapsed': elapsed})
        self.aggregate_and_dump_recon(devices, elapsed, now)
        return elapsed

    def _gather_recon_stats(self):
        """Gather stats for device recon dumps."""
        stats = {
            'failures_oldest_timestamp':
                self.oldest_async_pendings.get_oldest_timestamp(),
            'failures_oldest_timestamp_age':
                self.oldest_async_pendings.get_oldest_timestamp_age(),
            'failures_account_container_count':
                len(self.oldest_async_pendings.ac_to_timestamp),
            'failures_oldest_timestamp_account_containers':
                self.oldest_async_pendings.get_n_oldest_timestamp_acs(
                    self.dump_count),
            'tracker_memory_usage':
                self.oldest_async_pendings.get_memory_usage(),
        }
        return stats

    def dump_device_recon(self, device):
        """Dump recon stats for a single device."""
        disk_recon_stats = self._gather_recon_stats()
        dump_recon_cache(
            {'object_updater_per_device': {device: disk_recon_stats}},
            self.rcache,
            self.logger,
        )

    def aggregate_and_dump_recon(self, devices, elapsed, now):
        """
        Aggregate recon stats across devices and dump the result to the
        recon cache.
        """
        recon_cache = load_recon_cache(self.rcache)
        device_stats = recon_cache.get('object_updater_per_device', {})
        if not isinstance(device_stats, dict):
            raise TypeError('object_updater_per_device must be a dict')
        devices_to_remove = set(device_stats) - set(devices)
        device_stats = {dev: device_stats.get(dev) or {}
                        for dev in devices}

        aggregated_oldest_entries = []

        for stats in device_stats.values():
            container_data = stats.get(
                'failures_oldest_timestamp_account_containers', {})
            aggregated_oldest_entries.extend(container_data.get(
                'oldest_entries', []))
        aggregated_oldest_entries.sort(key=lambda x: x['timestamp'])
        aggregated_oldest_entries = aggregated_oldest_entries[:self.dump_count]
        aggregated_oldest_count = len(aggregated_oldest_entries)

        aggregated_stats = {
            'failures_account_container_count': max(
                list(
                    stats.get('failures_account_container_count', 0)
                    for stats in device_stats.values()
                )
                or [0],
            ),
            'tracker_memory_usage': (
                float(sum(
                    stats.get('tracker_memory_usage', 0)
                    for stats in device_stats.values()
                ))
                / float(len(device_stats))
            )
            * max(1, min(self.updater_workers, len(device_stats)))
            if device_stats
            else 0,
            'failures_oldest_timestamp': min(
                list(filter(lambda x: x is not None,
                            [stats.get('failures_oldest_timestamp')
                             for stats in device_stats.values()]
                            )) or [None],
            ),
            'failures_oldest_timestamp_age': max(
                list(filter(lambda x: x is not None,
                            [stats.get('failures_oldest_timestamp_age')
                             for stats in device_stats.values()]
                            )) or [None],
            ),
            'failures_oldest_timestamp_account_containers': {
                'oldest_count': aggregated_oldest_count,
                'oldest_entries': aggregated_oldest_entries,
            },
        }

        recon_dump = {
            'object_updater_sweep': elapsed,
            'object_updater_stats': aggregated_stats,
            'object_updater_last': now
        }
        if devices_to_remove:
            recon_dump['object_updater_per_device'] = {
                d: {} for d in devices_to_remove}
        dump_recon_cache(
            recon_dump,
            self.rcache,
            self.logger,
        )

    def _load_update(self, device, update_path):
        try:
            return pickle.load(open(update_path, 'rb'))  # nosec: B301
        except Exception as e:
            if getattr(e, 'errno', None) == errno.ENOENT:
                return
            self.logger.exception(
                'ERROR Pickle problem, quarantining %s', update_path)
            self.stats.quarantines += 1
            self.logger.increment('quarantines')
            target_path = os.path.join(device, 'quarantined', 'objects',
                                       os.path.basename(update_path))
            renamer(update_path, target_path, fsync=False)
            try:
                # If this was the last async_pending in the directory,
                # then this will succeed. Otherwise, it'll fail, and
                # that's okay.
                os.rmdir(os.path.dirname(update_path))
            except OSError:
                pass
            return

    def _iter_async_pendings(self, device):
        """
        Locate and yield an update context for all the async pending files on
        the device. Each update context contains details of the async pending
        file location, its timestamp and the un-pickled update data.

        Async pending files that fail to load will be quarantined.

        Only the most recent update for the same object is yielded; older
        (stale) async pending files are unlinked as they are located.

        The iterator tries to clean up empty directories as it goes.
        """
        # loop through async pending dirs for all policies
        for asyncdir in self._listdir(device):
            # we only care about directories
            async_pending = os.path.join(device, asyncdir)
            if not asyncdir.startswith(ASYNCDIR_BASE):
                # skip stuff like "accounts", "containers", etc.
                continue
            if not os.path.isdir(async_pending):
                continue
            try:
                base, policy = split_policy_string(asyncdir)
            except PolicyError as e:
                # This isn't an error, but a misconfiguration. Logging a
                # warning should be sufficient.
                self.logger.warning('Directory %(directory)r does not map '
                                    'to a valid policy (%(error)s)', {
                                        'directory': asyncdir, 'error': e})
                continue
            prefix_dirs = self._listdir(async_pending)
            shuffle(prefix_dirs)
            for prefix in prefix_dirs:
                prefix_path = os.path.join(async_pending, prefix)
                if not os.path.isdir(prefix_path):
                    continue
                last_obj_hash = None
                for update_file in sorted(self._listdir(prefix_path),
                                          reverse=True):
                    update_path = os.path.join(prefix_path, update_file)
                    if not os.path.isfile(update_path):
                        continue
                    try:
                        obj_hash, timestamp = update_file.split('-')
                    except ValueError:
                        self.stats.errors += 1
                        self.logger.increment('errors')
                        self.logger.error(
                            'ERROR async pending file with unexpected '
                            'name %s', update_path)
                        continue
                    # Async pendings are stored on disk like this:
                    #
                    # <device>/async_pending/<suffix>/<obj_hash>-<timestamp>
                    #
                    # If there are multiple updates for a given object,
                    # they'll look like this:
                    #
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp1>
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp2>
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp3>
                    #
                    # Async updates also have the property that newer
                    # updates contain all the information in older updates.
                    # Since we sorted the directory listing in reverse
                    # order, we'll see timestamp3 first, yield it, and then
                    # unlink timestamp2 and timestamp1 since we know they
                    # are obsolete.
                    #
                    # This way, our caller only gets useful async_pendings.
                    if obj_hash == last_obj_hash:
                        self.stats.outdated_unlinks += 1
                        self.logger.increment('outdated_unlinks')
                        try:
                            os.unlink(update_path)
                        except OSError as e:
                            if e.errno != errno.ENOENT:
                                raise
                    else:
                        last_obj_hash = obj_hash
                        update = self._load_update(device, update_path)
                        if update is not None:
                            yield {'device': device,
                                   'policy': policy,
                                   'update_path': update_path,
                                   'obj_hash': obj_hash,
                                   'timestamp': timestamp,
                                   'update': update}

    def object_sweep(self, device):
        """
        If there are async pendings on the device, walk each one and update.

        :param device: path to device
        """
        start_time = time.time()
        last_status_update = start_time
        start_stats = self.stats.copy()
        my_pid = os.getpid()
        self.logger.info("Object update sweep starting on %s (pid: %d)",
                         device, my_pid)

        ap_iter = RateLimitedIterator(
            self._iter_async_pendings(device),
            elements_per_second=self.max_objects_per_second)
        ap_iter = BucketizedUpdateSkippingLimiter(
            ap_iter, self.logger, self.stats,
            self.per_container_ratelimit_buckets,
            self.max_objects_per_container_per_second,
            max_deferred_elements=self.max_deferred_updates,
            drain_until=self.begin + self.interval)
        with ContextPool(self.concurrency) as pool:
            for update_ctx in ap_iter:
                pool.spawn(self.process_object_update, **update_ctx)
                now = time.time()
                if now - last_status_update >= self.report_interval:
                    this_sweep = self.stats.since(start_stats)
                    self.logger.info(
                        ('Object update sweep progress on %(device)s: '
                         '%(elapsed).02fs, %(stats)s (pid: %(pid)d)'),
                        {'device': device,
                         'elapsed': now - start_time,
                         'pid': my_pid,
                         'stats': this_sweep})
                    last_status_update = now
            pool.waitall()

        self.logger.timing_since('timing', start_time)
        sweep_totals = self.stats.since(start_stats)
        self.logger.info(
            ('Object update sweep completed on %(device)s '
             'in %(elapsed).02fs seconds:, '
             '%(successes)d successes, %(failures)d failures, '
             '%(quarantines)d quarantines, '
             '%(unlinks)d unlinks, '
             '%(outdated_unlinks)d outdated_unlinks, '
             '%(errors)d errors, '
             '%(redirects)d redirects, '
             '%(skips)d skips, '
             '%(deferrals)d deferrals, '
             '%(drains)d drains '
             '(pid: %(pid)d)'),
            {'device': device,
             'elapsed': time.time() - start_time,
             'pid': my_pid,
             'successes': sweep_totals.successes,
             'failures': sweep_totals.failures,
             'quarantines': sweep_totals.quarantines,
             'unlinks': sweep_totals.unlinks,
             'outdated_unlinks': sweep_totals.outdated_unlinks,
             'errors': sweep_totals.errors,
             'redirects': sweep_totals.redirects,
             'skips': sweep_totals.skips,
             'deferrals': sweep_totals.deferrals,
             'drains': sweep_totals.drains
             })

    def process_object_update(self, update_path, device, policy, update,
                              **kwargs):
        """
        Process the object information to be updated and update.

        :param update_path: path to pickled object update file
        :param device: path to device
        :param policy: storage policy of object update
        :param update: the un-pickled update data
        :param kwargs: un-used keys from update_ctx
        """

        def do_update():
            successes = update.get('successes', [])
            headers_out = HeaderKeyDict(update['headers'].copy())
            headers_out['user-agent'] = 'object-updater %s' % os.getpid()
            headers_out.setdefault('X-Backend-Storage-Policy-Index',
                                   str(int(policy)))
            headers_out.setdefault('X-Backend-Accept-Redirect', 'true')
            headers_out.setdefault('X-Backend-Accept-Quoted-Location', 'true')
            acct, cont = split_update_path(update)
            part, nodes = self.get_container_ring().get_nodes(acct, cont)
            path = '/%s/%s/%s' % (acct, cont, update['obj'])
            events = [spawn(self.object_update,
                            node, part, update['op'], path, headers_out)
                      for node in nodes if node['id'] not in successes]
            success = True
            new_successes = rewrite_pickle = False
            redirect = None
            redirects = set()
            for event in events:
                event_success, node_id, redirect = event.wait()
                if event_success is True:
                    successes.append(node_id)
                    new_successes = True
                else:
                    success = False
                if redirect:
                    redirects.add(redirect)

            if success:
                self.stats.successes += 1
                self.logger.increment('successes')
                self.logger.debug('Update sent for %(path)s %(update_path)s',
                                  {'path': path, 'update_path': update_path})
                self.stats.unlinks += 1
                self.logger.increment('unlinks')
                os.unlink(update_path)
                try:
                    # If this was the last async_pending in the directory,
                    # then this will succeed. Otherwise, it'll fail, and
                    # that's okay.
                    os.rmdir(os.path.dirname(update_path))
                except OSError:
                    pass
            elif redirects:
                # erase any previous successes
                update.pop('successes', None)
                redirect = max(redirects, key=lambda x: x[-1])[0]
                redirect_history = update.setdefault('redirect_history', [])
                if redirect in redirect_history:
                    # force next update to be sent to root, reset history
                    update['container_path'] = None
                    update['redirect_history'] = []
                else:
                    update['container_path'] = redirect
                    redirect_history.append(redirect)
                self.stats.redirects += 1
                self.logger.increment("redirects")
                self.logger.debug(
                    'Update redirected for %(path)s %(update_path)s to '
                    '%(shard)s',
                    {'path': path, 'update_path': update_path,
                     'shard': update['container_path']})
                rewrite_pickle = True
            else:
                self.stats.failures += 1
                self.logger.increment('failures')
                self.logger.debug('Update failed for %(path)s %(update_path)s',
                                  {'path': path, 'update_path': update_path})
                self.oldest_async_pendings.add_update(
                    acct, cont, kwargs['timestamp']
                )
                if new_successes:
                    update['successes'] = successes
                    rewrite_pickle = True

            return rewrite_pickle, redirect

        rewrite_pickle, redirect = do_update()
        if redirect:
            # make one immediate retry to the redirect location
            rewrite_pickle, redirect = do_update()
        if rewrite_pickle:
            write_pickle(update, update_path, os.path.join(
                device, get_tmp_dir(policy)))

    def object_update(self, node, part, op, path, headers_out):
        """
        Perform the object update to the container

        :param node: node dictionary from the container ring
        :param part: partition that holds the container
        :param op: operation performed (ex: 'PUT' or 'DELETE')
        :param path: /<acct>/<cont>/<obj> path being updated
        :param headers_out: headers to send with the update
        :return: a tuple of (``success``, ``node_id``, ``redirect``)
            where ``success`` is True if the update succeeded, ``node_id`` is
            the_id of the node updated and ``redirect`` is either None or a
            tuple of (a path, a timestamp string).
        """
        redirect = None
        start = time.time()
        # Assume an error until we hear otherwise
        status = 500
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(
                    node['replication_ip'], node['replication_port'],
                    node['device'], part, op, path, headers_out)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.read()
            status = resp.status

            if status == HTTP_MOVED_PERMANENTLY:
                try:
                    redirect = get_redirect_data(resp)
                except ValueError as err:
                    self.logger.error(
                        'Container update failed for %r; problem with '
                        'redirect location: %s' % (path, err))

            success = is_success(status)
            if not success:
                self.logger.debug(
                    'Error code %(status)d is returned from remote '
                    'server %(node)s',
                    {'status': resp.status,
                     'node': node_to_string(node, replication=True)})
            return success, node['id'], redirect
        except Exception:
            self.logger.exception('ERROR with remote server %s',
                                  node_to_string(node, replication=True))
        except Timeout as exc:
            action = 'connecting to'
            if not isinstance(exc, ConnectionTimeout):
                # i.e., we definitely made the request but gave up
                # waiting for the response
                status = 499
                action = 'waiting on'
            self.logger.info(
                'Timeout %s remote server %s: %s',
                action, node_to_string(node, replication=True), exc)
        finally:
            elapsed = time.time() - start
            self.logger.timing('updater.timing.status.%s' % status,
                               elapsed * 1000)
        return HTTP_INTERNAL_SERVER_ERROR, node['id'], redirect


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ObjectUpdater, conf_file, **options)


if __name__ == '__main__':
    main()
