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

"""
    cmdline utility to perform cluster reconnaissance
"""

from __future__ import print_function

from eventlet.green import socket
from six import string_types
from six.moves.urllib.parse import urlparse

from swift.common.utils import (
    SWIFT_CONF_FILE, md5_hash_for_file, set_swift_dir)
from swift.common.ring import Ring
from swift.common.storage_policy import POLICIES, reload_storage_policies
import eventlet
import json
import optparse
import time
import sys
import six
import os

if six.PY3:
    from eventlet.green.urllib import request as urllib2
else:
    from eventlet.green import urllib2


def seconds2timeunit(seconds):
    elapsed = seconds
    unit = 'seconds'
    if elapsed >= 60:
        elapsed = elapsed / 60.0
        unit = 'minutes'
        if elapsed >= 60:
            elapsed = elapsed / 60.0
            unit = 'hours'
            if elapsed >= 24:
                elapsed = elapsed / 24.0
                unit = 'days'
    return elapsed, unit


def size_suffix(size):
    suffixes = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    for suffix in suffixes:
        if size < 1000:
            return "%s %s" % (size, suffix)
        size = size // 1000
    return "%s %s" % (size, suffix)


class Scout(object):
    """
    Obtain swift recon information
    """

    def __init__(self, recon_type, verbose=False, suppress_errors=False,
                 timeout=5):
        self.recon_type = recon_type
        self.verbose = verbose
        self.suppress_errors = suppress_errors
        self.timeout = timeout

    def scout_host(self, base_url, recon_type):
        """
        Perform the actual HTTP request to obtain swift recon telemetry.

        :param base_url: the base url of the host you wish to check. str of the
                        format 'http://127.0.0.1:6200/recon/'
        :param recon_type: the swift recon check to request.
        :returns: tuple of (recon url used, response body, and status)
        """
        url = base_url + recon_type
        try:
            body = urllib2.urlopen(url, timeout=self.timeout).read()
            if six.PY3 and isinstance(body, six.binary_type):
                body = body.decode('utf8')
            content = json.loads(body)
            if self.verbose:
                print("-> %s: %s" % (url, content))
            status = 200
        except urllib2.HTTPError as err:
            if not self.suppress_errors or self.verbose:
                print("-> %s: %s" % (url, err))
            content = err
            status = err.code
        except (urllib2.URLError, socket.timeout) as err:
            if not self.suppress_errors or self.verbose:
                print("-> %s: %s" % (url, err))
            content = err
            status = -1
        return url, content, status

    def scout(self, host):
        """
        Obtain telemetry from a host running the swift recon middleware.

        :param host: host to check
        :returns: tuple of (recon url used, response body, status, time start
                  and time end)
        """
        base_url = "http://%s:%s/recon/" % (host[0], host[1])
        ts_start = time.time()
        url, content, status = self.scout_host(base_url, self.recon_type)
        ts_end = time.time()
        return url, content, status, ts_start, ts_end

    def scout_server_type(self, host):
        """
        Obtain Server header by calling OPTIONS.

        :param host: host to check
        :returns: Server type, status
        """
        try:
            url = "http://%s:%s/" % (host[0], host[1])
            req = urllib2.Request(url)
            req.get_method = lambda: 'OPTIONS'
            conn = urllib2.urlopen(req)
            header = conn.info().get('Server')
            server_header = header.split('/')
            content = server_header[0]
            status = 200
        except urllib2.HTTPError as err:
            if not self.suppress_errors or self.verbose:
                print("-> %s: %s" % (url, err))
            content = err
            status = err.code
        except (urllib2.URLError, socket.timeout) as err:
            if not self.suppress_errors or self.verbose:
                print("-> %s: %s" % (url, err))
            content = err
            status = -1
        return url, content, status


class SwiftRecon(object):
    """
    Retrieve and report cluster info from hosts running recon middleware.
    """

    def __init__(self):
        self.verbose = False
        self.suppress_errors = False
        self.timeout = 5
        self.pool_size = 30
        self.pool = eventlet.GreenPool(self.pool_size)
        self.check_types = ['account', 'container', 'object']
        self.server_type = 'object'

    def _gen_stats(self, stats, name=None):
        """Compute various stats from a list of values."""
        cstats = [x for x in stats if x is not None]
        if len(cstats) > 0:
            ret_dict = {'low': min(cstats), 'high': max(cstats),
                        'total': sum(cstats), 'reported': len(cstats),
                        'number_none': len(stats) - len(cstats), 'name': name}
            ret_dict['average'] = \
                ret_dict['total'] / float(len(cstats))
            ret_dict['perc_none'] = \
                ret_dict['number_none'] * 100.0 / len(stats)
        else:
            ret_dict = {'reported': 0}
        return ret_dict

    def _print_stats(self, stats):
        """
        print out formatted stats to console

        :param stats: dict of stats generated by _gen_stats
        """
        print('[%(name)s] low: %(low)d, high: %(high)d, avg: '
              '%(average).1f, total: %(total)d, '
              'Failed: %(perc_none).1f%%, no_result: %(number_none)d, '
              'reported: %(reported)d' % stats)

    def _ptime(self, timev=None):
        """
        :param timev: a unix timestamp or None
        :returns: a pretty string of the current time or provided time in UTC
        """
        if timev:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timev))
        else:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def get_hosts(self, region_filter, zone_filter, swift_dir, ring_names):
        """
        Get a list of hosts in the rings.

        :param region_filter: Only list regions matching given filter
        :param zone_filter: Only list zones matching given filter
        :param swift_dir: Directory of swift config, usually /etc/swift
        :param ring_names: Collection of ring names, such as
         ['object', 'object-2']
        :returns: a set of tuples containing the ip and port of hosts
        """
        rings = [Ring(swift_dir, ring_name=n) for n in ring_names]
        devs = [d for r in rings for d in r.devs if d]
        if region_filter is not None:
            devs = [d for d in devs if d['region'] == region_filter]
        if zone_filter is not None:
            devs = [d for d in devs if d['zone'] == zone_filter]
        return set((d['ip'], d['port']) for d in devs)

    def get_ringmd5(self, hosts, swift_dir):
        """
        Compare ring md5sum's with those on remote host

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        :param swift_dir: The local directory with the ring files.
        """
        matches = 0
        errors = 0
        ring_names = set()
        if self.server_type == 'object':
            for ring_name in os.listdir(swift_dir):
                if ring_name.startswith('object') and \
                        ring_name.endswith('.ring.gz'):
                    ring_names.add(ring_name)
        else:
            ring_name = '%s.ring.gz' % self.server_type
            ring_names.add(ring_name)
        rings = {}
        for ring_name in ring_names:
            rings[ring_name] = md5_hash_for_file(
                os.path.join(swift_dir, ring_name))
        recon = Scout("ringmd5", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking ring md5sums" % self._ptime())
        if self.verbose:
            for ring_file, ring_sum in rings.items():
                print("-> On disk %s md5sum: %s" % (ring_file, ring_sum))
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status != 200:
                errors = errors + 1
                continue
            success = True
            for remote_ring_file, remote_ring_sum in response.items():
                remote_ring_name = os.path.basename(remote_ring_file)
                if not remote_ring_name.startswith(self.server_type):
                    continue
                ring_sum = rings.get(remote_ring_name, None)
                if remote_ring_sum != ring_sum:
                    success = False
                    print("!! %s (%s => %s) doesn't match on disk md5sum" % (
                        url, remote_ring_name, remote_ring_sum))
            if not success:
                errors += 1
                continue
            matches += 1
            if self.verbose:
                print("-> %s matches." % url)
        print("%s/%s hosts matched, %s error[s] while checking hosts." % (
            matches, len(hosts), errors))
        print("=" * 79)

    def get_swiftconfmd5(self, hosts, printfn=print):
        """
        Compare swift.conf md5sum with that on remote hosts

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        :param printfn: function to print text; defaults to print()
        """
        matches = 0
        errors = 0
        conf_sum = md5_hash_for_file(SWIFT_CONF_FILE)
        recon = Scout("swiftconfmd5", self.verbose, self.suppress_errors,
                      self.timeout)
        printfn("[%s] Checking swift.conf md5sum" % self._ptime())
        if self.verbose:
            printfn("-> On disk swift.conf md5sum: %s" % (conf_sum,))
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                if response[SWIFT_CONF_FILE] != conf_sum:
                    printfn("!! %s (%s) doesn't match on disk md5sum" %
                            (url, response[SWIFT_CONF_FILE]))
                else:
                    matches = matches + 1
                    if self.verbose:
                        printfn("-> %s matches." % url)
            else:
                errors = errors + 1
        printfn("%s/%s hosts matched, %s error[s] while checking hosts."
                % (matches, len(hosts), errors))
        printfn("=" * 79)

    def async_check(self, hosts):
        """
        Obtain and print async pending statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        scan = {}
        recon = Scout("async", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking async pendings" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                scan[url] = response['async_pending']
        stats = self._gen_stats(scan.values(), 'async_pending')
        if stats['reported'] > 0:
            self._print_stats(stats)
        else:
            print("[async_pending] - No hosts returned valid data.")
        print("=" * 79)

    def driveaudit_check(self, hosts):
        """
        Obtain and print drive audit error statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)]
        """
        scan = {}
        recon = Scout("driveaudit", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking drive-audit errors" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                scan[url] = response['drive_audit_errors']
        stats = self._gen_stats(scan.values(), 'drive_audit_errors')
        if stats['reported'] > 0:
            self._print_stats(stats)
        else:
            print("[drive_audit_errors] - No hosts returned valid data.")
        print("=" * 79)

    def umount_check(self, hosts):
        """
        Check for and print unmounted drives

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        unmounted = {}
        errors = {}
        recon = Scout("unmounted", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Getting unmounted drives from %s hosts..." %
              (self._ptime(), len(hosts)))
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                unmounted[url] = []
                errors[url] = []
                for i in response:
                    if not isinstance(i['mounted'], bool):
                        errors[url].append(i['device'])
                    else:
                        unmounted[url].append(i['device'])
        for host in unmounted:
            node = urlparse(host).netloc
            for entry in unmounted[host]:
                print("Not mounted: %s on %s" % (entry, node))
        for host in errors:
            node = urlparse(host).netloc
            for entry in errors[host]:
                print("Device errors: %s on %s" % (entry, node))
        print("=" * 79)

    def server_type_check(self, hosts):
        """
        Check for server types on the ring

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        errors = {}
        recon = Scout("server_type_check", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Validating server type '%s' on %s hosts..." %
              (self._ptime(), self.server_type, len(hosts)))
        for url, response, status in self.pool.imap(
                recon.scout_server_type, hosts):
            if status == 200:
                if response != self.server_type + '-server':
                    errors[url] = response
        print("%s/%s hosts ok, %s error[s] while checking hosts." % (
            len(hosts) - len(errors), len(hosts), len(errors)))
        for host in errors:
            print("Invalid: %s is %s" % (host, errors[host]))
        print("=" * 79)

    def expirer_check(self, hosts):
        """
        Obtain and print expirer statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        stats = {'object_expiration_pass': [], 'expired_last_pass': []}
        recon = Scout("expirer/%s" % self.server_type, self.verbose,
                      self.suppress_errors, self.timeout)
        print("[%s] Checking on expirers" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                stats['object_expiration_pass'].append(
                    response.get('object_expiration_pass'))
                stats['expired_last_pass'].append(
                    response.get('expired_last_pass'))
        for k in stats:
            if stats[k]:
                computed = self._gen_stats(stats[k], name=k)
                if computed['reported'] > 0:
                    self._print_stats(computed)
                else:
                    print("[%s] - No hosts returned valid data." % k)
            else:
                print("[%s] - No hosts returned valid data." % k)
        print("=" * 79)

    def replication_check(self, hosts):
        """
        Obtain and print replication statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        stats = {'replication_time': [], 'failure': [], 'success': [],
                 'attempted': []}
        recon = Scout("replication/%s" % self.server_type, self.verbose,
                      self.suppress_errors, self.timeout)
        print("[%s] Checking on replication" % self._ptime())
        least_recent_time = 9999999999
        least_recent_url = None
        most_recent_time = 0
        most_recent_url = None
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                stats['replication_time'].append(
                    response.get('replication_time',
                                 response.get('object_replication_time', 0)))
                repl_stats = response.get('replication_stats')
                if repl_stats:
                    for stat_key in ['attempted', 'failure', 'success']:
                        stats[stat_key].append(repl_stats.get(stat_key))
                last = response.get('replication_last',
                                    response.get('object_replication_last', 0))
                if last < least_recent_time:
                    least_recent_time = last
                    least_recent_url = url
                if last > most_recent_time:
                    most_recent_time = last
                    most_recent_url = url
        for k in stats:
            if stats[k]:
                if k != 'replication_time':
                    computed = self._gen_stats(stats[k],
                                               name='replication_%s' % k)
                else:
                    computed = self._gen_stats(stats[k], name=k)
                if computed['reported'] > 0:
                    self._print_stats(computed)
                else:
                    print("[%s] - No hosts returned valid data." % k)
            else:
                print("[%s] - No hosts returned valid data." % k)
        if least_recent_url is not None:
            host = urlparse(least_recent_url).netloc
            if not least_recent_time:
                print('Oldest completion was NEVER by %s.' % host)
            else:
                elapsed = time.time() - least_recent_time
                elapsed, elapsed_unit = seconds2timeunit(elapsed)
                print('Oldest completion was %s (%d %s ago) by %s.' % (
                    self._ptime(least_recent_time),
                    elapsed, elapsed_unit, host))
        if most_recent_url is not None:
            host = urlparse(most_recent_url).netloc
            elapsed = time.time() - most_recent_time
            elapsed, elapsed_unit = seconds2timeunit(elapsed)
            print('Most recent completion was %s (%d %s ago) by %s.' % (
                self._ptime(most_recent_time),
                elapsed, elapsed_unit, host))
        print("=" * 79)

    def updater_check(self, hosts):
        """
        Obtain and print updater statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        stats = []
        recon = Scout("updater/%s" % self.server_type, self.verbose,
                      self.suppress_errors, self.timeout)
        print("[%s] Checking updater times" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                if response['%s_updater_sweep' % self.server_type]:
                    stats.append(response['%s_updater_sweep' %
                                          self.server_type])
        if len(stats) > 0:
            computed = self._gen_stats(stats, name='updater_last_sweep')
            if computed['reported'] > 0:
                self._print_stats(computed)
            else:
                print("[updater_last_sweep] - No hosts returned valid data.")
        else:
            print("[updater_last_sweep] - No hosts returned valid data.")
        print("=" * 79)

    def auditor_check(self, hosts):
        """
        Obtain and print obj auditor statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        scan = {}
        adone = '%s_auditor_pass_completed' % self.server_type
        afail = '%s_audits_failed' % self.server_type
        apass = '%s_audits_passed' % self.server_type
        asince = '%s_audits_since' % self.server_type
        recon = Scout("auditor/%s" % self.server_type, self.verbose,
                      self.suppress_errors, self.timeout)
        print("[%s] Checking auditor stats" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                scan[url] = response
        if len(scan) < 1:
            print("Error: No hosts available")
            return
        stats = {}
        stats[adone] = [scan[i][adone] for i in scan
                        if scan[i][adone] is not None]
        stats[afail] = [scan[i][afail] for i in scan
                        if scan[i][afail] is not None]
        stats[apass] = [scan[i][apass] for i in scan
                        if scan[i][apass] is not None]
        stats[asince] = [scan[i][asince] for i in scan
                         if scan[i][asince] is not None]
        for k in stats:
            if len(stats[k]) < 1:
                print("[%s] - No hosts returned valid data." % k)
            else:
                if k != asince:
                    computed = self._gen_stats(stats[k], k)
                    if computed['reported'] > 0:
                        self._print_stats(computed)
        if len(stats[asince]) >= 1:
            low = min(stats[asince])
            high = max(stats[asince])
            total = sum(stats[asince])
            average = total / len(stats[asince])
            print('[last_pass] oldest: %s, newest: %s, avg: %s' %
                  (self._ptime(low), self._ptime(high), self._ptime(average)))
        print("=" * 79)

    def nested_get_value(self, key, recon_entry):
        """
        Generator that yields all values for given key in a recon cache entry.
        This is for use with object auditor recon cache entries.  If the
        object auditor has run in parallel, the recon cache will have entries
        of the form:  {'object_auditor_stats_ALL': { 'disk1': {..},
                                                     'disk2': {..},
                                                     'disk3': {..},
                                                   ...}}
        If the object auditor hasn't run in parallel, the recon cache will have
        entries of the form:  {'object_auditor_stats_ALL': {...}}.
        The ZBF auditor doesn't run in parallel.  However, if a subset of
        devices is selected for auditing, the recon cache will have an entry
        of the form:  {'object_auditor_stats_ZBF': { 'disk1disk2..diskN': {}}
        We use this generator to find all instances of a particular key in
        these multi-level dictionaries.
        """
        for k, v in recon_entry.items():
            if isinstance(v, dict):
                for value in self.nested_get_value(key, v):
                    yield value
            if k == key:
                yield v

    def object_auditor_check(self, hosts):
        """
        Obtain and print obj auditor statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        all_scan = {}
        zbf_scan = {}
        atime = 'audit_time'
        bprocessed = 'bytes_processed'
        passes = 'passes'
        errors = 'errors'
        quarantined = 'quarantined'
        recon = Scout("auditor/object", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking auditor stats " % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                if response['object_auditor_stats_ALL']:
                    all_scan[url] = response['object_auditor_stats_ALL']
                if response['object_auditor_stats_ZBF']:
                    zbf_scan[url] = response['object_auditor_stats_ZBF']
        if len(all_scan) > 0:
            stats = {}
            stats[atime] = [sum(self.nested_get_value(atime, all_scan[i]))
                            for i in all_scan]
            stats[bprocessed] = [sum(self.nested_get_value(bprocessed,
                                 all_scan[i])) for i in all_scan]
            stats[passes] = [sum(self.nested_get_value(passes, all_scan[i]))
                             for i in all_scan]
            stats[errors] = [sum(self.nested_get_value(errors, all_scan[i]))
                             for i in all_scan]
            stats[quarantined] = [sum(self.nested_get_value(quarantined,
                                  all_scan[i])) for i in all_scan]
            for k in stats:
                if None in stats[k]:
                    stats[k] = [x for x in stats[k] if x is not None]
                if len(stats[k]) < 1:
                    print("[Auditor %s] - No hosts returned valid data." % k)
                else:
                    computed = self._gen_stats(stats[k],
                                               name='ALL_%s_last_path' % k)
                    if computed['reported'] > 0:
                        self._print_stats(computed)
                    else:
                        print("[ALL_auditor] - No hosts returned valid data.")
        else:
            print("[ALL_auditor] - No hosts returned valid data.")
        if len(zbf_scan) > 0:
            stats = {}
            stats[atime] = [sum(self.nested_get_value(atime, zbf_scan[i]))
                            for i in zbf_scan]
            stats[bprocessed] = [sum(self.nested_get_value(bprocessed,
                                 zbf_scan[i])) for i in zbf_scan]
            stats[errors] = [sum(self.nested_get_value(errors, zbf_scan[i]))
                             for i in zbf_scan]
            stats[quarantined] = [sum(self.nested_get_value(quarantined,
                                  zbf_scan[i])) for i in zbf_scan]
            for k in stats:
                if None in stats[k]:
                    stats[k] = [x for x in stats[k] if x is not None]
                if len(stats[k]) < 1:
                    print("[Auditor %s] - No hosts returned valid data." % k)
                else:
                    computed = self._gen_stats(stats[k],
                                               name='ZBF_%s_last_path' % k)
                    if computed['reported'] > 0:
                        self._print_stats(computed)
                    else:
                        print("[ZBF_auditor] - No hosts returned valid data.")
        else:
            print("[ZBF_auditor] - No hosts returned valid data.")
        print("=" * 79)

    def load_check(self, hosts):
        """
        Obtain and print load average statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        load1 = {}
        load5 = {}
        load15 = {}
        recon = Scout("load", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking load averages" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                load1[url] = response['1m']
                load5[url] = response['5m']
                load15[url] = response['15m']
        stats = {"1m": load1, "5m": load5, "15m": load15}
        for item in stats:
            if len(stats[item]) > 0:
                computed = self._gen_stats(stats[item].values(),
                                           name='%s_load_avg' % item)
                self._print_stats(computed)
            else:
                print("[%s_load_avg] - No hosts returned valid data." % item)
        print("=" * 79)

    def quarantine_check(self, hosts):
        """
        Obtain and print quarantine statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        objq = {}
        conq = {}
        acctq = {}
        stats = {}
        recon = Scout("quarantined", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking quarantine" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                objq[url] = response['objects']
                conq[url] = response['containers']
                acctq[url] = response['accounts']
                for key in response.get('policies', {}):
                    pkey = "objects_%s" % key
                    stats.setdefault(pkey, {})
                    stats[pkey][url] = response['policies'][key]['objects']
        stats.update({"objects": objq, "containers": conq, "accounts": acctq})
        for item in stats:
            if len(stats[item]) > 0:
                computed = self._gen_stats(stats[item].values(),
                                           name='quarantined_%s' % item)
                self._print_stats(computed)
            else:
                print("No hosts returned valid data.")
        print("=" * 79)

    def socket_usage(self, hosts):
        """
        Obtain and print /proc/net/sockstat statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        inuse4 = {}
        mem = {}
        inuse6 = {}
        timewait = {}
        orphan = {}
        recon = Scout("sockstat", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking socket usage" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                inuse4[url] = response['tcp_in_use']
                mem[url] = response['tcp_mem_allocated_bytes']
                inuse6[url] = response.get('tcp6_in_use', 0)
                timewait[url] = response['time_wait']
                orphan[url] = response['orphan']
        stats = {"tcp_in_use": inuse4, "tcp_mem_allocated_bytes": mem,
                 "tcp6_in_use": inuse6, "time_wait": timewait,
                 "orphan": orphan}
        for item in stats:
            if len(stats[item]) > 0:
                computed = self._gen_stats(stats[item].values(), item)
                self._print_stats(computed)
            else:
                print("No hosts returned valid data.")
        print("=" * 79)

    def disk_usage(self, hosts, top=0, lowest=0, human_readable=False):
        """
        Obtain and print disk usage statistics

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        stats = {}
        highs = []
        lows = []
        raw_total_used = []
        raw_total_avail = []
        percents = {}
        top_percents = [(None, 0)] * top
        low_percents = [(None, 100)] * lowest
        recon = Scout("diskusage", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking disk usage now" % self._ptime())
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status == 200:
                hostusage = []
                for entry in response:
                    if not isinstance(entry['mounted'], bool):
                        print("-> %s/%s: Error: %s" % (url, entry['device'],
                                                       entry['mounted']))
                    elif entry['mounted']:
                        used = float(entry['used']) / float(entry['size']) \
                            * 100.0
                        raw_total_used.append(entry['used'])
                        raw_total_avail.append(entry['avail'])
                        hostusage.append(round(used, 2))
                        for ident, oused in top_percents:
                            if oused < used:
                                top_percents.append(
                                    (url + ' ' + entry['device'], used))
                                top_percents.sort(key=lambda x: -x[1])
                                top_percents.pop()
                                break
                        for ident, oused in low_percents:
                            if oused > used:
                                low_percents.append(
                                    (url + ' ' + entry['device'], used))
                                low_percents.sort(key=lambda x: x[1])
                                low_percents.pop()
                                break
                stats[url] = hostusage

        for url in stats:
            if len(stats[url]) > 0:
                # get per host hi/los for another day
                low = min(stats[url])
                high = max(stats[url])
                highs.append(high)
                lows.append(low)
                for percent in stats[url]:
                    percents[int(percent)] = percents.get(int(percent), 0) + 1
            else:
                print("-> %s: Error. No drive info available." % url)

        if len(lows) > 0:
            low = min(lows)
            high = max(highs)
            # dist graph shamelessly stolen from https://github.com/gholt/tcod
            print("Distribution Graph:")
            mul = 69.0 / max(percents.values())
            for percent in sorted(percents):
                print('% 3d%%%5d %s' % (percent, percents[percent],
                                        '*' * int(percents[percent] * mul)))
            raw_used = sum(raw_total_used)
            raw_avail = sum(raw_total_avail)
            raw_total = raw_used + raw_avail
            avg_used = 100.0 * raw_used / raw_total
            if human_readable:
                raw_used = size_suffix(raw_used)
                raw_avail = size_suffix(raw_avail)
                raw_total = size_suffix(raw_total)
            print("Disk usage: space used: %s of %s" % (raw_used, raw_total))
            print("Disk usage: space free: %s of %s" % (raw_avail, raw_total))
            print("Disk usage: lowest: %s%%, highest: %s%%, avg: %s%%" %
                  (low, high, avg_used))
        else:
            print("No hosts returned valid data.")
        print("=" * 79)
        if top_percents:
            print('TOP %s' % top)
            for ident, used in top_percents:
                if ident:
                    url, device = ident.split()
                    host = urlparse(url).netloc.split(':')[0]
                    print('%.02f%%  %s' % (used, '%-15s %s' % (host, device)))
        if low_percents:
            print('LOWEST %s' % lowest)
            for ident, used in low_percents:
                if ident:
                    url, device = ident.split()
                    host = urlparse(url).netloc.split(':')[0]
                    print('%.02f%%  %s' % (used, '%-15s %s' % (host, device)))

    def time_check(self, hosts, jitter=0.0):
        """
        Check a time synchronization of hosts with current time

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        :param jitter: Maximal allowed time jitter
        """

        jitter = abs(jitter)
        matches = 0
        errors = 0
        recon = Scout("time", self.verbose, self.suppress_errors,
                      self.timeout)
        print("[%s] Checking time-sync" % self._ptime())
        for url, ts_remote, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status != 200:
                errors = errors + 1
                continue
            if (ts_remote + jitter < ts_start or ts_remote - jitter > ts_end):
                diff = abs(ts_end - ts_remote)
                ts_end_f = self._ptime(ts_end)
                ts_remote_f = self._ptime(ts_remote)

                print("!! %s current time is %s, but remote is %s, "
                      "differs by %.4f sec" % (
                          url,
                          ts_end_f,
                          ts_remote_f,
                          diff))
                continue
            matches += 1
            if self.verbose:
                print("-> %s matches." % url)
        print("%s/%s hosts matched, %s error[s] while checking hosts." % (
            matches, len(hosts), errors))
        print("=" * 79)

    def version_check(self, hosts):
        """
        Check OS Swift version of hosts. Inform if differs.

        :param hosts: set of hosts to check. in the format of:
            set([('127.0.0.1', 6020), ('127.0.0.2', 6030)])
        """
        versions = set()
        errors = 0
        print("[%s] Checking versions" % self._ptime())
        recon = Scout("version", self.verbose, self.suppress_errors,
                      self.timeout)
        for url, response, status, ts_start, ts_end in self.pool.imap(
                recon.scout, hosts):
            if status != 200:
                errors = errors + 1
                continue
            versions.add(response['version'])
            if self.verbose:
                print("-> %s installed version %s" % (
                    url, response['version']))

        if not len(versions):
            print("No hosts returned valid data.")
        elif len(versions) == 1:
            print("Versions matched (%s), "
                  "%s error[s] while checking hosts." % (
                      versions.pop(), errors))
        else:
            print("Versions not matched (%s), "
                  "%s error[s] while checking hosts." % (
                      ", ".join(sorted(versions)), errors))

        print("=" * 79)

    def _get_ring_names(self, policy=None):
        """
        Retrieve name of ring files.

        If no policy is passed and the server type is object,
        the ring names of all storage-policies are retrieved.

        :param policy: name or index of storage policy, only applicable
         with server_type==object.
         :returns: list of ring names.
        """
        if self.server_type == 'object':
            ring_names = [p.ring_name for p in POLICIES if (
                p.name == policy or not policy or (
                    policy.isdigit() and int(policy) == int(p) or
                    (isinstance(policy, string_types)
                     and policy in p.aliases)))]
        else:
            ring_names = [self.server_type]

        return ring_names

    def main(self):
        """
        Retrieve and report cluster info from hosts running recon middleware.
        """
        print("=" * 79)
        usage = '''
        usage: %prog <server_type> [<server_type> [<server_type>]]
        [-v] [--suppress] [-a] [-r] [-u] [-d]
        [-l] [-T] [--md5] [--auditor] [--updater] [--expirer] [--sockstat]
        [--human-readable]

        <server_type>\taccount|container|object
        Defaults to object server.

        ex: %prog container -l --auditor
        '''
        args = optparse.OptionParser(usage)
        args.add_option('--verbose', '-v', action="store_true",
                        help="Print verbose info")
        args.add_option('--suppress', action="store_true",
                        help="Suppress most connection related errors")
        args.add_option('--async', '-a',
                        action="store_true", dest="async_check",
                        help="Get async stats")
        args.add_option('--replication', '-r', action="store_true",
                        help="Get replication stats")
        args.add_option('--auditor', action="store_true",
                        help="Get auditor stats")
        args.add_option('--updater', action="store_true",
                        help="Get updater stats")
        args.add_option('--expirer', action="store_true",
                        help="Get expirer stats")
        args.add_option('--unmounted', '-u', action="store_true",
                        help="Check cluster for unmounted devices")
        args.add_option('--diskusage', '-d', action="store_true",
                        help="Get disk usage stats")
        args.add_option('--human-readable', action="store_true",
                        help="Use human readable suffix for disk usage stats")
        args.add_option('--loadstats', '-l', action="store_true",
                        help="Get cluster load average stats")
        args.add_option('--quarantined', '-q', action="store_true",
                        help="Get cluster quarantine stats")
        args.add_option('--validate-servers', action="store_true",
                        help="Validate servers on the ring")
        args.add_option('--md5', action="store_true",
                        help="Get md5sum of servers ring and compare to "
                        "local copy")
        args.add_option('--sockstat', action="store_true",
                        help="Get cluster socket usage stats")
        args.add_option('--driveaudit', action="store_true",
                        help="Get drive audit error stats")
        args.add_option('--time', '-T', action="store_true",
                        help="Check time synchronization")
        args.add_option('--jitter', type="float", default=0.0,
                        help="Maximal allowed time jitter")
        args.add_option('--swift-versions', action="store_true",
                        help="Check swift versions")
        args.add_option('--top', type='int', metavar='COUNT', default=0,
                        help='Also show the top COUNT entries in rank order.')
        args.add_option('--lowest', type='int', metavar='COUNT', default=0,
                        help='Also show the lowest COUNT entries in rank \
                        order.')
        args.add_option('--all', action="store_true",
                        help="Perform all checks. Equal to \t\t\t-arudlqT "
                        "--md5 --sockstat --auditor --updater --expirer "
                        "--driveaudit --validate-servers --swift-versions")
        args.add_option('--region', type="int",
                        help="Only query servers in specified region")
        args.add_option('--zone', '-z', type="int",
                        help="Only query servers in specified zone")
        args.add_option('--timeout', '-t', type="int", metavar="SECONDS",
                        help="Time to wait for a response from a server",
                        default=5)
        args.add_option('--swiftdir', default="/etc/swift",
                        help="Default = /etc/swift")
        args.add_option('--policy', '-p',
                        help='Only query object servers in specified '
                        'storage policy (specified as name or index).')
        options, arguments = args.parse_args()

        if len(sys.argv) <= 1 or len(arguments) > len(self.check_types):
            args.print_help()
            sys.exit(0)

        if arguments:
            arguments = set(arguments)
            if arguments.issubset(self.check_types):
                server_types = arguments
            else:
                print("Invalid Server Type")
                args.print_help()
                sys.exit(1)
        else:  # default
            server_types = ['object']

        swift_dir = options.swiftdir
        if set_swift_dir(swift_dir):
            reload_storage_policies()

        self.verbose = options.verbose
        self.suppress_errors = options.suppress
        self.timeout = options.timeout

        for server_type in server_types:
            self.server_type = server_type
            ring_names = self._get_ring_names(options.policy)
            if not ring_names:
                print('Invalid Storage Policy: %s' % options.policy)
                args.print_help()
                sys.exit(0)
            hosts = self.get_hosts(options.region, options.zone,
                                   swift_dir, ring_names)
            print("--> Starting reconnaissance on %s hosts (%s)" %
                  (len(hosts), self.server_type))
            print("=" * 79)
            if options.all:
                if self.server_type == 'object':
                    self.async_check(hosts)
                    self.object_auditor_check(hosts)
                    self.updater_check(hosts)
                    self.expirer_check(hosts)
                elif self.server_type == 'container':
                    self.auditor_check(hosts)
                    self.updater_check(hosts)
                elif self.server_type == 'account':
                    self.auditor_check(hosts)
                self.replication_check(hosts)
                self.umount_check(hosts)
                self.load_check(hosts)
                self.disk_usage(hosts, options.top, options.lowest,
                                options.human_readable)
                self.get_ringmd5(hosts, swift_dir)
                self.get_swiftconfmd5(hosts)
                self.quarantine_check(hosts)
                self.socket_usage(hosts)
                self.server_type_check(hosts)
                self.driveaudit_check(hosts)
                self.time_check(hosts, options.jitter)
                self.version_check(hosts)
            else:
                if options.async_check:
                    if self.server_type == 'object':
                        self.async_check(hosts)
                    else:
                        print("Error: Can't check asyncs on non object "
                              "servers.")
                        print("=" * 79)
                if options.unmounted:
                    self.umount_check(hosts)
                if options.replication:
                    self.replication_check(hosts)
                if options.auditor:
                    if self.server_type == 'object':
                        self.object_auditor_check(hosts)
                    else:
                        self.auditor_check(hosts)
                if options.updater:
                    if self.server_type == 'account':
                        print("Error: Can't check updaters on account "
                              "servers.")
                        print("=" * 79)
                    else:
                        self.updater_check(hosts)
                if options.expirer:
                    if self.server_type == 'object':
                        self.expirer_check(hosts)
                    else:
                        print("Error: Can't check expired on non object "
                              "servers.")
                        print("=" * 79)
                if options.validate_servers:
                    self.server_type_check(hosts)
                if options.loadstats:
                    self.load_check(hosts)
                if options.diskusage:
                    self.disk_usage(hosts, options.top, options.lowest,
                                    options.human_readable)
                if options.md5:
                    self.get_ringmd5(hosts, swift_dir)
                    self.get_swiftconfmd5(hosts)
                if options.quarantined:
                    self.quarantine_check(hosts)
                if options.sockstat:
                    self.socket_usage(hosts)
                if options.driveaudit:
                    self.driveaudit_check(hosts)
                if options.time:
                    self.time_check(hosts, options.jitter)
                if options.swift_versions:
                    self.version_check(hosts)


def main():
    try:
        reconnoiter = SwiftRecon()
        reconnoiter.main()
    except KeyboardInterrupt:
        print('\n')
