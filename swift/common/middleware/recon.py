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

import errno
import json
import os
import time
from resource import getpagesize

from swift import __version__ as swiftver
from swift.common.constraints import check_mount
from swift.common.storage_policy import POLICIES
from swift.common.swob import Request, Response
from swift.common.utils import get_logger, SWIFT_CONF_FILE, md5_hash_for_file
from swift.common.recon import RECON_OBJECT_FILE, RECON_CONTAINER_FILE, \
    RECON_ACCOUNT_FILE, RECON_DRIVE_FILE, RECON_RELINKER_FILE, \
    DEFAULT_RECON_CACHE_PATH


class ReconMiddleware(object):
    """
    Recon middleware used for monitoring.

    /recon/load|mem|async... will return various system metrics.

    Needs to be added to the pipeline and requires a filter
    declaration in the [account|container|object]-server conf file:

    [filter:recon]
    use = egg:swift#recon
    recon_cache_path = /var/cache/swift
    """

    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.devices = conf.get('devices', '/srv/node')
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.logger = get_logger(conf, log_route='recon')
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.object_recon_cache = os.path.join(self.recon_cache_path,
                                               RECON_OBJECT_FILE)
        self.container_recon_cache = os.path.join(self.recon_cache_path,
                                                  RECON_CONTAINER_FILE)
        self.account_recon_cache = os.path.join(self.recon_cache_path,
                                                RECON_ACCOUNT_FILE)
        self.drive_recon_cache = os.path.join(self.recon_cache_path,
                                              RECON_DRIVE_FILE)
        self.relink_recon_cache = os.path.join(self.recon_cache_path,
                                               RECON_RELINKER_FILE)
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')

        self.rings = [self.account_ring_path, self.container_ring_path]
        # include all object ring files (for all policies)
        for policy in POLICIES:
            self.rings.append(os.path.join(swift_dir,
                                           policy.ring_name + '.ring.gz'))

    def _from_recon_cache(self, cache_keys, cache_file, openr=open,
                          ignore_missing=False):
        """retrieve values from a recon cache file

        :params cache_keys: list of cache items to retrieve
        :params cache_file: cache file to retrieve items from.
        :params openr: open to use [for unittests]
        :params ignore_missing: Some recon stats are very temporary, in this
            case it would be better to not log if things are missing.
        :return: dict of cache items and their values or none if not found
        """
        try:
            with openr(cache_file, 'r') as f:
                recondata = json.load(f)
                return {key: recondata.get(key) for key in cache_keys}
        except IOError as err:
            if err.errno == errno.ENOENT and ignore_missing:
                pass
            else:
                self.logger.exception('Error reading recon cache file')
        except ValueError:
            self.logger.exception('Error parsing recon cache file')
        except Exception:
            self.logger.exception('Error retrieving recon data')
        return dict((key, None) for key in cache_keys)

    def get_version(self):
        """get swift version"""
        verinfo = {'version': swiftver}
        return verinfo

    def get_mounted(self, openr=open):
        """get ALL mounted fs from /proc/mounts"""
        mounts = []
        with openr('/proc/mounts', 'r') as procmounts:
            for line in procmounts:
                mount = {}
                mount['device'], mount['path'], opt1, opt2, opt3, \
                    opt4 = line.rstrip().split()
                mounts.append(mount)
        return mounts

    def get_load(self, openr=open):
        """get info from /proc/loadavg"""
        loadavg = {}
        with openr('/proc/loadavg', 'r') as f:
            onemin, fivemin, ftmin, tasks, procs = f.read().rstrip().split()
        loadavg['1m'] = float(onemin)
        loadavg['5m'] = float(fivemin)
        loadavg['15m'] = float(ftmin)
        loadavg['tasks'] = tasks
        loadavg['processes'] = int(procs)
        return loadavg

    def get_mem(self, openr=open):
        """get info from /proc/meminfo"""
        meminfo = {}
        with openr('/proc/meminfo', 'r') as memlines:
            for i in memlines:
                entry = i.rstrip().split(":")
                meminfo[entry[0]] = entry[1].strip()
        return meminfo

    def get_async_info(self):
        """get # of async pendings"""
        return self._from_recon_cache(['async_pending', 'async_pending_last'],
                                      self.object_recon_cache)

    def get_driveaudit_error(self):
        """get # of drive audit errors"""
        return self._from_recon_cache(['drive_audit_errors'],
                                      self.drive_recon_cache)

    def get_sharding_info(self):
        """get sharding info"""
        return self._from_recon_cache(["sharding_stats",
                                       "sharding_time",
                                       "sharding_last"],
                                      self.container_recon_cache)

    def get_replication_info(self, recon_type):
        """get replication info"""
        replication_list = ['replication_time',
                            'replication_stats',
                            'replication_last']
        if recon_type == 'account':
            return self._from_recon_cache(replication_list,
                                          self.account_recon_cache)
        elif recon_type == 'container':
            return self._from_recon_cache(replication_list,
                                          self.container_recon_cache)
        elif recon_type == 'object':
            replication_list += ['object_replication_time',
                                 'object_replication_last']
            return self._from_recon_cache(replication_list,
                                          self.object_recon_cache)
        else:
            return None

    def get_reconstruction_info(self):
        """get reconstruction info"""
        reconstruction_list = ['object_reconstruction_last',
                               'object_reconstruction_time']
        return self._from_recon_cache(reconstruction_list,
                                      self.object_recon_cache)

    def get_device_info(self):
        """get devices"""
        try:
            return {self.devices: os.listdir(self.devices)}
        except Exception:
            self.logger.exception('Error listing devices')
            return {self.devices: None}

    def get_updater_info(self, recon_type):
        """get updater info"""
        if recon_type == 'container':
            return self._from_recon_cache(['container_updater_sweep'],
                                          self.container_recon_cache)
        elif recon_type == 'object':
            return self._from_recon_cache(['object_updater_sweep',
                                           'object_updater_stats',
                                           'object_updater_last'],
                                          self.object_recon_cache)
        else:
            return None

    def get_expirer_info(self, recon_type):
        """get expirer info"""
        if recon_type == 'object':
            return self._from_recon_cache(['object_expiration_pass',
                                           'expired_last_pass'],
                                          self.object_recon_cache)

    def get_auditor_info(self, recon_type):
        """get auditor info"""
        if recon_type == 'account':
            return self._from_recon_cache(['account_audits_passed',
                                           'account_auditor_pass_completed',
                                           'account_audits_since',
                                           'account_audits_failed'],
                                          self.account_recon_cache)
        elif recon_type == 'container':
            return self._from_recon_cache(['container_audits_passed',
                                           'container_auditor_pass_completed',
                                           'container_audits_since',
                                           'container_audits_failed'],
                                          self.container_recon_cache)
        elif recon_type == 'object':
            return self._from_recon_cache(['object_auditor_stats_ALL',
                                           'object_auditor_stats_ZBF'],
                                          self.object_recon_cache)
        else:
            return None

    def get_unmounted(self):
        """list unmounted (failed?) devices"""
        mountlist = []
        for entry in os.listdir(self.devices):
            if not os.path.isdir(os.path.join(self.devices, entry)):
                continue

            try:
                check_mount(self.devices, entry)
            except OSError as err:
                mounted = str(err)
            except ValueError:
                mounted = False
            else:
                continue
            mountlist.append({'device': entry, 'mounted': mounted})
        return mountlist

    def get_diskusage(self):
        """get disk utilization statistics"""
        devices = []
        for entry in os.listdir(self.devices):
            if not os.path.isdir(os.path.join(self.devices, entry)):
                continue

            try:
                check_mount(self.devices, entry)
            except OSError as err:
                devices.append({'device': entry, 'mounted': str(err),
                                'size': '', 'used': '', 'avail': ''})
            except ValueError:
                devices.append({'device': entry, 'mounted': False,
                                'size': '', 'used': '', 'avail': ''})
            else:
                path = os.path.join(self.devices, entry)
                disk = os.statvfs(path)
                capacity = disk.f_bsize * disk.f_blocks
                available = disk.f_bsize * disk.f_bavail
                used = disk.f_bsize * (disk.f_blocks - disk.f_bavail)
                devices.append({'device': entry, 'mounted': True,
                                'size': capacity, 'used': used,
                                'avail': available})
        return devices

    def get_ring_md5(self):
        """get all ring md5sum's"""
        sums = {}
        for ringfile in self.rings:
            if os.path.exists(ringfile):
                try:
                    sums[ringfile] = md5_hash_for_file(ringfile)
                except IOError as err:
                    sums[ringfile] = None
                    if err.errno != errno.ENOENT:
                        self.logger.exception('Error reading ringfile')
        return sums

    def get_swift_conf_md5(self):
        """get md5 of swift.conf"""
        hexsum = None
        try:
            hexsum = md5_hash_for_file(SWIFT_CONF_FILE)
        except IOError as err:
            if err.errno != errno.ENOENT:
                self.logger.exception('Error reading swift.conf')
        return {SWIFT_CONF_FILE: hexsum}

    def get_quarantine_count(self):
        """get obj/container/account quarantine counts"""
        qcounts = {"objects": 0, "containers": 0, "accounts": 0,
                   "policies": {}}
        qdir = "quarantined"
        for device in os.listdir(self.devices):
            qpath = os.path.join(self.devices, device, qdir)
            if os.path.exists(qpath):
                for qtype in os.listdir(qpath):
                    qtgt = os.path.join(qpath, qtype)
                    linkcount = os.lstat(qtgt).st_nlink
                    if linkcount > 2:
                        if qtype.startswith('objects'):
                            if '-' in qtype:
                                pkey = qtype.split('-', 1)[1]
                            else:
                                pkey = '0'
                            qcounts['policies'].setdefault(pkey,
                                                           {'objects': 0})
                            qcounts['policies'][pkey]['objects'] \
                                += linkcount - 2
                            qcounts['objects'] += linkcount - 2
                        else:
                            qcounts[qtype] += linkcount - 2
        return qcounts

    def get_socket_info(self, openr=open):
        """
        get info from /proc/net/sockstat and sockstat6

        Note: The mem value is actually kernel pages, but we return bytes
        allocated based on the systems page size.
        """
        sockstat = {}
        try:
            with openr('/proc/net/sockstat', 'r') as proc_sockstat:
                for entry in proc_sockstat:
                    if entry.startswith("TCP: inuse"):
                        tcpstats = entry.split()
                        sockstat['tcp_in_use'] = int(tcpstats[2])
                        sockstat['orphan'] = int(tcpstats[4])
                        sockstat['time_wait'] = int(tcpstats[6])
                        sockstat['tcp_mem_allocated_bytes'] = \
                            int(tcpstats[10]) * getpagesize()
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
        try:
            with openr('/proc/net/sockstat6', 'r') as proc_sockstat6:
                for entry in proc_sockstat6:
                    if entry.startswith("TCP6: inuse"):
                        sockstat['tcp6_in_use'] = int(entry.split()[2])
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
        return sockstat

    def get_time(self):
        """get current time"""

        return time.time()

    def get_relinker_info(self):
        """get relinker info, if any"""

        stat_keys = ['devices', 'workers']
        return self._from_recon_cache(stat_keys,
                                      self.relink_recon_cache,
                                      ignore_missing=True)

    def GET(self, req):
        root, rcheck, rtype = req.split_path(1, 3, True)
        all_rtypes = ['account', 'container', 'object']
        if rcheck == "mem":
            content = self.get_mem()
        elif rcheck == "load":
            content = self.get_load()
        elif rcheck == "async":
            content = self.get_async_info()
        elif rcheck == 'replication' and rtype in all_rtypes:
            content = self.get_replication_info(rtype)
        elif rcheck == 'replication' and rtype is None:
            # handle old style object replication requests
            content = self.get_replication_info('object')
        elif rcheck == "devices":
            content = self.get_device_info()
        elif rcheck == "updater" and rtype in ['container', 'object']:
            content = self.get_updater_info(rtype)
        elif rcheck == "auditor" and rtype in all_rtypes:
            content = self.get_auditor_info(rtype)
        elif rcheck == "expirer" and rtype == 'object':
            content = self.get_expirer_info(rtype)
        elif rcheck == "mounted":
            content = self.get_mounted()
        elif rcheck == "unmounted":
            content = self.get_unmounted()
        elif rcheck == "diskusage":
            content = self.get_diskusage()
        elif rcheck == "ringmd5":
            content = self.get_ring_md5()
        elif rcheck == "swiftconfmd5":
            content = self.get_swift_conf_md5()
        elif rcheck == "quarantined":
            content = self.get_quarantine_count()
        elif rcheck == "sockstat":
            content = self.get_socket_info()
        elif rcheck == "version":
            content = self.get_version()
        elif rcheck == "driveaudit":
            content = self.get_driveaudit_error()
        elif rcheck == "time":
            content = self.get_time()
        elif rcheck == "sharding":
            content = self.get_sharding_info()
        elif rcheck == "relinker":
            content = self.get_relinker_info()
        elif rcheck == "reconstruction" and rtype == 'object':
            content = self.get_reconstruction_info()
        else:
            content = "Invalid path: %s" % req.path
            return Response(request=req, status="404 Not Found",
                            body=content, content_type="text/plain")
        if content is not None:
            return Response(request=req, body=json.dumps(content),
                            content_type="application/json")
        else:
            return Response(request=req, status="500 Server Error",
                            body="Internal server error.",
                            content_type="text/plain")

    def __call__(self, env, start_response):
        req = Request(env)
        if req.path.startswith('/recon/'):
            return self.GET(req)(env, start_response)
        else:
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def recon_filter(app):
        return ReconMiddleware(app, conf)
    return recon_filter
