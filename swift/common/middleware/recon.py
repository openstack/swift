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

import errno
import os

from swift.common.swob import Request, Response
from swift.common.utils import get_logger, config_true_value, json
from swift.common.constraints import check_mount
from resource import getpagesize
from hashlib import md5


class ReconMiddleware(object):
    """
    Recon middleware used for monitoring.

    /recon/load|mem|async... will return various system metrics.

    Needs to be added to the pipeline and a requires a filter
    declaration in the object-server.conf:

    [filter:recon]
    use = egg:swift#recon
    recon_cache_path = /var/cache/swift
    """

    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.devices = conf.get('devices', '/srv/node/')
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.logger = get_logger(conf, log_route='recon')
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.object_recon_cache = os.path.join(self.recon_cache_path,
                                               'object.recon')
        self.container_recon_cache = os.path.join(self.recon_cache_path,
                                                  'container.recon')
        self.account_recon_cache = os.path.join(self.recon_cache_path,
                                                'account.recon')
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')
        self.object_ring_path = os.path.join(swift_dir, 'object.ring.gz')
        self.rings = [self.account_ring_path, self.container_ring_path,
                      self.object_ring_path]
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))

    def _from_recon_cache(self, cache_keys, cache_file, openr=open):
        """retrieve values from a recon cache file

        :params cache_keys: list of cache items to retrieve
        :params cache_file: cache file to retrieve items from.
        :params openr: open to use [for unittests]
        :return: dict of cache items and their value or none if not found
        """
        try:
            with openr(cache_file, 'r') as f:
                recondata = json.load(f)
                return dict((key, recondata.get(key)) for key in cache_keys)
        except IOError:
            self.logger.exception(_('Error reading recon cache file'))
        except ValueError:
            self.logger.exception(_('Error parsing recon cache file'))
        except Exception:
            self.logger.exception(_('Error retrieving recon data'))
        return dict((key, None) for key in cache_keys)

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
        return self._from_recon_cache(['async_pending'],
                                      self.object_recon_cache)

    def get_replication_info(self, recon_type):
        """get replication info"""
        if recon_type == 'account':
            return self._from_recon_cache(['replication_time',
                                           'replication_stats',
                                           'replication_last'],
                                          self.account_recon_cache)
        elif recon_type == 'container':
            return self._from_recon_cache(['replication_time',
                                           'replication_stats',
                                           'replication_last'],
                                          self.container_recon_cache)
        elif recon_type == 'object':
            return self._from_recon_cache(['object_replication_time',
                                           'object_replication_last'],
                                          self.object_recon_cache)
        else:
            return None

    def get_device_info(self):
        """get devices"""
        try:
            return {self.devices: os.listdir(self.devices)}
        except Exception:
            self.logger.exception(_('Error listing devices'))
            return {self.devices: None}

    def get_updater_info(self, recon_type):
        """get updater info"""
        if recon_type == 'container':
            return self._from_recon_cache(['container_updater_sweep'],
                                          self.container_recon_cache)
        elif recon_type == 'object':
            return self._from_recon_cache(['object_updater_sweep'],
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
            mpoint = {'device': entry,
                      'mounted': check_mount(self.devices, entry)}
            if not mpoint['mounted']:
                mountlist.append(mpoint)
        return mountlist

    def get_diskusage(self):
        """get disk utilization statistics"""
        devices = []
        for entry in os.listdir(self.devices):
            if check_mount(self.devices, entry):
                path = os.path.join(self.devices, entry)
                disk = os.statvfs(path)
                capacity = disk.f_bsize * disk.f_blocks
                available = disk.f_bsize * disk.f_bavail
                used = disk.f_bsize * (disk.f_blocks - disk.f_bavail)
                devices.append({'device': entry, 'mounted': True,
                                'size': capacity, 'used': used,
                                'avail': available})
            else:
                devices.append({'device': entry, 'mounted': False,
                                'size': '', 'used': '', 'avail': ''})
        return devices

    def get_ring_md5(self, openr=open):
        """get all ring md5sum's"""
        sums = {}
        for ringfile in self.rings:
            md5sum = md5()
            if os.path.exists(ringfile):
                try:
                    with openr(ringfile, 'rb') as f:
                        block = f.read(4096)
                        while block:
                            md5sum.update(block)
                            block = f.read(4096)
                    sums[ringfile] = md5sum.hexdigest()
                except IOError, err:
                    sums[ringfile] = None
                    if err.errno != errno.ENOENT:
                        self.logger.exception(_('Error reading ringfile'))
        return sums

    def get_quarantine_count(self):
        """get obj/container/account quarantine counts"""
        qcounts = {"objects": 0, "containers": 0, "accounts": 0}
        qdir = "quarantined"
        for device in os.listdir(self.devices):
            for qtype in qcounts:
                qtgt = os.path.join(self.devices, device, qdir, qtype)
                if os.path.exists(qtgt):
                    linkcount = os.lstat(qtgt).st_nlink
                    if linkcount > 2:
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
            #handle old style object replication requests
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
        elif rcheck == "quarantined":
            content = self.get_quarantine_count()
        elif rcheck == "sockstat":
            content = self.get_socket_info()
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
