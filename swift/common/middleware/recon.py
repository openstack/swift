# Copyright (c) 2010-2011 OpenStack, LLC.
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

from webob import Request, Response
from swift.common.utils import split_path, cache_from_env, get_logger
from swift.common.constraints import check_mount
from hashlib import md5
import simplejson as json
import os


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
        self.recon_cache_path = conf.get('recon_cache_path', \
            '/var/cache/swift')
        self.object_recon_cache = "%s/object.recon" % self.recon_cache_path
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')
        self.object_ring_path = os.path.join(swift_dir, 'object.ring.gz')
        self.rings = [self.account_ring_path, self.container_ring_path, \
            self.object_ring_path]
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')

    def get_mounted(self):
        """get ALL mounted fs from /proc/mounts"""
        mounts = []
        with open('/proc/mounts', 'r') as procmounts:
            for line in procmounts:
                mount = {}
                mount['device'], mount['path'], opt1, opt2, opt3, \
                    opt4 = line.rstrip().split()
                mounts.append(mount)
        return mounts

    def get_load(self):
        """get info from /proc/loadavg"""
        loadavg = {}
        onemin, fivemin, ftmin, tasks, procs \
            = open('/proc/loadavg', 'r').readline().rstrip().split()
        loadavg['1m'] = float(onemin)
        loadavg['5m'] = float(fivemin)
        loadavg['15m'] = float(ftmin)
        loadavg['tasks'] = tasks
        loadavg['processes'] = int(procs)
        return loadavg

    def get_mem(self):
        """get info from /proc/meminfo"""
        meminfo = {}
        with open('/proc/meminfo', 'r') as memlines:
            for i in memlines:
                entry = i.rstrip().split(":")
                meminfo[entry[0]] = entry[1].strip()
        return meminfo

    def get_async_info(self):
        """get # of async pendings"""
        asyncinfo = {}
        with open(self.object_recon_cache, 'r') as f:
            recondata = json.load(f)
            if 'async_pending' in recondata:
                asyncinfo['async_pending'] = recondata['async_pending']
            else:
                self.logger.notice( \
                    _('NOTICE: Async pendings not in recon data.'))
                asyncinfo['async_pending'] = -1
        return asyncinfo

    def get_replication_info(self):
        """grab last object replication time"""
        repinfo = {}
        with open(self.object_recon_cache, 'r') as f:
            recondata = json.load(f)
            if 'object_replication_time' in recondata:
                repinfo['object_replication_time'] = \
                    recondata['object_replication_time']
            else:
                self.logger.notice( \
                    _('NOTICE: obj replication time not in recon data'))
                repinfo['object_replication_time'] = -1
        return repinfo

    def get_device_info(self):
        """place holder, grab dev info"""
        return self.devices

    def get_unmounted(self):
        """list unmounted (failed?) devices"""
        mountlist = []
        for entry in os.listdir(self.devices):
            mpoint = {'device': entry, \
                "mounted": check_mount(self.devices, entry)}
            if not mpoint['mounted']:
                mountlist.append(mpoint)
        return mountlist

    def get_diskusage(self):
        """get disk utilization statistics"""
        devices = []
        for entry in os.listdir(self.devices):
            if check_mount(self.devices, entry):
                path = "%s/%s" % (self.devices, entry)
                disk = os.statvfs(path)
                capacity = disk.f_bsize * disk.f_blocks
                available = disk.f_bsize * disk.f_bavail
                used = disk.f_bsize * (disk.f_blocks - disk.f_bavail)
                devices.append({'device': entry, 'mounted': True, \
                    'size': capacity, 'used': used, 'avail': available})
            else:
                devices.append({'device': entry, 'mounted': False, \
                    'size': '', 'used': '', 'avail': ''})
        return devices

    def get_ring_md5(self):
        """get all ring md5sum's"""
        sums = {}
        for ringfile in self.rings:
            md5sum = md5()
            with open(ringfile, 'rb') as f:
                block = f.read(4096)
                while block:
                    md5sum.update(block)
                    block = f.read(4096)
            sums[ringfile] = md5sum.hexdigest()
        return sums

    def get_quarantine_count(self):
        """get obj/container/account quarantine counts"""
        qcounts = {"objects": 0, "containers": 0, "accounts": 0}
        qdir = "quarantined"
        for device in os.listdir(self.devices):
            for qtype in qcounts:
                qtgt = os.path.join(self.devices, device, qdir, qtype)
                if os.path.exists(qtgt):
                    qcounts[qtype] += os.lstat(qtgt).st_nlink
        return qcounts

    def GET(self, req):
        error = False
        root, type = split_path(req.path, 1, 2, False)
        try:
            if type == "mem":
                content = json.dumps(self.get_mem())
            elif type == "load":
                try:
                    content = json.dumps(self.get_load(), sort_keys=True)
                except IOError as e:
                    error = True
                    content = "load - %s" % e
            elif type == "async":
                try:
                    content = json.dumps(self.get_async_info())
                except IOError as e:
                    error = True
                    content = "async - %s" % e
            elif type == "replication":
                try:
                    content = json.dumps(self.get_replication_info())
                except IOError as e:
                    error = True
                    content = "replication - %s" % e
            elif type == "mounted":
                content = json.dumps(self.get_mounted())
            elif type == "unmounted":
                content = json.dumps(self.get_unmounted())
            elif type == "diskusage":
                content = json.dumps(self.get_diskusage())
            elif type == "ringmd5":
                content = json.dumps(self.get_ring_md5())
            elif type == "quarantined":
                content = json.dumps(self.get_quarantine_count())
            else:
                content = "Invalid path: %s" % req.path
                return Response(request=req, status="400 Bad Request", \
                    body=content, content_type="text/plain")
        except ValueError as e:
            error = True
            content = "ValueError: %s" % e

        if not error:
            return Response(request=req, body=content, \
                content_type="application/json")
        else:
            msg = 'CRITICAL recon - %s' % str(content)
            self.logger.critical(msg)
            body = "Internal server error."
            return Response(request=req, status="500 Server Error", \
                body=body, content_type="text/plain")

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
