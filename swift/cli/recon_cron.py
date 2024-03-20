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

import os
import sys
import time

from eventlet import Timeout

from swift.common.utils import get_logger, dump_recon_cache, readconf, \
    lock_path, listdir
from swift.common.recon import RECON_OBJECT_FILE, DEFAULT_RECON_CACHE_PATH
from swift.obj.diskfile import ASYNCDIR_BASE


def get_async_count(device_dir):
    async_count = 0
    for i in listdir(device_dir):
        device = os.path.join(device_dir, i)
        if not os.path.isdir(device):
            continue
        for asyncdir in listdir(device):
            # skip stuff like "accounts", "containers", etc.
            if not (asyncdir == ASYNCDIR_BASE or
                    asyncdir.startswith(ASYNCDIR_BASE + '-')):
                continue
            async_pending = os.path.join(device, asyncdir)

            if os.path.isdir(async_pending):
                for entry in listdir(async_pending):
                    if os.path.isdir(os.path.join(async_pending, entry)):
                        async_hdir = os.path.join(async_pending, entry)
                        async_count += len(listdir(async_hdir))
    return async_count


def main():
    try:
        conf_path = sys.argv[1]
    except Exception:
        print("Usage: %s CONF_FILE" % sys.argv[0].split('/')[-1])
        print("ex: swift-recon-cron /etc/swift/object-server.conf")
        return 1
    conf = readconf(conf_path, 'filter:recon')
    device_dir = conf.get('devices', '/srv/node')
    recon_cache_path = conf.get('recon_cache_path', DEFAULT_RECON_CACHE_PATH)
    recon_lock_path = conf.get('recon_lock_path', '/var/lock')
    cache_file = os.path.join(recon_cache_path, RECON_OBJECT_FILE)
    lock_dir = os.path.join(recon_lock_path, "swift-recon-object-cron")
    conf['log_name'] = conf.get('log_name', 'recon-cron')
    logger = get_logger(conf, log_route='recon-cron')
    try:
        with lock_path(lock_dir):
            asyncs = get_async_count(device_dir)
            dump_recon_cache({
                'async_pending': asyncs,
                'async_pending_last': time.time(),
            }, cache_file, logger)
    except (Exception, Timeout) as err:
        msg = 'Exception during recon-cron while accessing devices'
        logger.exception(msg)
        print('%s: %s' % (msg, err))
        return 1
