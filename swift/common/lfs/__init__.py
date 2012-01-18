# Copyright (c) 2010-2011 OpenStack, LLC.
# Copyright (c) Nexenta Systems Inc.
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

import os

from swift.common.utils import readconf

from lfs import LFS, LFSDefault, LFSStatus

zfs_supported = True if os.uname()[0] == 'SunOS' else False

if zfs_supported:
    from lfszfs import LFSZFS

def get_lfs(conf, srvdir, logger):
    fs = conf.get('fs', 'xfs')
    if conf.has_key('__file__'):
        fs_conf = readconf(conf['__file__'], fs)
        conf = dict(conf, **fs_conf)
    if fs == 'xfs':
        return LFSDefault(conf, srvdir, logger)
    elif zfs_supported and fs == 'zfs':
        return LFSZFS(conf, srvdir, logger)
    else:
        raise 'Cannot load LFS. Invalid FS : %s' %fs