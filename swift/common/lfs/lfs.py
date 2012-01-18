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
import time
from threading import Thread

from swift.common.utils import mkdirs

class LFS(object):
    """Base class for all FS"""

    def __init__(self, conf, srvdir, logger):
        self.logger = logger
        self.conf = conf
        self.root = conf.get('devices', '/srv/node')
        self.srvdir = srvdir

    def setup_partition(self, device, partition):
        path = os.path.join(self.root, device, self.srvdir, partition)
        mkdirs(path)

    def setup_objdir(self, device, partition, name_hash):
        """Creation of objdir is done by object server by default."""
        pass

    def tmp_dir(self, device, partition, name_hash):
        return os.path.join(self.root, device, self.srvdir, 'tmp')

class LFSDefault(LFS):
    def __init__(self, conf, srvdir, logger):
        LFS.__init__(self, conf, srvdir, logger)
        self.fs = "xfs"


class LFSStatus(Thread):
    """
    Status Checker thread which checks the status of filesystem and
    calls back to LFS if it sees any issues.
    This thread periodically calls the check_func, if the filesystem is
    healthy. If check_func returns non-zero, it temporarily stops checking
    until the fault is cleared.

    :param interval: interval in seconds for checking FS
    :param check_func: method for checking FS. Takes exactly one argument
                       which should be a tuple. Returns 0 if FS is healthy
    :param check_args: tuple argument to check_func
    """

    def __init__(self, interval, check_func, check_args):
        Thread.__init__(self)
        self.interval = interval
        self.check_func = check_func
        self.check_args = check_args
        self.faulty = False
        self.daemon = True

    def run(self):
        while True:
            if not self.faulty:
                ret = self.check_func(self.check_args)
                if ret != None:
                    # ret must be a tuple (<callback function>, <args>)
                    self.faulty = True
                    ret[0](ret[1])

            time.sleep(self.interval)

    def clear_fault(self):
        """
        Clears the fault, so that status check thread can resume.
        """
        self.faulty = False
