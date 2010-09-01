# Copyright (c) 2010 OpenStack, LLC.
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
import sys
import signal
from swift.common import utils

class Daemon(object):
    """Daemon base class"""

    def __init__(self, conf):
        self.conf = conf
        self.logger = utils.get_logger(conf, 'swift-daemon')

    def run_once(self):
        """Override this to run the script once"""
        raise NotImplementedError('run_once not implemented')

    def run_forever(self):
        """Override this to run forever"""
        raise NotImplementedError('run_forever not implemented')

    def run(self, once=False):
        """Run the daemon"""
        # log uncaught exceptions
        sys.excepthook = lambda *exc_info: \
            self.logger.critical('UNCAUGHT EXCEPTION', exc_info=exc_info)
        sys.stdout = sys.stderr = utils.LoggerFileObject(self.logger)

        utils.drop_privileges(self.conf.get('user', 'swift'))

        try:
            os.setsid()
        except OSError:
            pass

        def kill_children(*args):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            sys.exit()

        signal.signal(signal.SIGTERM, kill_children)

        if once:
            self.run_once()
        else:
            self.run_forever()
