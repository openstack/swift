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

import os
import sys
import signal
from re import sub

from swift.common import utils


class Daemon(object):
    """Daemon base class"""

    def __init__(self, conf):
        self.conf = conf
        self.logger = utils.get_logger(conf, log_route='daemon')

    def run_once(self, *args, **kwargs):
        """Override this to run the script once"""
        raise NotImplementedError('run_once not implemented')

    def run_forever(self, *args, **kwargs):
        """Override this to run forever"""
        raise NotImplementedError('run_forever not implemented')

    def run(self, once=False, **kwargs):
        """Run the daemon"""
        utils.validate_configuration()
        utils.drop_privileges(self.conf.get('user', 'swift'))
        utils.capture_stdio(self.logger, **kwargs)

        def kill_children(*args):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            sys.exit()

        signal.signal(signal.SIGTERM, kill_children)
        if once:
            self.run_once(**kwargs)
        else:
            self.run_forever(**kwargs)


def run_daemon(klass, conf_file, section_name='', once=False, **kwargs):
    """
    Loads settings from conf, then instantiates daemon "klass" and runs the
    daemon with the specified once kwarg.  The section_name will be derived
    from the daemon "klass" if not provided (e.g. ObjectReplicator =>
    object-replicator).

    :param klass: Class to instantiate, subclass of common.daemon.Daemon
    :param conf_file: Path to configuration file
    :param section_name: Section name from conf file to load config from
    :param once: Passed to daemon run method
    """
    # very often the config section_name is based on the class name
    # the None singleton will be passed through to readconf as is
    if section_name is '':
        section_name = sub(r'([a-z])([A-Z])', r'\1-\2',
                           klass.__name__).lower()
    conf = utils.readconf(conf_file, section_name,
                          log_name=kwargs.get('log_name'))

    # once on command line (i.e. daemonize=false) will over-ride config
    once = once or \
            conf.get('daemonize', 'true').lower() not in utils.TRUE_VALUES

    # pre-configure logger
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = utils.get_logger(conf, conf.get('log_name', section_name),
           log_to_console=kwargs.pop('verbose', False), log_route=section_name)
    try:
        klass(conf).run(once=once, **kwargs)
    except KeyboardInterrupt:
        logger.info('User quit')
    logger.info('Exited')
