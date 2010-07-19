#!/usr/bin/python
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

import sys
from ConfigParser import ConfigParser
import logging
import time

from eventlet import sleep, hubs
hubs.use_hub('poll')

from swift.obj.replicator import ObjectReplicator
from swift.common.utils import get_logger, drop_privileges, LoggerFileObject

TRUE_VALUES = set(('true', '1', 'yes', 'True', 'Yes'))

def read_configs(conf_file):
    c = ConfigParser()
    if not c.read(conf_file):
        print "Unable to read config file: %s" % conf_file
        sys.exit(1)
    conf = dict(c.items('object-server'))
    repl_conf = dict(c.items('object-replicator'))
    if not repl_conf:
        sys.exit()
    conf['replication_concurrency'] = repl_conf.get('concurrency',1)
    conf['vm_test_mode'] = repl_conf.get('vm_test_mode', 'no')
    conf['daemonize'] = repl_conf.get('daemonize', 'yes')
    conf['run_pause'] = repl_conf.get('run_pause', '30')
    conf['log_facility'] = repl_conf.get('log_facility', 'LOG_LOCAL1')
    conf['log_level'] = repl_conf.get('log_level', 'INFO')
    conf['timeout'] = repl_conf.get('timeout', '5')
    conf['stats_interval'] = repl_conf.get('stats_interval', '3600')
    conf['reclaim_age'] = int(repl_conf.get('reclaim_age', 86400))

    return conf

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: object-replicator CONFIG_FILE [once]"
        sys.exit()
    try:
        conf = read_configs(sys.argv[1])
    except:
        print "Problem reading the config.  Aborting object replication."
        sys.exit()
    once = len(sys.argv) > 2 and sys.argv[2] == 'once'
    logger = get_logger(conf, 'object-replicator')
    # log uncaught exceptions
    sys.excepthook = lambda *exc_info: \
                logger.critical('UNCAUGHT EXCEPTION', exc_info=exc_info)
    sys.stdout = sys.stderr = LoggerFileObject(logger)
    drop_privileges(conf.get('user', 'swift'))
    if not once and conf.get('daemonize', 'true') in TRUE_VALUES:
        logger.info("Starting object replicator in daemon mode.")
        # Run the replicator continually
        while True:
            start = time.time()
            logger.info("Starting object replication pass.")
            # Run the replicator
            replicator = ObjectReplicator(conf, logger)
            replicator.run()
            total = (time.time() - start)/60
            # Reload the config
            logger.info("Object replication complete. (%.02f minutes)" % total)
            conf = read_configs(sys.argv[1])
            if conf.get('daemonize', 'true') not in TRUE_VALUES:
                # Stop running
                logger.info("Daemon mode turned off in config, stopping.")
                break
            logger.debug('Replication sleeping for %s seconds.' %
                conf['run_pause'])
            sleep(int(conf['run_pause']))
    else:
        start = time.time()
        logger.info("Running object replicator in script mode.")
        replicator = ObjectReplicator(conf, logger)
        replicator.run()
        total = (time.time() - start)/60
        logger.info("Object replication complete. (%.02f minutes)" % total)
