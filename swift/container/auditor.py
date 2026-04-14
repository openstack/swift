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

import os

from swift.container.backend import ContainerBroker
from swift.common.daemon import run_daemon
from swift.common.db_auditor import DatabaseAuditor
from swift.common.exceptions import DatabaseHashLocationMismatch
from swift.common.utils import parse_options, hash_path


class ContainerAuditor(DatabaseAuditor):
    """Audit containers."""

    server_type = "container"
    broker_class = ContainerBroker

    def _audit(self, info, broker):
        account_name = info.get('account')
        container_name = info.get('container')

        if account_name and container_name:
            expected_hash = hash_path(account_name, container_name)
            actual_hash = os.path.basename(broker.db_dir)
            if expected_hash != actual_hash:
                raise DatabaseHashLocationMismatch(
                    'Hash of account/container names does not match directory '
                    'name for container %s/%s (possible hash_path_prefix/'
                    'suffix change in swift.conf)' % (account_name,
                                                      container_name))
        return None


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerAuditor, conf_file, **options)


if __name__ == '__main__':
    main()
