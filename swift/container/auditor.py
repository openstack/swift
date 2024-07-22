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


from swift.container.backend import ContainerBroker
from swift.common.daemon import run_daemon
from swift.common.db_auditor import DatabaseAuditor
from swift.common.utils import parse_options


class ContainerAuditor(DatabaseAuditor):
    """Audit containers."""

    server_type = "container"
    broker_class = ContainerBroker

    def _audit(self, job, broker):
        return None


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerAuditor, conf_file, **options)


if __name__ == '__main__':
    main()
