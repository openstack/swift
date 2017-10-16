#!/usr/bin/env python
# Copyright (c) 2014 OpenStack Foundation
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
from swift.common.utils import parse_options
from swift.common import utils

utils.SWIFT_CONF_FILE = 'conf/swift.conf'

from swift.common.wsgi import run_wsgi

if __name__ == '__main__':
    server = sys.argv.pop(1)
    port = sys.argv.pop(1)
    conf_file, options = parse_options()
    sys.exit(run_wsgi(conf_file, server + '-server', default_port=port,
                      **options))
