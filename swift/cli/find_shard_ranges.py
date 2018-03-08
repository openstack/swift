# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import print_function
import argparse
import json
import sys
import time

from swift.container.backend import ContainerBroker


def main(args=None):
    parser = argparse.ArgumentParser('Find and display shard ranges')
    parser.add_argument('container_db')
    parser.add_argument('rows_per_shard', nargs='?', type=int, default=500000)
    args = parser.parse_args(args)

    broker = ContainerBroker(args.container_db)

    start = time.time()
    ranges = broker.find_shard_ranges(args.rows_per_shard)[0]
    delta_t = time.time() - start

    print(json.dumps([dict(r) for r in ranges], sort_keys=True, indent=2))
    print('Found %d ranges in %gs' % (len(ranges), delta_t), file=sys.stderr)
    return 0

if __name__ == '__main__':
    exit(main())
