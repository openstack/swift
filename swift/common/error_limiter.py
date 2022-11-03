# Copyright (c) 2021 NVIDIA
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
import collections
from time import time

from swift.common.utils import node_to_string


class ErrorLimiter(object):
    """
    Tracks the number of errors that have occurred for nodes. A node will be
    considered to be error-limited for a given interval of time after it has
    accumulated more errors than a given limit.

    :param suppression_interval: The number of seconds for which a node is
        error-limited once it has accumulated more than ``suppression_limit``
        errors. Should be a float value.
    :param suppression_limit: The number of errors that a node must accumulate
        before it is considered to be error-limited. Should be an int value.
    """
    def __init__(self, suppression_interval, suppression_limit):
        self.suppression_interval = float(suppression_interval)
        self.suppression_limit = int(suppression_limit)
        self.stats = collections.defaultdict(dict)

    def node_key(self, node):
        """
        Get the key under which a node's error stats will be stored.

        :param node: dictionary describing a node.
        :return: string key.
        """
        return node_to_string(node)

    def is_limited(self, node):
        """
        Check if the node is currently error limited.

        :param node: dictionary of node to check
        :returns: True if error limited, False otherwise
        """
        now = time()
        node_key = self.node_key(node)
        error_stats = self.stats.get(node_key)

        if error_stats is None or 'errors' not in error_stats:
            return False

        if 'last_error' in error_stats and error_stats['last_error'] < \
                now - self.suppression_interval:
            self.stats.pop(node_key)
            return False
        return error_stats['errors'] > self.suppression_limit

    def limit(self, node):
        """
        Mark a node as error limited. This immediately pretends the
        node received enough errors to trigger error suppression. Use
        this for errors like Insufficient Storage. For other errors
        use :func:`increment`.

        :param node: dictionary of node to error limit
        """
        node_key = self.node_key(node)
        error_stats = self.stats[node_key]
        error_stats['errors'] = self.suppression_limit + 1
        error_stats['last_error'] = time()

    def increment(self, node):
        """
        Increment the error count and update the time of the last error for
        the given ``node``.

        :param node: dictionary describing a node.
        :returns: True if suppression_limit is exceeded, False otherwise
        """
        node_key = self.node_key(node)
        error_stats = self.stats[node_key]
        error_stats['errors'] = error_stats.get('errors', 0) + 1
        error_stats['last_error'] = time()
        return error_stats['errors'] > self.suppression_limit
