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

import re
import sys


CANONICAL_SAMPLE_NAME = re.compile(r'^dispersion_[0-9]+$')
OBSOLETE_CONTAINER_NAME = re.compile(r'^dispersion_[0-9]+_[0-9]+$')


def container_dispersion_names(account_listing):
    """Return canonical samples for the shared container ring.

    The numeric suffix selects a hash path, not a storage policy. Indexed
    policy-era container names are obsolete and are only used for cleanup.
    """
    return [item['name'] for item in account_listing
            if CANONICAL_SAMPLE_NAME.match(item['name'])]


def obsolete_container_dispersion_names(account_listing):
    """Return policy-era container samples for warnings and cleanup."""
    return [item['name'] for item in account_listing
            if OBSOLETE_CONTAINER_NAME.match(item['name'])]


def obsolete_object_dispersion_containers(account_listing, policy_index):
    """Only policy 0 has an obsolete object sample container."""
    if policy_index != 0:
        return []
    return [item['name'] for item in account_listing
            if item['name'] == 'dispersion_objects_0']


def warn_on_obsolete_samples(sample_type, names):
    if names:
        print('WARNING: Ignoring %d obsolete %s dispersion names; '
              'use populate with --purge to replace the population and '
              'remove obsolete data.' %
              (len(names), sample_type), file=sys.stderr)
        sys.stderr.flush()


def all_container_dispersion_names(account_listing):
    """Return every container-ring dispersion sample name."""
    return [
        item['name'] for item in account_listing
        if (CANONICAL_SAMPLE_NAME.match(item['name']) or
            OBSOLETE_CONTAINER_NAME.match(item['name']))
    ]


def object_dispersion_names(container_listing):
    """Return the expected object sample names from a policy container."""
    return [
        item['name'] for item in container_listing
        if CANONICAL_SAMPLE_NAME.match(item['name'])
    ]


def object_dispersion_container(policy_index):
    """Return the canonical object sample container for a policy."""
    if policy_index == 0:
        return 'dispersion_objects'
    return 'dispersion_objects_%d' % policy_index


def object_dispersion_containers_to_purge(policy_index):
    """Return canonical and obsolete containers to clean up during purge."""
    container = object_dispersion_container(policy_index)
    if policy_index == 0:
        return [container, 'dispersion_objects_0']
    return [container]
