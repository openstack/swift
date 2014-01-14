# Copyright (c) 2010-2012 OpenStack, LLC.
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

from ConfigParser import NoOptionError, ConfigParser
from swift.common.utils import config_true_value, SWIFT_CONF_FILE

POLICY = 'X-Storage-Policy'
POLICY_INDEX = 'X-Storage-Policy-Index'


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use get_storage_policies().
    """
    def __init__(self, idx, name, is_default=False, object_ring=None):
        self.name = name
        self.idx = int(idx)
        self.is_default = is_default
        self.object_ring = object_ring

    def __repr__(self):
        return "StoragePolicy(%d, %r, is_default=%s, object_ring=%r)" % (
            self.idx, self.name, self.is_default, self.object_ring)

    @property
    def ring_name(self):
        if self.idx == 0:
            return 'object'
        else:
            return 'object-%d' % self.idx


class StoragePolicyCollection(object):
    """
    This class represents the collection of valid storage policies for
    the cluster and is instantiated when swift.conf is parsed; as policy
    objects (StoragePolicy) are added to the collection it assures that
    only one is specified as the default.  It also provides various
    accessor functions for the rest of the code.  Note:
    default:  means that the policy is used when creating a new container
              and no policy was explicitly specified
    0 policy: is the policy that is used when accessing a container where
              no policy was identified in the container metadata
    """
    def __init__(self, pols):
        global POLICIES
        # keep them indexed for quicker lookups
        self.pols_by_name = dict((pol.name, pol) for pol in pols)
        self.pols_by_index = dict((int(pol.idx), pol) for pol in pols)
        defaults = [pol for pol in pols if pol.is_default]
        if len(defaults) > 1:
            msg = "Too many default storage policies: %s" % \
                (", ".join((pol.name for pol in defaults)))
            raise ValueError(msg)
        self.default = defaults[0]
        POLICIES = self

    def __len__(self):
        return len(self.pols_by_index)

    def __getitem__(self, key):
        return self.pols[key]

    def __iter__(self):
        return self.pols_by_name.itervalues()

    def get_default(self):
        return self.default

    def get_by_name(self, name):
        """
        Find a storage policy by its name.

        :param name: name of the policy
        :returns: storage policy, or None
        """
        return self.pols_by_name.get(name)

    def get_by_index(self, index):
        """
        Find a storage policy by its index.

        An index of None will be treated as 0.

        :param index: numeric index of the storage policy
        :returns: storage policy, or None if no such policy
        """
        if index is None:
            index = 0
        return self.pols_by_index.get(int(index))


def parse_storage_policies(conf):
    """
    Parse storage policies in swift.conf making sure the syntax is correct
    and assuring that a "0 policy" will exist even if not specified and
    also that a "default policy" will exist even if not specified

    :param conf: ConfigParser parser object for swift.conf
    """
    policies = []
    names = []
    need_default = True
    need_pol0 = True
    for section in conf.sections():
        section_policy = section.split(':')
        if len(section_policy) > 1 and section_policy[0] == 'storage-policy':
            if section_policy[1].isdigit():
                policy_idx = int(section_policy[1])
                if policy_idx == 0:
                    need_pol0 = False
            else:
                raise ValueError("Malformed storage policy %s" % section)
            try:
                is_default = conf.get(section, 'default')
                if config_true_value(is_default):
                    need_default = False
            except NoOptionError:
                is_default = False
            try:
                policy_name = conf.get(section, 'name')
            except NoOptionError:
                policy_name = ''

            """ names must be unique if provided """
            if policy_name is not '':
                if policy_name in names:
                    raise ValueError("Duplicate policy name %s" % policy_name)
                else:
                    names.append(policy_name)

            policies.append(StoragePolicy(
                policy_idx,
                policy_name,
                is_default=config_true_value(is_default)))

    # If a 0 policy wasn't explicitly given, or nothing was
    # provided, create the 0 policy now
    if not policies or need_pol0:
        policies.append(StoragePolicy(0, '', False))

    # if needed, specify default of policy 0
    if need_default:
        for policy in policies:
            if policy.idx == 0:
                policy.is_default = True
    return StoragePolicyCollection(policies)

policy_conf = ConfigParser()
policy_conf.read(SWIFT_CONF_FILE)
POLICIES = parse_storage_policies(policy_conf)
