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
from swift.common.ring import Ring

POLICY = 'X-Storage-Policy'
POLICY_INDEX = 'X-Storage-Policy-Index'


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use get_storage_policies().
    """
    def __init__(self, idx, name, is_default=False, object_ring=None,
                 policy_type='replication'):
        self.name = name
        self.idx = int(idx)
        self.is_default = is_default
        self.object_ring = object_ring
        self.policy_type = policy_type

    def __repr__(self):
        return "StoragePolicy(%d, %r, is_default=%s, type=%s " \
            "object_ring=%r)" % (self.idx, self.name, self.is_default,
            self.policy_type, self.object_ring)

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
        # keep them indexed for quicker lookups
        self.pols_by_name = dict((pol.name, pol) for pol in pols)
        self.pols_by_index = dict((int(pol.idx), pol) for pol in pols)
        defaults = [pol for pol in pols if pol.is_default]
        if len(defaults) > 1:
            msg = "Too many default storage policies: %s" % \
                (", ".join((pol.name for pol in defaults)))
            raise ValueError(msg)
        self.default = defaults[0]

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

    def get_object_ring(self, policy_idx, swift_dir):
        """
        Get the ring object to use to handle a request based on its policy.

        :policy_idx: policy index as defined in swift.conf
        :policy_idx: swift_dir used by the caller
        :returns: appropriate ring object
        """
        if policy_idx is None:
            policy_idx = 0
        else:
            # makes it easier for callers to just pass in a header value
            policy_idx = int(policy_idx)
        policy = self.get_by_index(policy_idx)
        if not policy:
            raise ValueError("No policy with index %d" % policy_idx)
        if not policy.object_ring:
            policy.object_ring = Ring(swift_dir,
                                      ring_name=policy.ring_name)
        return policy.object_ring

    def get_policy_info(self):
        """
        Build info about policies for the /Info endpoint

        :returns: list of dicts containing relevant policy information
        """
        policy_info = []
        for pol in self:
            policy_entry = {}
            policy_entry['name'] = pol.name
            policy_entry['type'] = pol.policy_type
            policy_info.append(policy_entry)
        return policy_info


def parse_storage_policies(conf):
    """
    Parse storage policies in swift.conf making sure the syntax is correct
    and assuring that a "0 policy" will exist even if not specified and
    also that a "default policy" will exist even if not specified

    :param conf: ConfigParser parser object for swift.conf
    """
    policies = []
    names = []
    valid_types = ['replication']
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
                raise ValueError("Missing policy name %s" % section)
            """ names must be unique """
            if policy_name in names:
                raise ValueError("Duplicate policy name %s" % policy_name)
            else:
                names.append(policy_name)
            try:
                policy_type = conf.get(section, 'type')
                # validate policy type, if unknown then error
                if policy_type not in valid_types:
                    raise ValueError("Invalid policy type %s" % policy_type)
            except NoOptionError:
                policy_type = 'replication'

            policies.append(StoragePolicy(
                policy_idx,
                policy_name,
                is_default=config_true_value(is_default),
                policy_type=policy_type))

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


class StoragePolicySingleton(object):
    """
    An instance of this class is global references as the module level global
    POLICIES.   This global references wraps _POLICIES who is normally
    instanticated by parsing swift.conf and will result in an instance of
    StoragePolicyCollection.

    You should never patch this instance directly, it services all requests to
    the module level global _POLICIES
    """

    def __iter__(self):
        return iter(_POLICIES)

    def __len__(self):
        return len(_POLICIES)

    def __getattribute__(self, name):
        return getattr(_POLICIES, name)

# parse configuration and setup singleton
policy_conf = ConfigParser()
policy_conf.read(SWIFT_CONF_FILE)
_POLICIES = parse_storage_policies(policy_conf)
POLICIES = StoragePolicySingleton()
