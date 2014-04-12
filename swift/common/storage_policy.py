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


def get_policy_string(base, policy_idx):
    """
    Helper function to construct a string from a base and the policy
    index.  Used to encode the policy index into either a file name
    or a directory name by various modules.

    :param base: the base string
    :param policy_idx: the storage policy index

    :returns: base name with policy index added
    """
    if policy_idx == 0 or policy_idx is None:
        return_string = base
    else:
        if POLICIES.get_by_index(policy_idx) is None:
            raise ValueError("No policy with index %r" % policy_idx)
        return_string = base + "-%d" % int(policy_idx)
    return return_string


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use get_storage_policies().
    """
    def __init__(self, idx, name, is_default=False, object_ring=None,
                 policy_type='replication'):
        self.name = name
        try:
            self.idx = int(idx)
        except ValueError:
            # we'll raise this when we validate the entire policy collection
            self.idx = 'error'
        self.is_default = is_default
        self.object_ring = object_ring
        self.policy_type = policy_type

    def __repr__(self):
        return "StoragePolicy(%d, %r, is_default=%s, type=%s, " \
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
        self.default = []
        self._validate_policies(pols)
        # keep them indexed for quicker lookups
        self.pols_by_name = dict((pol.name, pol) for pol in pols)
        self.pols_by_index = dict((int(pol.idx), pol) for pol in pols)

    def __len__(self):
        return len(self.pols_by_index)

    def __getitem__(self, key):
        return self.pols_by_index[key]

    def __iter__(self):
        return self.pols_by_index.itervalues()

    def _validate_policies(self, policies):
        """
        Validates semantics of policies read in from conf file.
        - if no pol 0 or no policies defined, pol 0 is declared
        - policy index must be integer
        - if no policy is decalred default, pol 0 is the default
        - policy indexes must be unique
        - policy names are required
        - policy names must be unique
        - no more than 1 policy can be declared default
        - only supported types are allowed
        - if no type is provided, 'replication' is the default

        :param policies: list of policies (modified here)
        """
        valid_types = ['replication']
        names = []
        indexes = []
        defaults = 0
        need_default = True
        need_pol0 = True
        msg = ''

        for pol in policies:
            # check the index
            try:
                pol.idx = int(pol.idx)
                if pol.idx < 0:
                    raise ValueError
            except ValueError:
                msg = 'Invalid policy index %s' % pol.idx
                break
            if pol.idx in indexes:
                msg = 'Duplicate policy index %s' % pol.idx
                break
            indexes.append(pol.idx)
            if pol.idx == 0:
                need_pol0 = False

            # check the name
            if pol.name == '':
                msg = 'Missing policy name for index %s' % pol.idx
                break
            if pol.name in names:
                msg = 'Duplicate policy name %s' % pol.name
                break
            names.append(pol.name)

            # check the type
            if pol.policy_type == '':
                pol.policy_type = 'replication'
            if pol.policy_type not in valid_types:
                msg = 'Invalid policy type %s' % pol.policy_type
                break

            # only allowed one default
            if pol.is_default is True:
                defaults += 1
                if defaults > 1:
                    msg = 'More than one default specified, index %s' % \
                        pol.idx
                    break
                need_default = False
                self.default = pol

        if msg == '':
            # If a 0 policy wasn't explicitly given, or nothing was
            # provided, create the 0 policy now
            if not policies or need_pol0:
                policies.append(StoragePolicy(0, 'Policy_0', False))

            # if needed, specify default of policy 0
            if need_default:
                for pol in policies:
                    if pol.idx == 0:
                        pol.is_default = True
                        self.default = pol

        if msg:
            raise ValueError(msg)

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
            if pol.is_default:
                policy_entry['default'] = pol.is_default
            policy_info.append(policy_entry)
        return policy_info


def parse_storage_policies(conf):
    """
    Parse storage policies in swift.conf - note that validation
    is done when the StoragePolicyCollection is instantiated

    :param conf: ConfigParser parser object for swift.conf
    """
    policies = []
    for section in conf.sections():
        section_policy = section.split(':')
        if len(section_policy) >= 1 and section_policy[0] == 'storage-policy':
            if len(section_policy) != 2:
                raise ValueError('Wrong storage policy section name: \
                                 [%s]' % (section))
            policy = {'idx': '', 'name': '', 'type': '', 'default': ''}
            policy['idx'] = section_policy[1]
            # accept any value and will valiate later
            for sec_name in ('name', 'type', 'default'):
                try:
                    policy[sec_name] = conf.get(section, sec_name)
                except NoOptionError:
                # just ignore missing sections, will handle those
                # within StoragePolicyCollection policy validation
                    pass

            policies.append(StoragePolicy(
                policy['idx'], policy['name'],
                is_default=config_true_value(policy['default']),
                policy_type=policy['type']))

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

    def __getitem__(self, key):
        return _POLICIES[key]

    def __getattribute__(self, name):
        return getattr(_POLICIES, name)

# parse configuration and setup singleton
policy_conf = ConfigParser()
policy_conf.read(SWIFT_CONF_FILE)
_POLICIES = parse_storage_policies(policy_conf)
POLICIES = StoragePolicySingleton()
