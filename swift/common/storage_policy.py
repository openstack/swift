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

from ConfigParser import ConfigParser
import textwrap

from swift.common.utils import config_true_value, SWIFT_CONF_FILE
from swift.common.ring import Ring

POLICY = 'X-Storage-Policy'
POLICY_INDEX = 'X-Backend-Storage-Policy-Index'
VALID_TYPES = [
    'replication',
]
DEFAULT_TYPE = VALID_TYPES[0]


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


class PolicyError(ValueError):

    def __init__(self, msg, index=None):
        if index is not None:
            msg += ', for index %r' % index
        super(PolicyError, self).__init__(msg)


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use get_storage_policies().

    The object_ring property is lazy loaded once the service's swift_dir
    is known via the Collection's get_object_ring method, but it may be
    over-ridden via object_ring kwarg at create time for testing.
    """
    def __init__(self, idx, name='', is_default=False,
                 policy_type=DEFAULT_TYPE, object_ring=None):
        try:
            self.idx = int(idx)
        except ValueError:
            raise PolicyError('Invalid index', idx)
        if self.idx < 0:
            raise PolicyError('Invalid index', idx)
        if not name:
            raise PolicyError('Invalid name', idx)
        self.name = name
        self.is_default = config_true_value(is_default)
        if policy_type not in VALID_TYPES:
            raise PolicyError('Invalid type', idx)
        self.policy_type = policy_type
        self.ring_name = 'object'
        if self.idx > 0:
            self.ring_name += '-%d' % self.idx
        self.object_ring = object_ring

    def __int__(self):
        return self.idx

    def __repr__(self):
        return "StoragePolicy(%d, %r, is_default=%s, policy_type=%r)" % (
            self.idx, self.name, self.is_default, self.policy_type)

    def load_ring(self, swift_dir):
        if self.object_ring:
            return
        self.object_ring = Ring(swift_dir, ring_name=self.ring_name)


class StoragePolicyCollection(object):
    """
    This class represents the collection of valid storage policies for
    the cluster and is instantiated when swift.conf is parsed; as policy
    objects (StoragePolicy) are added to the collection it assures that
    only one is specified as the default.  It also provides various
    accessor functions for the rest of the code.  Note:
    default:  means that the policy is used when creating a new container
    and no policy was explicitly specified.
    0 policy: is the policy that is used when accessing a container where
    no policy was identified in the container metadata.
    """
    def __init__(self, pols):
        self.default = []
        self.by_name = {}
        self.by_index = {}
        self._validate_policies(pols)

    def add_policy(self, policy):
        # keep them indexed for quicker lookups
        self.by_name[policy.name] = policy
        self.by_index[int(policy)] = policy

    def __repr__(self):
        return (textwrap.dedent("""
    StoragePolicyCollection([
        %s
    ])
    """) % ',\n    '.join(repr(p) for p in self)).strip()

    def __len__(self):
        return len(self.by_index)

    def __getitem__(self, key):
        return self.by_index[key]

    def __iter__(self):
        return self.by_index.itervalues()

    def _validate_policies(self, policies):
        """
        :param policies: list of policies (modified here)
        """

        for policy in policies:
            if int(policy) in self.by_index:
                raise PolicyError('Duplicate index %s conflicts with %s' % (
                    policy, self.get_by_index(int(policy))))
            if policy.name in self.by_name:
                raise PolicyError('Duplicate name %s conflicts with %s' % (
                    policy, self.get_by_name(policy.name)))
            if policy.is_default:
                if not self.default:
                    self.default = policy
                else:
                    raise PolicyError(
                        'Duplicate default %s conflicts with %s' % (
                            policy, self.default))
            self.add_policy(policy)

        # If a 0 policy wasn't explicitly given, or nothing was
        # provided, create the 0 policy now
        if 0 not in self.by_index:
            self.add_policy(StoragePolicy(0, 'Policy_0', False))

        # if needed, specify default of policy 0
        if not self.default:
            self.default = self[0]
            self.default.is_default = True

    def get_by_name(self, name):
        """
        Find a storage policy by its name.

        :param name: name of the policy
        :returns: storage policy, or None
        """
        return self.by_name.get(name)

    def get_by_index(self, index):
        """
        Find a storage policy by its index.

        An index of None will be treated as 0.

        :param index: numeric index of the storage policy
        :returns: storage policy, or None if no such policy
        """
        if index is None:
            index = 0
        return self.by_index.get(int(index))

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
            policy.load_ring(swift_dir)
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

    When a StoragePolicyCollection is created, the following validation
    is enforced:
        * if no pol 0 or no policies defined, pol 0 is declared
        * policy index must be integer
        * if no policy is decalred default, pol 0 is the default
        * policy indexes must be unique
        * policy names are required
        * policy names must be unique
        * no more than 1 policy can be declared default
        * only supported types are allowed
        * if no type is provided, 'replication' is the default

    """
    policies = []
    for section in conf.sections():
        if not section.startswith('storage-policy'):
            continue
        try:
            policy_prefix, policy_index = section.split(':', 1)
        except ValueError:
            raise PolicyError('Invalid storage-policy section %r' % section)
        if policy_prefix != 'storage-policy':
            raise PolicyError('Invalid storage-policy section %r' % section)
        # map config option name to StoragePolicy paramater name
        config_to_policy_option_map = {
            'name': 'name',
            'default': 'is_default',
            'type': 'policy_type',
        }
        policy_options = {}
        for config_option, value in conf.items(section):
            try:
                policy_option = config_to_policy_option_map[config_option]
            except KeyError:
                raise PolicyError('Invalid option %r in '
                                  'storage-policy section %r' % (
                                      config_option, section))
            policy_options[policy_option] = value
        policy = StoragePolicy(policy_index, **policy_options)
        policies.append(policy)

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

    def __repr__(self):
        return repr(_POLICIES)


def reload_storage_policies():
    global _POLICIES
    policy_conf = ConfigParser()
    policy_conf.read(SWIFT_CONF_FILE)
    try:
        _POLICIES = parse_storage_policies(policy_conf)
    except PolicyError as e:
        raise SystemExit('ERROR: Invalid Storage Policy Configuration '
                         'in %s (%s)' % (SWIFT_CONF_FILE, e))


# parse configuration and setup singleton
_POLICIES = None
reload_storage_policies()
POLICIES = StoragePolicySingleton()
