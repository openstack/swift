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
import string

from swift.common.utils import config_true_value, SWIFT_CONF_FILE
from swift.common.ring import Ring

LEGACY_POLICY_NAME = 'Policy-0'
VALID_CHARS = '-' + string.letters + string.digits


class PolicyError(ValueError):

    def __init__(self, msg, index=None):
        if index is not None:
            msg += ', for index %r' % index
        super(PolicyError, self).__init__(msg)


def _get_policy_string(base, policy_index):
    if policy_index == 0 or policy_index is None:
        return_string = base
    else:
        return_string = base + "-%d" % int(policy_index)
    return return_string


def get_policy_string(base, policy_index):
    """
    Helper function to construct a string from a base and the policy
    index.  Used to encode the policy index into either a file name
    or a directory name by various modules.

    :param base: the base string
    :param policy_index: the storage policy index

    :returns: base name with policy index added
    """
    if POLICIES.get_by_index(policy_index) is None:
        raise PolicyError("No policy with index %r" % policy_index)
    return _get_policy_string(base, policy_index)


class StoragePolicy(object):
    """
    Represents a storage policy.
    Not meant to be instantiated directly; use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.

    The object_ring property is lazy loaded once the service's ``swift_dir``
    is known via :meth:`~StoragePolicyCollection.get_object_ring`, but it may
    be over-ridden via object_ring kwarg at create time for testing or
    actively loaded with :meth:`~StoragePolicy.load_ring`.
    """
    def __init__(self, idx, name='', is_default=False, is_deprecated=False,
                 object_ring=None):
        try:
            self.idx = int(idx)
        except ValueError:
            raise PolicyError('Invalid index', idx)
        if self.idx < 0:
            raise PolicyError('Invalid index', idx)
        if not name:
            raise PolicyError('Invalid name %r' % name, idx)
        # this is defensively restrictive, but could be expanded in the future
        if not all(c in VALID_CHARS for c in name):
            raise PolicyError('Names are used as HTTP headers, and can not '
                              'reliably contain any characters not in %r. '
                              'Invalid name %r' % (VALID_CHARS, name))
        if name.upper() == LEGACY_POLICY_NAME.upper() and self.idx != 0:
            msg = 'The name %s is reserved for policy index 0. ' \
                'Invalid name %r' % (LEGACY_POLICY_NAME, name)
            raise PolicyError(msg, idx)
        self.name = name
        self.is_deprecated = config_true_value(is_deprecated)
        self.is_default = config_true_value(is_default)
        if self.is_deprecated and self.is_default:
            raise PolicyError('Deprecated policy can not be default.  '
                              'Invalid config', self.idx)
        self.ring_name = _get_policy_string('object', self.idx)
        self.object_ring = object_ring

    def __int__(self):
        return self.idx

    def __cmp__(self, other):
        return cmp(self.idx, int(other))

    def __repr__(self):
        return ("StoragePolicy(%d, %r, is_default=%s, is_deprecated=%s)") % (
            self.idx, self.name, self.is_default, self.is_deprecated)

    def load_ring(self, swift_dir):
        """
        Load the ring for this policy immediately.

        :param swift_dir: path to rings
        """
        if self.object_ring:
            return
        self.object_ring = Ring(swift_dir, ring_name=self.ring_name)


class StoragePolicyCollection(object):
    """
    This class represents the collection of valid storage policies for the
    cluster and is instantiated as :class:`StoragePolicy` objects are added to
    the collection when ``swift.conf`` is parsed by
    :func:`parse_storage_policies`.

    When a StoragePolicyCollection is created, the following validation
    is enforced:

    * If a policy with index 0 is not declared and no other policies defined,
      Swift will create one
    * The policy index must be a non-negative integer
    * If no policy is declared as the default and no other policies are
      defined, the policy with index 0 is set as the default
    * Policy indexes must be unique
    * Policy names are required
    * Policy names are case insensitive
    * Policy names must contain only letters, digits or a dash
    * Policy names must be unique
    * The policy name 'Policy-0' can only be used for the policy with index 0
    * If any policies are defined, exactly one policy must be declared default
    * Deprecated policies can not be declared the default

    """
    def __init__(self, pols):
        self.default = []
        self.by_name = {}
        self.by_index = {}
        self._validate_policies(pols)

    def _add_policy(self, policy):
        """
        Add pre-validated policies to internal indexes.
        """
        self.by_name[policy.name.upper()] = policy
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
        return iter(self.by_index.values())

    def _validate_policies(self, policies):
        """
        :param policies: list of policies
        """

        for policy in policies:
            if int(policy) in self.by_index:
                raise PolicyError('Duplicate index %s conflicts with %s' % (
                    policy, self.get_by_index(int(policy))))
            if policy.name.upper() in self.by_name:
                raise PolicyError('Duplicate name %s conflicts with %s' % (
                    policy, self.get_by_name(policy.name)))
            if policy.is_default:
                if not self.default:
                    self.default = policy
                else:
                    raise PolicyError(
                        'Duplicate default %s conflicts with %s' % (
                            policy, self.default))
            self._add_policy(policy)

        # If a 0 policy wasn't explicitly given, or nothing was
        # provided, create the 0 policy now
        if 0 not in self.by_index:
            if len(self) != 0:
                raise PolicyError('You must specify a storage policy '
                                  'section for policy index 0 in order '
                                  'to define multiple policies')
            self._add_policy(StoragePolicy(0, name=LEGACY_POLICY_NAME))

        # at least one policy must be enabled
        enabled_policies = [p for p in self if not p.is_deprecated]
        if not enabled_policies:
            raise PolicyError("Unable to find policy that's not deprecated!")

        # if needed, specify default
        if not self.default:
            if len(self) > 1:
                raise PolicyError("Unable to find default policy")
            self.default = self[0]
            self.default.is_default = True

    def get_by_name(self, name):
        """
        Find a storage policy by its name.

        :param name: name of the policy
        :returns: storage policy, or None
        """
        return self.by_name.get(name.upper())

    def get_by_index(self, index):
        """
        Find a storage policy by its index.

        An index of None will be treated as 0.

        :param index: numeric index of the storage policy
        :returns: storage policy, or None if no such policy
        """
        # makes it easier for callers to just pass in a header value
        index = int(index) if index else 0
        return self.by_index.get(index)

    def get_object_ring(self, policy_idx, swift_dir):
        """
        Get the ring object to use to handle a request based on its policy.

        An index of None will be treated as 0.

        :param policy_idx: policy index as defined in swift.conf
        :param swift_dir: swift_dir used by the caller
        :returns: appropriate ring object
        """
        policy = self.get_by_index(policy_idx)
        if not policy:
            raise PolicyError("No policy with index %s" % policy_idx)
        if not policy.object_ring:
            policy.load_ring(swift_dir)
        return policy.object_ring

    def get_policy_info(self):
        """
        Build info about policies for the /info endpoint

        :returns: list of dicts containing relevant policy information
        """
        policy_info = []
        for pol in self:
            # delete from /info if deprecated
            if pol.is_deprecated:
                continue
            policy_entry = {}
            policy_entry['name'] = pol.name
            if pol.is_default:
                policy_entry['default'] = pol.is_default
            policy_info.append(policy_entry)
        return policy_info


def parse_storage_policies(conf):
    """
    Parse storage policies in ``swift.conf`` - note that validation
    is done when the :class:`StoragePolicyCollection` is instantiated.

    :param conf: ConfigParser parser object for swift.conf
    """
    policies = []
    for section in conf.sections():
        if not section.startswith('storage-policy:'):
            continue
        policy_index = section.split(':', 1)[1]
        # map config option name to StoragePolicy parameter name
        config_to_policy_option_map = {
            'name': 'name',
            'default': 'is_default',
            'deprecated': 'is_deprecated',
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
    An instance of this class is the primary interface to storage policies
    exposed as a module level global named ``POLICIES``.  This global
    reference wraps ``_POLICIES`` which is normally instantiated by parsing
    ``swift.conf`` and will result in an instance of
    :class:`StoragePolicyCollection`.

    You should never patch this instance directly, instead patch the module
    level ``_POLICIES`` instance so that swift code which imported
    ``POLICIES`` directly will reference the patched
    :class:`StoragePolicyCollection`.
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
    """
    Reload POLICIES from ``swift.conf``.
    """
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
