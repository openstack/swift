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


import logging
import os
import string
import sys
import textwrap
import six
from six.moves.configparser import ConfigParser
from swift.common.utils import (
    config_true_value, quorum_size, whataremyips, list_from_csv,
    config_positive_int_value, get_zero_indexed_base_string, load_pkg_resource)
from swift.common.ring import Ring, RingData
from swift.common import utils
from swift.common.exceptions import RingLoadError
from pyeclib.ec_iface import ECDriver, ECDriverError, VALID_EC_TYPES

LEGACY_POLICY_NAME = 'Policy-0'
VALID_CHARS = '-' + string.ascii_letters + string.digits

DEFAULT_POLICY_TYPE = REPL_POLICY = 'replication'
EC_POLICY = 'erasure_coding'

DEFAULT_EC_OBJECT_SEGMENT_SIZE = 1048576


class BindPortsCache(object):
    def __init__(self, swift_dir, bind_ip):
        self.swift_dir = swift_dir
        self.mtimes_by_ring_path = {}
        self.portsets_by_ring_path = {}
        self.my_ips = set(whataremyips(bind_ip))

    def all_bind_ports_for_node(self):
        """
        Given an iterable of IP addresses identifying a storage backend server,
        return a set of all bind ports defined in all rings for this storage
        backend server.

        The caller is responsible for not calling this method (which performs
        at least a stat on all ring files) too frequently.
        """
        # NOTE: we don't worry about disappearing rings here because you can't
        # ever delete a storage policy.

        for policy in POLICIES:
            # NOTE: we must NOT use policy.load_ring to load the ring.  Users
            # of this utility function will not need the actual ring data, just
            # the bind ports.
            #
            # This is duplicated with Ring.__init__ just a bit...
            serialized_path = os.path.join(self.swift_dir,
                                           policy.ring_name + '.ring.gz')
            try:
                new_mtime = os.path.getmtime(serialized_path)
            except OSError:
                continue
            old_mtime = self.mtimes_by_ring_path.get(serialized_path)
            if not old_mtime or old_mtime != new_mtime:
                self.portsets_by_ring_path[serialized_path] = set(
                    dev['port']
                    for dev in RingData.load(serialized_path,
                                             metadata_only=True).devs
                    if dev and dev['ip'] in self.my_ips)
                self.mtimes_by_ring_path[serialized_path] = new_mtime
                # No "break" here so that the above line will update the
                # mtimes_by_ring_path entry for any ring that changes, not just
                # the first one we notice.

        # Return the requested set of ports from our (now-freshened) cache
        return six.moves.reduce(set.union,
                                self.portsets_by_ring_path.values(), set())


class PolicyError(ValueError):
    def __init__(self, msg, index=None):
        if index is not None:
            msg += ', for index %r' % index
        super(PolicyError, self).__init__(msg)


def _get_policy_string(base, policy_index):
    return get_zero_indexed_base_string(base, policy_index)


def get_policy_string(base, policy_or_index):
    """
    Helper function to construct a string from a base and the policy.
    Used to encode the policy index into either a file name or a
    directory name by various modules.

    :param base: the base string
    :param policy_or_index: StoragePolicy instance, or an index
                            (string or int), if None the legacy
                            storage Policy-0 is assumed.

    :returns: base name with policy index added
    :raises PolicyError: if no policy exists with the given policy_index
    """
    if isinstance(policy_or_index, BaseStoragePolicy):
        policy = policy_or_index
    else:
        policy = POLICIES.get_by_index(policy_or_index)
        if policy is None:
            raise PolicyError("Unknown policy", index=policy_or_index)
    return _get_policy_string(base, int(policy))


def split_policy_string(policy_string):
    """
    Helper function to convert a string representing a base and a
    policy.  Used to decode the policy from either a file name or
    a directory name by various modules.

    :param policy_string: base name with policy index added

    :raises PolicyError: if given index does not map to a valid policy
    :returns: a tuple, in the form (base, policy) where base is the base
              string and policy is the StoragePolicy instance for the
              index encoded in the policy_string.
    """
    if '-' in policy_string:
        base, policy_index = policy_string.rsplit('-', 1)
    else:
        base, policy_index = policy_string, None
    policy = POLICIES.get_by_index(policy_index)
    if get_policy_string(base, policy) != policy_string:
        raise PolicyError("Unknown policy", index=policy_index)
    return base, policy


class BaseStoragePolicy(object):
    """
    Represents a storage policy.  Not meant to be instantiated directly;
    implement a derived subclasses (e.g. StoragePolicy, ECStoragePolicy, etc)
    or use :func:`~swift.common.storage_policy.reload_storage_policies` to
    load POLICIES from ``swift.conf``.

    The object_ring property is lazy loaded once the service's ``swift_dir``
    is known via :meth:`~StoragePolicyCollection.get_object_ring`, but it may
    be over-ridden via object_ring kwarg at create time for testing or
    actively loaded with :meth:`~StoragePolicy.load_ring`.
    """

    policy_type_to_policy_cls = {}

    def __init__(self, idx, name='', is_default=False, is_deprecated=False,
                 object_ring=None, aliases='',
                 diskfile_module='egg:swift#replication.fs'):
        # do not allow BaseStoragePolicy class to be instantiated directly
        if type(self) == BaseStoragePolicy:
            raise TypeError("Can't instantiate BaseStoragePolicy directly")
        # policy parameter validation
        try:
            self.idx = int(idx)
        except ValueError:
            raise PolicyError('Invalid index', idx)
        if self.idx < 0:
            raise PolicyError('Invalid index', idx)
        self.alias_list = []
        self.add_name(name)
        if aliases:
            names_list = list_from_csv(aliases)
            for alias in names_list:
                if alias == name:
                    continue
                self.add_name(alias)
        self.is_deprecated = config_true_value(is_deprecated)
        self.is_default = config_true_value(is_default)
        if self.policy_type not in BaseStoragePolicy.policy_type_to_policy_cls:
            raise PolicyError('Invalid type', self.policy_type)
        if self.is_deprecated and self.is_default:
            raise PolicyError('Deprecated policy can not be default.  '
                              'Invalid config', self.idx)

        self.ring_name = _get_policy_string('object', self.idx)
        self.object_ring = object_ring

        self.diskfile_module = diskfile_module

    @property
    def name(self):
        return self.alias_list[0]

    @name.setter
    def name_setter(self, name):
        self._validate_policy_name(name)
        self.alias_list[0] = name

    @property
    def aliases(self):
        return ", ".join(self.alias_list)

    def __int__(self):
        return self.idx

    def __eq__(self, other):
        return self.idx == int(other)

    def __ne__(self, other):
        return self.idx != int(other)

    def __lt__(self, other):
        return self.idx < int(other)

    def __gt__(self, other):
        return self.idx > int(other)

    def __repr__(self):
        return ("%s(%d, %r, is_default=%s, "
                "is_deprecated=%s, policy_type=%r)") % \
               (self.__class__.__name__, self.idx, self.alias_list,
                self.is_default, self.is_deprecated, self.policy_type)

    @classmethod
    def register(cls, policy_type):
        """
        Decorator for Storage Policy implementations to register
        their StoragePolicy class.  This will also set the policy_type
        attribute on the registered implementation.
        """

        def register_wrapper(policy_cls):
            if policy_type in cls.policy_type_to_policy_cls:
                raise PolicyError(
                    '%r is already registered for the policy_type %r' % (
                        cls.policy_type_to_policy_cls[policy_type],
                        policy_type))
            cls.policy_type_to_policy_cls[policy_type] = policy_cls
            policy_cls.policy_type = policy_type
            return policy_cls

        return register_wrapper

    @classmethod
    def _config_options_map(cls):
        """
        Map config option name to StoragePolicy parameter name.
        """
        return {
            'name': 'name',
            'aliases': 'aliases',
            'policy_type': 'policy_type',
            'default': 'is_default',
            'deprecated': 'is_deprecated',
            'diskfile_module': 'diskfile_module'
        }

    @classmethod
    def from_config(cls, policy_index, options):
        config_to_policy_option_map = cls._config_options_map()
        policy_options = {}
        for config_option, value in options.items():
            try:
                policy_option = config_to_policy_option_map[config_option]
            except KeyError:
                raise PolicyError('Invalid option %r in '
                                  'storage-policy section' % config_option,
                                  index=policy_index)
            policy_options[policy_option] = value
        return cls(policy_index, **policy_options)

    def get_info(self, config=False):
        """
        Return the info dict and conf file options for this policy.

        :param config: boolean, if True all config options are returned
        """
        info = {}
        for config_option, policy_attribute in \
                self._config_options_map().items():
            info[config_option] = getattr(self, policy_attribute)
        if not config:
            # remove some options for public consumption
            if not self.is_default:
                info.pop('default')
            if not self.is_deprecated:
                info.pop('deprecated')
            info.pop('policy_type')
            info.pop('diskfile_module')
        return info

    def _validate_policy_name(self, name):
        """
        Helper function to determine the validity of a policy name. Used
        to check policy names before setting them.

        :param name: a name string for a single policy name.
        :raises PolicyError: if the policy name is invalid.
        """
        if not name:
            raise PolicyError('Invalid name %r' % name, self.idx)
        # this is defensively restrictive, but could be expanded in the future
        if not all(c in VALID_CHARS for c in name):
            msg = 'Names are used as HTTP headers, and can not ' \
                  'reliably contain any characters not in %r. ' \
                  'Invalid name %r' % (VALID_CHARS, name)
            raise PolicyError(msg, self.idx)
        if name.upper() == LEGACY_POLICY_NAME.upper() and self.idx != 0:
            msg = 'The name %s is reserved for policy index 0. ' \
                  'Invalid name %r' % (LEGACY_POLICY_NAME, name)
            raise PolicyError(msg, self.idx)
        if name.upper() in (existing_name.upper() for existing_name
                            in self.alias_list):
            msg = 'The name %s is already assigned to this policy.' % name
            raise PolicyError(msg, self.idx)

    def add_name(self, name):
        """
        Adds an alias name to the storage policy. Shouldn't be called
        directly from the storage policy but instead through the
        storage policy collection class, so lookups by name resolve
        correctly.

        :param name: a new alias for the storage policy
        """
        self._validate_policy_name(name)
        self.alias_list.append(name)

    def remove_name(self, name):
        """
        Removes an alias name from the storage policy. Shouldn't be called
        directly from the storage policy but instead through the storage
        policy collection class, so lookups by name resolve correctly. If
        the name removed is the primary name then the next available alias
        will be adopted as the new primary name.

        :param name: a name assigned to the storage policy
        """
        if name not in self.alias_list:
            raise PolicyError("%s is not a name assigned to policy %s"
                              % (name, self.idx))
        if len(self.alias_list) == 1:
            raise PolicyError("Cannot remove only name %s from policy %s. "
                              "Policies must have at least one name."
                              % (name, self.idx))
        else:
            self.alias_list.remove(name)

    def change_primary_name(self, name):
        """
        Changes the primary/default name of the policy to a specified name.

        :param name: a string name to replace the current primary name.
        """
        if name == self.name:
            return
        elif name in self.alias_list:
            self.remove_name(name)
        else:
            self._validate_policy_name(name)
        self.alias_list.insert(0, name)

    def load_ring(self, swift_dir):
        """
        Load the ring for this policy immediately.

        :param swift_dir: path to rings
        """
        if self.object_ring:
            return
        self.object_ring = Ring(swift_dir, ring_name=self.ring_name)

    @property
    def quorum(self):
        """
        Number of successful backend requests needed for the proxy to
        consider the client request successful.
        """
        raise NotImplementedError()

    def get_diskfile_manager(self, *args, **kwargs):
        """
        Return an instance of the diskfile manager class configured for this
        storage policy.

        :param args: positional args to pass to the diskfile manager
            constructor.
        :param kwargs: keyword args to pass to the diskfile manager
            constructor.
        :return: A disk file manager instance.
        """
        try:
            dfm_cls = load_pkg_resource('swift.diskfile', self.diskfile_module)
        except ImportError as err:
            raise PolicyError(
                'Unable to load diskfile_module %s for policy %s: %s' %
                (self.diskfile_module, self.name, err))
        try:
            dfm_cls.check_policy(self)
        except ValueError as err:
            raise PolicyError(
                'Invalid diskfile_module %s for policy %s:%s (%s)' %
                (self.diskfile_module, int(self), self.name, self.policy_type))

        return dfm_cls(*args, **kwargs)


@BaseStoragePolicy.register(REPL_POLICY)
class StoragePolicy(BaseStoragePolicy):
    """
    Represents a storage policy of type 'replication'.  Default storage policy
    class unless otherwise overridden from swift.conf.

    Not meant to be instantiated directly; use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.
    """

    @property
    def quorum(self):
        """
        Quorum concept in the replication case:
            floor(number of replica / 2) + 1
        """
        if not self.object_ring:
            raise PolicyError('Ring is not loaded')
        return quorum_size(self.object_ring.replica_count)


@BaseStoragePolicy.register(EC_POLICY)
class ECStoragePolicy(BaseStoragePolicy):
    """
    Represents a storage policy of type 'erasure_coding'.

    Not meant to be instantiated directly; use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.
    """

    def __init__(self, idx, name='', aliases='', is_default=False,
                 is_deprecated=False, object_ring=None,
                 diskfile_module='egg:swift#erasure_coding.fs',
                 ec_segment_size=DEFAULT_EC_OBJECT_SEGMENT_SIZE,
                 ec_type=None, ec_ndata=None, ec_nparity=None,
                 ec_duplication_factor=1):

        super(ECStoragePolicy, self).__init__(
            idx=idx, name=name, aliases=aliases, is_default=is_default,
            is_deprecated=is_deprecated, object_ring=object_ring,
            diskfile_module=diskfile_module)

        # Validate erasure_coding policy specific members
        # ec_type is one of the EC implementations supported by PyEClib
        if ec_type is None:
            raise PolicyError('Missing ec_type')
        if ec_type not in VALID_EC_TYPES:
            raise PolicyError('Wrong ec_type %s for policy %s, should be one'
                              ' of "%s"' % (ec_type, self.name,
                                            ', '.join(VALID_EC_TYPES)))
        self._ec_type = ec_type

        # Define _ec_ndata as the number of EC data fragments
        # Accessible as the property "ec_ndata"
        try:
            value = int(ec_ndata)
            if value <= 0:
                raise ValueError
            self._ec_ndata = value
        except (TypeError, ValueError):
            raise PolicyError('Invalid ec_num_data_fragments %r' %
                              ec_ndata, index=self.idx)

        # Define _ec_nparity as the number of EC parity fragments
        # Accessible as the property "ec_nparity"
        try:
            value = int(ec_nparity)
            if value <= 0:
                raise ValueError
            self._ec_nparity = value
        except (TypeError, ValueError):
            raise PolicyError('Invalid ec_num_parity_fragments %r'
                              % ec_nparity, index=self.idx)

        # Define _ec_segment_size as the encode segment unit size
        # Accessible as the property "ec_segment_size"
        try:
            value = int(ec_segment_size)
            if value <= 0:
                raise ValueError
            self._ec_segment_size = value
        except (TypeError, ValueError):
            raise PolicyError('Invalid ec_object_segment_size %r' %
                              ec_segment_size, index=self.idx)

        if self._ec_type == 'isa_l_rs_vand' and self._ec_nparity >= 5:
            logger = logging.getLogger("swift.common.storage_policy")
            if not logger.handlers:
                # If nothing else, log to stderr
                logger.addHandler(logging.StreamHandler(sys.__stderr__))
            logger.warning(
                'Storage policy %s uses an EC configuration known to harm '
                'data durability. Any data in this policy should be migrated. '
                'See https://bugs.launchpad.net/swift/+bug/1639691 for '
                'more information.' % self.name)
            if not is_deprecated:
                raise PolicyError(
                    'Storage policy %s uses an EC configuration known to harm '
                    'data durability. This policy MUST be deprecated.'
                    % self.name)

        # Initialize PyECLib EC backend
        try:
            self.pyeclib_driver = \
                ECDriver(k=self._ec_ndata, m=self._ec_nparity,
                         ec_type=self._ec_type)
        except ECDriverError as e:
            raise PolicyError("Error creating EC policy (%s)" % e,
                              index=self.idx)

        # quorum size in the EC case depends on the choice of EC scheme.
        self._ec_quorum_size = \
            self._ec_ndata + self.pyeclib_driver.min_parity_fragments_needed()
        self._fragment_size = None

        self._ec_duplication_factor = \
            config_positive_int_value(ec_duplication_factor)

    @property
    def ec_type(self):
        return self._ec_type

    @property
    def ec_ndata(self):
        return self._ec_ndata

    @property
    def ec_nparity(self):
        return self._ec_nparity

    @property
    def ec_n_unique_fragments(self):
        return self._ec_ndata + self._ec_nparity

    @property
    def ec_segment_size(self):
        return self._ec_segment_size

    @property
    def fragment_size(self):
        """
        Maximum length of a fragment, including header.

        NB: a fragment archive is a sequence of 0 or more max-length
        fragments followed by one possibly-shorter fragment.
        """
        # Technically pyeclib's get_segment_info signature calls for
        # (data_len, segment_size) but on a ranged GET we don't know the
        # ec-content-length header before we need to compute where in the
        # object we should request to align with the fragment size.  So we
        # tell pyeclib a lie - from it's perspective, as long as data_len >=
        # segment_size it'll give us the answer we want.  From our
        # perspective, because we only use this answer to calculate the
        # *minimum* size we should read from an object body even if data_len <
        # segment_size we'll still only read *the whole one and only last
        # fragment* and pass than into pyeclib who will know what to do with
        # it just as it always does when the last fragment is < fragment_size.
        if self._fragment_size is None:
            self._fragment_size = self.pyeclib_driver.get_segment_info(
                self.ec_segment_size, self.ec_segment_size)['fragment_size']

        return self._fragment_size

    @property
    def ec_scheme_description(self):
        """
        This short hand form of the important parts of the ec schema is stored
        in Object System Metadata on the EC Fragment Archives for debugging.
        """
        return "%s %d+%d" % (self._ec_type, self._ec_ndata, self._ec_nparity)

    @property
    def ec_duplication_factor(self):
        return self._ec_duplication_factor

    def __repr__(self):
        extra_info = ''
        if self.ec_duplication_factor != 1:
            extra_info = ', ec_duplication_factor=%d' % \
                self.ec_duplication_factor
        return ("%s, EC config(ec_type=%s, ec_segment_size=%d, "
                "ec_ndata=%d, ec_nparity=%d%s)") % \
               (super(ECStoragePolicy, self).__repr__(), self.ec_type,
                self.ec_segment_size, self.ec_ndata, self.ec_nparity,
                extra_info)

    @classmethod
    def _config_options_map(cls):
        options = super(ECStoragePolicy, cls)._config_options_map()
        options.update({
            'ec_type': 'ec_type',
            'ec_object_segment_size': 'ec_segment_size',
            'ec_num_data_fragments': 'ec_ndata',
            'ec_num_parity_fragments': 'ec_nparity',
            'ec_duplication_factor': 'ec_duplication_factor',
        })
        return options

    def get_info(self, config=False):
        info = super(ECStoragePolicy, self).get_info(config=config)
        if not config:
            info.pop('ec_object_segment_size')
            info.pop('ec_num_data_fragments')
            info.pop('ec_num_parity_fragments')
            info.pop('ec_type')
            info.pop('ec_duplication_factor')
        return info

    @property
    def quorum(self):
        """
        Number of successful backend requests needed for the proxy to consider
        the client PUT request successful.

        The quorum size for EC policies defines the minimum number
        of data + parity elements required to be able to guarantee
        the desired fault tolerance, which is the number of data
        elements supplemented by the minimum number of parity
        elements required by the chosen erasure coding scheme.

        For example, for Reed-Solomon, the minimum number parity
        elements required is 1, and thus the quorum_size requirement
        is ec_ndata + 1.

        Given the number of parity elements required is not the same
        for every erasure coding scheme, consult PyECLib for
        min_parity_fragments_needed()
        """
        return self._ec_quorum_size * self.ec_duplication_factor

    def load_ring(self, swift_dir):
        """
        Load the ring for this policy immediately.

        :param swift_dir: path to rings
        """
        if self.object_ring:
            return

        def validate_ring_data(ring_data):
            """
            EC specific validation

            Replica count check - we need _at_least_ (#data + #parity) replicas
            configured.  Also if the replica count is larger than exactly that
            number there's a non-zero risk of error for code that is
            considering the number of nodes in the primary list from the ring.
            """

            configured_fragment_count = ring_data.replica_count
            required_fragment_count = \
                (self.ec_n_unique_fragments) * self.ec_duplication_factor
            if configured_fragment_count != required_fragment_count:
                raise RingLoadError(
                    'EC ring for policy %s needs to be configured with '
                    'exactly %d replicas. Got %s.' % (
                        self.name, required_fragment_count,
                        configured_fragment_count))

        self.object_ring = Ring(
            swift_dir, ring_name=self.ring_name,
            validation_hook=validate_ring_data)

    def get_backend_index(self, node_index):
        """
        Backend index for PyECLib

        :param node_index: integer of node index
        :return: integer of actual fragment index. if param is not an integer,
                 return None instead
        """
        try:
            node_index = int(node_index)
        except ValueError:
            return None

        return node_index % self.ec_n_unique_fragments


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
        for name in policy.alias_list:
            self.by_name[name.upper()] = policy
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
            for name in policy.alias_list:
                if name.upper() in self.by_name:
                    raise PolicyError('Duplicate name %s conflicts with %s' % (
                        policy, self.get_by_name(name)))
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
        if index in ('', None):
            index = 0
        else:
            try:
                index = int(index)
            except ValueError:
                return None
        return self.by_index.get(index)

    @property
    def legacy(self):
        return self.get_by_index(None)

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
            policy_entry = pol.get_info()
            policy_info.append(policy_entry)
        return policy_info

    def add_policy_alias(self, policy_index, *aliases):
        """
        Adds a new name or names to a policy

        :param policy_index: index of a policy in this policy collection.
        :param aliases: arbitrary number of string policy names to add.
        """
        policy = self.get_by_index(policy_index)
        for alias in aliases:
            if alias.upper() in self.by_name:
                raise PolicyError('Duplicate name %s in use '
                                  'by policy %s' % (alias,
                                                    self.get_by_name(alias)))
            else:
                policy.add_name(alias)
                self.by_name[alias.upper()] = policy

    def remove_policy_alias(self, *aliases):
        """
        Removes a name or names from a policy. If the name removed is the
        primary name then the next available alias will be adopted
        as the new primary name.

        :param aliases: arbitrary number of existing policy names to remove.
        """
        for alias in aliases:
            policy = self.get_by_name(alias)
            if not policy:
                raise PolicyError('No policy with name %s exists.' % alias)
            if len(policy.alias_list) == 1:
                raise PolicyError('Policy %s with name %s has only one name. '
                                  'Policies must have at least one name.' % (
                                      policy, alias))
            else:
                policy.remove_name(alias)
                del self.by_name[alias.upper()]

    def change_policy_primary_name(self, policy_index, new_name):
        """
        Changes the primary or default name of a policy. The new primary
        name can be an alias that already belongs to the policy or a
        completely new name.

        :param policy_index: index of a policy in this policy collection.
        :param new_name: a string name to set as the new default name.
        """
        policy = self.get_by_index(policy_index)
        name_taken = self.get_by_name(new_name)
        # if the name belongs to some other policy in the collection
        if name_taken and name_taken != policy:
            raise PolicyError('Other policy %s with name %s exists.' %
                              (self.get_by_name(new_name).idx, new_name))
        else:
            policy.change_primary_name(new_name)
            self.by_name[new_name.upper()] = policy


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
        config_options = dict(conf.items(section))
        policy_type = config_options.pop('policy_type', DEFAULT_POLICY_TYPE)
        policy_cls = BaseStoragePolicy.policy_type_to_policy_cls[policy_type]
        policy = policy_cls.from_config(policy_index, config_options)
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
    if six.PY2:
        policy_conf = ConfigParser()
    else:
        # Python 3.2 disallows section or option duplicates by default
        # strict=False allows us to preserve the older behavior
        policy_conf = ConfigParser(strict=False)
    policy_conf.read(utils.SWIFT_CONF_FILE)
    try:
        _POLICIES = parse_storage_policies(policy_conf)
    except PolicyError as e:
        raise SystemExit('ERROR: Invalid Storage Policy Configuration '
                         'in %s (%s)' % (utils.SWIFT_CONF_FILE, e))


# parse configuration and setup singleton
_POLICIES = None
reload_storage_policies()
POLICIES = StoragePolicySingleton()
