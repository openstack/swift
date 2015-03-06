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
import collections
import textwrap
import string

from swift.common.utils import config_true_value, SWIFT_CONF_FILE
from swift.common.ring import Ring
from swift.common.utils import config_auto_int_value, replication_quorum_size
from swift.common.exceptions import RingValidationError
from pyeclib.ec_iface import ECDriver, ECDriverError
from pyeclib.core import ECPyECLibException

LEGACY_POLICY_NAME = 'Policy-0'
VALID_CHARS = '-' + string.letters + string.digits
VALID_TYPES = (
    REPL_POLICY,
    EC_POLICY,
) = (
    'replication',
    'erasure_coding',
)
DEFAULT_POLICY_TYPE = VALID_TYPES[0]
DEFAULT_EC_OBJECT_SEGMENT_SIZE = 1048576


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
    Not meant to be instantiated directly; implement one of the derived
    classes (ReplicationStoragePolicy, ECStoragePolicy, etc) or use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.

    The object_ring property is lazy loaded once the service's ``swift_dir``
    is known via :meth:`~StoragePolicyCollection.get_object_ring`, but it may
    be over-ridden via object_ring kwarg at create time for testing or
    actively loaded with :meth:`~StoragePolicy.load_ring`.
    """
    def __init__(self, idx, name='', is_default=False, is_deprecated=False,
                 object_ring=None, policy_type=DEFAULT_POLICY_TYPE):
        # do not allow StoragePolicy class to be instatiated directly
        if self.__class__ == StoragePolicy:
            raise TypeError("Can't instantiate StoragePolicy class directly")
        # policy parameter validation
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
        if policy_type not in VALID_TYPES:
            raise PolicyError('Invalid type', policy_type)
        self.policy_type = policy_type
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
        return ("%s(%d, %r, is_default=%s, "
                "is_deprecated=%s, policy_type=%r)") % \
            (self.__class__.__name__, self.idx, self.name,
             self.is_default, self.is_deprecated, self.policy_type)

    @staticmethod
    def from_conf(policy_type, policy_conf):
        """
        Factory method to create StoragePolicy objects from a config (dict)
        """
        if policy_type == EC_POLICY:
            return ECStoragePolicy(**policy_conf)
        else:
            return ReplicationStoragePolicy(**policy_conf)

    def load_ring(self, swift_dir):
        """
        Load the ring for this policy immediately.

        :param swift_dir: path to rings
        """
        if self.object_ring:
            return
        self.object_ring = Ring(swift_dir, ring_name=self.ring_name)

        # Validate ring to make sure enough primary nodes are configured
        self.validate_ring_node_count()

    def quorum_size(self, n):
        raise NotImplementedError("quorum_size is undefined for base "
                                  "StoragePolicy class ")

    @property
    def stores_objects_verbatim(self):
        raise NotImplementedError

    @property
    def needs_trailing_object_metadata(self):
        return False

    @property
    def needs_multiphase_put(self):
        return False


class ReplicationStoragePolicy(StoragePolicy):
    """
    Represents a storage policy of type 'replication'.  Default storage policy
    class unless otherwise overridden from swift.conf.

    Not meant to be instantiated directly; use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.
    """
    def __init__(self, idx, name='', is_default=False, is_deprecated=False,
                 object_ring=None):

        super(ReplicationStoragePolicy, self).__init__(
            idx, name, is_default, is_deprecated, object_ring,
            policy_type=REPL_POLICY)

    def validate_ring_node_count(self):
        pass

    def quorum_size(self, n):
        """
        Number of successful backend requests needed for the proxy to
        consider the client request successful.
        Quorum concept in the replication case:
            floor(number of replica / 2) + 1
        """
        return replication_quorum_size(n)

    def chunk_transformer(self, nstreams):
        chunk = yield
        while chunk:
            chunk = yield [chunk] * nstreams
        # Be nice to the person sending us the empty-string to indicate
        # end-of-message; otherwise, they get a StopIteration
        chunk = yield [chunk] * nstreams

    @property
    def stores_objects_verbatim(self):
        return True

    def trailing_metadata(self, *a, **kw):
        # Replicated objects don't need to store anything extra.
        return {}


class ECStoragePolicy(StoragePolicy):
    """
    Represents a storage policy of type 'erasure_coding'.

    Not meant to be instantiated directly; use
    :func:`~swift.common.storage_policy.reload_storage_policies` to load
    POLICIES from ``swift.conf``.
    """
    def __init__(self, idx, name='', is_default=False,
                 is_deprecated=False, object_ring=None,
                 ec_segment_size=DEFAULT_EC_OBJECT_SEGMENT_SIZE,
                 ec_type=None, ec_ndata=None, ec_nparity=None):

        super(ECStoragePolicy, self).__init__(
            idx, name, is_default, is_deprecated, object_ring,
            policy_type=EC_POLICY)

        self._fragment_header_len = None

        # Validate erasure_coding policy specific members
        # ec_type is one of the EC implementations supported by PyEClib
        if ec_type is None:
            raise PolicyError('Missing ec_type')
        self._ec_type = ec_type

        # Define _ec_ndata as the number of EC data fragments
        # Accessible as the property "ec_ndata"
        try:
            value = config_auto_int_value(ec_ndata, -1)
            if value <= 0:
                raise ValueError
            self._ec_ndata = value
        except ValueError:
            raise PolicyError('Invalid ec_num_data_fragments', ec_ndata)

        # Define _ec_nparity as the number of EC parity fragments
        # Accessible as the property "ec_nparity"
        try:
            value = config_auto_int_value(ec_nparity, -1)
            if value <= 0:
                raise ValueError
            self._ec_nparity = value
        except ValueError:
            raise PolicyError('Invalid ec_num_parity_fragments', ec_nparity)

        # Define _ec_segment_size as the encode segment unit size
        # Accessible as the property "ec_segment_size"
        try:
            value = config_auto_int_value(ec_segment_size, -1)
            if value <= 0:
                raise ValueError
            self._ec_segment_size = value
        except ValueError:
            raise PolicyError('Invalid ec_object_segment_size',
                              ec_segment_size)

        # Initialize PyECLib EC backend
        # raises ECDriverError
        try:
            self.pyeclib_driver = \
                ECDriver(k=self._ec_ndata, m=self._ec_nparity,
                         ec_type=self._ec_type)
        except ECDriverError as e:
            raise PolicyError("Error creating erasure_coding policy. "
                              "Please check policy configuration "
                              "(swift.conf). Policy %s, error detail: %s"
                              % (self.name, str(e)))
        except ECPyECLibException as e:
            raise PolicyError("Error creating erasure_coding policy. "
                              "Unsupported ec_type: %s for policy %s"
                              % (self._ec_type, self.name))

        # quorum_size is the minimum number of data + parity elements required
        # to be able to guarantee the desired fault tolerance, which is the
        # number of data elements supplemented by the minimum number of parity
        # elements required by the chosen erasure coding scheme.  For example,
        # for Reed-Soloman, the minimum number parity elements required is 1,
        # and thus the quorum_size requirement is ec_ndata + 1.  Given the
        # number of parity elements required is not the same for every erasure
        # coding scheme, consult PyECLib for min_parity_fragments_needed()
        self._ec_quorum_size = \
            self._ec_ndata + self.pyeclib_driver.min_parity_fragments_needed()

    def __repr__(self):
        return ("%s, EC config(ec_type=%s, ec_segment_size=%d, "
                "ec_ndata=%d, ec_nparity=%d)") % (
                    StoragePolicy.__repr__(self), self.ec_type,
                    self.ec_segment_size, self.ec_ndata, self.ec_nparity)

    def validate_ring_node_count(self):
        """
        EC specific validation

        Replica count check - we need _at_least_ (#data + #parity) replicas
        configured.  Also if the replica count is larger than exactly that
        number, we may confuse the EC reconstructor
        """
        if self.object_ring:
            nodes_configured = self.object_ring.replica_count
            if nodes_configured != (self.ec_ndata + self.ec_nparity):
                raise RingValidationError(
                    'EC ring for policy %s needs to be configured with '
                    'exactly %d nodes. Got %d.' % (self.name,
                    self.ec_ndata + self.ec_nparity, nodes_configured))

    @property
    def needs_trailing_object_metadata(self):
        return True

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
    def ec_segment_size(self):
        return self._ec_segment_size

    @property
    def ec_scheme_description(self):
        return "%s %d+%d" % (self._ec_type, self._ec_ndata, self._ec_nparity)

    def quorum_size(self, n):
        """
        Number of successful backend requests needed for the proxy to consider
        the client request successful.  Quorum size in the EC case depends on
        the choice of EC scheme.
        """
        return self._ec_quorum_size

    def chunk_transformer(self, nstreams):
        segment_size = self.ec_segment_size

        buf = collections.deque()
        total_buf_len = 0

        chunk = yield
        while chunk:
            buf.append(chunk)
            total_buf_len += len(chunk)
            if total_buf_len >= segment_size:
                chunks_to_encode = []
                # extract as many chunks as we can from the input buffer
                while total_buf_len >= segment_size:
                    to_take = segment_size
                    pieces = []
                    while to_take > 0:
                        piece = buf.popleft()
                        if len(piece) > to_take:
                            buf.appendleft(piece[to_take:])
                            piece = piece[:to_take]
                        pieces.append(piece)
                        to_take -= len(piece)
                        total_buf_len -= len(piece)
                    chunks_to_encode.append(''.join(pieces))

                frags_by_byte_order = []
                for chunk_to_encode in chunks_to_encode:
                    frags_by_byte_order.append(
                        self.pyeclib_driver.encode(chunk_to_encode))
                # Sequential calls to encode() have given us a list that
                # looks like this:
                #
                # [[frag_A1, frag_B1, frag_C1, ...],
                #  [frag_A2, frag_B2, frag_C2, ...], ...]
                #
                # What we need is a list like this:
                #
                # [(frag_A1 + frag_A2 + ...),  # destined for node A
                #  (frag_B1 + frag_B2 + ...),  # destined for node B
                #  (frag_C1 + frag_C2 + ...),  # destined for node C
                #  ...]
                obj_data = [''.join(frags)
                            for frags in zip(*frags_by_byte_order)]
                chunk = yield obj_data
            else:
                # didn't have enough data to encode
                chunk = yield None

        # Now we've gotten an empty chunk, which indicates end-of-input.
        # Take any leftover bytes and encode them.
        last_bytes = ''.join(buf)
        if last_bytes:
            last_frags = self.pyeclib_driver.encode(last_bytes)
            yield last_frags
        else:
            yield [''] * nstreams

    @property
    def n_streams_for_decode(self):
        return self._ec_ndata

    @property
    def fragment_size(self):
        """
        Maximum length of a fragment, including header.

        NB: a fragment archive is a sequence of 0 or more max-length
        fragments followed by one possibly-shorter fragment.
        """
        if self._fragment_header_len is None:
            # this header is a fixed size, so it's perfectly okay to learn
            # it by encoding some dummy data
            self._fragment_header_len = len(self.pyeclib_driver.encode("")[0])

        return (
            self.ec_segment_size // self._ec_ndata + self._fragment_header_len)

    def decode_fragments(self, frags):
        return self.pyeclib_driver.decode(frags)

    @property
    def stores_objects_verbatim(self):
        return False

    def trailing_metadata(self, client_obj_hasher,
                          bytes_transferred_from_client,
                          fragment_archive_index):
        return {
            # etag and size values are being added twice here.
            # The container override header is used to update the container db
            # with these values as they represent the correct etag
            # and size for the whole object and not just the FA.
            # The object sysmeta header will be saved on each FA of the object.
            'X-Object-Sysmeta-EC-Etag': client_obj_hasher.hexdigest(),
            'X-Object-Sysmeta-EC-Content-Length':
            str(bytes_transferred_from_client),
            'X-Backend-Container-Update-Override-Etag':
            client_obj_hasher.hexdigest(),
            'X-Backend-Container-Update-Override-Size':
            str(bytes_transferred_from_client),
            'X-Object-Sysmeta-EC-Archive-Index': str(fragment_archive_index),
            # These fields are for debuggability,
            # AKA "what is this thing?"
            'X-Object-Sysmeta-EC-Scheme': self.ec_scheme_description,
            'X-Object-Sysmeta-EC-Segment-Size': str(self.ec_segment_size),
        }

    @property
    def needs_multiphase_put(self):
        return True


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
            self._add_policy(
                ReplicationStoragePolicy(0, name=LEGACY_POLICY_NAME))

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
            policy_entry['policy_type'] = pol.policy_type
            if pol.is_default:
                policy_entry['default'] = pol.is_default
            if pol.policy_type == EC_POLICY:
                policy_entry['ec_type'] = pol.ec_type
                policy_entry['ec_ndata'] = pol.ec_ndata
                policy_entry['ec_nparity'] = pol.ec_nparity
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
            'policy_type': 'policy_type',
            # ECStoragePolicy Specific options
            'ec_type': 'ec_type',
            'ec_object_segment_size': 'ec_segment_size',
            'ec_num_data_fragments': 'ec_ndata',
            'ec_num_parity_fragments': 'ec_nparity',
        }

        # assume DEFAULT policy type
        policy_type = DEFAULT_POLICY_TYPE

        policy_options = {}
        for config_option, value in conf.items(section):
            try:
                policy_option = config_to_policy_option_map[config_option]
            except KeyError:
                raise PolicyError('Invalid option %r in '
                                  'storage-policy section %r' % (
                                      config_option, section))
            policy_options[policy_option] = value

        # check policy type
        policy_type = policy_options.pop('policy_type', DEFAULT_POLICY_TYPE)
        if policy_type == EC_POLICY:
            policy = ECStoragePolicy(policy_index, **policy_options)
        else:
            # assume replication policy by default
            policy = ReplicationStoragePolicy(policy_index, **policy_options)

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
