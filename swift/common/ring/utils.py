# Copyright (c) 2010-2013 OpenStack Foundation
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
from collections import defaultdict
import optparse
import re
import socket

from swift.common import exceptions
from swift.common.utils import expand_ipv6, is_valid_ip, is_valid_ipv4, \
    is_valid_ipv6


def tiers_for_dev(dev):
    """
    Returns a tuple of tiers for a given device in ascending order by
    length.

    :returns: tuple of tiers
    """
    t1 = dev['region']
    t2 = dev['zone']
    t3 = dev['ip']
    t4 = dev['id']

    return ((t1,),
            (t1, t2),
            (t1, t2, t3),
            (t1, t2, t3, t4))


def build_tier_tree(devices):
    """
    Construct the tier tree from the zone layout.

    The tier tree is a dictionary that maps tiers to their child tiers.
    A synthetic root node of () is generated so that there's one tree,
    not a forest.

    Example:

    region 1 -+---- zone 1 -+---- 192.168.101.1 -+---- device id 0
              |             |                    |
              |             |                    +---- device id 1
              |             |                    |
              |             |                    +---- device id 2
              |             |
              |             +---- 192.168.101.2 -+---- device id 3
              |                                  |
              |                                  +---- device id 4
              |                                  |
              |                                  +---- device id 5
              |
              +---- zone 2 -+---- 192.168.102.1 -+---- device id 6
                            |                    |
                            |                    +---- device id 7
                            |                    |
                            |                    +---- device id 8
                            |
                            +---- 192.168.102.2 -+---- device id 9
                                                 |
                                                 +---- device id 10


    region 2 -+---- zone 1 -+---- 192.168.201.1 -+---- device id 12
                            |                    |
                            |                    +---- device id 13
                            |                    |
                            |                    +---- device id 14
                            |
                            +---- 192.168.201.2 -+---- device id 15
                                                 |
                                                 +---- device id 16
                                                 |
                                                 +---- device id 17

    The tier tree would look like:
    {
      (): [(1,), (2,)],

      (1,): [(1, 1), (1, 2)],
      (2,): [(2, 1)],

      (1, 1): [(1, 1, 192.168.101.1),
               (1, 1, 192.168.101.2)],
      (1, 2): [(1, 2, 192.168.102.1),
               (1, 2, 192.168.102.2)],
      (2, 1): [(2, 1, 192.168.201.1),
               (2, 1, 192.168.201.2)],

      (1, 1, 192.168.101.1): [(1, 1, 192.168.101.1, 0),
                              (1, 1, 192.168.101.1, 1),
                              (1, 1, 192.168.101.1, 2)],
      (1, 1, 192.168.101.2): [(1, 1, 192.168.101.2, 3),
                              (1, 1, 192.168.101.2, 4),
                              (1, 1, 192.168.101.2, 5)],
      (1, 2, 192.168.102.1): [(1, 2, 192.168.102.1, 6),
                              (1, 2, 192.168.102.1, 7),
                              (1, 2, 192.168.102.1, 8)],
      (1, 2, 192.168.102.2): [(1, 2, 192.168.102.2, 9),
                              (1, 2, 192.168.102.2, 10)],
      (2, 1, 192.168.201.1): [(2, 1, 192.168.201.1, 12),
                              (2, 1, 192.168.201.1, 13),
                              (2, 1, 192.168.201.1, 14)],
      (2, 1, 192.168.201.2): [(2, 1, 192.168.201.2, 15),
                              (2, 1, 192.168.201.2, 16),
                              (2, 1, 192.168.201.2, 17)],
    }

    :devices: device dicts from which to generate the tree
    :returns: tier tree

    """
    tier2children = defaultdict(set)
    for dev in devices:
        for tier in tiers_for_dev(dev):
            if len(tier) > 1:
                tier2children[tier[0:-1]].add(tier)
            else:
                tier2children[()].add(tier)
    return tier2children


def validate_and_normalize_ip(ip):
    """
    Return normalized ip if the ip is a valid ip.
    Otherwise raise ValueError Exception. The hostname is
    normalized to all lower case. IPv6-addresses are converted to
    lowercase and fully expanded.
    """
    # first convert to lower case
    new_ip = ip.lower()
    if is_valid_ipv4(new_ip):
        return new_ip
    elif is_valid_ipv6(new_ip):
        return expand_ipv6(new_ip)
    else:
        raise ValueError('Invalid ip %s' % ip)


def validate_and_normalize_address(address):
    """
    Return normalized address if the address is a valid ip or hostname.
    Otherwise raise ValueError Exception. The hostname is
    normalized to all lower case. IPv6-addresses are converted to
    lowercase and fully expanded.

    RFC1123 2.1 Host Names and Nubmers
    DISCUSSION
        This last requirement is not intended to specify the complete
        syntactic form for entering a dotted-decimal host number;
        that is considered to be a user-interface issue.  For
        example, a dotted-decimal number must be enclosed within
        "[ ]" brackets for SMTP mail (see Section 5.2.17).  This
        notation could be made universal within a host system,
        simplifying the syntactic checking for a dotted-decimal
        number.

        If a dotted-decimal number can be entered without such
        identifying delimiters, then a full syntactic check must be
        made, because a segment of a host domain name is now allowed
        to begin with a digit and could legally be entirely numeric
        (see Section 6.1.2.4).  However, a valid host name can never
        have the dotted-decimal form #.#.#.#, since at least the
        highest-level component label will be alphabetic.
    """
    new_address = address.lstrip('[').rstrip(']')
    if address.startswith('[') and address.endswith(']'):
        return validate_and_normalize_ip(new_address)

    new_address = new_address.lower()
    if is_valid_ipv4(new_address):
        return new_address
    elif is_valid_ipv6(new_address):
        return expand_ipv6(new_address)
    elif is_valid_hostname(new_address):
        return new_address
    else:
        raise ValueError('Invalid address %s' % address)


def is_valid_hostname(hostname):
    """
    Return True if the provided hostname is a valid hostname
    """
    if len(hostname) < 1 or len(hostname) > 255:
        return False
    if hostname.endswith('.'):
        # strip exactly one dot from the right, if present
        hostname = hostname[:-1]
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))


def is_local_device(my_ips, my_port, dev_ip, dev_port):
    """
    Return True if the provided dev_ip and dev_port are among the IP
    addresses specified in my_ips and my_port respectively.

    To support accurate locality determination in the server-per-port
    deployment, when my_port is None, only IP addresses are used for
    determining locality (dev_port is ignored).

    If dev_ip is a hostname then it is first translated to an IP
    address before checking it against my_ips.
    """
    candidate_ips = []
    if not is_valid_ip(dev_ip) and is_valid_hostname(dev_ip):
        try:
            # get the ip for this host; use getaddrinfo so that
            # it works for both ipv4 and ipv6 addresses
            addrinfo = socket.getaddrinfo(dev_ip, dev_port)
            for addr in addrinfo:
                family = addr[0]
                dev_ip = addr[4][0]  # get the ip-address
                if family == socket.AF_INET6:
                    dev_ip = expand_ipv6(dev_ip)
                candidate_ips.append(dev_ip)
        except socket.gaierror:
            return False
    else:
        if is_valid_ipv6(dev_ip):
            dev_ip = expand_ipv6(dev_ip)
        candidate_ips = [dev_ip]

    for dev_ip in candidate_ips:
        if dev_ip in my_ips and (my_port is None or dev_port == my_port):
            return True

    return False


def parse_search_value(search_value):
    """The <search-value> can be of the form::

        d<device_id>r<region>z<zone>-<ip>:<port>R<r_ip>:<r_port>/
         <device_name>_<meta>

    Where <r_ip> and <r_port> are replication ip and port.

    Any part is optional, but you must include at least one part.

    Examples::

        d74              Matches the device id 74
        r4               Matches devices in region 4
        z1               Matches devices in zone 1
        z1-1.2.3.4       Matches devices in zone 1 with the ip 1.2.3.4
        1.2.3.4          Matches devices in any zone with the ip 1.2.3.4
        z1:5678          Matches devices in zone 1 using port 5678
        :5678            Matches devices that use port 5678
        R5.6.7.8         Matches devices that use replication ip 5.6.7.8
        R:5678           Matches devices that use replication port 5678
        1.2.3.4R5.6.7.8  Matches devices that use ip 1.2.3.4 and replication ip
                         5.6.7.8
        /sdb1            Matches devices with the device name sdb1
        _shiny           Matches devices with shiny in the meta data
        _"snet: 5.6.7.8" Matches devices with snet: 5.6.7.8 in the meta data
        [::1]            Matches devices in any zone with the ip ::1
        z1-[::1]:5678    Matches devices in zone 1 with ip ::1 and port 5678

    Most specific example::

        d74r4z1-1.2.3.4:5678/sdb1_"snet: 5.6.7.8"

    Nerd explanation:

        All items require their single character prefix except the ip, in which
        case the - is optional unless the device id or zone is also included.
    """
    orig_search_value = search_value
    match = {}
    if search_value.startswith('d'):
        i = 1
        while i < len(search_value) and search_value[i].isdigit():
            i += 1
        match['id'] = int(search_value[1:i])
        search_value = search_value[i:]
    if search_value.startswith('r'):
        i = 1
        while i < len(search_value) and search_value[i].isdigit():
            i += 1
        match['region'] = int(search_value[1:i])
        search_value = search_value[i:]
    if search_value.startswith('z'):
        i = 1
        while i < len(search_value) and search_value[i].isdigit():
            i += 1
        match['zone'] = int(search_value[1:i])
        search_value = search_value[i:]
    if search_value.startswith('-'):
        search_value = search_value[1:]
    if search_value and search_value[0].isdigit():
        i = 1
        while i < len(search_value) and search_value[i] in '0123456789.':
            i += 1
        match['ip'] = search_value[:i]
        search_value = search_value[i:]
    elif search_value and search_value.startswith('['):
        i = 1
        while i < len(search_value) and search_value[i] != ']':
            i += 1
        i += 1
        match['ip'] = search_value[:i].lstrip('[').rstrip(']')
        search_value = search_value[i:]

    if 'ip' in match:
        # ipv6 addresses are converted to all lowercase
        # and use the fully expanded representation
        match['ip'] = validate_and_normalize_ip(match['ip'])

    if search_value.startswith(':'):
        i = 1
        while i < len(search_value) and search_value[i].isdigit():
            i += 1
        match['port'] = int(search_value[1:i])
        search_value = search_value[i:]
    # replication parameters
    if search_value.startswith('R'):
        search_value = search_value[1:]
        if search_value and search_value[0].isdigit():
            i = 1
            while (i < len(search_value) and
                   search_value[i] in '0123456789.'):
                i += 1
            match['replication_ip'] = search_value[:i]
            search_value = search_value[i:]
        elif search_value and search_value.startswith('['):
            i = 1
            while i < len(search_value) and search_value[i] != ']':
                i += 1
            i += 1
            match['replication_ip'] = search_value[:i].lstrip('[').rstrip(']')
            search_value = search_value[i:]

        if 'replication_ip' in match:
            # ipv6 addresses are converted to all lowercase
            # and use the fully expanded representation
            match['replication_ip'] = \
                validate_and_normalize_ip(match['replication_ip'])

        if search_value.startswith(':'):
            i = 1
            while i < len(search_value) and search_value[i].isdigit():
                i += 1
            match['replication_port'] = int(search_value[1:i])
            search_value = search_value[i:]
    if search_value.startswith('/'):
        i = 1
        while i < len(search_value) and search_value[i] != '_':
            i += 1
        match['device'] = search_value[1:i]
        search_value = search_value[i:]
    if search_value.startswith('_'):
        match['meta'] = search_value[1:]
        search_value = ''
    if search_value:
        raise ValueError('Invalid <search-value>: %s' %
                         repr(orig_search_value))
    return match


def parse_search_values_from_opts(opts):
    """
    Convert optparse style options into a dictionary for searching.

    :param opts: optparse style options
    :returns: a dictionary with search values to filter devices,
              supported parameters are id, region, zone, ip, port,
              replication_ip, replication_port, device, weight, meta
    """

    search_values = {}
    for key in ('id', 'region', 'zone', 'ip', 'port', 'replication_ip',
                'replication_port', 'device', 'weight', 'meta'):
        value = getattr(opts, key, None)
        if value:
            if key == 'ip' or key == 'replication_ip':
                value = validate_and_normalize_address(value)
        search_values[key] = value
    return search_values


def parse_change_values_from_opts(opts):
    """
    Convert optparse style options into a dictionary for changing.

    :param opts: optparse style options
    :returns: a dictonary with change values to filter devices,
              supported parameters are ip, port, replication_ip,
              replication_port
    """

    change_values = {}
    for key in ('change_ip', 'change_port', 'change_replication_ip',
                'change_replication_port', 'change_device', 'change_meta'):
        value = getattr(opts, key, None)
        if value:
            if key == 'change_ip' or key == 'change_replication_ip':
                value = validate_and_normalize_address(value)
            change_values[key.replace('change_', '')] = value
    return change_values


def parse_add_value(add_value):
    """
    Convert an add value, like 'r1z2-10.1.2.3:7878/sdf', to a dictionary.

    If the string does not start with 'r<N>', then the value of 'region' in
    the returned dictionary will be None. Callers should check for this and
    set a reasonable default. This is done so callers can emit errors or
    warnings if desired.

    Similarly, 'replication_ip' and 'replication_port' will be None if not
    specified.

    :returns: dictionary with keys 'region', 'zone', 'ip', 'port', 'device',
        'replication_ip', 'replication_port', 'meta'
    :raises ValueError: if add_value is malformed
    """
    region = None
    rest = add_value
    if add_value.startswith('r'):
        i = 1
        while i < len(add_value) and add_value[i].isdigit():
            i += 1
        region = int(add_value[1:i])
        rest = add_value[i:]

    if not rest.startswith('z'):
        raise ValueError('Invalid add value: %s' % add_value)
    i = 1
    while i < len(rest) and rest[i].isdigit():
        i += 1
    zone = int(rest[1:i])
    rest = rest[i:]

    if not rest.startswith('-'):
        raise ValueError('Invalid add value: %s' % add_value)

    ip, port, rest = parse_address(rest[1:])

    replication_ip = replication_port = None
    if rest.startswith('R'):
        replication_ip, replication_port, rest =  \
            parse_address(rest[1:])
    if not rest.startswith('/'):
        raise ValueError(
            'Invalid add value: %s' % add_value)
    i = 1
    while i < len(rest) and rest[i] != '_':
        i += 1
    device_name = rest[1:i]
    if not validate_device_name(device_name):
        raise ValueError('Invalid device name')

    rest = rest[i:]

    meta = ''
    if rest.startswith('_'):
        meta = rest[1:]

    return {'region': region, 'zone': zone, 'ip': ip, 'port': port,
            'device': device_name, 'replication_ip': replication_ip,
            'replication_port': replication_port, 'meta': meta}


def parse_address(rest):
    if rest.startswith('['):
        # remove first [] for ip
        rest = rest.replace('[', '', 1).replace(']', '', 1)

    pos = 0
    while (pos < len(rest) and
           not (rest[pos] == 'R' or rest[pos] == '/')):
        pos += 1
    address = rest[:pos]
    rest = rest[pos:]

    port_start = address.rfind(':')
    if port_start == -1:
        raise ValueError('Invalid port in add value')

    ip = address[:port_start]
    try:
        port = int(address[(port_start + 1):])
    except (TypeError, ValueError):
        raise ValueError(
            'Invalid port %s in add value' % address[port_start:])

    # if this is an ipv6 address then we want to convert it
    # to all lowercase and use its fully expanded representation
    # to make searches easier
    ip = validate_and_normalize_ip(ip)

    return (ip, port, rest)


def validate_args(argvish):
    """
    Build OptionParse and validate it whether the format is new command-line
    format or not.
    """
    opts, args = parse_args(argvish)
    # id can be 0 (swift starts generating id from 0),
    # also zone, region and weight can be set to zero.
    new_cmd_format = opts.id is not None or opts.region is not None or \
        opts.zone is not None or opts.ip or opts.port or \
        opts.replication_ip or opts.replication_port or \
        opts.device or opts.weight is not None or opts.meta
    return (new_cmd_format, opts, args)


def parse_args(argvish):
    """
    Build OptionParser and evaluate command line arguments.
    """
    parser = optparse.OptionParser()
    parser.add_option('-u', '--id', type="int",
                      help="Device ID")
    parser.add_option('-r', '--region', type="int",
                      help="Region")
    parser.add_option('-z', '--zone', type="int",
                      help="Zone")
    parser.add_option('-i', '--ip', type="string",
                      help="IP address")
    parser.add_option('-p', '--port', type="int",
                      help="Port number")
    parser.add_option('-j', '--replication-ip', type="string",
                      help="Replication IP address")
    parser.add_option('-q', '--replication-port', type="int",
                      help="Replication port number")
    parser.add_option('-d', '--device', type="string",
                      help="Device name (e.g. md0, sdb1)")
    parser.add_option('-w', '--weight', type="float",
                      help="Device weight")
    parser.add_option('-m', '--meta', type="string", default="",
                      help="Extra device info (just a string)")
    parser.add_option('-I', '--change-ip', type="string",
                      help="IP address for change")
    parser.add_option('-P', '--change-port', type="int",
                      help="Port number for change")
    parser.add_option('-J', '--change-replication-ip', type="string",
                      help="Replication IP address for change")
    parser.add_option('-Q', '--change-replication-port', type="int",
                      help="Replication port number for change")
    parser.add_option('-D', '--change-device', type="string",
                      help="Device name (e.g. md0, sdb1) for change")
    parser.add_option('-M', '--change-meta', type="string", default="",
                      help="Extra device info (just a string) for change")
    parser.add_option('-y', '--yes', default=False, action="store_true",
                      help="Assume a yes response to all questions")
    return parser.parse_args(argvish)


def parse_builder_ring_filename_args(argvish):
    first_arg = argvish[1]
    if first_arg.endswith('.ring.gz'):
        ring_file = first_arg
        builder_file = first_arg[:-len('.ring.gz')] + '.builder'
    else:
        builder_file = first_arg
        if not builder_file.endswith('.builder'):
            ring_file = first_arg
        else:
            ring_file = builder_file[:-len('.builder')]
        ring_file += '.ring.gz'
    return builder_file, ring_file


def build_dev_from_opts(opts):
    """
    Convert optparse stype options into a device dictionary.
    """
    for attribute, shortopt, longopt in (['region', '-r', '--region'],
                                         ['zone', '-z', '--zone'],
                                         ['ip', '-i', '--ip'],
                                         ['port', '-p', '--port'],
                                         ['device', '-d', '--device'],
                                         ['weight', '-w', '--weight']):
        if getattr(opts, attribute, None) is None:
            raise ValueError('Required argument %s/%s not specified.' %
                             (shortopt, longopt))

    ip = validate_and_normalize_address(opts.ip)
    replication_ip = validate_and_normalize_address(
        (opts.replication_ip or opts.ip))
    replication_port = opts.replication_port or opts.port

    if not validate_device_name(opts.device):
        raise ValueError('Invalid device name')

    return {'region': opts.region, 'zone': opts.zone, 'ip': ip,
            'port': opts.port, 'device': opts.device, 'meta': opts.meta,
            'replication_ip': replication_ip,
            'replication_port': replication_port, 'weight': opts.weight}


def dispersion_report(builder, search_filter=None,
                      verbose=False, recalculate=False):
    if recalculate or not builder._dispersion_graph:
        builder._build_dispersion_graph()
    max_allowed_replicas = builder._build_max_replicas_by_tier()
    worst_tier = None
    max_dispersion = 0.0
    sorted_graph = []
    for tier, replica_counts in sorted(builder._dispersion_graph.items()):
        tier_name = get_tier_name(tier, builder)
        if search_filter and not re.match(search_filter, tier_name):
            continue
        max_replicas = int(max_allowed_replicas[tier])
        at_risk_parts = sum(replica_counts[i] * (i - max_replicas)
                            for i in range(max_replicas + 1,
                                           len(replica_counts)))
        placed_parts = sum(replica_counts[i] * i for i in range(
            1, len(replica_counts)))
        tier_dispersion = 100.0 * at_risk_parts / placed_parts
        if tier_dispersion > max_dispersion:
            max_dispersion = tier_dispersion
            worst_tier = tier_name
        if not verbose:
            continue

        tier_report = {
            'max_replicas': max_replicas,
            'placed_parts': placed_parts,
            'dispersion': tier_dispersion,
            'replicas': replica_counts,
        }
        sorted_graph.append((tier_name, tier_report))

    return {
        'max_dispersion': max_dispersion,
        'worst_tier': worst_tier,
        'graph': sorted_graph,
    }


def validate_replicas_by_tier(replicas, replicas_by_tier):
    """
    Validate the sum of the replicas at each tier.
    The sum of the replicas at each tier should be less than or very close to
    the upper limit indicated by replicas

    :param replicas: float,the upper limit of replicas
    :param replicas_by_tier: defaultdict,the replicas by tier
    """
    tiers = ['cluster', 'regions', 'zones', 'servers', 'devices']
    for i, tier_name in enumerate(tiers):
        replicas_at_tier = sum(replicas_by_tier[t] for t in
                               replicas_by_tier if len(t) == i)
        if abs(replicas - replicas_at_tier) > 1e-10:
            raise exceptions.RingValidationError(
                '%s != %s at tier %s' % (
                    replicas_at_tier, replicas, tier_name))


def format_device(region=None, zone=None, ip=None, device=None, **kwargs):
    """
    Convert device dict or tier attributes to a representative string.

    :returns: a string, the normalized format of a device tier
    """
    return "r%sz%s-%s/%s" % (region, zone, ip, device)


def get_tier_name(tier, builder):
    if len(tier) == 1:
        return "r%s" % (tier[0], )
    if len(tier) == 2:
        return "r%sz%s" % (tier[0], tier[1])
    if len(tier) == 3:
        return "r%sz%s-%s" % (tier[0], tier[1], tier[2])
    if len(tier) == 4:
        device = builder.devs[tier[3]] or {}
        return format_device(tier[0], tier[1], tier[2], device.get(
            'device', 'IDd%s' % tier[3]))


def validate_device_name(device_name):
    return not (
        device_name.startswith(' ') or
        device_name.endswith(' ') or
        len(device_name) == 0)


def pretty_dev(device):
    return format_device(**device)
