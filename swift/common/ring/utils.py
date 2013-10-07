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


def tiers_for_dev(dev):
    """
    Returns a tuple of tiers for a given device in ascending order by
    length.

    :returns: tuple of tiers
    """
    t1 = dev['region']
    t2 = dev['zone']
    t3 = "{ip}:{port}".format(ip=dev.get('ip'), port=dev.get('port'))
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

    region 1 -+---- zone 1 -+---- 192.168.101.1:6000 -+---- device id 0
              |             |                         |
              |             |                         +---- device id 1
              |             |                         |
              |             |                         +---- device id 2
              |             |
              |             +---- 192.168.101.2:6000 -+---- device id 3
              |                                       |
              |                                       +---- device id 4
              |                                       |
              |                                       +---- device id 5
              |
              +---- zone 2 -+---- 192.168.102.1:6000 -+---- device id 6
                            |                         |
                            |                         +---- device id 7
                            |                         |
                            |                         +---- device id 8
                            |
                            +---- 192.168.102.2:6000 -+---- device id 9
                                                      |
                                                      +---- device id 10


    region 2 -+---- zone 1 -+---- 192.168.201.1:6000 -+---- device id 12
                            |                         |
                            |                         +---- device id 13
                            |                         |
                            |                         +---- device id 14
                            |
                            +---- 192.168.201.2:6000 -+---- device id 15
                                                      |
                                                      +---- device id 16
                                                      |
                                                      +---- device id 17

    The tier tree would look like:
    {
      (): [(1,), (2,)],

      (1,): [(1, 1), (1, 2)],
      (2,): [(2, 1)],

      (1, 1): [(1, 1, 192.168.101.1:6000),
               (1, 1, 192.168.101.2:6000)],
      (1, 2): [(1, 2, 192.168.102.1:6000),
               (1, 2, 192.168.102.2:6000)],
      (2, 1): [(2, 1, 192.168.201.1:6000),
               (2, 1, 192.168.201.2:6000)],

      (1, 1, 192.168.101.1:6000): [(1, 1, 192.168.101.1:6000, 0),
                                   (1, 1, 192.168.101.1:6000, 1),
                                   (1, 1, 192.168.101.1:6000, 2)],
      (1, 1, 192.168.101.2:6000): [(1, 1, 192.168.101.2:6000, 3),
                                   (1, 1, 192.168.101.2:6000, 4),
                                   (1, 1, 192.168.101.2:6000, 5)],
      (1, 2, 192.168.102.1:6000): [(1, 2, 192.168.102.1:6000, 6),
                                   (1, 2, 192.168.102.1:6000, 7),
                                   (1, 2, 192.168.102.1:6000, 8)],
      (1, 2, 192.168.102.2:6000): [(1, 2, 192.168.102.2:6000, 9),
                                   (1, 2, 192.168.102.2:6000, 10)],
      (2, 1, 192.168.201.1:6000): [(2, 1, 192.168.201.1:6000, 12),
                                   (2, 1, 192.168.201.1:6000, 13),
                                   (2, 1, 192.168.201.1:6000, 14)],
      (2, 1, 192.168.201.2:6000): [(2, 1, 192.168.201.2:6000, 15),
                                   (2, 1, 192.168.201.2:6000, 16),
                                   (2, 1, 192.168.201.2:6000, 17)],
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


def parse_search_value(search_value):
    """The <search-value> can be of the form::

        d<device_id>r<region>z<zone>-<ip>:<port>[R<r_ip>:<r_port>]/
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
    if len(search_value) and search_value[0].isdigit():
        i = 1
        while i < len(search_value) and search_value[i] in '0123456789.':
            i += 1
        match['ip'] = search_value[:i]
        search_value = search_value[i:]
    elif len(search_value) and search_value[0] == '[':
        i = 1
        while i < len(search_value) and search_value[i] != ']':
            i += 1
        i += 1
        match['ip'] = search_value[:i].lstrip('[').rstrip(']')
        search_value = search_value[i:]
    if search_value.startswith(':'):
        i = 1
        while i < len(search_value) and search_value[i].isdigit():
            i += 1
        match['port'] = int(search_value[1:i])
        search_value = search_value[i:]
    # replication parameters
    if search_value.startswith('R'):
        search_value = search_value[1:]
        if len(search_value) and search_value[0].isdigit():
            i = 1
            while (i < len(search_value) and
                   search_value[i] in '0123456789.'):
                i += 1
            match['replication_ip'] = search_value[:i]
            search_value = search_value[i:]
        elif len(search_value) and search_value[0] == '[':
            i = 1
            while i < len(search_value) and search_value[i] != ']':
                i += 1
            i += 1
            match['replication_ip'] = search_value[:i].lstrip('[').rstrip(']')
            search_value = search_value[i:]
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


def parse_args(argvish):
    """
    Build OptionParser and evaluate command line arguments.
    """
    parser = optparse.OptionParser()
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
    return parser.parse_args(argvish)


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
        if not getattr(opts, attribute, None):
            raise ValueError('Required argument %s/%s not specified.' %
                             (shortopt, longopt))

    replication_ip = opts.replication_ip or opts.ip
    replication_port = opts.replication_port or opts.port

    return {'region': opts.region, 'zone': opts.zone, 'ip': opts.ip,
            'port': opts.port, 'device': opts.device, 'meta': opts.meta,
            'replication_ip': replication_ip,
            'replication_port': replication_port, 'weight': opts.weight}
