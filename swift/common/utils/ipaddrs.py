# Copyright (c) 2010-2012 OpenStack Foundation
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

import netifaces
import re
import socket


# Used by the parse_socket_string() function to validate IPv6 addresses
IPV6_RE = re.compile(r"^\[(?P<address>.*)\](:(?P<port>[0-9]+))?$")


def is_valid_ip(ip):
    """
    Return True if the provided ip is a valid IP-address
    """
    return is_valid_ipv4(ip) or is_valid_ipv6(ip)


def is_valid_ipv4(ip):
    """
    Return True if the provided ip is a valid IPv4-address
    """
    try:
        socket.inet_pton(socket.AF_INET, ip)
    except socket.error:  # not a valid IPv4 address
        return False
    return True


def is_valid_ipv6(ip):
    """
    Returns True if the provided ip is a valid IPv6-address
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:  # not a valid IPv6 address
        return False
    return True


def expand_ipv6(address):
    """
    Expand ipv6 address.
    :param address: a string indicating valid ipv6 address
    :returns: a string indicating fully expanded ipv6 address

    """
    packed_ip = socket.inet_pton(socket.AF_INET6, address)
    return socket.inet_ntop(socket.AF_INET6, packed_ip)


def whataremyips(ring_ip=None):
    """
    Get "our" IP addresses ("us" being the set of services configured by
    one `*.conf` file). If our REST listens on a specific address, return it.
    Otherwise, if listen on '0.0.0.0' or '::' return all addresses, including
    the loopback.

    :param str ring_ip: Optional ring_ip/bind_ip from a config file; may be
                        IP address or hostname.
    :returns: list of Strings of ip addresses
    """
    if ring_ip:
        # See if bind_ip is '0.0.0.0'/'::'
        try:
            _, _, _, _, sockaddr = socket.getaddrinfo(
                ring_ip, None, 0, socket.SOCK_STREAM, 0,
                socket.AI_NUMERICHOST)[0]
            if sockaddr[0] not in ('0.0.0.0', '::'):
                return [ring_ip]
        except socket.gaierror:
            pass

    addresses = []
    for interface in netifaces.interfaces():
        try:
            iface_data = netifaces.ifaddresses(interface)
            for family in iface_data:
                if family not in (netifaces.AF_INET, netifaces.AF_INET6):
                    continue
                for address in iface_data[family]:
                    addr = address['addr']

                    # If we have an ipv6 address remove the
                    # %ether_interface at the end
                    if family == netifaces.AF_INET6:
                        addr = expand_ipv6(addr.split('%')[0])
                    addresses.append(addr)
        except ValueError:
            pass
    return addresses


def parse_socket_string(socket_string, default_port):
    """
    Given a string representing a socket, returns a tuple of (host, port).
    Valid strings are DNS names, IPv4 addresses, or IPv6 addresses, with an
    optional port. If an IPv6 address is specified it **must** be enclosed in
    [], like *[::1]* or *[::1]:11211*. This follows the accepted prescription
    for `IPv6 host literals`_.

    Examples::

        server.org
        server.org:1337
        127.0.0.1:1337
        [::1]:1337
        [::1]

    .. _IPv6 host literals: https://tools.ietf.org/html/rfc3986#section-3.2.2
    """
    port = default_port
    # IPv6 addresses must be between '[]'
    if socket_string.startswith('['):
        match = IPV6_RE.match(socket_string)
        if not match:
            raise ValueError("Invalid IPv6 address: %s" % socket_string)
        host = match.group('address')
        port = match.group('port') or port
    else:
        if ':' in socket_string:
            tokens = socket_string.split(':')
            if len(tokens) > 2:
                raise ValueError("IPv6 addresses must be between '[]'")
            host, port = tokens
        else:
            host = socket_string
    return (host, port)
