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

import ctypes
import ctypes.util
import os
import platform
import re
import socket
import warnings


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


libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
try:
    getifaddrs = libc.getifaddrs
    freeifaddrs = libc.freeifaddrs
    netifaces = None  # for patching
except AttributeError:
    getifaddrs = None
    freeifaddrs = None
    try:
        import netifaces
    except ImportError:
        raise ImportError('C function getifaddrs not available, '
                          'and netifaces not installed')
    else:
        warnings.warn('getifaddrs is not available; falling back to the '
                      'archived and no longer maintained netifaces project. '
                      'This fallback will be removed in a future release; '
                      'see https://bugs.launchpad.net/swift/+bug/2019233 for '
                      'more information.', FutureWarning)
else:
    class sockaddr_in4(ctypes.Structure):
        if platform.system() == 'Linux':
            _fields_ = [
                ("sin_family", ctypes.c_uint16),
                ("sin_port", ctypes.c_uint16),
                ("sin_addr", ctypes.c_ubyte * 4),
            ]
        else:
            # Assume BSD / OS X
            _fields_ = [
                ("sin_len", ctypes.c_uint8),
                ("sin_family", ctypes.c_uint8),
                ("sin_port", ctypes.c_uint16),
                ("sin_addr", ctypes.c_ubyte * 4),
            ]

    class sockaddr_in6(ctypes.Structure):
        if platform.system() == 'Linux':
            _fields_ = [
                ("sin6_family", ctypes.c_uint16),
                ("sin6_port", ctypes.c_uint16),
                ("sin6_flowinfo", ctypes.c_uint32),
                ("sin6_addr", ctypes.c_ubyte * 16),
            ]
        else:
            # Assume BSD / OS X
            _fields_ = [
                ("sin6_len", ctypes.c_uint8),
                ("sin6_family", ctypes.c_uint8),
                ("sin6_port", ctypes.c_uint16),
                ("sin6_flowinfo", ctypes.c_uint32),
                ("sin6_addr", ctypes.c_ubyte * 16),
            ]

    class ifaddrs(ctypes.Structure):
        pass

    # Have to do this a little later so we can self-reference
    ifaddrs._fields_ = [
        ("ifa_next", ctypes.POINTER(ifaddrs)),
        ("ifa_name", ctypes.c_char_p),
        ("ifa_flags", ctypes.c_int),
        # Use the smaller of the two to start, can cast later
        # when we *know* we're looking at INET6
        ("ifa_addr", ctypes.POINTER(sockaddr_in4)),
        # Don't care about the rest of the fields
    ]

    def errcheck(result, func, arguments):
        if result != 0:
            errno = ctypes.set_errno(0)
            raise OSError(errno, "getifaddrs: %s" % os.strerror(errno))
        return result

    getifaddrs.errcheck = errcheck


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

    if getifaddrs:
        addrs = ctypes.POINTER(ifaddrs)()
        getifaddrs(ctypes.byref(addrs))
        try:
            cur = addrs
            while cur:
                if not cur.contents.ifa_addr:
                    # Not all interfaces will have addresses; move on
                    cur = cur.contents.ifa_next
                    continue
                sa_family = cur.contents.ifa_addr.contents.sin_family
                if sa_family == socket.AF_INET:
                    addresses.append(
                        socket.inet_ntop(
                            socket.AF_INET,
                            cur.contents.ifa_addr.contents.sin_addr,
                        )
                    )
                elif sa_family == socket.AF_INET6:
                    addr = ctypes.cast(cur.contents.ifa_addr,
                                       ctypes.POINTER(sockaddr_in6))
                    addresses.append(
                        socket.inet_ntop(
                            socket.AF_INET6,
                            addr.contents.sin6_addr,
                        )
                    )
                cur = cur.contents.ifa_next
        finally:
            freeifaddrs(addrs)
        return addresses

    # getifaddrs not available; try netifaces
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
