#!/usr/bin/python -u
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
from __future__ import print_function
import sys
import itertools
import uuid
from optparse import OptionParser
import random

import six
from six.moves.urllib.parse import urlparse

from swift.common.manager import Manager
from swift.common import utils, ring
from swift.common.storage_policy import POLICIES
from swift.common.http import HTTP_NOT_FOUND

from swiftclient import client, get_auth, ClientException

from test.probe.common import ENABLED_POLICIES

TIMEOUT = 60


def meta_command(name, bases, attrs):
    """
    Look for attrs with a truthy attribute __command__ and add them to an
    attribute __commands__ on the type that maps names to decorated methods.
    The decorated methods' doc strings also get mapped in __docs__.

    Also adds a method run(command_name, *args, **kwargs) that will
    execute the method mapped to the name in __commands__.
    """
    commands = {}
    docs = {}
    for attr, value in attrs.items():
        if getattr(value, '__command__', False):
            commands[attr] = value
            # methods have always have a __doc__ attribute, sometimes empty
            docs[attr] = (getattr(value, '__doc__', None) or
                          'perform the %s command' % attr).strip()
    attrs['__commands__'] = commands
    attrs['__docs__'] = docs

    def run(self, command, *args, **kwargs):
        return self.__commands__[command](self, *args, **kwargs)
    attrs.setdefault('run', run)
    return type(name, bases, attrs)


def command(f):
    f.__command__ = True
    return f


@six.add_metaclass(meta_command)
class BrainSplitter(object):
    def __init__(self, url, token, container_name='test', object_name='test',
                 server_type='container', policy=None):
        self.url = url
        self.token = token
        self.account = utils.split_path(urlparse(url).path, 2, 2)[1]
        self.container_name = container_name
        self.object_name = object_name
        server_list = ['%s-server' % server_type] if server_type else ['all']
        self.servers = Manager(server_list)
        policies = list(ENABLED_POLICIES)
        random.shuffle(policies)
        self.policies = itertools.cycle(policies)

        o = object_name if server_type == 'object' else None
        c = container_name if server_type in ('object', 'container') else None
        if server_type in ('container', 'account'):
            if policy:
                raise TypeError('Metadata server brains do not '
                                'support specific storage policies')
            self.policy = None
            self.ring = ring.Ring(
                '/etc/swift/%s.ring.gz' % server_type)
        elif server_type == 'object':
            if not policy:
                raise TypeError('Object BrainSplitters need to '
                                'specify the storage policy')
            self.policy = policy
            policy.load_ring('/etc/swift')
            self.ring = policy.object_ring
        else:
            raise ValueError('Unknown server_type: %r' % server_type)
        self.server_type = server_type

        self.part, self.nodes = self.ring.get_nodes(self.account, c, o)

        self.node_numbers = [n['id'] + 1 for n in self.nodes]
        if 1 in self.node_numbers and 2 in self.node_numbers:
            self.primary_numbers = (1, 2)
            self.handoff_numbers = (3, 4)
        else:
            self.primary_numbers = (3, 4)
            self.handoff_numbers = (1, 2)

    @command
    def start_primary_half(self):
        """
        start servers 1 & 2
        """
        tuple(self.servers.start(number=n) for n in self.primary_numbers)

    @command
    def stop_primary_half(self):
        """
        stop servers 1 & 2
        """
        tuple(self.servers.stop(number=n) for n in self.primary_numbers)

    @command
    def start_handoff_half(self):
        """
        start servers 3 & 4
        """
        tuple(self.servers.start(number=n) for n in self.handoff_numbers)

    @command
    def stop_handoff_half(self):
        """
        stop servers 3 & 4
        """
        tuple(self.servers.stop(number=n) for n in self.handoff_numbers)

    @command
    def put_container(self, policy_index=None):
        """
        put container with next storage policy
        """

        if policy_index is not None:
            policy = POLICIES.get_by_index(int(policy_index))
            if not policy:
                raise ValueError('Unknown policy with index %s' % policy)
        elif not self.policy:
            policy = next(self.policies)
        else:
            policy = self.policy

        headers = {'X-Storage-Policy': policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

    @command
    def delete_container(self):
        """
        delete container
        """
        client.delete_container(self.url, self.token, self.container_name)

    @command
    def put_object(self, headers=None, contents=None):
        """
        issue put for test object
        """
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, headers=headers, contents=contents)

    @command
    def delete_object(self):
        """
        issue delete for test object
        """
        try:
            client.delete_object(self.url, self.token, self.container_name,
                                 self.object_name)
        except ClientException as err:
            if err.http_status != HTTP_NOT_FOUND:
                raise

parser = OptionParser('%prog [options] '
                      '<command>[:<args>[,<args>...]] [<command>...]')
parser.usage += '\n\nCommands:\n\t' + \
    '\n\t'.join("%s - %s" % (name, doc) for name, doc in
                BrainSplitter.__docs__.items())
parser.add_option('-c', '--container', default='container-%s' % uuid.uuid4(),
                  help='set container name')
parser.add_option('-o', '--object', default='object-%s' % uuid.uuid4(),
                  help='set object name')
parser.add_option('-s', '--server_type', default='container',
                  help='set server type')
parser.add_option('-P', '--policy_name', default=None,
                  help='set policy')


def main():
    options, commands = parser.parse_args()
    if not commands:
        parser.print_help()
        return 'ERROR: must specify at least one command'
    for cmd_args in commands:
        cmd = cmd_args.split(':', 1)[0]
        if cmd not in BrainSplitter.__commands__:
            parser.print_help()
            return 'ERROR: unknown command %s' % cmd
    url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                          'test:tester', 'testing')
    if options.server_type == 'object' and not options.policy_name:
        options.policy_name = POLICIES.default.name
    if options.policy_name:
        options.server_type = 'object'
        policy = POLICIES.get_by_name(options.policy_name)
        if not policy:
            return 'ERROR: unknown policy %r' % options.policy_name
    else:
        policy = None
    brain = BrainSplitter(url, token, options.container, options.object,
                          options.server_type, policy=policy)
    for cmd_args in commands:
        parts = cmd_args.split(':', 1)
        command = parts[0]
        if len(parts) > 1:
            args = utils.list_from_csv(parts[1])
        else:
            args = ()
        try:
            brain.run(command, *args)
        except ClientException as e:
            print('**WARNING**: %s raised %s' % (command, e))
    print('STATUS'.join(['*' * 25] * 2))
    brain.servers.status()
    sys.exit()


if __name__ == "__main__":
    sys.exit(main())
