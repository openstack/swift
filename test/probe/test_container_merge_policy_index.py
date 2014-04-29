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

import sys
import itertools
import unittest
import uuid
from optparse import OptionParser
from urlparse import urlparse
import random

from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES
from swift.common import utils, ring
from swift.common.http import HTTP_NOT_FOUND
from test.probe.common import reset_environment

from swiftclient import client, get_auth, ClientException

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


class BrainSplitter(object):

    __metaclass__ = meta_command

    def __init__(self, url, token, container_name='test', object_name='test'):
        self.url = url
        self.token = token
        self.account = utils.split_path(urlparse(url).path, 2, 2)[1]
        self.container_name = container_name
        self.object_name = object_name
        self.servers = Manager(['container-server'])
        policies = list(POLICIES)
        random.shuffle(policies)
        self.policies = itertools.cycle(policies)

        container_part, container_nodes = ring.Ring(
            '/etc/swift/container.ring.gz').get_nodes(
                self.account, self.container_name)
        container_node_ids = [n['id'] for n in container_nodes]
        if all(n_id in container_node_ids for n_id in (0, 1)):
            self.primary_numbers = (1, 2)
            self.handoff_numbers = (3, 4)
        else:
            self.primary_numbers = (3, 4)
            self.handoff_numbers = (1, 2)

    @command
    def start_primary_half(self):
        """
        start container servers 1 & 2
        """
        tuple(self.servers.start(number=n) for n in self.primary_numbers)

    @command
    def stop_primary_half(self):
        """
        stop container servers 1 & 2
        """
        tuple(self.servers.stop(number=n) for n in self.primary_numbers)

    @command
    def start_handoff_half(self):
        """
        start container servers 3 & 4
        """
        tuple(self.servers.start(number=n) for n in self.handoff_numbers)

    @command
    def stop_handoff_half(self):
        """
        stop container servers 3 & 4
        """
        tuple(self.servers.stop(number=n) for n in self.handoff_numbers)

    @command
    def put_container(self, policy_index=None):
        """
        put container with next storage policy
        """
        policy = self.policies.next()
        if policy_index is not None:
            policy = POLICIES.get_by_index(int(policy_index))
            if not policy:
                raise ValueError('Unknown policy with index %s' % policy)
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
    def put_object(self, headers=None):
        """
        issue put for zero byte test object
        """
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, headers=headers)

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

parser = OptionParser('%prog split-brain [options] '
                      '<command>[:<args>[,<args>...]] [<command>...]')
parser.usage += '\n\nCommands:\n\t' + \
    '\n\t'.join("%s - %s" % (name, doc) for name, doc in
                BrainSplitter.__docs__.items())
parser.add_option('-c', '--container', default='container-%s' % uuid.uuid4(),
                  help='set container name')
parser.add_option('-o', '--object', default='object-%s' % uuid.uuid4(),
                  help='set object name')


class TestContainerMergePolicyIndex(unittest.TestCase):

    def setUp(self):
        if len(POLICIES) < 2:
            raise unittest.SkipTest()
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.url, self.token,
         self.account, self.configs) = reset_environment()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name)


def main():
    options, commands = parser.parse_args()
    commands.remove('split-brain')
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
    brain = BrainSplitter(url, token, options.container, options.object)
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
            print '**WARNING**: %s raised %s' % (command, e)
    print 'STATUS'.join(['*' * 25] * 2)
    brain.servers.status()
    sys.exit()


if __name__ == "__main__":
    if any('split-brain' in arg for arg in sys.argv):
        sys.exit(main())
    unittest.main()
