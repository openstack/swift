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
import functools
import sys
from io import BytesIO
import itertools
import uuid
from optparse import OptionParser
import random

from urllib.parse import urlparse, parse_qs, quote

from swift.common.manager import Manager
from swift.common import utils, ring
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.storage_policy import POLICIES
from swift.common.http import HTTP_NOT_FOUND

from swiftclient import client, get_auth, ClientException

from test.probe import PROXY_BASE_URL
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
            # methods always have a __doc__ attribute, sometimes empty
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


class BaseBrain(object, metaclass=meta_command):
    def _setup(self, account, container_name, object_name,
               server_type, policy):
        self.account = account
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
    def put_container(self, policy_index=None, extra_headers=None):
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
        if extra_headers:
            headers.update(extra_headers)
        self.client.put_container(self.container_name, headers=headers)

    @command
    def delete_container(self):
        """
        delete container
        """
        self.client.delete_container(self.container_name)

    @command
    def put_object(self, headers=None, contents=None):
        """
        issue put for test object
        """
        self.client.put_object(self.container_name, self.object_name,
                               headers=headers, contents=contents)

    @command
    def delete_object(self):
        """
        issue delete for test object
        """
        self.client.delete_object(self.container_name, self.object_name)

    @command
    def get_object(self):
        """
        issue GET for test object
        """
        return self.client.get_object(self.container_name, self.object_name)


class PublicBrainClient(object):
    def __init__(self, url, token):
        self.url = url
        self.token = token
        self.account = utils.split_path(urlparse(url).path, 2, 2)[1]

    def put_container(self, container_name, headers):
        return client.put_container(self.url, self.token, container_name,
                                    headers=headers)

    def post_container(self, container_name, headers):
        return client.post_container(self.url, self.token, container_name,
                                     headers)

    def get_container(self, container_name, headers=None, query_string=None):
        return client.get_container(self.url, self.token, container_name,
                                    headers=headers, query_string=query_string)

    def delete_container(self, container_name):
        return client.delete_container(self.url, self.token, container_name)

    def put_object(self, container_name, object_name, headers, contents,
                   query_string=None):
        return client.put_object(self.url, self.token, container_name,
                                 object_name, headers=headers,
                                 contents=contents, query_string=query_string)

    def delete_object(self, container_name, object_name):
        try:
            client.delete_object(self.url, self.token,
                                 container_name, object_name)
        except ClientException as err:
            if err.http_status != HTTP_NOT_FOUND:
                raise

    def head_object(self, container_name, object_name):
        return client.head_object(self.url, self.token, container_name,
                                  object_name)

    def get_object(self, container_name, object_name, query_string=None):
        return client.get_object(self.url, self.token,
                                 container_name, object_name,
                                 query_string=query_string)


def translate_client_exception(m):
    @functools.wraps(m)
    def wrapper(*args, **kwargs):
        try:
            return m(*args, **kwargs)
        except UnexpectedResponse as err:
            raise ClientException(
                err.args[0],
                http_scheme=err.resp.environ['wsgi.url_scheme'],
                http_host=err.resp.environ['SERVER_NAME'],
                http_port=err.resp.environ['SERVER_PORT'],
                http_path=quote(err.resp.environ['PATH_INFO']),
                http_query=err.resp.environ['QUERY_STRING'],
                http_status=err.resp.status_int,
                http_reason=err.resp.explanation,
                http_response_content=err.resp.body,
                http_response_headers=err.resp.headers,
            )
    return wrapper


class InternalBrainClient(object):

    def __init__(self, conf_file, account='AUTH_test'):
        self.swift = InternalClient(conf_file, 'probe-test', 3)
        self.account = account

    @translate_client_exception
    def put_container(self, container_name, headers):
        return self.swift.create_container(self.account, container_name,
                                           headers=headers)

    @translate_client_exception
    def post_container(self, container_name, headers):
        return self.swift.set_container_metadata(self.account, container_name,
                                                 headers)

    @translate_client_exception
    def delete_container(self, container_name):
        return self.swift.delete_container(self.account, container_name)

    def parse_qs(self, query_string):
        if query_string is not None:
            return {k: v[-1] for k, v in parse_qs(query_string).items()}

    @translate_client_exception
    def put_object(self, container_name, object_name, headers, contents,
                   query_string=None):
        return self.swift.upload_object(BytesIO(contents), self.account,
                                        container_name, object_name,
                                        headers=headers,
                                        params=self.parse_qs(query_string))

    @translate_client_exception
    def delete_object(self, container_name, object_name):
        return self.swift.delete_object(
            self.account, container_name, object_name)

    @translate_client_exception
    def head_object(self, container_name, object_name):
        return self.swift.get_object_metadata(
            self.account, container_name, object_name)

    @translate_client_exception
    def get_object(self, container_name, object_name, query_string=None):
        status, headers, resp_iter = self.swift.get_object(
            self.account, container_name, object_name,
            params=self.parse_qs(query_string))
        return headers, b''.join(resp_iter)


class BrainSplitter(BaseBrain):
    def __init__(self, url, token, container_name='test', object_name='test',
                 server_type='container', policy=None):
        self.client = PublicBrainClient(url, token)
        self._setup(self.client.account, container_name, object_name,
                    server_type, policy)


class InternalBrainSplitter(BaseBrain):
    def __init__(self, conf, container_name='test', object_name='test',
                 server_type='container', policy=None):
        self.client = InternalBrainClient(conf)
        self._setup(self.client.account, container_name, object_name,
                    server_type, policy)


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
    url, token = get_auth(PROXY_BASE_URL + '/auth/v1.0',
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
