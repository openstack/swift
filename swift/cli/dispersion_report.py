#!/usr/bin/env python
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

from __future__ import print_function
import json
from collections import defaultdict
from six.moves.configparser import ConfigParser
from optparse import OptionParser
from sys import exit, stdout, stderr
from time import time

from eventlet import GreenPool, hubs, patcher, Timeout
from eventlet.pools import Pool

from swift.common import direct_client
try:
    from swiftclient import get_auth
except ImportError:
    from swift.common.internal_client import get_auth
from swift.common.internal_client import SimpleClient
from swift.common.ring import Ring
from swift.common.exceptions import ClientException
from swift.common.utils import compute_eta, get_time_units, config_true_value
from swift.common.storage_policy import POLICIES


unmounted = []
notfound = []
json_output = False
debug = False
insecure = False


def get_error_log(prefix):

    def error_log(msg_or_exc):
        global debug, unmounted, notfound
        if hasattr(msg_or_exc, 'http_status'):
            identifier = '%s:%s/%s' % (msg_or_exc.http_host,
                                       msg_or_exc.http_port,
                                       msg_or_exc.http_device)
            if msg_or_exc.http_status == 507:
                if identifier not in unmounted:
                    unmounted.append(identifier)
                    print('ERROR: %s is unmounted -- This will '
                          'cause replicas designated for that device to be '
                          'considered missing until resolved or the ring is '
                          'updated.' % (identifier), file=stderr)
                    stderr.flush()
            if debug and identifier not in notfound:
                notfound.append(identifier)
                print('ERROR: %s returned a 404' % (identifier), file=stderr)
                stderr.flush()
        if not hasattr(msg_or_exc, 'http_status') or \
                msg_or_exc.http_status not in (404, 507):
            print('ERROR: %s: %s' % (prefix, msg_or_exc), file=stderr)
            stderr.flush()
    return error_log


def container_dispersion_report(coropool, connpool, account, container_ring,
                                retries, output_missing_partitions, policy):
    with connpool.item() as conn:
        containers = [c['name'] for c in conn.get_account(
            prefix='dispersion_%d' % policy.idx, full_listing=True)[1]]
    containers_listed = len(containers)
    if not containers_listed:
        print('No containers to query. Has '
              'swift-dispersion-populate been run?', file=stderr)
        stderr.flush()
        return
    retries_done = [0]
    containers_queried = [0]
    container_copies_missing = defaultdict(int)
    container_copies_found = [0]
    container_copies_expected = [0]
    begun = time()
    next_report = [time() + 2]

    def direct(container, part, nodes):
        found_count = 0
        for node in nodes:
            error_log = get_error_log('%(ip)s:%(port)s/%(device)s' % node)
            try:
                attempts, _junk = direct_client.retry(
                    direct_client.direct_head_container, node, part, account,
                    container, error_log=error_log, retries=retries)
                retries_done[0] += attempts - 1
                found_count += 1
            except ClientException as err:
                if err.http_status not in (404, 507):
                    error_log('Giving up on /%s/%s/%s: %s' % (part, account,
                              container, err))
            except (Exception, Timeout) as err:
                error_log('Giving up on /%s/%s/%s: %s' % (part, account,
                          container, err))
        if output_missing_partitions and \
                found_count < len(nodes):
            missing = len(nodes) - found_count
            print('\r\x1B[K', end='')
            stdout.flush()
            print('# Container partition %s missing %s cop%s' % (
                part, missing, 'y' if missing == 1 else 'ies'), file=stderr)
        container_copies_found[0] += found_count
        containers_queried[0] += 1
        container_copies_missing[len(nodes) - found_count] += 1
        if time() >= next_report[0]:
            next_report[0] = time() + 5
            eta, eta_unit = compute_eta(begun, containers_queried[0],
                                        containers_listed)
            if not json_output:
                print('\r\x1B[KQuerying containers: %d of %d, %d%s left, %d '
                      'retries' % (containers_queried[0], containers_listed,
                                   round(eta), eta_unit, retries_done[0]),
                      end='')
                stdout.flush()
    container_parts = {}
    for container in containers:
        part, nodes = container_ring.get_nodes(account, container)
        if part not in container_parts:
            container_copies_expected[0] += len(nodes)
            container_parts[part] = part
            coropool.spawn(direct, container, part, nodes)
    coropool.waitall()
    distinct_partitions = len(container_parts)
    copies_found = container_copies_found[0]
    copies_expected = container_copies_expected[0]
    value = 100.0 * copies_found / copies_expected
    elapsed, elapsed_unit = get_time_units(time() - begun)
    container_copies_missing.pop(0, None)
    if not json_output:
        print('\r\x1B[KQueried %d containers for dispersion reporting, '
              '%d%s, %d retries' % (containers_listed, round(elapsed),
                                    elapsed_unit, retries_done[0]))
        if containers_listed - distinct_partitions:
            print('There were %d overlapping partitions' % (
                  containers_listed - distinct_partitions))
        for missing_copies, num_parts in container_copies_missing.items():
            print(missing_string(num_parts, missing_copies,
                                 container_ring.replica_count))
        print('%.02f%% of container copies found (%d of %d)' % (
            value, copies_found, copies_expected))
        print('Sample represents %.02f%% of the container partition space' % (
            100.0 * distinct_partitions / container_ring.partition_count))
        stdout.flush()
        return None
    else:
        results = {'retries': retries_done[0],
                   'overlapping': containers_listed - distinct_partitions,
                   'pct_found': value,
                   'copies_found': copies_found,
                   'copies_expected': copies_expected}
        for missing_copies, num_parts in container_copies_missing.items():
            results['missing_%d' % (missing_copies)] = num_parts
        return results


def object_dispersion_report(coropool, connpool, account, object_ring,
                             retries, output_missing_partitions, policy):
    container = 'dispersion_objects_%d' % policy.idx
    with connpool.item() as conn:
        try:
            objects = [o['name'] for o in conn.get_container(
                container, prefix='dispersion_', full_listing=True)[1]]
        except ClientException as err:
            if err.http_status != 404:
                raise

            print('No objects to query. Has '
                  'swift-dispersion-populate been run?', file=stderr)
            stderr.flush()
            return
    objects_listed = len(objects)
    if not objects_listed:
        print('No objects to query. Has swift-dispersion-populate '
              'been run?', file=stderr)
        stderr.flush()
        return
    retries_done = [0]
    objects_queried = [0]
    object_copies_found = [0]
    object_copies_expected = [0]
    object_copies_missing = defaultdict(int)
    begun = time()
    next_report = [time() + 2]

    headers = None
    if policy is not None:
        headers = {}
        headers['X-Backend-Storage-Policy-Index'] = int(policy)

    def direct(obj, part, nodes):
        found_count = 0
        for node in nodes:
            error_log = get_error_log('%(ip)s:%(port)s/%(device)s' % node)
            try:
                attempts, _junk = direct_client.retry(
                    direct_client.direct_head_object, node, part, account,
                    container, obj, error_log=error_log, retries=retries,
                    headers=headers)
                retries_done[0] += attempts - 1
                found_count += 1
            except ClientException as err:
                if err.http_status not in (404, 507):
                    error_log('Giving up on /%s/%s/%s/%s: %s' % (part, account,
                              container, obj, err))
            except (Exception, Timeout) as err:
                error_log('Giving up on /%s/%s/%s/%s: %s' % (part, account,
                          container, obj, err))
        if output_missing_partitions and \
                found_count < len(nodes):
            missing = len(nodes) - found_count
            print('\r\x1B[K', end='')
            stdout.flush()
            print('# Object partition %s missing %s cop%s' % (
                part, missing, 'y' if missing == 1 else 'ies'), file=stderr)
        object_copies_found[0] += found_count
        object_copies_missing[len(nodes) - found_count] += 1
        objects_queried[0] += 1
        if time() >= next_report[0]:
            next_report[0] = time() + 5
            eta, eta_unit = compute_eta(begun, objects_queried[0],
                                        objects_listed)
            if not json_output:
                print('\r\x1B[KQuerying objects: %d of %d, %d%s left, %d '
                      'retries' % (objects_queried[0], objects_listed,
                                   round(eta), eta_unit, retries_done[0]),
                      end='')
            stdout.flush()
    object_parts = {}
    for obj in objects:
        part, nodes = object_ring.get_nodes(account, container, obj)
        if part not in object_parts:
            object_copies_expected[0] += len(nodes)
            object_parts[part] = part
            coropool.spawn(direct, obj, part, nodes)
    coropool.waitall()
    distinct_partitions = len(object_parts)
    copies_found = object_copies_found[0]
    copies_expected = object_copies_expected[0]
    value = 100.0 * copies_found / copies_expected
    elapsed, elapsed_unit = get_time_units(time() - begun)
    if not json_output:
        print('\r\x1B[KQueried %d objects for dispersion reporting, '
              '%d%s, %d retries' % (objects_listed, round(elapsed),
                                    elapsed_unit, retries_done[0]))
        if objects_listed - distinct_partitions:
            print('There were %d overlapping partitions' % (
                  objects_listed - distinct_partitions))

        for missing_copies, num_parts in object_copies_missing.items():
            print(missing_string(num_parts, missing_copies,
                                 object_ring.replica_count))

        print('%.02f%% of object copies found (%d of %d)' %
              (value, copies_found, copies_expected))
        print('Sample represents %.02f%% of the object partition space' % (
            100.0 * distinct_partitions / object_ring.partition_count))
        stdout.flush()
        return None
    else:
        results = {'retries': retries_done[0],
                   'overlapping': objects_listed - distinct_partitions,
                   'pct_found': value,
                   'copies_found': copies_found,
                   'copies_expected': copies_expected}

        for missing_copies, num_parts in object_copies_missing.items():
            results['missing_%d' % (missing_copies,)] = num_parts
        return results


def missing_string(partition_count, missing_copies, copy_count):
    exclamations = ''
    missing_string = str(missing_copies)
    if missing_copies == copy_count:
        exclamations = '!!! '
        missing_string = 'all'
    elif copy_count - missing_copies == 1:
        exclamations = '! '

    verb_string = 'was'
    partition_string = 'partition'
    if partition_count > 1:
        verb_string = 'were'
        partition_string = 'partitions'

    copy_string = 'copies'
    if missing_copies == 1:
        copy_string = 'copy'

    return '%sThere %s %d %s missing %s %s.' % (
        exclamations, verb_string, partition_count, partition_string,
        missing_string, copy_string
    )


def main():
    patcher.monkey_patch()
    hubs.get_hub().debug_exceptions = False

    conffile = '/etc/swift/dispersion.conf'

    parser = OptionParser(usage='''
Usage: %%prog [options] [conf_file]

[conf_file] defaults to %s'''.strip() % conffile)
    parser.add_option('-j', '--dump-json', action='store_true', default=False,
                      help='dump dispersion report in json format')
    parser.add_option('-d', '--debug', action='store_true', default=False,
                      help='print 404s to standard error')
    parser.add_option('-p', '--partitions', action='store_true', default=False,
                      help='print missing partitions to standard error')
    parser.add_option('--container-only', action='store_true', default=False,
                      help='Only run container report')
    parser.add_option('--object-only', action='store_true', default=False,
                      help='Only run object report')
    parser.add_option('--insecure', action='store_true', default=False,
                      help='Allow accessing insecure keystone server. '
                           'The keystone\'s certificate will not be verified.')
    parser.add_option('-P', '--policy-name', dest='policy_name',
                      help="Specify storage policy name")

    options, args = parser.parse_args()
    if args:
        conffile = args.pop(0)

    if options.debug:
        global debug
        debug = True

    c = ConfigParser()
    if not c.read(conffile):
        exit('Unable to read config file: %s' % conffile)
    conf = dict(c.items('dispersion'))

    if options.dump_json:
        conf['dump_json'] = 'yes'
    if options.object_only:
        conf['container_report'] = 'no'
    if options.container_only:
        conf['object_report'] = 'no'
    if options.insecure:
        conf['keystone_api_insecure'] = 'yes'
    if options.partitions:
        conf['partitions'] = 'yes'

    output = generate_report(conf, options.policy_name)

    if json_output:
        print(json.dumps(output))


def generate_report(conf, policy_name=None):
    global json_output
    json_output = config_true_value(conf.get('dump_json', 'no'))
    if policy_name is None:
        policy = POLICIES.default
    else:
        policy = POLICIES.get_by_name(policy_name)
        if policy is None:
            exit('Unable to find policy: %s' % policy_name)
    if not json_output:
        print('Using storage policy: %s ' % policy.name)

    swift_dir = conf.get('swift_dir', '/etc/swift')
    retries = int(conf.get('retries', 5))
    concurrency = int(conf.get('concurrency', 25))
    endpoint_type = str(conf.get('endpoint_type', 'publicURL'))
    region_name = str(conf.get('region_name', ''))
    container_report = config_true_value(conf.get('container_report', 'yes'))
    object_report = config_true_value(conf.get('object_report', 'yes'))
    if not (object_report or container_report):
        exit("Neither container or object report is set to run")
    user_domain_name = str(conf.get('user_domain_name', ''))
    project_domain_name = str(conf.get('project_domain_name', ''))
    project_name = str(conf.get('project_name', ''))
    insecure = config_true_value(conf.get('keystone_api_insecure', 'no'))

    coropool = GreenPool(size=concurrency)

    os_options = {'endpoint_type': endpoint_type}
    if user_domain_name:
        os_options['user_domain_name'] = user_domain_name
    if project_domain_name:
        os_options['project_domain_name'] = project_domain_name
    if project_name:
        os_options['project_name'] = project_name
    if region_name:
        os_options['region_name'] = region_name

    url, token = get_auth(conf['auth_url'], conf['auth_user'],
                          conf['auth_key'],
                          auth_version=conf.get('auth_version', '1.0'),
                          os_options=os_options,
                          insecure=insecure)
    account = url.rsplit('/', 1)[1]
    connpool = Pool(max_size=concurrency)
    connpool.create = lambda: SimpleClient(
        url=url, token=token, retries=retries)

    container_ring = Ring(swift_dir, ring_name='container')
    object_ring = Ring(swift_dir, ring_name=policy.ring_name)

    output = {}
    if container_report:
        output['container'] = container_dispersion_report(
            coropool, connpool, account, container_ring, retries,
            conf.get('partitions'), policy)
    if object_report:
        output['object'] = object_dispersion_report(
            coropool, connpool, account, object_ring, retries,
            conf.get('partitions'), policy)

    return output


if __name__ == '__main__':
    main()
