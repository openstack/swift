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

import io
import traceback
from optparse import OptionParser
from sys import exit, stdout
from time import time

from eventlet import GreenPool, patcher, sleep
from eventlet.pools import Pool
from configparser import ConfigParser

from swift.common.internal_client import SimpleClient
from swift.common.ring import Ring
from swift.common.utils import compute_eta, get_time_units, config_true_value
from swift.common.storage_policy import POLICIES

insecure = False


def put_container(connpool, container, report, headers):
    global retries_done
    try:
        with connpool.item() as conn:
            conn.put_container(container, headers=headers)
            retries_done += conn.attempts - 1
        if report:
            report(True)
    except Exception:
        if report:
            report(False)
        raise


def put_object(connpool, container, obj, report):
    global retries_done
    try:
        with connpool.item() as conn:
            data = io.BytesIO(obj.encode('utf8'))
            conn.put_object(container, obj, data,
                            headers={'x-object-meta-dispersion': obj})
            retries_done += conn.attempts - 1
        if report:
            report(True)
    except Exception:
        if report:
            report(False)
        raise


def report(success):
    global begun, created, item_type, next_report, need_to_create, retries_done
    if not success:
        traceback.print_exc()
        exit('Gave up due to error(s).')
    created += 1
    if time() < next_report:
        return
    next_report = time() + 5
    eta, eta_unit = compute_eta(begun, created, need_to_create)
    print('\r\x1B[KCreating %s: %d of %d, %d%s left, %d retries'
          % (item_type, created, need_to_create, round(eta), eta_unit,
             retries_done), end='')
    stdout.flush()


def main():
    global begun, created, item_type, next_report, need_to_create, retries_done
    patcher.monkey_patch()
    try:
        # Delay importing so urllib3 will import monkey-patched modules
        from swiftclient import get_auth
    except ImportError:
        from swift.common.internal_client import get_auth

    conffile = '/etc/swift/dispersion.conf'

    parser = OptionParser(usage='''
Usage: %%prog [options] [conf_file]

[conf_file] defaults to %s'''.strip() % conffile)
    parser.add_option('--container-only', action='store_true', default=False,
                      help='Only run container population')
    parser.add_option('--object-only', action='store_true', default=False,
                      help='Only run object population')
    parser.add_option('--container-suffix-start', type=int, default=0,
                      help='container suffix start value, defaults to 0')
    parser.add_option('--object-suffix-start', type=int, default=0,
                      help='object suffix start value, defaults to 0')
    parser.add_option('--insecure', action='store_true', default=False,
                      help='Allow accessing insecure keystone server. '
                           'The keystone\'s certificate will not be verified.')
    parser.add_option('--no-overlap', action='store_true', default=False,
                      help="No overlap of partitions if running populate \
                      more than once. Will increase coverage by amount shown \
                      in dispersion.conf file")
    parser.add_option('-P', '--policy-name', dest='policy_name',
                      help="Specify storage policy name")

    options, args = parser.parse_args()

    if args:
        conffile = args.pop(0)

    c = ConfigParser()
    if not c.read(conffile):
        exit('Unable to read config file: %s' % conffile)
    conf = dict(c.items('dispersion'))

    if options.policy_name is None:
        policy = POLICIES.default
    else:
        policy = POLICIES.get_by_name(options.policy_name)
        if policy is None:
            exit('Unable to find policy: %s' % options.policy_name)
    print('Using storage policy: %s ' % policy.name)

    swift_dir = conf.get('swift_dir', '/etc/swift')
    dispersion_coverage = float(conf.get('dispersion_coverage', 1))
    retries = int(conf.get('retries', 5))
    concurrency = int(conf.get('concurrency', 25))
    endpoint_type = str(conf.get('endpoint_type', 'publicURL'))
    region_name = str(conf.get('region_name', ''))
    user_domain_name = str(conf.get('user_domain_name', ''))
    project_domain_name = str(conf.get('project_domain_name', ''))
    project_name = str(conf.get('project_name', ''))
    insecure = options.insecure \
        or config_true_value(conf.get('keystone_api_insecure', 'no'))
    container_populate = config_true_value(
        conf.get('container_populate', 'yes')) and not options.object_only
    object_populate = config_true_value(
        conf.get('object_populate', 'yes')) and not options.container_only

    if not (object_populate or container_populate):
        exit("Neither container or object populate is set to run")

    coropool = GreenPool(size=concurrency)
    retries_done = 0

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
    headers = {}
    headers['X-Storage-Policy'] = policy.name
    connpool.create = lambda: SimpleClient(
        url=url, token=token, retries=retries)

    if container_populate:
        container_ring = Ring(swift_dir, ring_name='container')
        parts_left = dict((x, x)
                          for x in range(container_ring.partition_count))

        if options.no_overlap:
            with connpool.item() as conn:
                containers = [cont['name'] for cont in conn.get_account(
                    prefix='dispersion_%d' % policy.idx, full_listing=True)[1]]
            containers_listed = len(containers)
            if containers_listed > 0:
                for container in containers:
                    partition, _junk = container_ring.get_nodes(account,
                                                                container)
                    if partition in parts_left:
                        del parts_left[partition]

        item_type = 'containers'
        created = 0
        retries_done = 0
        need_to_create = need_to_queue = \
            dispersion_coverage / 100.0 * container_ring.partition_count
        begun = next_report = time()
        next_report += 2
        suffix = 0
        while need_to_queue >= 1 and parts_left:
            container = 'dispersion_%d_%d' % (policy.idx, suffix)
            part = container_ring.get_part(account, container)
            if part in parts_left:
                if suffix >= options.container_suffix_start:
                    coropool.spawn(put_container, connpool, container, report,
                                   headers)
                    sleep()
                else:
                    report(True)
                del parts_left[part]
                need_to_queue -= 1
            suffix += 1
        coropool.waitall()
        elapsed, elapsed_unit = get_time_units(time() - begun)
        print('\r\x1B[KCreated %d containers for dispersion reporting, '
              '%d%s, %d retries' %
              ((need_to_create - need_to_queue), round(elapsed), elapsed_unit,
               retries_done))
        if options.no_overlap:
            con_coverage = container_ring.partition_count - len(parts_left)
            print('\r\x1B[KTotal container coverage is now %.2f%%.' %
                  ((float(con_coverage) / container_ring.partition_count
                    * 100)))
        stdout.flush()

    if object_populate:
        container = 'dispersion_objects_%d' % policy.idx
        put_container(connpool, container, None, headers)
        object_ring = Ring(swift_dir, ring_name=policy.ring_name)
        parts_left = dict((x, x) for x in range(object_ring.partition_count))

        if options.no_overlap:
            with connpool.item() as conn:
                obj_container = [cont_b['name'] for cont_b in conn.get_account(
                    prefix=container, full_listing=True)[1]]
            if obj_container:
                with connpool.item() as conn:
                    objects = [o['name'] for o in
                               conn.get_container(container,
                                                  prefix='dispersion_',
                                                  full_listing=True)[1]]
                for my_object in objects:
                    partition = object_ring.get_part(account, container,
                                                     my_object)
                    if partition in parts_left:
                        del parts_left[partition]

        item_type = 'objects'
        created = 0
        retries_done = 0
        need_to_create = need_to_queue = \
            dispersion_coverage / 100.0 * object_ring.partition_count
        begun = next_report = time()
        next_report += 2
        suffix = 0
        while need_to_queue >= 1 and parts_left:
            obj = 'dispersion_%d' % suffix
            part = object_ring.get_part(account, container, obj)
            if part in parts_left:
                if suffix >= options.object_suffix_start:
                    coropool.spawn(
                        put_object, connpool, container, obj, report)
                    sleep()
                else:
                    report(True)
                del parts_left[part]
                need_to_queue -= 1
            suffix += 1
        coropool.waitall()
        elapsed, elapsed_unit = get_time_units(time() - begun)
        print('\r\x1B[KCreated %d objects for dispersion reporting, '
              '%d%s, %d retries' %
              ((need_to_create - need_to_queue), round(elapsed), elapsed_unit,
               retries_done))
        if options.no_overlap:
            obj_coverage = object_ring.partition_count - len(parts_left)
            print('\r\x1B[KTotal object coverage is now %.2f%%.' %
                  ((float(obj_coverage) / object_ring.partition_count * 100)))
        stdout.flush()


if __name__ == '__main__':
    main()
