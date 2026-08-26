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

from swift.common.concurrency import GreenPool, patcher, sleep, Pool
from configparser import ConfigParser

from swift.common.internal_client import SimpleClient
from swift.common.exceptions import ClientException
from swift.common.ring import Ring
from swift.common.utils import compute_eta, get_time_units, config_true_value
from swift.common.storage_policy import POLICIES
from swift.cli.dispersion import (
    all_container_dispersion_names, container_dispersion_names,
    object_dispersion_container, object_dispersion_containers_to_purge,
    object_dispersion_names, obsolete_container_dispersion_names,
    obsolete_object_dispersion_containers, warn_on_obsolete_samples)

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


def delete_container(connpool, container, report):
    global retries_done
    try:
        with connpool.item() as conn:
            conn.delete_container(container)
            retries_done += conn.attempts - 1
        report(True)
    except Exception:
        report(False)
        raise


def delete_object(connpool, container, obj, report):
    global retries_done
    try:
        with connpool.item() as conn:
            conn.delete_object(container, obj)
            retries_done += conn.attempts - 1
        report(True)
    except Exception:
        report(False)
        raise


def purge_report(success):
    """Record worker results for the failure check after waitall."""
    global deleted, purge_failed
    if not success:
        traceback.print_exc()
        purge_failed = True
    else:
        deleted += 1


def report(success):
    global created, next_report
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
    global deleted, purge_failed
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
                      help='Add configured coverage on partitions not already '
                           'covered by canonical samples; cannot be used '
                           'with --purge')
    parser.add_option('-P', '--policy-name', dest='policy_name',
                      help="Specify storage policy name")
    parser.add_option('-s', '--section', dest='section',
                      help="Read overrides from this dispersion.conf section")
    parser.add_option(
        '--purge', action='store_true', default=False,
        help='Delete dispersion data selected by --container-only or '
             '--object-only before repopulating it')

    options, args = parser.parse_args()

    if args:
        conffile = args.pop(0)

    c = ConfigParser()
    if not c.read(conffile):
        exit('Unable to read config file: %s' % conffile)
    if not c.has_section('dispersion'):
        exit('Unable to find [dispersion] in config file: %s' % conffile)
    conf = dict(c.items('dispersion'))
    if options.section:
        if not c.has_section(options.section):
            exit('Unable to find [%s] in config file: %s' %
                 (options.section, conffile))
        conf.update(c.items(options.section))

    policy_name = options.policy_name or conf.get('policy_name')
    if policy_name is None:
        policy = POLICIES.default
    else:
        policy = POLICIES.get_by_name(policy_name)
        if policy is None:
            exit('Unable to find policy: %s' % policy_name)
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

    if options.purge and options.no_overlap:
        exit('--purge and --no-overlap are mutually exclusive')
    if options.purge and options.container_only == options.object_only:
        exit('--purge requires exactly one of --container-only or '
             '--object-only')
    if options.purge and options.object_only and policy_name is None:
        exit('--object-only --purge requires an explicit policy_name')
    if not (object_populate or container_populate):
        exit("Neither container or object populate is set to run")
    if container_populate and policy is not POLICIES.default:
        exit('Container dispersion is shared by all storage policies; '
             'use --object-only with a non-default policy.')

    coropool = GreenPool(size=concurrency)
    retries_done = 0
    deleted = 0
    purge_failed = False

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
    object_headers = {'X-Storage-Policy': policy.name}
    connpool.create = lambda: SimpleClient(
        url=url, token=token, retries=retries)

    if container_populate:
        with connpool.item() as conn:
            listing = conn.get_account(
                prefix='dispersion_', full_listing=True)[1]
        existing_containers = []
        if options.purge:
            for container in all_container_dispersion_names(listing):
                coropool.spawn(delete_container, connpool, container,
                               purge_report)
                sleep()
            coropool.waitall()
            if purge_failed:
                exit('Gave up due to error(s).')
            print('Deleted %d container samples, %d retries' %
                  (deleted, retries_done))
        else:
            warn_on_obsolete_samples(
                'container', obsolete_container_dispersion_names(listing))
            if options.no_overlap:
                existing_containers = container_dispersion_names(listing)

        if dispersion_coverage > 0:
            container_ring = Ring(swift_dir, ring_name='container')
            parts_left = dict((x, x)
                              for x in range(container_ring.partition_count))

            for container in existing_containers:
                partition, _junk = container_ring.get_nodes(account, container)
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
                container = 'dispersion_%d' % suffix
                part = container_ring.get_part(account, container)
                if part in parts_left:
                    if suffix >= options.container_suffix_start:
                        coropool.spawn(put_container, connpool, container,
                                       report, {})
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
                  ((need_to_create - need_to_queue), round(elapsed),
                   elapsed_unit, retries_done))
            if options.no_overlap:
                con_coverage = container_ring.partition_count - len(parts_left)
                print('\r\x1B[KTotal container coverage is now %.2f%%.' %
                      ((float(con_coverage) / container_ring.partition_count
                        * 100)))
            stdout.flush()

    if object_populate:
        container = object_dispersion_container(policy.idx)
        existing_objects = []
        if options.purge:
            for sample_container in object_dispersion_containers_to_purge(
                    policy.idx):
                with connpool.item() as conn:
                    try:
                        listing = conn.get_container(
                            sample_container, prefix='dispersion_',
                            full_listing=True)[1]
                    except ClientException as err:
                        if err.http_status != 404:
                            raise
                        continue
                for obj in object_dispersion_names(listing):
                    coropool.spawn(delete_object, connpool, sample_container,
                                   obj, purge_report)
                    sleep()
                coropool.waitall()
                if purge_failed:
                    exit('Gave up due to error(s).')
                with connpool.item() as conn:
                    conn.delete_container(sample_container)
                    retries_done += conn.attempts - 1
                print('Deleted object dispersion container %s.' %
                      sample_container)
            print('Deleted %d object samples for policy %s, %d retries' %
                  (deleted, policy.name, retries_done))
        else:
            with connpool.item() as conn:
                listing = conn.get_account(
                    prefix='dispersion_objects', full_listing=True)[1]
            warn_on_obsolete_samples(
                'object', obsolete_object_dispersion_containers(
                    listing, policy.idx))
            if options.no_overlap and dispersion_coverage > 0 and \
                    container in {item['name'] for item in listing}:
                with connpool.item() as conn:
                    listing = conn.get_container(
                        container, prefix='dispersion_',
                        full_listing=True)[1]
                existing_objects = object_dispersion_names(listing)

        if dispersion_coverage > 0:
            container_created = False
            object_ring = Ring(swift_dir, ring_name=policy.ring_name)
            parts_left = dict((x, x)
                              for x in range(object_ring.partition_count))

            for obj in existing_objects:
                partition = object_ring.get_part(account, container, obj)
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
                        if not container_created:
                            put_container(connpool, container, None,
                                          object_headers)
                            container_created = True
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
                  ((need_to_create - need_to_queue), round(elapsed),
                   elapsed_unit, retries_done))
            if options.no_overlap:
                obj_coverage = object_ring.partition_count - len(parts_left)
                print('\r\x1B[KTotal object coverage is now %.2f%%.' %
                      ((float(obj_coverage) / object_ring.partition_count
                        * 100)))
            stdout.flush()


if __name__ == '__main__':
    main()
