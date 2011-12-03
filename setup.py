#!/usr/bin/python
# Copyright (c) 2010-2011 OpenStack, LLC.
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

from setuptools import setup, find_packages

from swift import __canonical_version__ as version


name = 'swift'


setup(
    name=name,
    version=version,
    description='Swift',
    license='Apache License (2.0)',
    author='OpenStack, LLC.',
    author_email='openstack-admins@lists.launchpad.net',
    url='https://launchpad.net/swift',
    packages=find_packages(exclude=['test', 'bin']),
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    install_requires=[],  # removed for better compat
    scripts=[
        'bin/swift', 'bin/swift-account-auditor',
        'bin/swift-account-audit', 'bin/swift-account-reaper',
        'bin/swift-account-replicator', 'bin/swift-account-server',
        'bin/swift-container-auditor',
        'bin/swift-container-replicator', 'bin/swift-container-sync',
        'bin/swift-container-server', 'bin/swift-container-updater',
        'bin/swift-drive-audit', 'bin/swift-get-nodes',
        'bin/swift-init', 'bin/swift-object-auditor',
        'bin/swift-object-expirer', 'bin/swift-object-info',
        'bin/swift-object-replicator',
        'bin/swift-object-server',
        'bin/swift-object-updater', 'bin/swift-proxy-server',
        'bin/swift-ring-builder', 'bin/swift-stats-populate',
        'bin/swift-stats-report',
        'bin/swift-dispersion-populate', 'bin/swift-dispersion-report',
        'bin/swift-bench',
        'bin/swift-recon', 'bin/swift-recon-cron', 'bin/swift-orphans',
        'bin/swift-oldies'
        ],
    entry_points={
        'paste.app_factory': [
            'proxy=swift.proxy.server:app_factory',
            'object=swift.obj.server:app_factory',
            'container=swift.container.server:app_factory',
            'account=swift.account.server:app_factory',
            ],
        'paste.filter_factory': [
            'healthcheck=swift.common.middleware.healthcheck:filter_factory',
            'memcache=swift.common.middleware.memcache:filter_factory',
            'ratelimit=swift.common.middleware.ratelimit:filter_factory',
            'cname_lookup=swift.common.middleware.cname_lookup:filter_factory',
            'catch_errors=swift.common.middleware.catch_errors:filter_factory',
            'domain_remap=swift.common.middleware.domain_remap:filter_factory',
            'swift3=swift.common.middleware.swift3:filter_factory',
            'staticweb=swift.common.middleware.staticweb:filter_factory',
            'tempauth=swift.common.middleware.tempauth:filter_factory',
            'recon=swift.common.middleware.recon:filter_factory',
            ],
        },
    )
