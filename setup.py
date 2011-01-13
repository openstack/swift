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
from setuptools.command.sdist import sdist
import os
import subprocess

from swift import __version__ as version


class local_sdist(sdist):
    """Customized sdist hook - builds the ChangeLog file from VC first"""

    def run(self):
        if os.path.isdir('.bzr'):
            # We're in a bzr branch

            log_cmd = subprocess.Popen(["bzr", "log", "--gnu"],
                                       stdout=subprocess.PIPE)
            changelog = log_cmd.communicate()[0]
            with open("ChangeLog", "w") as changelog_file:
                changelog_file.write(changelog)
        sdist.run(self)


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
    cmdclass={'sdist': local_sdist},
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    install_requires=[],  # removed for better compat
    scripts=[
        'bin/st', 'bin/swift-account-auditor',
        'bin/swift-account-audit', 'bin/swift-account-reaper',
        'bin/swift-account-replicator', 'bin/swift-account-server',
        'bin/swift-auth-add-user',
        'bin/swift-auth-recreate-accounts', 'bin/swift-auth-server',
        'bin/swift-auth-update-reseller-prefixes',
        'bin/swift-container-auditor',
        'bin/swift-container-replicator',
        'bin/swift-container-server', 'bin/swift-container-updater',
        'bin/swift-drive-audit', 'bin/swift-get-nodes',
        'bin/swift-init', 'bin/swift-object-auditor',
        'bin/swift-object-info',
        'bin/swift-object-replicator',
        'bin/swift-object-server',
        'bin/swift-object-updater', 'bin/swift-proxy-server',
        'bin/swift-ring-builder', 'bin/swift-stats-populate',
        'bin/swift-stats-report',
        'bin/swift-bench',
        'bin/swift-log-uploader',
        'bin/swift-log-stats-collector',
        'bin/swift-account-stats-logger',
        'bin/swauth-add-account', 'bin/swauth-add-user',
        'bin/swauth-cleanup-tokens', 'bin/swauth-delete-account',
        'bin/swauth-delete-user', 'bin/swauth-list', 'bin/swauth-prep',
        'bin/swauth-set-account-service', 'bin/swift-auth-to-swauth',
        ],
    entry_points={
        'paste.app_factory': [
            'proxy=swift.proxy.server:app_factory',
            'object=swift.obj.server:app_factory',
            'container=swift.container.server:app_factory',
            'account=swift.account.server:app_factory',
            'auth=swift.auth.server:app_factory',
            ],
        'paste.filter_factory': [
            'auth=swift.common.middleware.auth:filter_factory',
            'swauth=swift.common.middleware.swauth:filter_factory',
            'healthcheck=swift.common.middleware.healthcheck:filter_factory',
            'memcache=swift.common.middleware.memcache:filter_factory',
            'ratelimit=swift.common.middleware.ratelimit:filter_factory',
            'cname_lookup=swift.common.middleware.cname_lookup:filter_factory',
            'catch_errors=swift.common.middleware.catch_errors:filter_factory',
            'domain_remap=swift.common.middleware.domain_remap:filter_factory',
            'swift3=swift.common.middleware.swift3:filter_factory',
            ],
        },
    )
