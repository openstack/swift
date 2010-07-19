#!/usr/bin/python
# Copyright (c) 2010 OpenStack, LLC.
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

name='swift'
version='1.0.1'

setup(
    name=name,
    version=version,
    description='Swift',
    license='Apache License (2.0)',
    author='OpenStack, LLC.',
    url='https://launchpad.net/swift',
    packages=find_packages(exclude=['tests','bin']),
    test_suite = 'nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
    ],
    scripts=['bin/st', 'bin/swift-account-auditor',
             'bin/swift-account-audit', 'bin/swift-account-reaper',
             'bin/swift-account-replicator', 'bin/swift-account-server',
             'bin/swift-auth-create-account',
             'bin/swift-auth-recreate-accounts', 'bin/swift-auth-server',
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
             'bin/swift-stats-report']
)
