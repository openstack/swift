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

setup(
    name='swift',
    version='1.0.0-1',
    description='Swift',
    license='Apache License (2.0)',
    author='OpenStack, LLC.',
    url='https://launchpad.net/swift',
    packages=find_packages(exclude=['tests','bin']),
    #Uncomment this once unittests work without /etc
    #Also, figure out how to make this only run unit tests
    #test_suite = 'nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
    ],
    scripts=['bin/st.py', 'bin/swift-account-auditor.py',
             'bin/swift-account-audit.py', 'bin/swift-account-reaper.py',
             'bin/swift-account-replicator.py', 'bin/swift-account-server.py',
             'bin/swift-auth-create-account.py',
             'bin/swift-auth-recreate-accounts.py', 'bin/swift-auth-server.py',
             'bin/swift-container-auditor.py',
             'bin/swift-container-replicator.py',
             'bin/swift-container-server.py', 'bin/swift-container-updater.py',
             'bin/swift-drive-audit.py', 'bin/swift-get-nodes.py',
             'bin/swift-init.py', 'bin/swift-object-auditor.py',
             'bin/swift-object-info.py', 'bin/swift-object-server.py',
             'bin/swift-object-updater.py', 'bin/swift-proxy-server.py',
             'bin/swift-ring-builder.py', 'bin/swift-stats-populate.py',
             'bin/swift-stats-report.py']
)
