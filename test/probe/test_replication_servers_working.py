#!/usr/bin/python -u
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

from io import BytesIO
from unittest import main
from uuid import uuid4
import os
import time
import shutil
import re

from swiftclient import client
from swift.obj.diskfile import get_data_dir

from test.probe.common import ReplProbeTest
from swift.common.request_helpers import get_reserved_name
from swift.common.utils import readconf

EXCLUDE_FILES = re.compile(r'^(hashes\.(pkl|invalid)|lock(-\d+)?)$')


def collect_info(path_list):
    """
    Recursive collect dirs and files in path_list directory.

    :param path_list: start directory for collecting
    :return: files_list, dir_list tuple of included
    directories and files
    """
    files_list = []
    dir_list = []
    for path in path_list:
        temp_files_list = []
        temp_dir_list = []
        for root, dirs, files in os.walk(path):
            files = [f for f in files if not EXCLUDE_FILES.match(f)]
            temp_files_list += files
            temp_dir_list += dirs
        files_list.append(temp_files_list)
        dir_list.append(temp_dir_list)
    return files_list, dir_list


def find_max_occupancy_node(dir_list):
    """
    Find node with maximum occupancy.

    :param dir_list: list of directories for each node.
    :return: number number node in list_dir
    """
    count = 0
    number = 0
    length = 0
    for dirs in dir_list:
        if length < len(dirs):
            length = len(dirs)
            number = count
        count += 1
    return number


class TestReplicatorFunctions(ReplProbeTest):
    """
    Class for testing replicators and replication servers.

    By default configuration - replication servers not used.
    For testing separate replication servers servers need to change
    ring's files using set_info command or new ring's files with
    different port values.
    """

    def put_data(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, 'VERIFY')

    def test_main(self):
        # Create one account, container and object file.
        # Find node with account, container and object replicas.
        # Delete all directories and files from this node (device).
        # Wait 60 seconds and check replication results.
        # Delete directories and files in objects storage without
        # deleting file "hashes.pkl".
        # Check, that files not replicated.
        # Delete file "hashes.pkl".
        # Check, that all files were replicated.
        path_list = []
        data_dir = get_data_dir(self.policy)
        # Figure out where the devices are
        for node_id in range(1, 5):
            conf = readconf(self.configs['object-server'][node_id])
            device_path = conf['app:object-server']['devices']
            for dev in self.object_ring.devs:
                if dev['port'] == int(conf['app:object-server']['bind_port']):
                    device = dev['device']
            path_list.append(os.path.join(device_path, device))

        # Put data to storage nodes
        self.put_data()

        # Get all data file information
        (files_list, dir_list) = collect_info(path_list)
        num = find_max_occupancy_node(dir_list)
        test_node = path_list[num]
        test_node_files_list = []
        for files in files_list[num]:
            if not files.endswith('.pending'):
                test_node_files_list.append(files)
        test_node_dir_list = []
        for d in dir_list[num]:
            if not d.startswith('tmp'):
                test_node_dir_list.append(d)
        # Run all replicators
        try:
            # Delete some files
            for directory in os.listdir(test_node):
                shutil.rmtree(os.path.join(test_node, directory))

            self.assertFalse(os.listdir(test_node))

            self.replicators.start()

            # We will keep trying these tests until they pass for up to 60s
            begin = time.time()
            while True:
                (new_files_list, new_dir_list) = collect_info([test_node])

                try:
                    # Check replicate files and dir
                    for files in test_node_files_list:
                        self.assertIn(files, new_files_list[0])

                    for directory in test_node_dir_list:
                        self.assertIn(directory, new_dir_list[0])

                    # We want to make sure that replication is completely
                    # settled; any invalidated hashes should be rehashed so
                    # hashes.pkl is stable
                    for directory in os.listdir(
                            os.path.join(test_node, data_dir)):
                        hashes_invalid_path = os.path.join(
                            test_node, data_dir, directory, 'hashes.invalid')
                        self.assertEqual(os.stat(
                            hashes_invalid_path).st_size, 0)
                    break
                except Exception:
                    if time.time() - begin > 60:
                        raise
                    time.sleep(1)

            self.replicators.stop()

            # Delete directories and files in objects storage without
            # deleting file "hashes.pkl".
            for directory in os.listdir(os.path.join(test_node, data_dir)):
                for input_dir in os.listdir(os.path.join(
                        test_node, data_dir, directory)):
                    if os.path.isdir(os.path.join(
                            test_node, data_dir, directory, input_dir)):
                        shutil.rmtree(os.path.join(
                            test_node, data_dir, directory, input_dir))

            self.replicators.once()
            # Check, that files not replicated.
            for directory in os.listdir(os.path.join(
                    test_node, data_dir)):
                for input_dir in os.listdir(os.path.join(
                        test_node, data_dir, directory)):
                    self.assertFalse(os.path.isdir(
                        os.path.join(test_node, data_dir,
                                     directory, input_dir)))

            self.replicators.start()
            # Now, delete file "hashes.pkl".
            # Check, that all files were replicated.
            for directory in os.listdir(os.path.join(test_node, data_dir)):
                os.remove(os.path.join(
                    test_node, data_dir, directory, 'hashes.pkl'))

            # We will keep trying these tests until they pass for up to 60s
            begin = time.time()
            while True:
                try:
                    (new_files_list, new_dir_list) = collect_info([test_node])

                    # Check replicate files and dirs
                    for files in test_node_files_list:
                        self.assertIn(files, new_files_list[0])

                    for directory in test_node_dir_list:
                        self.assertIn(directory, new_dir_list[0])
                    break
                except Exception:
                    if time.time() - begin > 60:
                        raise
                    time.sleep(1)
        finally:
            self.replicators.stop()


class TestReplicatorFunctionsReservedNames(TestReplicatorFunctions):
    def put_data(self):
        int_client = self.make_internal_client()
        int_client.create_account(self.account)
        container = get_reserved_name('container', str(uuid4()))
        int_client.create_container(self.account, container,
                                    headers={'X-Storage-Policy':
                                             self.policy.name})

        obj = get_reserved_name('object', str(uuid4()))
        int_client.upload_object(
            BytesIO(b'VERIFY'), self.account, container, obj)


if __name__ == '__main__':
    main()
