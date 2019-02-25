# Copyright (c) 2013 - 2015 OpenStack Foundation
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
import hashlib
import os
import shutil
import tempfile
import unittest

from swift.common import utils
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp


def write_diskfile(df, timestamp, data=b'test data', frag_index=None,
                   commit=True, legacy_durable=False, extra_metadata=None):
    # Helper method to write some data and metadata to a diskfile.
    # Optionally do not commit the diskfile, or commit but using a legacy
    # durable file
    with df.create() as writer:
        writer.write(data)
        metadata = {
            'ETag': hashlib.md5(data).hexdigest(),
            'X-Timestamp': timestamp.internal,
            'Content-Length': str(len(data)),
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        if frag_index is not None:
            metadata['X-Object-Sysmeta-Ec-Frag-Index'] = str(frag_index)
        writer.put(metadata)
        if commit and legacy_durable:
            # simulate legacy .durable file creation
            durable_file = os.path.join(df._datadir,
                                        timestamp.internal + '.durable')
            with open(durable_file, 'wb'):
                pass
        elif commit:
            writer.commit(timestamp)
        # else: don't make it durable
    return metadata


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.device = 'dev'
        self.partition = '9'
        self.tmpdir = tempfile.mkdtemp()
        # sender side setup
        self.tx_testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_sender')
        utils.mkdirs(os.path.join(self.tx_testdir, self.device))
        self.daemon_conf = {
            'devices': self.tx_testdir,
            'mount_check': 'false',
        }
        # daemon will be set in subclass setUp
        self.daemon = None

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_diskfile(self, device='dev', partition='9',
                       account='a', container='c', obj='o', body='test',
                       extra_metadata=None, policy=None,
                       frag_index=None, timestamp=None, df_mgr=None,
                       commit=True, verify=True):
        policy = policy or POLICIES.legacy
        object_parts = account, container, obj
        timestamp = Timestamp.now() if timestamp is None else timestamp
        if df_mgr is None:
            df_mgr = self.daemon._df_router[policy]
        df = df_mgr.get_diskfile(
            device, partition, *object_parts, policy=policy,
            frag_index=frag_index)
        write_diskfile(df, timestamp, data=body, extra_metadata=extra_metadata,
                       commit=commit)
        if commit and verify:
            # when we write and commit stub data, sanity check it's readable
            # and not quarantined because of any validation check
            with df.open():
                self.assertEqual(b''.join(df.reader()), body)
            # sanity checks
            listing = os.listdir(df._datadir)
            self.assertTrue(listing)
            for filename in listing:
                self.assertTrue(filename.startswith(timestamp.internal))
        return df

    def _make_open_diskfile(self, device='dev', partition='9',
                            account='a', container='c', obj='o', body=b'test',
                            extra_metadata=None, policy=None,
                            frag_index=None, timestamp=None, df_mgr=None):
        df = self._make_diskfile(device, partition, account, container, obj,
                                 body, extra_metadata, policy, frag_index,
                                 timestamp, df_mgr)
        df.open()
        return df
