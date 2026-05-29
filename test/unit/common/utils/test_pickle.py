# Copyright (c) 2010-2026 OpenStack Foundation
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

"""Tests for swift.common.utils.pickle"""

import os
import pickle
import shutil
import tempfile
import unittest

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import Timestamp
from swift.common.utils.pickle import unpickle, write_pickle


class TestUnpickle(unittest.TestCase):

    def test_loads_allowlisted_types(self):
        obj = {
            'headers': HeaderKeyDict({'X-Foo': 'bar'}),
            'body': b'\x00\xff\xfe',
            'empty_bytes': b'',
            'count': 3,
            'bool': True,
            'float': 1.23,
            'str': 'string',
            'list': [1, 2, 3],
            'none': None,
        }
        obj['nested'] = dict(obj)
        # proto 0 (Swift's on-disk default) and the pickle default
        # reach for different globals — exercise both.
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            with self.subTest(protocol=protocol):
                restored = unpickle(pickle.dumps(obj, protocol=protocol))
                self.assertEqual(obj, restored)
                self.assertIsInstance(restored['headers'], HeaderKeyDict)

    def test_blocks_os_system(self):
        evil = b"cos\nsystem\n(S'id'\ntR."
        with self.assertRaises(pickle.UnpicklingError) as ctx:
            unpickle(evil)
        self.assertIn("os.system", str(ctx.exception))
        self.assertIn("forbidden", str(ctx.exception))

    def test_loads_legacy_py2_metadata(self):
        # Old py2 Swift clusters wrote pickles like this into object
        # xattrs; diskfile still reads them with encoding='bytes'.
        legacy = b'(dp0\nU\x01kU\x01vs.'
        self.assertEqual({b'k': b'v'},
                         unpickle(legacy, encoding='bytes'))

    def test_blocks_other_globals(self):
        for module, name in [
            ('subprocess', 'Popen'),
            ('builtins', 'eval'),
            ('builtins', '__import__'),
            ('builtins', 'list'),
            ('swift.common.utils', 'Timestamp'),
        ]:
            stream = (b'c' + module.encode() + b'\n'
                      + name.encode() + b'\n)R.')
            with self.assertRaises(pickle.UnpicklingError):
                unpickle(stream)


class TestWritePickle(unittest.TestCase):
    # write_pickle was moved here from swift.common.utils and had no
    # dedicated tests; verify it still composes with the new unpickle().

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)

    def test_writes_loadable_pickle(self):
        dest = os.path.join(self.tmpdir, 'sub', 'obj.pkl')
        obj = {'headers': HeaderKeyDict({'X-Foo': 'bar'}),
               'body': b'\xff'}
        write_pickle(obj, dest)
        with open(dest, 'rb') as fp:
            restored = unpickle(fp.read())
        self.assertEqual(obj, restored)
        self.assertIsInstance(restored['headers'], HeaderKeyDict)

    def test_writes_forbidden_pickle_nested_dict(self):
        dest = os.path.join(self.tmpdir, 'sub', 'obj.pkl')

        def do_test(obj):
            write_pickle(obj, dest)
            with open(dest, 'rb') as fp:
                with self.assertRaises(pickle.UnpicklingError):
                    unpickle(fp.read())

        do_test(Timestamp(123))
        do_test({'ts': Timestamp(123)})
        do_test({'nested': {'ts': Timestamp(123)}})


if __name__ == '__main__':
    unittest.main()
