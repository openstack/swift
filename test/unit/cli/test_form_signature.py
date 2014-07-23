# -*- coding: utf-8 -*-
# Copyright (c) 2014 Samuel Merritt <sam@swiftstack.com>
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
import hmac
import mock
import unittest
from StringIO import StringIO

from swift.cli import form_signature


class TestFormSignature(unittest.TestCase):
    def test_prints_signature(self):
        the_time = 1406143563.020043
        key = 'secret squirrel'
        expires = 3600
        path = '/v1/a/c/o'
        redirect = 'https://example.com/done.html'
        max_file_size = str(int(1024 * 1024 * 1024 * 3.14159))  # π GiB
        max_file_count = '3'

        expected_signature = hmac.new(
            key,
            "\n".join((
                path, redirect, max_file_size, max_file_count,
                str(int(the_time + expires)))),
            hashlib.sha1).hexdigest()

        out = StringIO()
        with mock.patch('swift.cli.form_signature.time', lambda: the_time):
            with mock.patch('sys.stdout', out):
                exitcode = form_signature.main([
                    '/path/to/swift-form-signature',
                    path, redirect, max_file_size,
                    max_file_count, str(expires), key])

        self.assertEqual(exitcode, 0)
        self.assertTrue("Signature: %s" % expected_signature
                        in out.getvalue())
        self.assertTrue("Expires: %d" % (the_time + expires,)
                        in out.getvalue())

        sig_input = ('<input type="hidden" name="signature" value="%s" />'
                     % expected_signature)
        self.assertTrue(sig_input in out.getvalue())

    def test_too_few_args(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            exitcode = form_signature.main([
                '/path/to/swift-form-signature',
                '/v1/a/c/o', '', '12', '34', '3600'])

        self.assertNotEqual(exitcode, 0)
        usage = 'Syntax: swift-form-signature <path>'
        self.assertTrue(usage in out.getvalue())


if __name__ == '__main__':
    unittest.main()
