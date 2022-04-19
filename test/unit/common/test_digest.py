# Copyright (c) 2022 NVIDIA
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
import unittest

from swift.common import digest
from test.debug_logger import debug_logger


class TestDigestUtils(unittest.TestCase):
    """Tests for swift.common.middleware.digest """
    def setUp(self):
        self.logger = debug_logger('test_digest_utils')

    def test_get_hmac(self):
        self.assertEqual(
            digest.get_hmac('GET', '/path', 1, 'abc'),
            'b17f6ff8da0e251737aa9e3ee69a881e3e092e2f')

    def test_get_hmac_ip_range(self):
        self.assertEqual(
            digest.get_hmac('GET', '/path', 1, 'abc', ip_range='127.0.0.1'),
            'b30dde4d2b8562b8496466c3b46b2b9ac5054461')

    def test_get_hmac_ip_range_non_binary_type(self):
        self.assertEqual(
            digest.get_hmac(
                u'GET', u'/path', 1, u'abc', ip_range=u'127.0.0.1'),
            'b30dde4d2b8562b8496466c3b46b2b9ac5054461')

    def test_get_hmac_digest(self):
        self.assertEqual(
            digest.get_hmac(u'GET', u'/path', 1, u'abc', digest='sha256'),
            '64c5558394f86b042ce1e929b34907abd9d0a57f3e20cd3f93cffd83de0206a7')
        self.assertEqual(
            digest.get_hmac(
                u'GET', u'/path', 1, u'abc', digest=hashlib.sha256),
            '64c5558394f86b042ce1e929b34907abd9d0a57f3e20cd3f93cffd83de0206a7')

        self.assertEqual(
            digest.get_hmac(u'GET', u'/path', 1, u'abc', digest='sha512'),
            '7e95af818aec1b69b53fc2cb6d69456ec64ebda6c17b8fc8b7303b78acc8ca'
            '14fc4aed96c1614a8e9d6ff45a6237711d8be294cda679624825d79aa6959b'
            '5229')
        self.assertEqual(
            digest.get_hmac(
                u'GET', u'/path', 1, u'abc', digest=hashlib.sha512),
            '7e95af818aec1b69b53fc2cb6d69456ec64ebda6c17b8fc8b7303b78acc8ca'
            '14fc4aed96c1614a8e9d6ff45a6237711d8be294cda679624825d79aa6959b'
            '5229')

    def test_extract_digest_and_algorithm(self):
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'b17f6ff8da0e251737aa9e3ee69a881e3e092e2f'),
            ('sha1', 'b17f6ff8da0e251737aa9e3ee69a881e3e092e2f'))
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha1:sw3eTSuFYrhJZGbDtGsrmsUFRGE='),
            ('sha1', 'b30dde4d2b8562b8496466c3b46b2b9ac5054461'))
        # also good with '=' stripped
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha1:sw3eTSuFYrhJZGbDtGsrmsUFRGE'),
            ('sha1', 'b30dde4d2b8562b8496466c3b46b2b9ac5054461'))

        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'b963712313cd4236696fb4c4cf11fc56'
                'ff4158e0bcbf1d4424df147783fd1045'),
            ('sha256', 'b963712313cd4236696fb4c4cf11fc56'
                       'ff4158e0bcbf1d4424df147783fd1045'))
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha256:uWNxIxPNQjZpb7TEzxH8Vv9BWOC8vx1EJN8Ud4P9EEU='),
            ('sha256', 'b963712313cd4236696fb4c4cf11fc56'
                       'ff4158e0bcbf1d4424df147783fd1045'))
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha256:uWNxIxPNQjZpb7TEzxH8Vv9BWOC8vx1EJN8Ud4P9EEU'),
            ('sha256', 'b963712313cd4236696fb4c4cf11fc56'
                       'ff4158e0bcbf1d4424df147783fd1045'))

        self.assertEqual(
            digest.extract_digest_and_algorithm(
                '26df3d9d59da574d6f8d359cb2620b1b'
                '86737215c38c412dfee0a410acea1ac4'
                '285ad0c37229ca74e715c443979da17d'
                '3d77a97d2ac79cc5e395b05bfa4bdd30'),
            ('sha512', '26df3d9d59da574d6f8d359cb2620b1b'
                       '86737215c38c412dfee0a410acea1ac4'
                       '285ad0c37229ca74e715c443979da17d'
                       '3d77a97d2ac79cc5e395b05bfa4bdd30'))
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha512:Jt89nVnaV01vjTWcsmILG4ZzchXDjEEt/uCkEKzq'
                'GsQoWtDDcinKdOcVxEOXnaF9PXepfSrHnMXjlbBb+kvdMA=='),
            ('sha512', '26df3d9d59da574d6f8d359cb2620b1b'
                       '86737215c38c412dfee0a410acea1ac4'
                       '285ad0c37229ca74e715c443979da17d'
                       '3d77a97d2ac79cc5e395b05bfa4bdd30'))
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'sha512:Jt89nVnaV01vjTWcsmILG4ZzchXDjEEt_uCkEKzq'
                'GsQoWtDDcinKdOcVxEOXnaF9PXepfSrHnMXjlbBb-kvdMA'),
            ('sha512', '26df3d9d59da574d6f8d359cb2620b1b'
                       '86737215c38c412dfee0a410acea1ac4'
                       '285ad0c37229ca74e715c443979da17d'
                       '3d77a97d2ac79cc5e395b05bfa4bdd30'))

        with self.assertRaises(ValueError):
            digest.extract_digest_and_algorithm('')
        with self.assertRaises(ValueError):
            digest.extract_digest_and_algorithm(
                'exactly_forty_chars_but_not_hex_encoded!')
        # Too short (md5)
        with self.assertRaises(ValueError):
            digest.extract_digest_and_algorithm(
                'd41d8cd98f00b204e9800998ecf8427e')
        # but you can slip it in via the prefix notation!
        self.assertEqual(
            digest.extract_digest_and_algorithm(
                'md5:1B2M2Y8AsgTpgAmY7PhCfg'),
            ('md5', 'd41d8cd98f00b204e9800998ecf8427e'))

    def test_get_allowed_digests(self):
        # start with defaults
        allowed, deprecated = digest.get_allowed_digests(
            ''.split(), self.logger)
        self.assertEqual(allowed, {'sha256', 'sha512', 'sha1'})
        self.assertEqual(deprecated, {'sha1'})
        warning_lines = self.logger.get_lines_for_level('warning')
        expected_warning_line = (
            'The following digest algorithms are allowed by default but '
            'deprecated: sha1. Support will be disabled by default in a '
            'future release, and later removed entirely.')
        self.assertIn(expected_warning_line, warning_lines)
        self.logger.clear()

        # now with a subset
        allowed, deprecated = digest.get_allowed_digests(
            'sha1 sha256'.split(), self.logger)
        self.assertEqual(allowed, {'sha256', 'sha1'})
        self.assertEqual(deprecated, {'sha1'})
        warning_lines = self.logger.get_lines_for_level('warning')
        expected_warning_line = (
            'The following digest algorithms are configured but '
            'deprecated: sha1. Support will be removed in a future release.')
        self.assertIn(expected_warning_line, warning_lines)
        self.logger.clear()

        # Now also with an unsupported digest
        allowed, deprecated = digest.get_allowed_digests(
            'sha1 sha256 md5'.split(), self.logger)
        self.assertEqual(allowed, {'sha256', 'sha1'})
        self.assertEqual(deprecated, {'sha1'})
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn(expected_warning_line, warning_lines)
        expected_unsupported_warning_line = (
            'The following digest algorithms are configured but not '
            'supported: md5')
        self.assertIn(expected_unsupported_warning_line, warning_lines)
        self.logger.clear()

        # Now with no deprecated digests
        allowed, deprecated = digest.get_allowed_digests(
            'sha256 sha512'.split(), self.logger)
        self.assertEqual(allowed, {'sha256', 'sha512'})
        self.assertEqual(deprecated, set())
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertFalse(warning_lines)
        self.logger.clear()

        # no valid digest
        # Now also with an unsupported digest
        with self.assertRaises(ValueError):
            digest.get_allowed_digests(['md5'], self.logger)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn(expected_unsupported_warning_line, warning_lines)
