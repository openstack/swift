# Copyright (c) 2010-2012 OpenStack, LLC.
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

# TODO: Tests

import unittest

from swift.account import reaper
from swift.common.utils import normalize_timestamp


class FakeBroker(object):

    def __init__(self):
        self.info = {}

    def get_info(self):
        return self.info


class TestReaper(unittest.TestCase):

    def test_delay_reaping_conf_default(self):
        r = reaper.AccountReaper({})
        self.assertEquals(r.delay_reaping, 0)
        r = reaper.AccountReaper({'delay_reaping': ''})
        self.assertEquals(r.delay_reaping, 0)

    def test_delay_reaping_conf_set(self):
        r = reaper.AccountReaper({'delay_reaping': '123'})
        self.assertEquals(r.delay_reaping, 123)

    def test_delay_reaping_conf_bad_value(self):
        self.assertRaises(ValueError, reaper.AccountReaper,
                          {'delay_reaping': 'abc'})

    def test_reap_delay(self):
        time_value = [100]

        def _time():
            return time_value[0]

        time_orig = reaper.time
        try:
            reaper.time = _time
            r = reaper.AccountReaper({'delay_reaping': '10'})
            b = FakeBroker()
            b.info['delete_timestamp'] = normalize_timestamp(110)
            self.assertFalse(r.reap_account(b, 0, None))
            b.info['delete_timestamp'] = normalize_timestamp(100)
            self.assertFalse(r.reap_account(b, 0, None))
            b.info['delete_timestamp'] = normalize_timestamp(90)
            self.assertFalse(r.reap_account(b, 0, None))
            # KeyError raised immediately as reap_account tries to get the
            # account's name to do the reaping.
            b.info['delete_timestamp'] = normalize_timestamp(89)
            self.assertRaises(KeyError, r.reap_account, b, 0, None)
            b.info['delete_timestamp'] = normalize_timestamp(1)
            self.assertRaises(KeyError, r.reap_account, b, 0, None)
        finally:
            reaper.time = time_orig


if __name__ == '__main__':
    unittest.main()
