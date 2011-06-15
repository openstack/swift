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

import unittest

from swift.container import sync


class FakeRing():

    def __init__(self):
        self.replica_count = 3
        self.devs = [{'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}
                     for x in xrange(3)]

    def get_nodes(self, account, container=None, obj=None):
        return 1, list(self.devs)


class TestContainerSync(unittest.TestCase):

    def test_Iter2FileLikeObject(self):
        flo = sync._Iter2FileLikeObject(iter(['123', '4567', '89', '0']))
        expect = '1234567890'

        got = flo.read(2)
        self.assertTrue(len(got) <= 2)
        self.assertEquals(got, expect[:len(got)])
        expect = expect[len(got):]

        got = flo.read(5)
        self.assertTrue(len(got) <= 5)
        self.assertEquals(got, expect[:len(got)])
        expect = expect[len(got):]

        self.assertEquals(flo.read(), expect)
        self.assertEquals(flo.read(), '')
        self.assertEquals(flo.read(2), '')

        flo = sync._Iter2FileLikeObject(iter(['123', '4567', '89', '0']))
        self.assertEquals(flo.read(), '1234567890')
        self.assertEquals(flo.read(), '')
        self.assertEquals(flo.read(2), '')

    def test_init(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        self.assertTrue(cs.container_ring is cring)
        self.assertTrue(cs.object_ring is oring)

    def test_run_forever(self):
        # This runs runs_forever with fakes to succeed for two loops, the first
        # causing a report but no interval sleep, the second no report but an
        # interval sleep.
        time_calls = [0]
        sleep_calls = []
        audit_location_generator_calls = [0]

        def fake_time():
            time_calls[0] += 1
            returns = [1,     # Initialized reported time
                       1,     # Start time
                       3602,  # Is it report time (yes)
                       3602,  # Report time
                       3602,  # Elapsed time for "under interval" (no)
                       3602,  # Start time
                       3603,  # Is it report time (no)
                       3603,  # Elapsed time for "under interval" (yes)
                      ]
            if time_calls[0] == len(returns) + 1:
                raise Exception('we are now done')
            return returns[time_calls[0] - 1]

        def fake_sleep(amount):
            sleep_calls.append(amount)

        def fake_audit_location_generator(*args, **kwargs):
            audit_location_generator_calls[0] += 1
            # Makes .container_sync() short-circuit because 'path' doesn't end
            # with .db
            return [('path', 'device', 'partition')]

        orig_time = sync.time
        orig_sleep = sync.sleep
        orig_audit_location_generator = sync.audit_location_generator
        try:
            sync.time = fake_time
            sync.sleep = fake_sleep
            sync.audit_location_generator = fake_audit_location_generator
            cs = sync.ContainerSync({})
            cs.run_forever()
        except Exception, err:
            if str(err) != 'we are now done':
                raise
        finally:
            sync.time = orig_time
            sync.sleep = orig_sleep
            sync.audit_location_generator = orig_audit_location_generator

        self.assertEquals(time_calls, [9])
        self.assertEquals(len(sleep_calls), 2)
        self.assertTrue(sleep_calls[0] <= cs.interval)
        self.assertTrue(sleep_calls[1] == cs.interval - 1)
        self.assertEquals(audit_location_generator_calls, [2])
        self.assertEquals(cs.reported, 3602)

    def test_run_once(self):
        # This runs runs_once with fakes twice, the first causing an interim
        # report, the second with no interm report.
        time_calls = [0]
        audit_location_generator_calls = [0]

        def fake_time():
            time_calls[0] += 1
            returns = [1,     # Initialized reported time
                       1,     # Start time
                       3602,  # Is it report time (yes)
                       3602,  # Report time
                       3602,  # End report time
                       3602,  # For elapsed
                       3602,  # Start time
                       3603,  # Is it report time (no)
                       3604,  # End report time
                       3605,  # For elapsed
                      ]
            if time_calls[0] == len(returns) + 1:
                raise Exception('we are now done')
            return returns[time_calls[0] - 1]

        def fake_audit_location_generator(*args, **kwargs):
            audit_location_generator_calls[0] += 1
            # Makes .container_sync() short-circuit because 'path' doesn't end
            # with .db
            return [('path', 'device', 'partition')]

        orig_time = sync.time
        orig_audit_location_generator = sync.audit_location_generator
        try:
            sync.time = fake_time
            sync.audit_location_generator = fake_audit_location_generator
            cs = sync.ContainerSync({})
            cs.run_once()
            self.assertEquals(time_calls, [6])
            self.assertEquals(audit_location_generator_calls, [1])
            self.assertEquals(cs.reported, 3602)
            cs.run_once()
        except Exception, err:
            if str(err) != 'we are now done':
                raise
        finally:
            sync.time = orig_time
            sync.audit_location_generator = orig_audit_location_generator

        self.assertEquals(time_calls, [10])
        self.assertEquals(audit_location_generator_calls, [2])
        self.assertEquals(cs.reported, 3604)


if __name__ == '__main__':
    unittest.main()
