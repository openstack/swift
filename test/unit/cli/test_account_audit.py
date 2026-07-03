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

import threading
import time
import unittest

from swift.cli import account_audit
from swift.common.concurrency import USE_EVENTLET, CooperativeLock, SwiftPool


@unittest.skipIf(USE_EVENTLET, "race only manifests under native threads")
class TestAuditorConcurrency(unittest.TestCase):

    def _make_auditor(self):
        # Build an Auditor without loading rings, then stub the network work
        # so audit_container/audit_account just populate the cache.
        auditor = account_audit.Auditor.__new__(account_audit.Auditor)
        auditor.in_progress = {}
        auditor.list_cache = {}
        auditor._cache_lock = CooperativeLock()
        auditor.pool = SwiftPool(4)
        auditor.containers_checked = 0
        auditor.accounts_checked = 0
        auditor.container_not_found = 0
        auditor.container_exceptions = 0
        auditor.container_obj_mismatch = 0
        auditor.container_count_mismatch = 0
        auditor.error_file = None

        class FakeRing(object):
            def get_nodes(self, *a):
                return 0, []  # no nodes -> no network, minimal work
        auditor.container_ring = FakeRing()
        # audit_container calls audit_account; stub it to a cheap cached value
        auditor.audit_account = lambda account, recurse=False: set()
        return auditor

    def test_concurrent_audit_container_same_key_no_race(self):
        # Without the lock, two threads create an Event for the same key, and
        # completion signals/deletes the wrong entry (KeyError or orphaned
        # waiter).
        auditor = self._make_auditor()
        errors = []
        barrier = threading.Barrier(8)

        def worker():
            try:
                barrier.wait()
                auditor.audit_container('a', 'c')
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertIn(('a', 'c'), auditor.list_cache)
        # the in_progress entry must be cleaned up exactly once
        self.assertEqual(auditor.in_progress, {})
        # only one thread actually performed the audit
        self.assertEqual(auditor.containers_checked, 1)

    def test_owner_failure_wakes_waiters(self):
        # If the owning thread's audit raises, waiters must still be woken
        # (the completion event fires in a finally) instead of blocking on
        # evt.wait() forever.
        auditor = self._make_auditor()
        in_work = threading.Event()
        release = threading.Event()

        def failing_audit_account(account, recurse=False):
            in_work.set()
            release.wait(5)
            raise RuntimeError('boom')
        auditor.audit_account = failing_audit_account

        owner_err = []

        def owner():
            try:
                auditor.audit_container('a', 'c')
            except RuntimeError as e:
                owner_err.append(e)

        results = []
        t_owner = threading.Thread(target=owner, daemon=True)
        t_owner.start()
        self.assertTrue(in_work.wait(5))
        t_waiter = threading.Thread(
            target=lambda: results.append(auditor.audit_container('a', 'c')),
            daemon=True)
        t_waiter.start()
        time.sleep(0.1)  # let the waiter block on the event
        release.set()
        t_owner.join(5)
        t_waiter.join(5)
        self.assertFalse(t_waiter.is_alive())  # woken, not hung
        self.assertEqual(1, len(owner_err))
        self.assertEqual([None], results)  # nothing cached, but not stuck
        self.assertEqual(auditor.in_progress, {})


if __name__ == '__main__':
    unittest.main()
