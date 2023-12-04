# Copyright (c) 2013 OpenStack Foundation
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

# This stuff can't live in test/unit/__init__.py due to its swob dependency.


class UnreadableInput(object):
    # Some clients will send neither a Content-Length nor a Transfer-Encoding
    # header, which will cause (some versions of?) eventlet to bomb out on
    # reads. This class helps us simulate that behavior.
    def __init__(self, test_case):
        self.calls = 0
        self.test_case = test_case

    def read(self, *a, **kw):
        self.calls += 1
        # Calling wsgi.input.read with neither a Content-Length nor
        # a Transfer-Encoding header will raise TypeError (See
        # https://bugs.launchpad.net/swift3/+bug/1593870 in detail)
        # This unreadable class emulates the behavior
        raise TypeError

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.test_case.assertEqual(0, self.calls)
