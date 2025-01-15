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


class HeaderKeyDict(dict):
    """
    A dict that title-cases all keys on the way in, so as to be
    case-insensitive.

    Note that all keys and values are expected to be wsgi strings,
    though some allowances are made when setting values.
    """
    def __init__(self, base_headers=None, **kwargs):
        if base_headers:
            self.update(base_headers)
        self.update(kwargs)

    @staticmethod
    def _title(s):
        return s.encode('latin1').title().decode('latin1')

    def update(self, other):
        if hasattr(other, 'keys'):
            for key in other.keys():
                self[self._title(key)] = other[key]
        else:
            for key, value in other:
                self[self._title(key)] = value

    def __getitem__(self, key):
        return dict.get(self, self._title(key))

    def __setitem__(self, key, value):
        key = self._title(key)
        if value is None:
            self.pop(key, None)
        elif isinstance(value, bytes):
            return dict.__setitem__(self, key, value.decode('latin-1'))
        else:
            return dict.__setitem__(self, key, str(value))

    def __contains__(self, key):
        return dict.__contains__(self, self._title(key))

    def __delitem__(self, key):
        return dict.__delitem__(self, self._title(key))

    def get(self, key, default=None):
        return dict.get(self, self._title(key), default)

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        return self[key]

    def pop(self, key, default=None):
        return dict.pop(self, self._title(key), default)
