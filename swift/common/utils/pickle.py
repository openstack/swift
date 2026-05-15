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

import _codecs
import copyreg
import io
import os
import pickle  # nosec: B403
from tempfile import mkstemp

from swift.common import header_key_dict
from swift.common.utils import mkdirs, renamer


def write_pickle(obj, dest, tmp=None, pickle_protocol=0):
    """
    Ensure that a pickle file gets written to disk.  The file
    is first written to a tmp location, ensure it is synced to disk, then
    perform a move to its final location

    :param obj: python object to be pickled
    :param dest: path of final destination file
    :param tmp: path to tmp to use, defaults to None
    :param pickle_protocol: protocol to pickle the obj with, defaults to 0
    """
    if tmp is None:
        tmp = os.path.dirname(dest)
    mkdirs(tmp)
    fd, tmppath = mkstemp(dir=tmp, suffix='.tmp')
    with os.fdopen(fd, 'wb') as fo:
        pickle.dump(obj, fo, pickle_protocol)
        fo.flush()
        os.fsync(fd)
        renamer(tmppath, dest)


_ALLOWED_GLOBALS = {
    ('_codecs', 'encode'): _codecs.encode,
    # Needed for HeaderKeyDict
    ('copy_reg', '_reconstructor'): copyreg._reconstructor,
    ('copyreg', '_reconstructor'): copyreg._reconstructor,
    ('swift.common.header_key_dict', 'HeaderKeyDict'):
        header_key_dict.HeaderKeyDict,
    ('builtins', 'dict'): dict,
    ('__builtin__', 'dict'): dict,
    ('builtins', 'bytes'): bytes,
    ('__builtin__', 'bytes'): bytes,
}


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        try:
            return _ALLOWED_GLOBALS[(module, name)]
        except KeyError:
            # Forbid any other loading
            raise pickle.UnpicklingError(
                f"global '{module}.{name}' is forbidden")


def unpickle(source, encoding='ASCII'):
    """Helper function analogous to pickle.loads()."""
    if isinstance(source, bytes):
        source = io.BytesIO(source)
    return RestrictedUnpickler(source, encoding=encoding).load()
