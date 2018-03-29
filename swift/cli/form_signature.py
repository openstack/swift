# Copyright (c) 2010-2012 OpenStack Foundation
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

"""
Script for generating a form signature for use with FormPost middleware.
"""
from __future__ import print_function
import hmac
import six
from hashlib import sha1
from os.path import basename
from time import time


def main(argv):
    if len(argv) != 7:
        prog = basename(argv[0])
        print('Syntax: %s <path> <redirect> <max_file_size> '
              '<max_file_count> <seconds> <key>' % prog)
        print()
        print('Where:')
        print('  <path>            The prefix to use for form uploaded')
        print('                    objects. For example:')
        print('                    /v1/account/container/object_prefix_ would')
        print('                    ensure all form uploads have that path')
        print('                    prepended to the browser-given file name.')
        print('  <redirect>        The URL to redirect the browser to after')
        print('                    the uploads have completed.')
        print('  <max_file_size>   The maximum file size per file uploaded.')
        print('  <max_file_count>  The maximum number of uploaded files')
        print('                    allowed.')
        print('  <seconds>         The number of seconds from now to allow')
        print('                    the form post to begin.')
        print('  <key>             The X-Account-Meta-Temp-URL-Key for the')
        print('                    account.')
        print()
        print('Example output:')
        print('    Expires: 1323842228')
        print('  Signature: 18de97e47345a82c4dbfb3b06a640dbb')
        print()
        print('Sample form:')
        print()
        print('NOTE: the <form> tag\'s "action" attribute does not contain '
              'the Swift cluster\'s hostname.')
        print('You should manually add it before using the form.')
        print()
        print('<form action="/v1/a/c/o" method="POST" '
              'enctype="multipart/form-data">')
        print('  <input type="hidden" name="max_file_size" value="123" />')
        print('  ... more HTML ...')
        print('  <input type="submit" />')
        print('</form>')
        return 1
    path, redirect, max_file_size, max_file_count, seconds, key = argv[1:]
    try:
        max_file_size = int(max_file_size)
    except ValueError:
        max_file_size = -1
    if max_file_size < 0:
        print('Please use a <max_file_size> value greater than or equal to 0.')
        return 1
    try:
        max_file_count = int(max_file_count)
    except ValueError:
        max_file_count = 0
    if max_file_count < 1:
        print('Please use a positive <max_file_count> value.')
        return 1
    try:
        expires = int(time() + int(seconds))
    except ValueError:
        expires = 0
    if expires < 1:
        print('Please use a positive <seconds> value.')
        return 1
    parts = path.split('/', 4)
    # Must be four parts, ['', 'v1', 'a', 'c'], must be a v1 request, have
    # account and container values, and optionally have an object prefix.
    if len(parts) < 4 or parts[0] or parts[1] != 'v1' or not parts[2] or \
            not parts[3]:
        print('<path> must point to a container at least.')
        print('For example: /v1/account/container')
        print('         Or: /v1/account/container/object_prefix')
        return 1
    data = '%s\n%s\n%s\n%s\n%s' % (path, redirect, max_file_size,
                                   max_file_count, expires)
    if six.PY3:
        data = data if isinstance(data, six.binary_type) else \
            data.encode('utf8')
        key = key if isinstance(key, six.binary_type) else \
            key.encode('utf8')
    sig = hmac.new(key, data,
                   sha1).hexdigest()
    print('  Expires:', expires)
    print('Signature:', sig)
    print('')

    print('Sample form:\n')

    print('NOTE: the <form> tag\'s "action" attribute does not '
          'contain the Swift cluster\'s hostname.')
    print('You should manually add it before using the form.\n')

    print('<form action="%s" method="POST" enctype="multipart/form-data">'
          % path)
    if redirect:
        print('  <input type="hidden" name="redirect" value="%s" />'
              % redirect)
    print('  <input type="hidden" name="max_file_size" value="%d" />'
          % max_file_size)
    print('  <input type="hidden" name="max_file_count" value="%d" />'
          % max_file_count)
    print('  <input type="hidden" name="expires" value="%d" />' % expires)
    print('  <input type="hidden" name="signature" value="%s" />' % sig)
    print('  <!-- This signature allows for at most %d files, -->'
          % max_file_count)
    print('  <!-- but it may also have any smaller number. -->')
    print('  <!-- Remove file inputs as needed. -->')
    for i in range(max_file_count):
        print('  <input type="file" name="file%d" />' % i)
        print('  <br />')
    print('  <input type="submit" />')
    print('</form>')
    return 0
