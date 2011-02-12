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

from eventlet import spawn, TimeoutError, listen
from eventlet.timeout import Timeout

from swift.common import bufferedhttp


class TestBufferedHTTP(unittest.TestCase):

    def test_http_connect(self):
        bindsock = listen(('127.0.0.1', 0))
        def accept(expected_par):
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    fp = sock.makefile()
                    fp.write('HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n'
                             'RESPONSE')
                    fp.flush()
                    self.assertEquals(fp.readline(),
                        'PUT /dev/%s/path/..%%25/?omg&no=%%7f HTTP/1.1\r\n' %
                        expected_par)
                    headers = {}
                    line = fp.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = fp.readline()
                    self.assertEquals(headers['content-length'], '7')
                    self.assertEquals(headers['x-header'], 'value')
                    self.assertEquals(fp.readline(), 'REQUEST\r\n')
            except BaseException, err:
                return err
            return None
        for par in ('par', 1357):
            event = spawn(accept, par)
            try:
                with Timeout(3):
                    conn = bufferedhttp.http_connect('127.0.0.1',
                        bindsock.getsockname()[1], 'dev', par, 'PUT',
                        '/path/..%/', {'content-length': 7, 'x-header':
                        'value'}, query_string='omg&no=%7f')
                    conn.send('REQUEST\r\n')
                    resp = conn.getresponse()
                    body = resp.read()
                    conn.close()
                    self.assertEquals(resp.status, 200)
                    self.assertEquals(resp.reason, 'OK')
                    self.assertEquals(body, 'RESPONSE')
            finally:
                err = event.wait()
                if err:
                    raise Exception(err)


if __name__ == '__main__':
    unittest.main()
