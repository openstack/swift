#!/usr/bin/env python

# Copyright (c) 2020 SwiftStack, Inc.
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

import argparse
import json
import os
import os.path
import sys
import threading
import time
import traceback

import urllib.parse
import socketserver
import http.server

try:
    import selenium.webdriver
except ImportError:
    selenium = None
import swiftclient.client

DEFAULT_ENV = {
    'OS_AUTH_URL': os.environ.get('ST_AUTH',
                                  'http://localhost:8080/auth/v1.0'),
    'OS_USERNAME': os.environ.get('ST_USER', 'test:tester'),
    'OS_PASSWORD': os.environ.get('ST_KEY', 'testing'),
    'OS_STORAGE_URL': None,
    'S3_ENDPOINT': None,
    'S3_USER': 'test:tester',
    'S3_KEY': 'testing',
}
ENV = {key: os.environ.get(key, default)
       for key, default in DEFAULT_ENV.items()}

TEST_TIMEOUT = 120.0  # seconds
STEPS = 500


class CORSSiteHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            directory=os.path.realpath(os.path.dirname(__file__)),
            **kwargs,
        )

    def log_message(self, fmt, *args):
        pass  # quiet, you!


class CORSSiteServer(socketserver.TCPServer):
    allow_reuse_address = True


class CORSSite(threading.Thread):
    def __init__(self, bind_port=8000):
        super().__init__()
        self.server = None
        self.bind_port = bind_port

    def run(self):
        self.server = CORSSiteServer(
            ('0.0.0.0', self.bind_port),
            CORSSiteHandler)
        self.server.serve_forever()

    def terminate(self):
        if self.server is not None:
            self.server.shutdown()
            self.join()


class Zeroes(object):
    BUF = b'\x00' * 64 * 1024

    def __init__(self, size=0):
        self.pos = 0
        self.size = size

    def __iter__(self):
        while self.pos < self.size:
            chunk = self.BUF[:self.size - self.pos]
            self.pos += len(chunk)
            yield chunk

    def __len__(self):
        return self.size


def setup(args):
    conn = swiftclient.client.Connection(
        ENV['OS_AUTH_URL'],
        ENV['OS_USERNAME'],
        ENV['OS_PASSWORD'],
        timeout=30)  # We've seen request times as high as 7-8s in the gate
    cluster_info = conn.get_capabilities()
    conn.put_container('private', {
        'X-Container-Read': '',
        'X-Container-Meta-Access-Control-Allow-Origin': '',
    })
    conn.put_container('referrer-allowed', {
        'X-Container-Read': '.r:%s' % args.hostname,
        'X-Container-Meta-Access-Control-Allow-Origin': (
            'http://%s:%d' % (args.hostname, args.port)),
    })
    conn.put_container('other-referrer-allowed', {
        'X-Container-Read': '.r:other-host',
        'X-Container-Meta-Access-Control-Allow-Origin': 'http://other-host',
    })
    conn.put_container('public-with-cors', {
        'X-Container-Read': '.r:*,.rlistings',
        'X-Container-Meta-Access-Control-Allow-Origin': '*',
    })
    conn.put_container('private-with-cors', {
        'X-Container-Read': '',
        'X-Container-Meta-Access-Control-Allow-Origin': '*',
    })
    conn.put_container('public-no-cors', {
        'X-Container-Read': '.r:*,.rlistings',
        'X-Container-Meta-Access-Control-Allow-Origin': '',
    })
    conn.put_container('public-segments', {
        'X-Container-Read': '.r:*',
        'X-Container-Meta-Access-Control-Allow-Origin': '',
    })

    for container in ('private', 'referrer-allowed', 'other-referrer-allowed',
                      'public-with-cors', 'private-with-cors',
                      'public-no-cors'):
        conn.put_object(container, 'obj', Zeroes(1024), headers={
            'X-Object-Meta-Mtime': str(time.time())})
    for n in range(10):
        segment_etag = conn.put_object(
            'public-segments', 'seg%02d' % n, Zeroes(1024 * 1024),
            headers={'Content-Type': 'application/swiftclient-segment'})
        conn.put_object(
            'public-with-cors', 'dlo/seg%02d' % n, Zeroes(1024 * 1024),
            headers={'Content-Type': 'application/swiftclient-segment'})
    conn.put_object('public-with-cors', 'dlo-with-unlistable-segments', b'',
                    headers={'X-Object-Manifest': 'public-segments/seg'})
    conn.put_object('public-with-cors', 'dlo', b'',
                    headers={'X-Object-Manifest': 'public-with-cors/dlo/seg'})

    if 'slo' in cluster_info:
        conn.put_object('public-with-cors', 'slo', json.dumps([
            {'path': 'public-segments/seg%02d' % n, 'etag': segment_etag}
            for n in range(10)]), query_string='multipart-manifest=put')

    if 'symlink' in cluster_info:
        for tgt in ('private', 'public-with-cors', 'public-no-cors'):
            conn.put_object('public-with-cors', 'symlink-to-' + tgt, b'',
                            headers={'X-Symlink-Target': tgt + '/obj'})


def get_results_table(browser):
    result_table = browser.find_element_by_id('results')
    for row in result_table.find_elements_by_xpath('./tr'):
        cells = row.find_elements_by_xpath('td')
        yield (
            cells[0].text,
            browser.name + ': ' + cells[1].text,
            cells[2].text)


def run(args, url):
    results = []
    browsers = list(ALL_BROWSERS) if 'all' in args.browsers else args.browsers
    ran_one = False
    for browser_name in browsers:
        kwargs = {}
        try:
            options = getattr(
                selenium.webdriver, browser_name.title() + 'Options')()
            options.headless = True
            kwargs['options'] = options
        except AttributeError:
            # not all browser types have Options class
            pass

        driver = getattr(selenium.webdriver, browser_name.title())
        try:
            browser = driver(**kwargs)
        except Exception as e:
            if not any(x in str(e) for x in (
                'needs to be in PATH',
                'SafariDriver was not found',
                'OSError: [Errno 8] Exec format error',
                'safaridriver not available for download',
            )):
                traceback.print_exc()
            results.append(('SKIP', browser_name, str(e).strip()))
            continue
        ran_one = True
        try:
            browser.get(url)

            start = time.time()
            for _ in range(STEPS):
                status = browser.find_element_by_id('status').text
                if status.startswith('Complete'):
                    results.extend(get_results_table(browser))
                    break
                time.sleep(TEST_TIMEOUT / STEPS)
            else:
                try:
                    results.extend(get_results_table(browser))
                except Exception:
                    pass  # worth a shot
                # that took a sec; give it *one last chance* to succeed
                status = browser.find_element_by_id('status').text
                if not status.startswith('Complete'):
                    results.append((
                        'ERROR', browser_name, 'Timed out (%s)' % status))
                    continue
            sys.stderr.write('Tested %s in %.1fs\n' % (
                browser_name, time.time() - start))
        except Exception as e:
            results.append(('ERROR', browser_name, str(e).strip()))
        finally:
            browser.close()

    if args.output is not None:
        fp = open(args.output, 'w')
    else:
        fp = sys.stdout

    fp.write('1..%d\n' % len(results))
    rc = 0
    if not ran_one:
        rc += 1  # make sure "no tests ran" translates to "failed"
    for test, (status, name, details) in enumerate(results, start=1):
        if status == 'PASS':
            fp.write('ok %d - %s\n' % (test, name))
        elif status == 'SKIP':
            fp.write('ok %d - %s # skip %s\n' % (test, name, details))
        else:
            fp.write('not ok %d - %s\n' % (test, name))
            fp.write('  %s%s\n' % (status, ':' if details else ''))
            if details:
                fp.write(''.join(
                    '  ' + line + '\n'
                    for line in details.split('\n')))
            rc += 1

    if fp is not sys.stdout:
        fp.close()

    return rc


ALL_BROWSERS = [
    'firefox',
    'chrome',
    'safari',
    'edge',
    'ie',
]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Set up and run CORS functional tests',
        epilog='''The tests consist of three parts:

setup - Create several test containers with well-known names, set appropriate
        ACLs and CORS metadata, and upload some test objects.
serve - Serve a static website on localhost which, on load, will make several
        CORS requests and verify expected behavior.
run   - Use Selenium to load the website, wait for and scrape the results,
        and output them in TAP format.

By default, perform all three parts. You can skip some or all of the parts
with the --no-setup, --no-serve, and --no-run options.
''')
    parser.add_argument('-P', '--port', type=int, default=8000)
    parser.add_argument('-H', '--hostname', default='localhost')
    parser.add_argument('--no-setup', action='store_true')
    parser.add_argument('--no-serve', action='store_true')
    parser.add_argument('--no-run', action='store_true')
    parser.add_argument('-o', '--output')
    parser.add_argument('browsers', nargs='*',
                        default='all',
                        choices=['all'] + ALL_BROWSERS)
    args = parser.parse_args()
    if not args.no_setup:
        setup(args)

    if args.no_serve:
        site = None
    else:
        site = CORSSite(args.port)

    should_run = not args.no_run
    if should_run and not selenium:
        print('Selenium not available; cannot run tests automatically')
        should_run = False

    if ENV['OS_STORAGE_URL'] is None:
        ENV['OS_STORAGE_URL'] = swiftclient.client.get_auth(
            ENV['OS_AUTH_URL'],
            ENV['OS_USERNAME'],
            ENV['OS_PASSWORD'],
            timeout=1)[0]
    if ENV['S3_ENDPOINT'] is None:
        ENV['S3_ENDPOINT'] = ENV['OS_STORAGE_URL'].partition('/v1')[0]

    url = 'http://%s:%d/#%s' % (args.hostname, args.port, '&'.join(
        '%s=%s' % (urllib.parse.quote(key), urllib.parse.quote(val))
        for key, val in ENV.items()))

    rc = 0
    if should_run:
        if site:
            site.start()
        try:
            rc = run(args, url)
        finally:
            if site:
                site.terminate()
    else:
        if site:
            print('Serving test at %s' % url)
            try:
                site.run()
            except KeyboardInterrupt:
                pass
    exit(rc)
