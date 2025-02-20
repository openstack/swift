#!/usr/bin/python -u
# Copyright (c) 2010-2016 OpenStack Foundation
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

import base64
import email.parser
import itertools
import json
import time
from copy import deepcopy
from unittest import SkipTest

import urllib
from swift.common.swob import normalize_etag
from swift.common.utils import md5, config_true_value

from test.functional import check_response, retry
import test.functional as tf
from test.functional import cluster_info
from test.functional.tests import Utils, Base, Base2, BaseEnv
from test.functional.swift_test_client import Connection, ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


def group_file_contents(file_contents):
    # This looks a little funny, but iterating through a byte string on py3
    # yields a sequence of ints, not a sequence of single-byte byte strings
    # as it did on py2.
    byte_iter = (file_contents[i:i + 1] for i in range(len(file_contents)))
    return [
        (char, sum(1 for _ in grp))
        for char, grp in itertools.groupby(byte_iter)]


def md5hex(s):
    if not isinstance(s, bytes):
        s = s.encode('ascii')
    return md5(s, usedforsecurity=False).hexdigest()


class TestSloEnv(BaseEnv):
    slo_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def create_segments(cls, container):
        seg_info = {}
        for letter, size in (('a', 1024 * 1024),
                             ('b', 1024 * 1024),
                             ('c', 1024 * 1024),
                             ('d', 1024 * 1024),
                             ('e', 1)):
            seg_name = "seg_%s" % letter
            file_item = container.file(seg_name)
            file_item.write(letter.encode('ascii') * size)
            seg_info[seg_name] = {
                'size_bytes': size,
                'etag': file_item.md5,
                'path': '/%s/%s' % (container.name, seg_name)}
        return seg_info

    @classmethod
    def setUp(cls):
        if cls.slo_enabled is None:
            cls.slo_enabled = 'slo' in cluster_info
            if not cls.slo_enabled:
                return

        super(TestSloEnv, cls).setUp()

        if not tf.skip2:
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.conn2.authenticate()
            cls.account2 = cls.conn2.get_account()
            cls.account2.delete_containers()
        if not tf.skip3:
            config3 = tf.config.copy()
            config3['username'] = tf.config['username3']
            config3['password'] = tf.config['password3']
            cls.conn3 = Connection(config3)
            cls.conn3.authenticate()

        cls.container = cls.account.container(Utils.create_name())
        cls.container2 = cls.account.container(Utils.create_name())

        for cont in (cls.container, cls.container2):
            if not cont.create():
                raise ResponseError(cls.conn.response)

        cls.seg_info = seg_info = cls.create_segments(cls.container)

        file_item = cls.container.file("manifest-abcde")
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})

        cls.container.file('seg_with_%ff_funky_name').write(b'z' * 10)

        # Put the same manifest in the container2
        file_item = cls.container2.file("manifest-abcde")
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})

        file_item = cls.container.file('manifest-cd')
        cd_json = json.dumps([
            seg_info['seg_c'], seg_info['seg_d']]).encode('ascii')
        file_item.write(cd_json, parms={'multipart-manifest': 'put'})
        cd_etag = md5((
            seg_info['seg_c']['etag'] + seg_info['seg_d']['etag']
        ).encode('ascii'), usedforsecurity=False).hexdigest()

        file_item = cls.container.file("manifest-bcd-submanifest")
        file_item.write(
            json.dumps([seg_info['seg_b'],
                        {'etag': cd_etag,
                         'size_bytes': (seg_info['seg_c']['size_bytes'] +
                                        seg_info['seg_d']['size_bytes']),
                         'path': '/%s/%s' % (cls.container.name,
                                             'manifest-cd')}]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        bcd_submanifest_etag = md5((
            seg_info['seg_b']['etag'] + cd_etag).encode('ascii'),
            usedforsecurity=False).hexdigest()

        file_item = cls.container.file("manifest-abcde-submanifest")
        file_item.write(
            json.dumps([
                seg_info['seg_a'],
                {'etag': bcd_submanifest_etag,
                 'size_bytes': (seg_info['seg_b']['size_bytes'] +
                                seg_info['seg_c']['size_bytes'] +
                                seg_info['seg_d']['size_bytes']),
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-bcd-submanifest')},
                seg_info['seg_e']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        abcde_submanifest_etag = md5((
            seg_info['seg_a']['etag'] + bcd_submanifest_etag +
            seg_info['seg_e']['etag']).encode('ascii'),
            usedforsecurity=False).hexdigest()
        abcde_submanifest_size = (seg_info['seg_a']['size_bytes'] +
                                  seg_info['seg_b']['size_bytes'] +
                                  seg_info['seg_c']['size_bytes'] +
                                  seg_info['seg_d']['size_bytes'] +
                                  seg_info['seg_e']['size_bytes'])

        file_item = cls.container.file("ranged-manifest")
        file_item.write(
            json.dumps([
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '-1048578'},  # 'c' + ('d' * 2**20) + 'e'
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '524288-1572863'},  # 'a' * 2**19 + 'b' * 2**19
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '3145727-3145728'}]).encode('ascii'),  # 'cd'
            parms={'multipart-manifest': 'put'})
        ranged_manifest_etag = md5((
            abcde_submanifest_etag + ':3145727-4194304;' +
            abcde_submanifest_etag + ':524288-1572863;' +
            abcde_submanifest_etag + ':3145727-3145728;'
        ).encode('ascii'), usedforsecurity=False).hexdigest()
        ranged_manifest_size = 2 * 1024 * 1024 + 4

        file_item = cls.container.file("ranged-submanifest")
        file_item.write(
            json.dumps([
                seg_info['seg_c'],
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest')},
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest'),
                 'range': '524289-1572865'},
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest'),
                 'range': '-3'}]).encode('ascii'),
            parms={'multipart-manifest': 'put'})

        file_item = cls.container.file("manifest-db")
        file_item.write(
            json.dumps([
                {'path': seg_info['seg_d']['path'], 'etag': None,
                 'size_bytes': None},
                {'path': seg_info['seg_b']['path'], 'etag': None,
                 'size_bytes': None},
            ]).encode('ascii'), parms={'multipart-manifest': 'put'})

        file_item = cls.container.file("ranged-manifest-repeated-segment")
        file_item.write(
            json.dumps([
                {'path': seg_info['seg_a']['path'], 'etag': None,
                 'size_bytes': None, 'range': '-1048578'},
                {'path': seg_info['seg_a']['path'], 'etag': None,
                 'size_bytes': None},
                {'path': seg_info['seg_b']['path'], 'etag': None,
                 'size_bytes': None, 'range': '-1048578'},
            ]).encode('ascii'), parms={'multipart-manifest': 'put'})

        file_item = cls.container.file("mixed-object-data-manifest")
        file_item.write(
            json.dumps([
                {'data': base64.b64encode(b'APRE' * 8).decode('ascii')},
                {'path': seg_info['seg_a']['path']},
                {'data': base64.b64encode(b'APOS' * 16).decode('ascii')},
                {'path': seg_info['seg_b']['path']},
                {'data': base64.b64encode(b'BPOS' * 32).decode('ascii')},
                {'data': base64.b64encode(b'CPRE' * 64).decode('ascii')},
                {'path': seg_info['seg_c']['path']},
                {'data': base64.b64encode(b'CPOS' * 8).decode('ascii')},
            ]).encode('ascii'), parms={'multipart-manifest': 'put'}
        )

        file_item = cls.container.file("nested-data-manifest")
        file_item.write(
            json.dumps([
                {'path': '%s/%s' % (cls.container.name,
                                    "mixed-object-data-manifest")}
            ]).encode('ascii'), parms={'multipart-manifest': 'put'}
        )


class TestSlo(Base):
    env = TestSloEnv

    def setUp(self):
        super(TestSlo, self).setUp()
        if self.env.slo_enabled is False:
            raise SkipTest("SLO not enabled")
        elif self.env.slo_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected slo_enabled to be True/False, got %r" %
                (self.env.slo_enabled,))

        manifest_abcde_hash = md5(usedforsecurity=False)
        for letter in (b'a', b'b', b'c', b'd'):
            manifest_abcde_hash.update(
                md5(letter * 1024 * 1024, usedforsecurity=False)
                .hexdigest().encode('ascii'))
        manifest_abcde_hash.update(
            md5(b'e', usedforsecurity=False).hexdigest().encode('ascii'))
        self.manifest_abcde_etag = manifest_abcde_hash.hexdigest()

    def test_slo_get_simple_manifest(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read()
        self.assertEqual(file_item.conn.response.status, 200)
        headers = dict(
            (h.lower(), v)
            for h, v in file_item.conn.response.getheaders())
        self.assertIn('etag', headers)
        self.assertEqual(headers['etag'], '"%s"' % self.manifest_abcde_etag)
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_file_contents(file_contents))

    def test_slo_multipart_delete_part_number_ignored(self):
        # create a container just for this test because we're going to delete
        # objects that we create
        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create())
        # create segments in same container
        seg_info = self.env.create_segments(container)
        file_item = container.file("manifest-abcde")
        self.assertTrue(file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]).encode('ascii'),
            parms={'multipart-manifest': 'put'}))
        # sanity check, we have SLO...
        file_item.initialize(parms={'part-number': '5'})
        self.assertEqual(
            file_item.conn.response.getheader('X-Static-Large-Object'), 'True')
        self.assertEqual(
            file_item.conn.response.getheader('X-Parts-Count'), '5')
        self.assertEqual(6, len(container.files()))

        # part-number should be ignored
        status = file_item.conn.make_request(
            'DELETE', file_item.path,
            parms={'multipart-manifest': 'delete',
                   'part-number': '2'})
        self.assertEqual(200, status)
        # everything is gone
        self.assertFalse(container.files())

    def test_get_head_part_number_invalid(self):
        file_item = self.env.container.file('manifest-abcde')
        file_item.initialize()
        ok_resp = file_item.conn.response
        self.assertEqual(ok_resp.getheader('X-Static-Large-Object'), 'True')
        self.assertEqual(ok_resp.getheader('Etag'), file_item.etag)
        self.assertEqual(ok_resp.getheader('Content-Length'),
                         str(file_item.size))

        # part-number is 1-indexed
        self.assertRaises(ResponseError, file_item.read,
                          parms={'part-number': '0'})
        resp_body = file_item.conn.response.read()
        self.assertEqual(400, file_item.conn.response.status, resp_body)
        self.assertEqual(b'Part number must be an integer greater than 0',
                         resp_body)

        self.assertRaises(ResponseError, file_item.initialize,
                          parms={'part-number': '0'})
        resp_body = file_item.conn.response.read()
        self.assertEqual(400, file_item.conn.response.status)
        self.assertEqual(b'', resp_body)

    def test_get_head_part_number_out_of_range(self):
        file_item = self.env.container.file('manifest-abcde')
        file_item.initialize()
        ok_resp = file_item.conn.response
        self.assertEqual(ok_resp.getheader('X-Static-Large-Object'), 'True')
        self.assertEqual(ok_resp.getheader('Etag'), file_item.etag)
        self.assertEqual(ok_resp.getheader('Content-Length'),
                         str(file_item.size))
        manifest_etag = ok_resp.getheader('Manifest-Etag')

        def check_headers(resp):
            self.assertEqual(resp.getheader('X-Static-Large-Object'), 'True')
            self.assertEqual(resp.getheader('Etag'), file_item.etag)
            self.assertEqual(resp.getheader('Manifest-Etag'), manifest_etag)
            self.assertEqual(resp.getheader('X-Parts-Count'), '5')
            self.assertEqual(resp.getheader('Content-Range'),
                             'bytes */%s' % file_item.size)

        self.assertRaises(ResponseError, file_item.read,
                          parms={'part-number': '10001'})
        resp_body = file_item.conn.response.read()
        self.assertEqual(416, file_item.conn.response.status, resp_body)
        self.assertEqual(b'The requested part number is not satisfiable',
                         resp_body)
        check_headers(file_item.conn.response)
        self.assertEqual(file_item.conn.response.getheader('Content-Length'),
                         str(len(resp_body)))

        self.assertRaises(ResponseError, file_item.info,
                          parms={'part-number': '10001'})
        resp_body = file_item.conn.response.read()
        self.assertEqual(416, file_item.conn.response.status)
        self.assertEqual(b'', resp_body)
        check_headers(file_item.conn.response)
        self.assertEqual(file_item.conn.response.getheader('Content-Length'),
                         '0')

    def test_get_part_number_simple_manifest(self):
        file_item = self.env.container.file('manifest-abcde')
        seg_info_list = [
            self.env.seg_info["seg_%s" % letter]
            for letter in ['a', 'b', 'c', 'd', 'e']
        ]
        checksum = md5(usedforsecurity=False)
        total_size = 0
        for seg_info in seg_info_list:
            checksum.update(seg_info['etag'].encode('ascii'))
            total_size += seg_info['size_bytes']
        slo_etag = checksum.hexdigest()
        start = 0
        manifest_etag = None
        for i, seg_info in enumerate(seg_info_list, start=1):
            part_contents = file_item.read(parms={'part-number': i})
            self.assertEqual(len(part_contents), seg_info['size_bytes'])
            headers = dict(
                (h.lower(), v)
                for h, v in file_item.conn.response.getheaders())
            self.assertEqual(headers['content-length'],
                             str(seg_info['size_bytes']))
            self.assertEqual(headers['etag'], '"%s"' % slo_etag)
            if not manifest_etag:
                manifest_etag = headers['x-manifest-etag']
            else:
                self.assertEqual(headers['x-manifest-etag'], manifest_etag)
            end = start + seg_info['size_bytes'] - 1
            self.assertEqual(headers['content-range'],
                             'bytes %d-%d/%d' % (start, end, total_size), i)
            self.assertEqual(headers['x-parts-count'], '5')
            start = end + 1

    def test_head_part_number_simple_manifest(self):
        file_item = self.env.container.file('manifest-abcde')
        seg_info_list = [
            self.env.seg_info["seg_%s" % letter]
            for letter in ['a', 'b', 'c', 'd', 'e']
        ]
        checksum = md5(usedforsecurity=False)
        total_size = 0
        for seg_info in seg_info_list:
            checksum.update(seg_info['etag'].encode('ascii'))
            total_size += seg_info['size_bytes']
        slo_etag = checksum.hexdigest()
        start = 0
        manifest_etag = None
        for i, seg_info in enumerate(seg_info_list, start=1):
            part_info = file_item.info(parms={'part-number': i},
                                       exp_status=206)
            headers = dict(
                (h.lower(), v)
                for h, v in file_item.conn.response.getheaders())
            self.assertEqual(headers['content-length'],
                             str(seg_info['size_bytes']))
            self.assertEqual(headers['etag'], '"%s"' % slo_etag)
            self.assertEqual(headers['etag'], part_info['etag'])
            if not manifest_etag:
                manifest_etag = headers['x-manifest-etag']
            else:
                self.assertEqual(headers['x-manifest-etag'], manifest_etag)
            end = start + seg_info['size_bytes'] - 1
            self.assertEqual(headers['content-range'],
                             'bytes %d-%d/%d' % (start, end, total_size), i)
            self.assertEqual(headers['x-parts-count'], '5')
            start = end + 1

    def test_x_delete_at_with_part_number_and_open_expired(self):
        cont_name = self.env.account.container(self.env.container.name)
        allow_open_expired = config_true_value(tf.cluster_info['swift'].get(
            'allow_open_expired', 'false'))

        if not allow_open_expired:
            raise SkipTest('allow_open_expired is disabled')

        # data for segments
        segments = [b'one', b'two', b'three', b'four']
        etags = [md5hex(segment) for segment in segments]

        def put_manifest(url, token, parsed, conn, object_segments,
                         x_delete_after):
            manifest_data = []
            start = 0

            for segment_object in range(len(object_segments)):
                size = len(object_segments[segment_object])
                end = start + size - 1
                manifest_data.append({
                    'path': '/%s/segments/%s' % (cont_name,
                                                 str(segment_object)),
                    'etag': etags[segment_object],
                    'size_bytes': size,
                })
                start = end + 1

            conn.request(
                'PUT',
                '%s/%s/manifest?multipart-manifest=put' % (parsed.path,
                                                           cont_name),
                body=json.dumps(manifest_data),
                headers={
                    'X-Auth-Token': token,
                    'X-Delete-After': str(x_delete_after),
                    'X-Static-Large-Object': 'true',
                    'Content-Type': 'application/json'
                }
            )
            resp = check_response(conn)
            body = resp.read()
            self.assertEqual(resp.status, 201,
                             "Response status is not 201: %s" % body)

        def put_segments(url, token, parsed, conn, object_segments,
                         x_delete_after):
            for objnum in range(len(object_segments)):
                conn.request('PUT', '%s/%s/segments/%s' % (
                    parsed.path,
                    cont_name,
                    str(objnum)),
                    body=object_segments[objnum],
                    headers={
                        'X-Auth-Token': token,
                        'X-Delete-After': str(x_delete_after)})
                resp = check_response(conn)
                body = resp.read()
                self.assertEqual(resp.status, 201,
                                 "Response status is not 201: %s" % body)

        retry(put_segments, segments, 2)
        retry(put_manifest, segments, 1)

        # get the manifest
        def get_manifest(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'GET',
                '%s/%s/manifest?multipart-manifest=get' %
                (parsed.path, cont_name),
                '', headers)
            return check_response(conn)

        def get_manifest_still_expired(timeout=10, retry_delay=1):
            start_time = time.time()
            while time.time() - start_time < timeout:
                resp = retry(get_manifest)
                resp.read()
                if resp.status == 404:
                    break
                elif 200 <= resp.status < 300:
                    time.sleep(retry_delay)
                else:
                    self.fail(
                        f'Unexpected response status {resp.status} for'
                        f'manifest {resp.url}.')
            else:
                self.fail(
                    f'manifest still returns {resp.status} after '
                    f'we set x-delete-after to 2s post object '
                    f'creation')

            self.assertEqual(resp.status, 404,
                             resp.headers.get('x-trans-id'))

        get_manifest_still_expired()

        def get_manifest_with_open_expire():
            resp = retry(get_manifest, extra_headers={'X-Open-Expired': True})
            self.assertEqual(resp.status, 200)
            resp.read()

        get_manifest_with_open_expire()

        def get_or_head_part(url, token, parsed, conn,
                             extra_headers=None, method='GET',
                             part_number=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(method, '%s/%s/manifest?part-number=%s' % (
                parsed.path,
                cont_name,
                part_number
            ), '', headers)
            return check_response(conn)

        resp = retry(get_manifest, extra_headers={'X-Open-Expired': True})
        resp.read()
        # read the expired object with magic x-open-expired header
        self.assertEqual(resp.status, 200)

        def check_parts_with_open_expire(method='GET'):
            for objnum in range(len(segments)):
                part_num = str(objnum + 1)
                resp = retry(get_or_head_part,
                             extra_headers={'X-Open-Expired': True},
                             method=method, part_number=part_num)
                resp.read()
                self.assertEqual(resp.status, 206)

        check_parts_with_open_expire()
        check_parts_with_open_expire('HEAD')

        def check_part_still_expired(method='GET', timeout=10, retry_delay=1):
            for objnum, segment in enumerate(segments):
                part_num = str(objnum + 1)
                start_time = time.time()
                while time.time() - start_time < timeout:
                    resp = retry(get_or_head_part, method=method,
                                 part_number=part_num)
                    resp.read()
                    if resp.status == 404:
                        break
                    elif 200 <= resp.status < 300:
                        time.sleep(retry_delay)
                    else:
                        self.fail(
                            f'Unexpected response status {resp.status} for'
                            f' part {part_num}.')
                else:
                    self.fail(
                        f'Part {part_num} did not return 404 within {timeout}'
                        f' seconds.')

        check_part_still_expired()
        check_part_still_expired('HEAD', 5, 1)

        def head_manifest(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request('HEAD', '%s/%s/manifest' % (parsed.path,
                                                     cont_name),
                         '', headers)
            return check_response(conn)

        resp = retry(head_manifest, extra_headers={'X-Open-Expired': True})
        resp.read()
        # head expired object with magic x-open-expired header
        self.assertEqual(resp.status, 200)

    def test_slo_container_listing(self):
        # the listing object size should equal the sum of the size of the
        # segments, not the size of the manifest body
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(
            json.dumps([self.env.seg_info['seg_a']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        # The container listing exposes BOTH the MD5 of the manifest content
        # and the SLO MD5-of-MD5s by splitting the latter out into a separate
        # key. These should remain consistent when the object is updated with
        # a POST.
        file_item.initialize(parms={'multipart-manifest': 'get'})
        manifest_etag = file_item.etag
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            self.assertTrue(manifest_etag.startswith('"'))
            self.assertTrue(manifest_etag.endswith('"'))
            # ...but in the listing, it'll be stripped
            manifest_etag = manifest_etag[1:-1]
        else:
            self.assertFalse(manifest_etag.startswith('"'))
            self.assertFalse(manifest_etag.endswith('"'))

        file_item.initialize()
        slo_etag = file_item.etag
        self.assertTrue(slo_etag.startswith('"'))
        self.assertTrue(slo_etag.endswith('"'))

        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual('application/octet-stream',
                                 f_dict['content_type'])
                self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST updated content-type file
        file_item.content_type = 'image/jpeg'
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST with no change to content-type
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'},
                                cfg={'no_content_type': True})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

    def test_slo_get_nested_manifest(self):
        file_item = self.env.container.file('manifest-abcde-submanifest')
        file_contents = file_item.read()
        self.assertEqual(4 * 1024 * 1024 + 1, len(file_contents))
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_file_contents(file_item.read()))

    def test_slo_get_ranged_manifest(self):
        file_item = self.env.container.file('ranged-manifest')
        self.assertEqual([
            (b'c', 1),
            (b'd', 1024 * 1024),
            (b'e', 1),
            (b'a', 512 * 1024),
            (b'b', 512 * 1024),
            (b'c', 1),
            (b'd', 1),
        ], group_file_contents(file_item.read()))

    def test_slo_get_ranged_manifest_repeated_segment(self):
        file_item = self.env.container.file('ranged-manifest-repeated-segment')
        self.assertEqual(
            [(b'a', 2097152), (b'b', 1048576)],
            group_file_contents(file_item.read()))

    def test_slo_get_ranged_submanifest(self):
        file_item = self.env.container.file('ranged-submanifest')
        self.assertEqual([
            (b'c', 1024 * 1024 + 1),
            (b'd', 1024 * 1024),
            (b'e', 1),
            (b'a', 512 * 1024),
            (b'b', 512 * 1024),
            (b'c', 1),
            (b'd', 512 * 1024 + 1),
            (b'e', 1),
            (b'a', 512 * 1024),
            (b'b', 1),
            (b'c', 1),
            (b'd', 1),
        ], group_file_contents(file_item.read()))

    def test_slo_ranged_get(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(size=1024 * 1024 + 2,
                                       offset=1024 * 1024 - 1)
        self.assertEqual(file_item.conn.response.status, 206)
        headers = dict(
            (h.lower(), v)
            for h, v in file_item.conn.response.getheaders())
        self.assertIn('etag', headers)
        self.assertEqual(headers['etag'], '"%s"' % self.manifest_abcde_etag)
        self.assertEqual([
            (b'a', 1),
            (b'b', 1048576),
            (b'c', 1),
        ], group_file_contents(file_contents))

    def test_slo_ranged_get_half_open_on_right(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(
            hdrs={"Range": "bytes=1048571-"})
        self.assertEqual([
            (b'a', 5),
            (b'b', 1048576),
            (b'c', 1048576),
            (b'd', 1048576),
            (b'e', 1)
        ], group_file_contents(file_contents))

    def test_slo_ranged_get_half_open_on_left(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(
            hdrs={"Range": "bytes=-123456"})
        self.assertEqual([
            (b'd', 123455),
            (b'e', 1),
        ], group_file_contents(file_contents))

    def test_slo_multi_ranged_get(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(
            hdrs={"Range": "bytes=1048571-1048580,2097147-2097156"})

        # See testMultiRangeGets for explanation
        parser = email.parser.BytesFeedParser()
        parser.feed((
            "Content-Type: %s\r\n\r\n" % file_item.content_type).encode())
        parser.feed(file_contents)

        root_message = parser.close()
        self.assertTrue(root_message.is_multipart())  # sanity check

        byteranges = root_message.get_payload()
        self.assertEqual(len(byteranges), 2)

        self.assertEqual(byteranges[0]['Content-Type'],
                         "application/octet-stream")
        self.assertEqual(
            byteranges[0]['Content-Range'], "bytes 1048571-1048580/4194305")
        self.assertEqual(byteranges[0].get_payload(decode=True), b"aaaaabbbbb")

        self.assertEqual(byteranges[1]['Content-Type'],
                         "application/octet-stream")
        self.assertEqual(
            byteranges[1]['Content-Range'], "bytes 2097147-2097156/4194305")
        self.assertEqual(byteranges[1].get_payload(decode=True), b"bbbbbccccc")

    def test_slo_ranged_submanifest(self):
        file_item = self.env.container.file('manifest-abcde-submanifest')
        file_contents = file_item.read(size=1024 * 1024 + 2,
                                       offset=1024 * 1024 * 2 - 1)
        self.assertEqual([
            (b'b', 1),
            (b'c', 1024 * 1024),
            (b'd', 1),
        ], group_file_contents(file_contents))

    def test_slo_etag_is_quote_wrapped_hash_of_etags(self):
        # we have this check in test_slo_get_simple_manifest, too,
        # but verify that it holds for HEAD requests
        file_item = self.env.container.file('manifest-abcde')
        self.assertEqual('"%s"' % self.manifest_abcde_etag,
                         file_item.info()['etag'])

    def test_slo_etag_is_quote_wrapped_hash_of_etags_submanifests(self):

        def hd(x):
            return md5(x, usedforsecurity=False).hexdigest().encode('ascii')

        expected_etag = hd(hd(b'a' * 1024 * 1024) +
                           hd(hd(b'b' * 1024 * 1024) +
                              hd(hd(b'c' * 1024 * 1024) +
                                 hd(b'd' * 1024 * 1024))) +
                           hd(b'e'))

        file_item = self.env.container.file('manifest-abcde-submanifest')
        self.assertEqual('"%s"' % expected_etag.decode('ascii'),
                         file_item.info()['etag'])

    def test_slo_etag_mismatch(self):
        file_item = self.env.container.file("manifest-a-bad-etag")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024,
                    'etag': 'not it',
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
                }]).encode('ascii'),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_size_mismatch(self):
        file_item = self.env.container.file("manifest-a-bad-size")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024 - 1,
                    'etag': md5(
                        b'a' * 1024 * 1024,
                        usedforsecurity=False).hexdigest(),
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
                }]).encode('ascii'),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_client_etag_mismatch(self):
        file_item = self.env.container.file("manifest-a-mismatch-etag")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024,
                    'etag': md5(b'a' * 1024 * 1024,
                                usedforsecurity=False).hexdigest(),
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
                }]).encode('ascii'),
                parms={'multipart-manifest': 'put'},
                hdrs={'Etag': 'NOTetagofthesegments'})
        except ResponseError as err:
            self.assertEqual(422, err.status)

    def test_slo_client_etag(self):
        file_item = self.env.container.file("manifest-a-b-etag")
        etag_a = md5(b'a' * 1024 * 1024, usedforsecurity=False).hexdigest()
        etag_b = md5(b'b' * 1024 * 1024, usedforsecurity=False).hexdigest()
        file_item.write(
            json.dumps([{
                'size_bytes': 1024 * 1024,
                'etag': etag_a,
                'path': '/%s/%s' % (self.env.container.name, 'seg_a')}, {
                'size_bytes': 1024 * 1024,
                'etag': etag_b,
                'path': '/%s/%s' % (self.env.container.name, 'seg_b'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'},
            hdrs={'Etag': md5((etag_a + etag_b).encode(),
                              usedforsecurity=False).hexdigest()})
        self.assert_status(201)

    def test_slo_unspecified_etag(self):
        file_item = self.env.container.file("manifest-a-unspecified-etag")
        file_item.write(
            json.dumps([{
                'size_bytes': 1024 * 1024,
                'etag': None,
                'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_unspecified_size(self):
        file_item = self.env.container.file("manifest-a-unspecified-size")
        file_item.write(
            json.dumps([{
                'size_bytes': None,
                'etag': md5(b'a' * 1024 * 1024,
                            usedforsecurity=False).hexdigest(),
                'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_funky_segment(self):
        file_item = self.env.container.file("manifest-with-funky-segment")
        file_item.write(
            json.dumps([{
                'path': '/%s/%s' % (self.env.container.name,
                                    'seg_with_%ff_funky_name'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

        self.assertEqual(b'z' * 10, file_item.read())

    def test_slo_missing_etag(self):
        file_item = self.env.container.file("manifest-a-missing-etag")
        file_item.write(
            json.dumps([{
                'size_bytes': 1024 * 1024,
                'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_missing_size(self):
        file_item = self.env.container.file("manifest-a-missing-size")
        file_item.write(
            json.dumps([{
                'etag': md5(b'a' * 1024 * 1024,
                            usedforsecurity=False).hexdigest(),
                'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_path_only(self):
        file_item = self.env.container.file("manifest-a-path-only")
        file_item.write(
            json.dumps([{
                'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
            }]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_typo_etag(self):
        file_item = self.env.container.file("manifest-a-typo-etag")
        try:
            file_item.write(
                json.dumps([{
                    'teag': md5(b'a' * 1024 * 1024,
                                usedforsecurity=False).hexdigest(),
                    'size_bytes': 1024 * 1024,
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
                }]).encode('ascii'),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_typo_size(self):
        file_item = self.env.container.file("manifest-a-typo-size")
        try:
            file_item.write(
                json.dumps([{
                    'etag': md5(b'a' * 1024 * 1024,
                                usedforsecurity=False).hexdigest(),
                    'siz_bytes': 1024 * 1024,
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a'),
                }]).encode('ascii'),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_overwrite_segment_with_manifest(self):
        file_item = self.env.container.file("seg_b")
        with self.assertRaises(ResponseError) as catcher:
            file_item.write(
                json.dumps([
                    {'size_bytes': 1024 * 1024,
                     'etag': md5(b'a' * 1024 * 1024,
                                 usedforsecurity=False).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_a')},
                    {'size_bytes': 1024 * 1024,
                     'etag': md5(b'b' * 1024 * 1024,
                                 usedforsecurity=False).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_b')},
                    {'size_bytes': 1024 * 1024,
                     'etag': md5(b'c' * 1024 * 1024,
                                 usedforsecurity=False).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_c')},
                ]).encode('ascii'),
                parms={'multipart-manifest': 'put'})
        self.assertEqual(400, catcher.exception.status)

    def test_slo_copy(self):
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy(self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

    def test_slo_copy_using_x_copy_from(self):
        # as per test_slo_copy but using a PUT with x-copy-from
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_using_x_copy_from(
            self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

    def test_slo_copy_part_number(self):
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy(self.env.container.name, "copied-abcde",
                       parms={'part-number': '1'})

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        # just the first part is copied
        self.assertEqual(1024 * 1024, len(copied_contents))
        self.assertEqual(b'a' * 10, copied_contents[:10])

    def test_slo_copy_part_number_using_x_copy_from(self):
        # as per test_slo_copy_part_number but using a PUT with x-copy-from
        file_item = self.env.container.file("manifest-abcde")
        # part-number on the client PUT target is actually applied to the
        # internal GET source request
        file_item.copy_using_x_copy_from(
            self.env.container.name, "copied-abcde",
            parms={'part-number': '1'})

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        # just the first part is copied
        self.assertEqual(1024 * 1024, len(copied_contents))
        self.assertEqual(b'a' * 10, copied_contents[:10])

    def test_slo_copy_account(self):
        acct = urllib.parse.unquote(self.env.conn.account_name)
        # same account copy
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_account(acct, self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

        if not tf.skip2:
            # copy to different account
            acct = urllib.parse.unquote(self.env.conn2.account_name)
            dest_cont = self.env.account2.container(Utils.create_name())
            self.assertTrue(dest_cont.create(hdrs={
                'X-Container-Write': self.env.conn.user_acl
            }))
            file_item = self.env.container.file("manifest-abcde")
            file_item.copy_account(acct, dest_cont, "copied-abcde")

            copied = dest_cont.file("copied-abcde")
            copied_contents = copied.read(parms={'multipart-manifest': 'get'})
            self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

    def test_slo_copy_the_manifest(self):
        source = self.env.container.file("manifest-abcde")
        source.initialize(parms={'multipart-manifest': 'get'})
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        manifest_etag = md5(source_contents, usedforsecurity=False).hexdigest()
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            manifest_etag = '"%s"' % manifest_etag
        self.assertEqual(manifest_etag, source.etag)

        source.initialize()
        self.assertEqual('application/octet-stream', source.content_type)
        self.assertNotEqual(manifest_etag, source.etag)
        slo_etag = source.etag

        self.assertTrue(source.copy(self.env.container.name,
                                    "copied-abcde-manifest-only",
                                    parms={'multipart-manifest': 'get'}))

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied.initialize(parms={'multipart-manifest': 'get'})
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_json, copied_json)
        self.assertEqual(manifest_etag, copied.etag)

        copied.initialize()
        self.assertEqual('application/octet-stream', copied.content_type)
        self.assertEqual(slo_etag, copied.etag)

        # verify the listing metadata
        listing = self.env.container.files(parms={'format': 'json'})
        names = {}
        for f_dict in listing:
            if f_dict['name'] in ('manifest-abcde',
                                  'copied-abcde-manifest-only'):
                names[f_dict['name']] = f_dict

        self.assertIn('manifest-abcde', names)
        actual = names['manifest-abcde']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        self.assertEqual(normalize_etag(manifest_etag), actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

        self.assertIn('copied-abcde-manifest-only', names)
        actual = names['copied-abcde-manifest-only']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        self.assertEqual(normalize_etag(manifest_etag), actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

        # Test copy manifest including data segments
        source = self.env.container.file("mixed-object-data-manifest")
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        source.copy(
            self.env.container.name,
            "copied-mixed-object-data-manifest",
            parms={'multipart-manifest': 'get'})

        copied = self.env.container.file("copied-mixed-object-data-manifest")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_contents, copied_contents)
        self.assertEqual(copied_json[0], {
            'data': base64.b64encode(b'APRE' * 8).decode('ascii')})

    def test_slo_copy_the_manifest_using_x_copy_from(self):
        # as per test_slo_copy_the_manifest but using a PUT with x-copy-from
        source = self.env.container.file("manifest-abcde")
        source.initialize(parms={'multipart-manifest': 'get'})
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        manifest_etag = md5(source_contents, usedforsecurity=False).hexdigest()
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            manifest_etag = '"%s"' % manifest_etag
        self.assertEqual(manifest_etag, source.etag)

        source.initialize()
        self.assertEqual('application/octet-stream', source.content_type)
        self.assertNotEqual(manifest_etag, source.etag)

        # multipart-manifest=get on the client PUT target request actually
        # applies to the internal GET source request
        self.assertTrue(
            source.copy_using_x_copy_from(self.env.container.name,
                                          "copied-abcde-manifest-only",
                                          parms={'multipart-manifest': 'get'}))

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied.initialize(parms={'multipart-manifest': 'get'})
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_json, copied_json)
        self.assertEqual(manifest_etag, copied.etag)

    def test_slo_copy_the_manifest_updating_metadata(self):
        source = self.env.container.file("manifest-abcde")
        source.content_type = 'application/octet-stream'
        source.sync_metadata({'test': 'original'})
        source.initialize(parms={'multipart-manifest': 'get'})
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        manifest_etag = md5(source_contents, usedforsecurity=False).hexdigest()
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            manifest_etag = '"%s"' % manifest_etag
        self.assertEqual(manifest_etag, source.etag)

        source.initialize()
        self.assertEqual('application/octet-stream', source.content_type)
        self.assertNotEqual(manifest_etag, source.etag)
        slo_etag = source.etag
        self.assertEqual(source.metadata['test'], 'original')

        self.assertTrue(
            source.copy(self.env.container.name, "copied-abcde-manifest-only",
                        parms={'multipart-manifest': 'get'},
                        hdrs={'Content-Type': 'image/jpeg',
                              'X-Object-Meta-Test': 'updated'}))

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied.initialize(parms={'multipart-manifest': 'get'})
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_json, copied_json)
        self.assertEqual(manifest_etag, copied.etag)

        copied.initialize()
        self.assertEqual('image/jpeg', copied.content_type)
        self.assertEqual(slo_etag, copied.etag)
        self.assertEqual(copied.metadata['test'], 'updated')

        # verify the listing metadata
        listing = self.env.container.files(parms={'format': 'json'})
        names = {}
        for f_dict in listing:
            if f_dict['name'] in ('manifest-abcde',
                                  'copied-abcde-manifest-only'):
                names[f_dict['name']] = f_dict

        self.assertIn('manifest-abcde', names)
        actual = names['manifest-abcde']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        # the container listing should have the etag of the manifest contents
        self.assertEqual(normalize_etag(manifest_etag), actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

        self.assertIn('copied-abcde-manifest-only', names)
        actual = names['copied-abcde-manifest-only']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('image/jpeg', actual['content_type'])
        self.assertEqual(normalize_etag(manifest_etag), actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

    def test_slo_copy_the_manifest_account(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        acct = urllib.parse.unquote(self.env.conn.account_name)
        # same account
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_account(acct,
                               self.env.container.name,
                               "copied-abcde-manifest-only",
                               parms={'multipart-manifest': 'get'})

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")

        # different account
        acct = urllib.parse.unquote(self.env.conn2.account_name)
        dest_cont = self.env.account2.container(Utils.create_name())
        self.assertTrue(dest_cont.create(hdrs={
            'X-Container-Write': self.env.conn.user_acl
        }))

        # manifest copy will fail because there is no read access to segments
        # in destination account
        self.assertRaises(ResponseError, file_item.copy_account,
                          acct, dest_cont, "copied-abcde-manifest-only",
                          parms={'multipart-manifest': 'get'})
        self.assertEqual(400, file_item.conn.response.status)
        resp_body = file_item.conn.response.read()
        self.assertEqual(5, resp_body.count(b'403 Forbidden'),
                         'Unexpected response body %r' % resp_body)

        # create segments container in account2 with read access for account1
        segs_container = self.env.account2.container(self.env.container.name)
        self.assertTrue(segs_container.create(hdrs={
            'X-Container-Read': self.env.conn.user_acl
        }))

        # manifest copy will still fail because there are no segments in
        # destination account
        self.assertRaises(ResponseError, file_item.copy_account,
                          acct, dest_cont, "copied-abcde-manifest-only",
                          parms={'multipart-manifest': 'get'})
        self.assertEqual(400, file_item.conn.response.status)
        resp_body = file_item.conn.response.read()
        self.assertEqual(5, resp_body.count(b'404 Not Found'),
                         'Unexpected response body %r' % resp_body)

        # create segments in account2 container with same name as in account1,
        # manifest copy now succeeds
        self.env.create_segments(segs_container)

        self.assertTrue(file_item.copy_account(
            acct, dest_cont, "copied-abcde-manifest-only",
            parms={'multipart-manifest': 'get'}))

        copied = dest_cont.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")

    def test_slo_put_heartbeating(self):
        if 'yield_frequency' not in cluster_info['slo']:
            # old swift?
            raise SkipTest('Swift does not seem to support heartbeating')

        def do_put(headers=None, include_error=False):
            file_item = self.env.container.file("manifest-heartbeat")
            seg_info = self.env.seg_info
            manifest_data = [seg_info['seg_a'], seg_info['seg_b'],
                             seg_info['seg_c'], seg_info['seg_d'],
                             seg_info['seg_e']]
            if include_error:
                manifest_data.append({'path': 'non-existent/segment'})
            resp = file_item.write(
                json.dumps(manifest_data).encode('ascii'),
                parms={'multipart-manifest': 'put', 'heartbeat': 'on'},
                hdrs=headers, return_resp=True)
            self.assertEqual(resp.status, 202)
            self.assertTrue(resp.chunked)
            body_lines = resp.body.split(b'\n', 2)
            self.assertFalse(body_lines[0].strip())  # all whitespace
            self.assertEqual(b'\r', body_lines[1])
            return body_lines[2]

        body_lines = do_put().decode('utf8').split('\n')
        self.assertIn('Response Status: 201 Created', body_lines)
        self.assertIn('Etag', [line.split(':', 1)[0] for line in body_lines])
        self.assertIn('Last Modified', [line.split(':', 1)[0]
                                        for line in body_lines])

        body_lines = do_put(
            {'Accept': 'text/plain'}).decode('utf8').split('\n')
        self.assertIn('Response Status: 201 Created', body_lines)
        self.assertIn('Etag', [line.split(':', 1)[0] for line in body_lines])
        self.assertIn('Last Modified', [line.split(':', 1)[0]
                                        for line in body_lines])

        body = do_put({'Accept': 'application/json'})
        try:
            resp = json.loads(body)
        except ValueError:
            self.fail('Expected JSON, got %r' % body)
        self.assertIn('Etag', resp)
        del resp['Etag']
        self.assertIn('Last Modified', resp)
        del resp['Last Modified']
        self.assertEqual(resp, {
            'Response Status': '201 Created',
            'Response Body': '',
            'Errors': [],
        })

        body_lines = do_put(include_error=True).decode('utf8').split('\n')
        self.assertIn('Response Status: 400 Bad Request', body_lines)
        self.assertIn('Response Body: Bad Request', body_lines)
        self.assertNotIn('Etag', [line.split(':', 1)[0]
                                  for line in body_lines])
        self.assertNotIn('Last Modified', [line.split(':', 1)[0]
                                           for line in body_lines])
        self.assertEqual(body_lines[-3:], [
            'Errors:',
            'non-existent/segment, 404 Not Found',
            '',
        ])

        body = do_put({'Accept': 'application/json'}, include_error=True)
        try:
            resp = json.loads(body)
        except ValueError:
            self.fail('Expected JSON, got %r' % body)
        self.assertNotIn('Etag', resp)
        self.assertNotIn('Last Modified', resp)
        self.assertEqual(resp, {
            'Response Status': '400 Bad Request',
            'Response Body': 'Bad Request\nThe server could not comply with '
                             'the request since it is either malformed or '
                             'otherwise incorrect.',
            'Errors': [
                ['non-existent/segment', '404 Not Found'],
            ],
        })

        body = do_put({'Accept': 'application/json', 'ETag': 'bad etag'})
        try:
            resp = json.loads(body)
        except ValueError:
            self.fail('Expected JSON, got %r' % body)
        self.assertNotIn('Etag', resp)
        self.assertNotIn('Last Modified', resp)
        self.assertEqual(resp, {
            'Response Status': '422 Unprocessable Entity',
            'Response Body': 'Unprocessable Entity\nUnable to process the '
                             'contained instructions',
            'Errors': [],
        })

    def _make_manifest(self):
        file_item = self.env.container.file("manifest-post")
        seg_info = self.env.seg_info
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        return file_item

    def test_slo_post_the_manifest_metadata_update(self):
        file_item = self._make_manifest()
        # sanity check, check the object is an SLO manifest
        file_item.info()
        file_item.header_fields([('slo', 'x-static-large-object')])

        # POST a user metadata (i.e. x-object-meta-post)
        file_item.sync_metadata({'post': 'update'})

        updated = self.env.container.file("manifest-post")
        updated.info()
        updated.header_fields([('user-meta', 'x-object-meta-post')])  # sanity
        updated.header_fields([('slo', 'x-static-large-object')])
        updated_contents = updated.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(updated_contents)
        except ValueError:
            self.fail("Unexpected content on GET, expected a json body")

    def test_slo_post_the_manifest_metadata_update_with_qs(self):
        # multipart-manifest query should be ignored on post
        for verb in ('put', 'get', 'delete'):
            file_item = self._make_manifest()
            # sanity check, check the object is an SLO manifest
            file_item.info()
            file_item.header_fields([('slo', 'x-static-large-object')])
            # POST a user metadata (i.e. x-object-meta-post)
            file_item.sync_metadata(metadata={'post': 'update'},
                                    parms={'multipart-manifest': verb})
            updated = self.env.container.file("manifest-post")
            updated.info()
            updated.header_fields(
                [('user-meta', 'x-object-meta-post')])  # sanity
            updated.header_fields([('slo', 'x-static-large-object')])
            updated_contents = updated.read(
                parms={'multipart-manifest': 'get'})
            try:
                json.loads(updated_contents)
            except ValueError:
                self.fail(
                    "Unexpected content on GET, expected a json body")

    def test_slo_get_the_manifest(self):
        manifest = self.env.container.file("manifest-abcde")
        got_body = manifest.read(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         manifest.content_type)
        try:
            json.loads(got_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

    def test_slo_get_the_manifest_with_details_from_server(self):
        manifest = self.env.container.file("manifest-db")
        got_body = manifest.read(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         manifest.content_type)
        try:
            value = json.loads(got_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

        self.assertEqual(len(value), 2)
        self.assertEqual(value[0]['bytes'], 1024 * 1024)
        self.assertEqual(
            value[0]['hash'],
            md5(b'd' * 1024 * 1024, usedforsecurity=False).hexdigest())
        expected_name = '/%s/seg_d' % self.env.container.name
        self.assertEqual(value[0]['name'], expected_name)

        self.assertEqual(value[1]['bytes'], 1024 * 1024)
        self.assertEqual(
            value[1]['hash'],
            md5(b'b' * 1024 * 1024, usedforsecurity=False).hexdigest())
        expected_name = '/%s/seg_b' % self.env.container.name
        self.assertEqual(value[1]['name'], expected_name)

    def test_slo_get_raw_the_manifest_with_details_from_server(self):
        manifest = self.env.container.file("manifest-db")
        got_body = manifest.read(parms={'multipart-manifest': 'get',
                                        'format': 'raw'})
        self.assert_etag(
            md5(got_body, usedforsecurity=False).hexdigest())

        # raw format should have the actual manifest object content-type
        self.assertEqual('application/octet-stream', manifest.content_type)
        try:
            value = json.loads(got_body)
        except ValueError:
            msg = "GET with multipart-manifest=get&format=raw got invalid json"
            self.fail(msg)

        self.assertEqual(
            set(value[0].keys()), set(('size_bytes', 'etag', 'path')))
        self.assertEqual(len(value), 2)
        self.assertEqual(value[0]['size_bytes'], 1024 * 1024)
        self.assertEqual(
            value[0]['etag'],
            md5(b'd' * 1024 * 1024, usedforsecurity=False).hexdigest())
        expected_name = '/%s/seg_d' % self.env.container.name
        self.assertEqual(value[0]['path'], expected_name)
        self.assertEqual(value[1]['size_bytes'], 1024 * 1024)
        self.assertEqual(
            value[1]['etag'],
            md5(b'b' * 1024 * 1024, usedforsecurity=False).hexdigest())
        expected_name = '/%s/seg_b' % self.env.container.name
        self.assertEqual(value[1]['path'], expected_name)

        file_item = self.env.container.file("manifest-from-get-raw")
        file_item.write(got_body, parms={'multipart-manifest': 'put'})

        file_contents = file_item.read()
        self.assertEqual(2 * 1024 * 1024, len(file_contents))

    def test_slo_head_the_manifest(self):
        manifest = self.env.container.file("manifest-abcde")
        got_info = manifest.info(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         got_info['content_type'])

    def test_slo_if_match_get(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.read(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_slo_if_none_match_put(self):
        file_item = self.env.container.file("manifest-if-none-match")
        manifest = json.dumps([{
            'size_bytes': 1024 * 1024,
            'etag': None,
            'path': '/%s/%s' % (self.env.container.name, 'seg_a')}])

        self.assertRaises(ResponseError, file_item.write,
                          manifest.encode('ascii'),
                          parms={'multipart-manifest': 'put'},
                          hdrs={'If-None-Match': '"not-star"'})
        self.assert_status(400)

        file_item.write(manifest.encode('ascii'),
                        parms={'multipart-manifest': 'put'},
                        hdrs={'If-None-Match': '*'})
        self.assert_status(201)

        self.assertRaises(ResponseError, file_item.write,
                          manifest.encode('ascii'),
                          parms={'multipart-manifest': 'put'},
                          hdrs={'If-None-Match': '*'})
        self.assert_status(412)

    def test_slo_if_none_match_get(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.read(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_slo_if_match_head(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.info(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_slo_if_none_match_head(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.info(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_slo_referer_on_segment_container(self):
        if tf.skip3:
            raise SkipTest('Username3 not set')
        # First the account2 (test3) should fail
        headers = {'X-Auth-Token': self.env.conn3.storage_token,
                   'Referer': 'http://blah.example.com'}
        slo_file = self.env.container2.file('manifest-abcde')
        self.assertRaises(ResponseError, slo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Now set the referer on the slo container only
        referer_metadata = {'X-Container-Read': '.r:*.example.com,.rlistings'}
        self.env.container2.update_metadata(referer_metadata)

        self.assertRaises(ResponseError, slo_file.read,
                          hdrs=headers)
        self.assert_status(409)

        # Finally set the referer on the segment container
        self.env.container.update_metadata(referer_metadata)
        contents = slo_file.read(hdrs=headers)
        self.assertEqual(4 * 1024 * 1024 + 1, len(contents))
        self.assertEqual(b'a', contents[:1])
        self.assertEqual(b'a', contents[1024 * 1024 - 1:1024 * 1024])
        self.assertEqual(b'b', contents[1024 * 1024:1024 * 1024 + 1])
        self.assertEqual(b'd', contents[-2:-1])
        self.assertEqual(b'e', contents[-1:])

    def test_slo_data_segments(self):
        # len('APRE' * 8) == 32
        # len('APOS' * 16) == 64
        # len('BPOS' * 32) == 128
        # len('CPRE' * 64) == 256
        # len(a_pre + seg_a + post_a) == 32 + 1024 ** 2 + 64
        # len(seg_b + post_b) == 1024 ** 2 + 128
        # len(c_pre + seg_c) == 256 + 1024 ** 2
        # len(total) == 3146208

        for file_name in ("mixed-object-data-manifest",
                          "nested-data-manifest"):
            file_item = self.env.container.file(file_name)
            file_contents = file_item.read(size=3 * 1024 ** 2 + 456,
                                           offset=28)
            self.assertEqual([
                (b'A', 1),
                (b'P', 1),
                (b'R', 1),
                (b'E', 1),
                (b'a', 1024 * 1024),
            ] + [
                (b'A', 1),
                (b'P', 1),
                (b'O', 1),
                (b'S', 1),
            ] * 16 + [
                (b'b', 1024 * 1024),
            ] + [
                (b'B', 1),
                (b'P', 1),
                (b'O', 1),
                (b'S', 1),
            ] * 32 + [
                (b'C', 1),
                (b'P', 1),
                (b'R', 1),
                (b'E', 1),
            ] * 64 + [
                (b'c', 1024 * 1024),
            ] + [
                (b'C', 1),
                (b'P', 1),
                (b'O', 1),
                (b'S', 1),
            ], group_file_contents(file_contents))


class TestSloUTF8(Base2, TestSlo):
    pass
