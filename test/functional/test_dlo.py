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

import urllib

from swift.common.swob import str_to_wsgi
import test.functional as tf
from test.functional.tests import Utils, Base, Base2, BaseEnv
from test.functional.swift_test_client import Connection, ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestDloEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestDloEnv, cls).setUp()

        cls.container = cls.account.container(Utils.create_name())
        cls.container2 = cls.account.container(Utils.create_name())

        for cont in (cls.container, cls.container2):
            if not cont.create():
                raise ResponseError(cls.conn.response)

        prefix = Utils.create_name(10)
        cls.segment_prefix = prefix

        for letter in ('a', 'b', 'c', 'd', 'e'):
            file_item = cls.container.file("%s/seg_lower%s" % (prefix, letter))
            file_item.write(letter.encode('ascii') * 10)

            file_item = cls.container.file(
                "%s/seg_upper_%%ff%s" % (prefix, letter))
            file_item.write(letter.upper().encode('ascii') * 10)

        for letter in ('f', 'g', 'h', 'i', 'j'):
            file_item = cls.container2.file("%s/seg_lower%s" %
                                            (prefix, letter))
            file_item.write(letter.encode('ascii') * 10)

        man1 = cls.container.file("man1")
        man1.write(b'man1-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                         (cls.container.name, prefix)})

        man2 = cls.container.file("man2")
        man2.write(b'man2-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_upper_%%25ff" %
                         (cls.container.name, prefix)})

        manall = cls.container.file("manall")
        manall.write(b'manall-contents',
                     hdrs={"X-Object-Manifest": "%s/%s/seg" %
                           (cls.container.name, prefix)})

        mancont2 = cls.container.file("mancont2")
        mancont2.write(
            b'mancont2-contents',
            hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                                       (cls.container2.name, prefix)})


class TestDlo(Base):
    env = TestDloEnv

    def test_get_manifest(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")

        file_item = self.env.container.file('man2')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            b"AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE")

        file_item = self.env.container.file('manall')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            (b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee" +
             b"AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE"))

    def test_get_manifest_document_itself(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(file_contents, b"man1-contents")
        self.assertEqual(file_item.info()['x_object_manifest'],
                         "%s/%s/seg_lower" %
                         (self.env.container.name, self.env.segment_prefix))

    def test_get_range(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(size=25, offset=8)
        self.assertEqual(file_contents, b"aabbbbbbbbbbccccccccccddd")

        file_contents = file_item.read(size=1, offset=47)
        self.assertEqual(file_contents, b"e")

    def test_get_multiple_ranges(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(
            hdrs={'Range': 'bytes=0-4,10-14'})
        self.assert_status(200)  # *not* 206
        self.assertEqual(
            file_contents,
            b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")

    def test_get_range_out_of_range(self):
        file_item = self.env.container.file('man1')

        self.assertRaises(ResponseError, file_item.read, size=7, offset=50)
        self.assert_status(416)
        self.assert_header('content-range', 'bytes */50')

    def test_copy(self):
        # Adding a new segment, copying the manifest, and then deleting the
        # segment proves that the new object is really the concatenated
        # segments and not just a manifest.
        f_segment = self.env.container.file("%s/seg_lowerf" %
                                            (self.env.segment_prefix))
        f_segment.write(b'ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy(self.env.container.name, "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete(tolerate_missing=True)

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
        # The copied object must not have X-Object-Manifest
        self.assertNotIn("x_object_manifest", file_item.info())

    def test_copy_account(self):
        # dlo use same account and same container only
        acct = urllib.parse.unquote(self.env.conn.account_name)
        # Adding a new segment, copying the manifest, and then deleting the
        # segment proves that the new object is really the concatenated
        # segments and not just a manifest.
        f_segment = self.env.container.file("%s/seg_lowerf" %
                                            (self.env.segment_prefix))
        f_segment.write(b'ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy_account(acct,
                                   self.env.container.name,
                                   "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete(tolerate_missing=True)

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
        # The copied object must not have X-Object-Manifest
        self.assertNotIn("x_object_manifest", file_item.info())

    def test_copy_manifest(self):
        # Copying the manifest with multipart-manifest=get query string
        # should result in another manifest
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy(self.env.container.name, "copied-man1",
                           parms={'multipart-manifest': 'get'})

            copied = self.env.container.file("copied-man1")
            copied_contents = copied.read(parms={'multipart-manifest': 'get'})
            self.assertEqual(copied_contents, b"man1-contents")

            copied_contents = copied.read()
            self.assertEqual(
                copied_contents,
                b"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")
            self.assertEqual(man1_item.info()['x_object_manifest'],
                             copied.info()['x_object_manifest'])
        finally:
            # try not to leave this around for other tests to stumble over
            self.env.container.file("copied-man1").delete(
                tolerate_missing=True)

    def test_dlo_if_match_get(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.read(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_dlo_if_none_match_get(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.read(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_dlo_if_match_head(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.info(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_dlo_if_none_match_head(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.info(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_dlo_referer_on_segment_container(self):
        if 'username3' not in tf.config:
            self.skipTest('Requires user 3')
        # First the account2 (test3) should fail
        config2 = tf.config.copy()
        config2['username'] = tf.config['username3']
        config2['password'] = tf.config['password3']
        conn2 = Connection(config2)
        conn2.authenticate()
        headers = {'X-Auth-Token': conn2.storage_token,
                   'Referer': 'http://blah.example.com'}
        dlo_file = self.env.container.file("mancont2")
        self.assertRaises(ResponseError, dlo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Now set the referer on the dlo container only
        referer_metadata = {'X-Container-Read': '.r:*.example.com,.rlistings'}
        self.env.container.update_metadata(referer_metadata)

        self.assertRaises(ResponseError, dlo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Finally set the referer on the segment container
        self.env.container2.update_metadata(referer_metadata)

        contents = dlo_file.read(hdrs=headers)
        self.assertEqual(
            contents,
            b"ffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj")

    def test_dlo_post_with_manifest_header(self):
        # verify that performing a POST to a DLO manifest
        # preserves the fact that it is a manifest file.
        # verify that the x-object-manifest header may be updated.

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(b'manifest-contents',
                        hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'manifest-contents', manifest_contents)
        expected_contents = ''.join((c * 10) for c in 'abcde').encode('ascii')
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST a modified x-object-manifest value
        new_x_o_m = x_o_m.rstrip('lower') + 'upper'
        file_item.post({'x-object-meta-foo': 'bar',
                        'x-object-manifest': new_x_o_m})

        # verify that x-object-manifest was updated
        file_item.info()
        resp_headers = [(h.lower(), v)
                        for h, v in file_item.conn.response.getheaders()]
        self.assertIn(('x-object-manifest', str_to_wsgi(new_x_o_m)),
                      resp_headers)
        self.assertIn(('x-object-meta-foo', 'bar'), resp_headers)

        # verify that manifest content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'manifest-contents', manifest_contents)

        # verify that updated manifest points to new content
        expected_contents = ''.join((c * 10) for c in 'ABCDE').encode('ascii')
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # Now revert the manifest to point to original segments, including a
        # multipart-manifest=get param just to check that has no effect
        file_item.post({'x-object-manifest': x_o_m},
                       parms={'multipart-manifest': 'get'})

        # verify that x-object-manifest was reverted
        info = file_item.info()
        self.assertIn('x_object_manifest', info)
        self.assertEqual(x_o_m, info['x_object_manifest'])

        # verify that manifest content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'manifest-contents', manifest_contents)

        # verify that updated manifest points new content
        expected_contents = ''.join((c * 10) for c in 'abcde').encode('ascii')
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

    def test_dlo_post_without_manifest_header(self):
        # verify that a POST to a DLO manifest object with no
        # x-object-manifest header will cause the existing x-object-manifest
        # header to be lost

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(b'manifest-contents',
                        hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'manifest-contents', manifest_contents)
        expected_contents = ''.join((c * 10) for c in 'abcde').encode('ascii')
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST with no x-object-manifest header
        file_item.post({})

        # verify that existing x-object-manifest was removed
        info = file_item.info()
        self.assertNotIn('x_object_manifest', info)

        # verify that object content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'manifest-contents', manifest_contents)

        # verify that object is no longer a manifest
        contents = file_item.read(parms={})
        self.assertEqual(b'manifest-contents', contents)

    def test_dlo_post_with_manifest_regular_object(self):
        # verify that performing a POST to a regular object
        # with a manifest header will create a DLO.

        # Put a regular object
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(b'file contents', hdrs={})

        # sanity checks
        file_contents = file_item.read(parms={})
        self.assertEqual(b'file contents', file_contents)

        # get the path associated with man1
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']

        # POST a x-object-manifest value to the regular object
        file_item.post({'x-object-manifest': x_o_m})

        # verify that the file is now a manifest
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(b'file contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde']).encode()
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)
        file_item.info()
        resp_headers = [(h.lower(), v)
                        for h, v in file_item.conn.response.getheaders()]
        self.assertIn(('x-object-manifest', str_to_wsgi(x_o_m)), resp_headers)


class TestDloUTF8(Base2, TestDlo):
    pass
