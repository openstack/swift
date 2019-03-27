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

import unittest
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

        # avoid getting a prefix that stops halfway through an encoded
        # character
        prefix = Utils.create_name().decode("utf-8")[:10].encode("utf-8")
        cls.segment_prefix = prefix

        for letter in ('a', 'b', 'c', 'd', 'e'):
            file_item = cls.container.file("%s/seg_lower%s" % (prefix, letter))
            file_item.write(letter * 10)

            file_item = cls.container.file(
                "%s/seg_upper_%%ff%s" % (prefix, letter))
            file_item.write(letter.upper() * 10)

        for letter in ('f', 'g', 'h', 'i', 'j'):
            file_item = cls.container2.file("%s/seg_lower%s" %
                                            (prefix, letter))
            file_item.write(letter * 10)

        man1 = cls.container.file("man1")
        man1.write('man1-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                         (cls.container.name, prefix)})

        man2 = cls.container.file("man2")
        man2.write('man2-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_upper_%%25ff" %
                         (cls.container.name, prefix)})

        manall = cls.container.file("manall")
        manall.write('manall-contents',
                     hdrs={"X-Object-Manifest": "%s/%s/seg" %
                           (cls.container.name, prefix)})

        mancont2 = cls.container.file("mancont2")
        mancont2.write(
            'mancont2-contents',
            hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                                       (cls.container2.name, prefix)})


class TestDlo(Base):
    env = TestDloEnv

    def test_get_manifest(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")

        file_item = self.env.container.file('man2')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE")

        file_item = self.env.container.file('manall')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            ("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee" +
             "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE"))

    def test_get_manifest_document_itself(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(file_contents, "man1-contents")
        self.assertEqual(file_item.info()['x_object_manifest'],
                         "%s/%s/seg_lower" %
                         (self.env.container.name, self.env.segment_prefix))

    def test_get_range(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(size=25, offset=8)
        self.assertEqual(file_contents, "aabbbbbbbbbbccccccccccddd")

        file_contents = file_item.read(size=1, offset=47)
        self.assertEqual(file_contents, "e")

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
        f_segment.write('ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy(self.env.container.name, "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete()

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
        # The copied object must not have X-Object-Manifest
        self.assertNotIn("x_object_manifest", file_item.info())

    def test_copy_account(self):
        # dlo use same account and same container only
        acct = self.env.conn.account_name
        # Adding a new segment, copying the manifest, and then deleting the
        # segment proves that the new object is really the concatenated
        # segments and not just a manifest.
        f_segment = self.env.container.file("%s/seg_lowerf" %
                                            (self.env.segment_prefix))
        f_segment.write('ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy_account(acct,
                                   self.env.container.name,
                                   "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete()

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
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
            self.assertEqual(copied_contents, "man1-contents")

            copied_contents = copied.read()
            self.assertEqual(
                copied_contents,
                "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")
            self.assertEqual(man1_item.info()['x_object_manifest'],
                             copied.info()['x_object_manifest'])
        finally:
            # try not to leave this around for other tests to stumble over
            self.env.container.file("copied-man1").delete()

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

    @unittest.skipIf('username3' not in tf.config, "Requires user 3")
    def test_dlo_referer_on_segment_container(self):
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
            "ffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj")

    def test_dlo_post_with_manifest_header(self):
        # verify that performing a POST to a DLO manifest
        # preserves the fact that it is a manifest file.
        # verify that the x-object-manifest header may be updated.

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('manifest-contents', hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST a modified x-object-manifest value
        new_x_o_m = x_o_m.rstrip('lower') + 'upper'
        file_item.post({'x-object-meta-foo': 'bar',
                        'x-object-manifest': new_x_o_m})

        # verify that x-object-manifest was updated
        file_item.info()
        resp_headers = file_item.conn.response.getheaders()
        self.assertIn(('x-object-manifest', new_x_o_m), resp_headers)
        self.assertIn(('x-object-meta-foo', 'bar'), resp_headers)

        # verify that manifest content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that updated manifest points to new content
        expected_contents = ''.join([(c * 10) for c in 'ABCDE'])
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
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that updated manifest points new content
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

    def test_dlo_post_without_manifest_header(self):
        # verify that a POST to a DLO manifest object with no
        # x-object-manifest header will cause the existing x-object-manifest
        # header to be lost

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('manifest-contents', hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST with no x-object-manifest header
        file_item.post({})

        # verify that existing x-object-manifest was removed
        info = file_item.info()
        self.assertNotIn('x_object_manifest', info)

        # verify that object content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that object is no longer a manifest
        contents = file_item.read(parms={})
        self.assertEqual('manifest-contents', contents)

    def test_dlo_post_with_manifest_regular_object(self):
        # verify that performing a POST to a regular object
        # with a manifest header will create a DLO.

        # Put a regular object
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('file contents', hdrs={})

        # sanity checks
        file_contents = file_item.read(parms={})
        self.assertEqual('file contents', file_contents)

        # get the path associated with man1
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']

        # POST a x-object-manifest value to the regular object
        file_item.post({'x-object-manifest': x_o_m})

        # verify that the file is now a manifest
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('file contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)
        file_item.info()
        resp_headers = file_item.conn.response.getheaders()
        self.assertIn(('x-object-manifest', x_o_m), resp_headers)


class TestDloUTF8(Base2, TestDlo):
    pass
