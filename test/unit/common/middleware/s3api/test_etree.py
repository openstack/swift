# Copyright (c) 2014 OpenStack Foundation
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

from swift.common.middleware.s3api import etree


class TestS3ApiEtree(unittest.TestCase):
    def test_xml_namespace(self):
        def test_xml(ns, prefix):
            return '<A %(ns)s><%(prefix)sB>C</%(prefix)sB></A>' % \
                ({'ns': ns, 'prefix': prefix})

        # No namespace is same as having the S3 namespace.
        xml = test_xml('', '')
        elem = etree.fromstring(xml)
        self.assertEqual(elem.find('./B').text, 'C')

        # The S3 namespace is handled as no namespace.
        xml = test_xml('xmlns="%s"' % etree.XMLNS_S3, '')
        elem = etree.fromstring(xml)
        self.assertEqual(elem.find('./B').text, 'C')

        xml = test_xml('xmlns:s3="%s"' % etree.XMLNS_S3, 's3:')
        elem = etree.fromstring(xml)
        self.assertEqual(elem.find('./B').text, 'C')

        # Any namespaces without a prefix work as no namespace.
        xml = test_xml('xmlns="http://example.com/"', '')
        elem = etree.fromstring(xml)
        self.assertEqual(elem.find('./B').text, 'C')

        xml = test_xml('xmlns:s3="http://example.com/"', 's3:')
        elem = etree.fromstring(xml)
        self.assertIsNone(elem.find('./B'))

    def test_xml_with_comments(self):
        xml = '<A><!-- comment --><B>C</B></A>'
        elem = etree.fromstring(xml)
        self.assertEqual(elem.find('./B').text, 'C')

    def test_tostring_with_nonascii_text(self):
        elem = etree.Element('Test')
        sub = etree.SubElement(elem, 'FOO')
        sub.text = '\xef\xbc\xa1'
        self.assertIsInstance(sub.text, str)
        xml_string = etree.tostring(elem)
        self.assertIsInstance(xml_string, bytes)

    def test_fromstring_with_nonascii_text(self):
        input_str = b'<?xml version="1.0" encoding="UTF-8"?>\n' \
                    b'<Test><FOO>\xef\xbc\xa1</FOO></Test>'
        elem = etree.fromstring(input_str)
        text = elem.find('FOO').text
        self.assertEqual(text, b'\xef\xbc\xa1'.decode('utf8'))
        self.assertIsInstance(text, str)


if __name__ == '__main__':
    unittest.main()
