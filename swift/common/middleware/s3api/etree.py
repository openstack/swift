# Copyright (c) 2014 OpenStack Foundation.
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

import lxml.etree
from urllib import quote
from copy import deepcopy
from pkg_resources import resource_stream  # pylint: disable-msg=E0611
import sys

from swift.common.utils import get_logger
from swift.common.middleware.s3api.exception import S3Exception
from swift.common.middleware.s3api.utils import camel_to_snake, \
    utf8encode, utf8decode

XMLNS_S3 = 'http://s3.amazonaws.com/doc/2006-03-01/'
XMLNS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'


class XMLSyntaxError(S3Exception):
    pass


class DocumentInvalid(S3Exception):
    pass


def cleanup_namespaces(elem):
    def remove_ns(tag, ns):
        if tag.startswith('{%s}' % ns):
            tag = tag[len('{%s}' % ns):]
        return tag

    if not isinstance(elem.tag, basestring):
        # elem is a comment element.
        return

    # remove s3 namespace
    elem.tag = remove_ns(elem.tag, XMLNS_S3)

    # remove default namespace
    if elem.nsmap and None in elem.nsmap:
        elem.tag = remove_ns(elem.tag, elem.nsmap[None])

    for e in elem.iterchildren():
        cleanup_namespaces(e)


def fromstring(text, root_tag=None, logger=None):
    try:
        elem = lxml.etree.fromstring(text, parser)
    except lxml.etree.XMLSyntaxError as e:
        if logger:
            logger.debug(e)
        raise XMLSyntaxError(e)

    cleanup_namespaces(elem)

    if root_tag is not None:
        # validate XML
        try:
            path = 'schema/%s.rng' % camel_to_snake(root_tag)
            with resource_stream(__name__, path) as rng:
                lxml.etree.RelaxNG(file=rng).assertValid(elem)
        except IOError as e:
            # Probably, the schema file doesn't exist.
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger = logger or get_logger({}, log_route='s3api')
            logger.error(e)
            raise exc_type, exc_value, exc_traceback
        except lxml.etree.DocumentInvalid as e:
            if logger:
                logger.debug(e)
            raise DocumentInvalid(e)

    return elem


def tostring(tree, encoding_type=None, use_s3ns=True):
    if use_s3ns:
        nsmap = tree.nsmap.copy()
        nsmap[None] = XMLNS_S3

        root = Element(tree.tag, attrib=tree.attrib, nsmap=nsmap)
        root.text = tree.text
        root.extend(deepcopy(tree.getchildren()))
        tree = root

    if encoding_type == 'url':
        tree = deepcopy(tree)
        for e in tree.iter():
            # Some elements are not url-encoded even when we specify
            # encoding_type=url.
            blacklist = ['LastModified', 'ID', 'DisplayName', 'Initiated']
            if e.tag not in blacklist:
                if isinstance(e.text, basestring):
                    e.text = quote(e.text)

    return lxml.etree.tostring(tree, xml_declaration=True, encoding='UTF-8')


class _Element(lxml.etree.ElementBase):
    """
    Wrapper Element class of lxml.etree.Element to support
    a utf-8 encoded non-ascii string as a text.

    Why we need this?:
    Original lxml.etree.Element supports only unicode for the text.
    It declines maintainability because we have to call a lot of encode/decode
    methods to apply account/container/object name (i.e. PATH_INFO) to each
    Element instance. When using this class, we can remove such a redundant
    codes from swift.common.middleware.s3api middleware.
    """
    def __init__(self, *args, **kwargs):
        # pylint: disable-msg=E1002
        super(_Element, self).__init__(*args, **kwargs)

    @property
    def text(self):
        """
        utf-8 wrapper property of lxml.etree.Element.text
        """
        return utf8encode(lxml.etree.ElementBase.text.__get__(self))

    @text.setter
    def text(self, value):
        lxml.etree.ElementBase.text.__set__(self, utf8decode(value))


parser_lookup = lxml.etree.ElementDefaultClassLookup(element=_Element)
parser = lxml.etree.XMLParser()
parser.set_element_class_lookup(parser_lookup)

Element = parser.makeelement
SubElement = lxml.etree.SubElement
