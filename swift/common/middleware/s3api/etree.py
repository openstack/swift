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
from copy import deepcopy
try:
    # importlib.resources was introduced in py37, but couldn't handle
    # resources in subdirectories (which we use); files() added support
    from importlib.resources import files
    del files
except ImportError:
    # python < 3.9
    from pkg_resources import resource_stream  # pylint: disable-msg=E0611
else:
    import importlib.resources
    resource_stream = None

from swift.common.utils import get_logger
from swift.common.middleware.s3api.exception import S3Exception
from swift.common.middleware.s3api.utils import camel_to_snake, \
    utf8decode

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

    if not isinstance(elem.tag, str):
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
            if resource_stream:
                # python < 3.9
                stream = resource_stream(__name__, path)
            else:
                stream = importlib.resources.files(
                    __name__.rsplit('.', 1)[0]).joinpath(path).open('rb')
            with stream as rng:
                lxml.etree.RelaxNG(file=rng).assertValid(elem)
        except IOError as e:
            # Probably, the schema file doesn't exist.
            logger = logger or get_logger({}, log_route='s3api')
            logger.error(e)
            raise
        except lxml.etree.DocumentInvalid as e:
            if logger:
                logger.debug(e)
            raise DocumentInvalid(e)

    return elem


def tostring(tree, use_s3ns=True, xml_declaration=True):
    if use_s3ns:
        nsmap = tree.nsmap.copy()
        nsmap[None] = XMLNS_S3

        root = Element(tree.tag, attrib=tree.attrib, nsmap=nsmap)
        root.text = tree.text
        root.extend(deepcopy(list(tree)))
        tree = root

    return lxml.etree.tostring(tree, xml_declaration=xml_declaration,
                               encoding='UTF-8')


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
        return lxml.etree.ElementBase.text.__get__(self)

    @text.setter
    def text(self, value):
        lxml.etree.ElementBase.text.__set__(self, utf8decode(value))


parser_lookup = lxml.etree.ElementDefaultClassLookup(element=_Element)
parser = lxml.etree.XMLParser(resolve_entities=False, no_network=True)
parser.set_element_class_lookup(parser_lookup)

Element = parser.makeelement
SubElement = lxml.etree.SubElement
