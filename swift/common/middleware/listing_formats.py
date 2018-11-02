# Copyright (c) 2017 OpenStack Foundation
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

import json
import six
from xml.etree.cElementTree import Element, SubElement, tostring

from swift.common.constraints import valid_api_version
from swift.common.http import HTTP_NO_CONTENT
from swift.common.request_helpers import get_param
from swift.common.swob import HTTPException, HTTPNotAcceptable, Request, \
    RESPONSE_REASONS, HTTPBadRequest


#: Mapping of query string ``format=`` values to their corresponding
#: content-type values.
FORMAT2CONTENT_TYPE = {'plain': 'text/plain', 'json': 'application/json',
                       'xml': 'application/xml'}
#: Maximum size of a valid JSON container listing body. If we receive
#: a container listing response larger than this, assume it's a staticweb
#: response and pass it on to the client.
# Default max object length is 1024, default container listing limit is 1e4;
# add a fudge factor for things like hash, last_modified, etc.
MAX_CONTAINER_LISTING_CONTENT_LENGTH = 1024 * 10000 * 2


def get_listing_content_type(req):
    """
    Determine the content type to use for an account or container listing
    response.

    :param req: request object
    :returns: content type as a string (e.g. text/plain, application/json)
    :raises HTTPNotAcceptable: if the requested content type is not acceptable
    :raises HTTPBadRequest: if the 'format' query param is provided and
             not valid UTF-8
    """
    query_format = get_param(req, 'format')
    if query_format:
        req.accept = FORMAT2CONTENT_TYPE.get(
            query_format.lower(), FORMAT2CONTENT_TYPE['plain'])
    try:
        out_content_type = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
    except ValueError:
        raise HTTPBadRequest(request=req, body=b'Invalid Accept header')
    if not out_content_type:
        raise HTTPNotAcceptable(request=req)
    return out_content_type


def to_xml(document_element):
    result = tostring(document_element, encoding='UTF-8').replace(
        b"<?xml version='1.0' encoding='UTF-8'?>",
        b'<?xml version="1.0" encoding="UTF-8"?>', 1)
    if not result.startswith(b'<?xml '):
        # py3 tostring doesn't (necessarily?) include the XML declaration;
        # add it if it's missing.
        result = b'<?xml version="1.0" encoding="UTF-8"?>\n' + result
    return result


def account_to_xml(listing, account_name):
    if isinstance(account_name, bytes):
        account_name = account_name.decode('utf-8')
    doc = Element('account', name=account_name)
    doc.text = '\n'
    for record in listing:
        if 'subdir' in record:
            name = record.pop('subdir')
            sub = SubElement(doc, 'subdir', name=name)
        else:
            sub = SubElement(doc, 'container')
            for field in ('name', 'count', 'bytes', 'last_modified'):
                SubElement(sub, field).text = six.text_type(
                    record.pop(field))
        sub.tail = '\n'
    return to_xml(doc)


def container_to_xml(listing, base_name):
    if isinstance(base_name, bytes):
        base_name = base_name.decode('utf-8')
    doc = Element('container', name=base_name)
    for record in listing:
        if 'subdir' in record:
            name = record.pop('subdir')
            sub = SubElement(doc, 'subdir', name=name)
            SubElement(sub, 'name').text = name
        else:
            sub = SubElement(doc, 'object')
            for field in ('name', 'hash', 'bytes', 'content_type',
                          'last_modified'):
                SubElement(sub, field).text = six.text_type(
                    record.pop(field))
    return to_xml(doc)


def listing_to_text(listing):
    def get_lines():
        for item in listing:
            if 'name' in item:
                yield item['name'].encode('utf-8') + b'\n'
            else:
                yield item['subdir'].encode('utf-8') + b'\n'
    return b''.join(get_lines())


class ListingFilter(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            # account and container only
            version, acct, cont = req.split_path(2, 3)
        except ValueError:
            return self.app(env, start_response)

        if not valid_api_version(version) or req.method not in ('GET', 'HEAD'):
            return self.app(env, start_response)

        # OK, definitely have an account/container request.
        # Get the desired content-type, then force it to a JSON request.
        try:
            out_content_type = get_listing_content_type(req)
        except HTTPException as err:
            return err(env, start_response)

        params = req.params
        params['format'] = 'json'
        req.params = params

        status, headers, resp_iter = req.call_application(self.app)

        header_to_index = {}
        resp_content_type = resp_length = None
        for i, (header, value) in enumerate(headers):
            header = header.lower()
            if header == 'content-type':
                header_to_index[header] = i
                resp_content_type = value.partition(';')[0]
            elif header == 'content-length':
                header_to_index[header] = i
                resp_length = int(value)

        if not status.startswith('200 '):
            start_response(status, headers)
            return resp_iter

        if resp_content_type != 'application/json':
            start_response(status, headers)
            return resp_iter

        if resp_length is None or \
                resp_length > MAX_CONTAINER_LISTING_CONTENT_LENGTH:
            start_response(status, headers)
            return resp_iter

        def set_header(header, value):
            if value is None:
                del headers[header_to_index[header]]
            else:
                headers[header_to_index[header]] = (
                    headers[header_to_index[header]][0], str(value))

        if req.method == 'HEAD':
            set_header('content-type', out_content_type + '; charset=utf-8')
            set_header('content-length', None)  # don't know, can't determine
            start_response(status, headers)
            return resp_iter

        body = b''.join(resp_iter)
        try:
            listing = json.loads(body)
            # Do a couple sanity checks
            if not isinstance(listing, list):
                raise ValueError
            if not all(isinstance(item, dict) for item in listing):
                raise ValueError
        except ValueError:
            # Static web listing that's returning invalid JSON?
            # Just pass it straight through; that's about all we *can* do.
            start_response(status, headers)
            return [body]

        try:
            if out_content_type.endswith('/xml'):
                if cont:
                    body = container_to_xml(listing, cont)
                else:
                    body = account_to_xml(listing, acct)
            elif out_content_type == 'text/plain':
                body = listing_to_text(listing)
            # else, json -- we continue down here to be sure we set charset
        except KeyError:
            # listing was in a bad format -- funky static web listing??
            start_response(status, headers)
            return [body]

        if not body:
            status = '%s %s' % (HTTP_NO_CONTENT,
                                RESPONSE_REASONS[HTTP_NO_CONTENT][0])

        set_header('content-type', out_content_type + '; charset=utf-8')
        set_header('content-length', len(body))
        start_response(status, headers)
        return [body]


def filter_factory(global_conf, **local_conf):
    return ListingFilter
