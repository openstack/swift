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
from xml.etree.cElementTree import Element, SubElement, tostring

from swift.common.constraints import valid_api_version
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import HTTP_NO_CONTENT
from swift.common.request_helpers import get_param
from swift.common.swob import HTTPException, HTTPNotAcceptable, Request, \
    RESPONSE_REASONS, HTTPBadRequest, wsgi_quote, wsgi_to_bytes
from swift.common.utils import RESERVED, get_logger, list_from_csv


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
    doc = Element('account', name=account_name)
    doc.text = '\n'
    for record in listing:
        if 'subdir' in record:
            name = record.pop('subdir')
            sub = SubElement(doc, 'subdir', name=name)
        else:
            sub = SubElement(doc, 'container')
            for field in ('name', 'count', 'bytes', 'last_modified'):
                SubElement(sub, field).text = str(record.pop(field))
            for field in ('storage_policy',):
                if field in record:
                    SubElement(sub, field).text = str(record.pop(field))
        sub.tail = '\n'
    return to_xml(doc)


def container_to_xml(listing, base_name):
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
                SubElement(sub, field).text = str(record.pop(field))
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
    def __init__(self, app, conf, logger=None):
        self.app = app
        self.logger = logger or get_logger(conf, log_route='listing-filter')

    def filter_reserved(self, listing, account, container):
        new_listing = []
        for entry in list(listing):
            for key in ('name', 'subdir'):
                value = entry.get(key, '')
                if RESERVED in value:
                    if container:
                        self.logger.warning(
                            'Container listing for %s/%s had '
                            'reserved byte in %s: %r',
                            wsgi_quote(account), wsgi_quote(container),
                            key, value)
                    else:
                        self.logger.warning(
                            'Account listing for %s had '
                            'reserved byte in %s: %r',
                            wsgi_quote(account), key, value)
                    break  # out of the *key* loop; check next entry
            else:
                new_listing.append(entry)
        return new_listing

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            # account and container only
            version, acct, cont = req.split_path(2, 3)
        except ValueError:
            is_account_or_container_req = False
        else:
            is_account_or_container_req = True
        if not is_account_or_container_req:
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
        can_vary = 'format' not in params
        params['format'] = 'json'
        req.params = params

        # Give other middlewares a chance to be in charge
        env.setdefault('swift.format_listing', True)
        status, headers, resp_iter = req.call_application(self.app)
        if not env.get('swift.format_listing'):
            start_response(status, headers)
            return resp_iter

        if not status.startswith(('200 ', '204 ')):
            start_response(status, headers)
            return resp_iter

        headers_dict = HeaderKeyDict(headers)
        resp_content_type = headers_dict.get(
            'content-type', '').partition(';')[0]
        resp_length = headers_dict.get('content-length')

        if can_vary:
            if 'vary' in headers_dict:
                value = headers_dict['vary']
                if 'accept' not in list_from_csv(value.lower()):
                    headers_dict['vary'] = value + ', Accept'
            else:
                headers_dict['vary'] = 'Accept'

        if resp_content_type != 'application/json':
            start_response(status, list(headers_dict.items()))
            return resp_iter

        if req.method == 'HEAD':
            headers_dict['content-type'] = out_content_type + '; charset=utf-8'
            # proxy logging (and maybe other mw?) seem to be good about
            # sticking this on HEAD/204 but we do it here to be responsible
            # and explicit
            headers_dict['content-length'] = 0
            start_response(status, list(headers_dict.items()))
            return resp_iter

        if resp_length is None or \
                int(resp_length) > MAX_CONTAINER_LISTING_CONTENT_LENGTH:
            start_response(status, list(headers_dict.items()))
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
            start_response(status, list(headers_dict.items()))
            return [body]

        if not req.allow_reserved_names:
            listing = self.filter_reserved(listing, acct, cont)

        try:
            if out_content_type.endswith('/xml'):
                if cont:
                    body = container_to_xml(
                        listing, wsgi_to_bytes(cont).decode('utf-8'))
                else:
                    body = account_to_xml(
                        listing, wsgi_to_bytes(acct).decode('utf-8'))
            elif out_content_type == 'text/plain':
                body = listing_to_text(listing)
            else:
                body = json.dumps(listing).encode('ascii')
        except KeyError:
            # listing was in a bad format -- funky static web listing??
            start_response(status, list(headers_dict.items()))
            return [body]

        if not body:
            status = '%s %s' % (HTTP_NO_CONTENT,
                                RESPONSE_REASONS[HTTP_NO_CONTENT][0])

        headers_dict['content-type'] = out_content_type + '; charset=utf-8'
        headers_dict['content-length'] = len(body)
        start_response(status, list(headers_dict.items()))
        return [body]


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def listing_filter(app):
        return ListingFilter(app, conf)
    return listing_filter
