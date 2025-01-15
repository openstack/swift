# Copyright (c) 2010-2013 OpenStack Foundation
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

"""
Miscellaneous utility functions for use in generating responses.

Why not swift.common.utils, you ask? Because this way we can import things
from swob in here without creating circular imports.
"""

import itertools
import time

from swift.common.header_key_dict import HeaderKeyDict

from swift.common.constraints import AUTO_CREATE_ACCOUNT_PREFIX, \
    CONTAINER_LISTING_LIMIT
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.http import is_success, is_server_error
from swift.common.swob import HTTPBadRequest, \
    HTTPServiceUnavailable, Range, is_chunked, multi_range_iterator, \
    HTTPPreconditionFailed, wsgi_to_bytes, wsgi_unquote, wsgi_to_str
from swift.common.utils import split_path, validate_device_partition, \
    close_if_possible, friendly_close, \
    maybe_multipart_byteranges_to_document_iters, \
    multipart_byteranges_to_document_iters, parse_content_type, \
    parse_content_range, csv_append, list_from_csv, Spliterator, quote, \
    RESERVED, config_true_value, md5, CloseableChain, select_ip_port
from swift.common.wsgi import make_subrequest


OBJECT_TRANSIENT_SYSMETA_PREFIX = 'x-object-transient-sysmeta-'
OBJECT_SYSMETA_CONTAINER_UPDATE_OVERRIDE_PREFIX = \
    'x-object-sysmeta-container-update-override-'
USE_REPLICATION_NETWORK_HEADER = 'x-backend-use-replication-network'
MISPLACED_OBJECTS_ACCOUNT = '.misplaced_objects'


def get_param(req, name, default=None):
    """
    Get a parameter from an HTTP request ensuring proper handling UTF-8
    encoding.

    :param req: request object
    :param name: parameter name
    :param default: result to return if the parameter is not found
    :returns: HTTP request parameter value, as a native (not WSGI) string
    :raises HTTPBadRequest: if param not valid UTF-8 byte sequence
    """
    value = req.params.get(name, default)
    if value:
        # req.params is a dict of WSGI strings, so encoding will succeed
        value = value.encode('latin1')
        try:
            # Ensure UTF8ness since we're at it
            value = value.decode('utf8')
        except UnicodeDecodeError:
            raise HTTPBadRequest(
                request=req, content_type='text/plain',
                body='"%s" parameter not valid UTF-8' % name)
    return value


def get_valid_part_num(req):
    """
    Any non-range GET or HEAD request for a SLO object may include a
    part-number parameter in query string.  If the passed in request
    includes a part-number parameter it will be parsed into a valid integer
    and returned.  If the passed in request does not include a part-number
    param we will return None.  If the part-number parameter is invalid for
    the given request we will raise the appropriate HTTP exception

    :param req: the request object

    :returns: validated part-number value or None
    :raises HTTPBadRequest: if request or part-number param is not valid
    """
    part_number_param = get_param(req, 'part-number')
    if part_number_param is None:
        return None
    try:
        part_number = int(part_number_param)
        if part_number <= 0:
            raise ValueError
    except ValueError:
        raise HTTPBadRequest('Part number must be an integer greater '
                             'than 0')

    if req.range:
        raise HTTPBadRequest(req=req,
                             body='Range requests are not supported '
                                  'with part number queries')

    return part_number


def validate_params(req, names):
    """
    Get list of parameters from an HTTP request, validating the encoding of
    each parameter.

    :param req: request object
    :param names: parameter names
    :returns: a dict mapping parameter names to values for each name that
              appears in the request parameters
    :raises HTTPBadRequest: if any parameter value is not a valid UTF-8 byte
            sequence
    """
    params = {}
    for name in names:
        value = get_param(req, name)
        if value is None:
            continue
        params[name] = value
    return params


def constrain_req_limit(req, constrained_limit):
    given_limit = get_param(req, 'limit')
    limit = constrained_limit
    if given_limit and given_limit.isdigit():
        limit = int(given_limit)
        if limit > constrained_limit:
            raise HTTPPreconditionFailed(
                request=req, body='Maximum limit is %d' % constrained_limit)
    return limit


def validate_container_params(req):
    params = validate_params(req, ('marker', 'end_marker', 'prefix',
                                   'delimiter', 'path', 'format', 'reverse',
                                   'states', 'includes'))
    params['limit'] = constrain_req_limit(req, CONTAINER_LISTING_LIMIT)
    return params


def _validate_internal_name(name, type_='name'):
    if RESERVED in name and not name.startswith(RESERVED):
        raise HTTPBadRequest(body='Invalid reserved-namespace %s' % (type_))


def validate_internal_account(account):
    """
    Validate internal account name.

    :raises: HTTPBadRequest
    """
    _validate_internal_name(account, 'account')


def validate_internal_container(account, container):
    """
    Validate internal account and container names.

    :raises: HTTPBadRequest
    """
    if not account:
        raise ValueError('Account is required')
    validate_internal_account(account)
    if container:
        _validate_internal_name(container, 'container')


def validate_internal_obj(account, container, obj):
    """
    Validate internal account, container and object names.

    :raises: HTTPBadRequest
    """
    if not account:
        raise ValueError('Account is required')
    if not container:
        raise ValueError('Container is required')
    validate_internal_container(account, container)
    if obj and not (account.startswith(AUTO_CREATE_ACCOUNT_PREFIX) or
                    account == MISPLACED_OBJECTS_ACCOUNT):
        _validate_internal_name(obj, 'object')
        if container.startswith(RESERVED) and not obj.startswith(RESERVED):
            raise HTTPBadRequest(body='Invalid user-namespace object '
                                 'in reserved-namespace container')
        elif obj.startswith(RESERVED) and not container.startswith(RESERVED):
            raise HTTPBadRequest(body='Invalid reserved-namespace object '
                                 'in user-namespace container')


def get_name_and_placement(request, minsegs=1, maxsegs=None,
                           rest_with_last=False):
    """
    Utility function to split and validate the request path and storage
    policy.  The storage policy index is extracted from the headers of
    the request and converted to a StoragePolicy instance.  The
    remaining args are passed through to
    :meth:`split_and_validate_path`.

    :returns: a list, result of :meth:`split_and_validate_path` with
              the BaseStoragePolicy instance appended on the end
    :raises HTTPServiceUnavailable: if the path is invalid or no policy exists
             with the extracted policy_index.
    """
    policy_index = request.headers.get('X-Backend-Storage-Policy-Index')
    policy = POLICIES.get_by_index(policy_index)
    if not policy:
        raise HTTPServiceUnavailable(
            body="No policy with index %s" % policy_index,
            request=request, content_type='text/plain')
    results = split_and_validate_path(request, minsegs=minsegs,
                                      maxsegs=maxsegs,
                                      rest_with_last=rest_with_last)
    results.append(policy)
    return results


def split_and_validate_path(request, minsegs=1, maxsegs=None,
                            rest_with_last=False):
    """
    Utility function to split and validate the request path.

    :returns: result of :meth:`~swift.common.utils.split_path` if
              everything's okay, as native strings
    :raises HTTPBadRequest: if something's not okay
    """
    try:
        segs = request.split_path(minsegs, maxsegs, rest_with_last)
        validate_device_partition(segs[0], segs[1])
        return [wsgi_to_str(seg) for seg in segs]
    except ValueError as err:
        raise HTTPBadRequest(body=str(err), request=request,
                             content_type='text/plain')


def is_user_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the user
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    if len(key) <= 8 + len(server_type):
        return False
    return key.lower().startswith(get_user_meta_prefix(server_type))


def is_sys_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the system
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    if len(key) <= 11 + len(server_type):
        return False
    return key.lower().startswith(get_sys_meta_prefix(server_type))


def is_sys_or_user_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the user or system
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    return is_user_meta(server_type, key) or is_sys_meta(server_type, key)


def is_object_transient_sysmeta(key):
    """
    Tests if a header key starts with and is longer than the prefix for object
    transient system metadata.

    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    if len(key) <= len(OBJECT_TRANSIENT_SYSMETA_PREFIX):
        return False
    return key.lower().startswith(OBJECT_TRANSIENT_SYSMETA_PREFIX)


def strip_user_meta_prefix(server_type, key):
    """
    Removes the user metadata prefix for a given server type from the start
    of a header key.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: stripped header key
    """
    if not is_user_meta(server_type, key):
        raise ValueError('Key is not user meta')
    return key[len(get_user_meta_prefix(server_type)):]


def strip_sys_meta_prefix(server_type, key):
    """
    Removes the system metadata prefix for a given server type from the start
    of a header key.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: stripped header key
    """
    if not is_sys_meta(server_type, key):
        raise ValueError('Key is not sysmeta')
    return key[len(get_sys_meta_prefix(server_type)):]


def strip_object_transient_sysmeta_prefix(key):
    """
    Removes the object transient system metadata prefix from the start of a
    header key.

    :param key: header key
    :returns: stripped header key
    """
    if not is_object_transient_sysmeta(key):
        raise ValueError('Key is not object transient sysmeta')
    return key[len(OBJECT_TRANSIENT_SYSMETA_PREFIX):]


def get_user_meta_prefix(server_type):
    """
    Returns the prefix for user metadata headers for given server type.

    This prefix defines the namespace for headers that will be persisted
    by backend servers.

    :param server_type: type of backend server i.e. [account|container|object]
    :returns: prefix string for server type's user metadata headers
    """
    return 'x-%s-%s-' % (server_type.lower(), 'meta')


def get_sys_meta_prefix(server_type):
    """
    Returns the prefix for system metadata headers for given server type.

    This prefix defines the namespace for headers that will be persisted
    by backend servers.

    :param server_type: type of backend server i.e. [account|container|object]
    :returns: prefix string for server type's system metadata headers
    """
    return 'x-%s-%s-' % (server_type.lower(), 'sysmeta')


def get_object_transient_sysmeta(key):
    """
    Returns the Object Transient System Metadata header for key.
    The Object Transient System Metadata namespace will be persisted by
    backend object servers. These headers are treated in the same way as
    object user metadata i.e. all headers in this namespace will be
    replaced on every POST request.

    :param key: metadata key
    :returns: the entire object transient system metadata header for key
    """
    return '%s%s' % (OBJECT_TRANSIENT_SYSMETA_PREFIX, key)


def get_container_update_override_key(key):
    """
    Returns the full X-Object-Sysmeta-Container-Update-Override-* header key.

    :param key: the key you want to override in the container update
    :returns: the full header key
    """
    header = '%s%s' % (OBJECT_SYSMETA_CONTAINER_UPDATE_OVERRIDE_PREFIX, key)
    return header.title()


def get_reserved_name(*parts):
    """
    Generate a valid reserved name that joins the component parts.

    :returns: a string
    """
    if any(RESERVED in p for p in parts):
        raise ValueError('Invalid reserved part in components')
    return RESERVED + RESERVED.join(parts)


def split_reserved_name(name):
    """
    Separate a valid reserved name into the component parts.

    :returns: a list of strings
    """
    if not name.startswith(RESERVED):
        raise ValueError('Invalid reserved name')
    return name.split(RESERVED)[1:]


def remove_items(headers, condition):
    """
    Removes items from a dict whose keys satisfy
    the given condition.

    :param headers: a dict of headers
    :param condition: a function that will be passed the header key as a
                      single argument and should return True if the header
                      is to be removed.
    :returns: a dict, possibly empty, of headers that have been removed
    """
    removed = {}
    keys = [key for key in headers if condition(key)]
    removed.update((key, headers.pop(key)) for key in keys)
    return removed


def copy_header_subset(from_r, to_r, condition):
    """
    Will copy desired subset of headers from from_r to to_r.

    :param from_r: a swob Request or Response
    :param to_r: a swob Request or Response
    :param condition: a function that will be passed the header key as a
                      single argument and should return True if the header
                      is to be copied.
    """
    for k, v in from_r.headers.items():
        if condition(k):
            to_r.headers[k] = v


def check_path_header(req, name, length, error_msg):
    """
    Validate that the value of path-like header is
    well formatted. We assume the caller ensures that
    specific header is present in req.headers.

    :param req: HTTP request object
    :param name: header name
    :param length: length of path segment check
    :param error_msg: error message for client
    :returns: A tuple with path parts according to length
    :raise: HTTPPreconditionFailed if header value
            is not well formatted.
    """
    hdr = wsgi_unquote(req.headers.get(name))
    if not hdr.startswith('/'):
        hdr = '/' + hdr
    try:
        return split_path(hdr, length, length, True)
    except ValueError:
        raise HTTPPreconditionFailed(
            request=req,
            body=error_msg)


class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a large object.

    :param req: original request object
    :param app: WSGI application from which segments will come

    :param listing_iter: iterable yielding the object segments to fetch,
        along with the byte sub-ranges to fetch. Each yielded item should be a
        dict with the following keys: ``path`` or ``raw_data``,
        ``first-byte``, ``last-byte``, ``hash`` (optional), ``bytes``
        (optional).

        If ``hash`` is None, no MD5 verification will be done.

        If ``bytes`` is None, no length verification will be done.

        If ``first-byte`` and ``last-byte`` are None, then the entire object
        will be fetched.

    :param max_get_time: maximum permitted duration of a GET request (seconds)
    :param logger: logger object
    :param swift_source: value of swift.source in subrequest environ
                         (just for logging)
    :param ua_suffix: string to append to user-agent.
    :param name: name of manifest (used in logging only)
    :param response_body_length: optional response body length for
                                 the response being sent to the client.
    """

    def __init__(self, req, app, listing_iter, max_get_time,
                 logger, ua_suffix, swift_source,
                 name='<not specified>', response_body_length=None):
        self.req = req
        self.app = app
        self.listing_iter = listing_iter
        self.max_get_time = max_get_time
        self.logger = logger
        self.ua_suffix = " " + ua_suffix
        self.swift_source = swift_source
        self.name = name
        self.response_body_length = response_body_length
        self.peeked_chunk = None
        self.app_iter = self._internal_iter()
        self.validated_first_segment = False
        self.current_resp = None

    def _coalesce_requests(self):
        pending_req = pending_etag = pending_size = None
        try:
            for seg_dict in self.listing_iter:
                if 'raw_data' in seg_dict:
                    if pending_req:
                        yield pending_req, pending_etag, pending_size

                    to_yield = seg_dict['raw_data'][
                        seg_dict['first_byte']:seg_dict['last_byte'] + 1]
                    yield to_yield, None, len(seg_dict['raw_data'])
                    pending_req = pending_etag = pending_size = None
                    continue

                seg_path, seg_etag, seg_size, first_byte, last_byte = (
                    seg_dict['path'], seg_dict.get('hash'),
                    seg_dict.get('bytes'),
                    seg_dict['first_byte'], seg_dict['last_byte'])
                if seg_size is not None:
                    seg_size = int(seg_size)
                first_byte = first_byte or 0
                go_to_end = last_byte is None or (
                    seg_size is not None and last_byte == seg_size - 1)
                # The "multipart-manifest=get" query param ensures that the
                # segment is a plain old object, not some flavor of large
                # object; therefore, its etag is its MD5sum and hence we can
                # check it.
                path = quote(seg_path) + '?multipart-manifest=get'
                seg_req = make_subrequest(
                    self.req.environ, path=path, method='GET',
                    headers={h: self.req.headers.get(h)
                             for h in ('x-auth-token', 'x-open-expired')},
                    agent=('%(orig)s ' + self.ua_suffix),
                    swift_source=self.swift_source)

                seg_req_rangeval = None
                if first_byte != 0 or not go_to_end:
                    seg_req_rangeval = "%s-%s" % (
                        first_byte, '' if go_to_end else last_byte)
                    seg_req.headers['Range'] = "bytes=" + seg_req_rangeval

                # We can only coalesce if paths match and we know the segment
                # size (so we can check that the ranges will be allowed)
                if pending_req and pending_req.path == seg_req.path and \
                        seg_size is not None:

                    # Make a new Range object so that we don't goof up the
                    # existing one in case of invalid ranges. Note that a
                    # range set with too many individual byteranges is
                    # invalid, so we can combine N valid byteranges and 1
                    # valid byterange and get an invalid range set.
                    if pending_req.range:
                        new_range_str = str(pending_req.range)
                    else:
                        new_range_str = "bytes=0-%d" % (seg_size - 1)

                    if seg_req.range:
                        new_range_str += "," + seg_req_rangeval
                    else:
                        new_range_str += ",0-%d" % (seg_size - 1)

                    if Range(new_range_str).ranges_for_length(seg_size):
                        # Good news! We can coalesce the requests
                        pending_req.headers['Range'] = new_range_str
                        continue
                    # else, Too many ranges, or too much backtracking, or ...

                if pending_req:
                    yield pending_req, pending_etag, pending_size
                pending_req = seg_req
                pending_etag = seg_etag
                pending_size = seg_size

        except ListingIterError as e:
            if pending_req:
                yield pending_req, pending_etag, pending_size
            raise e

        if pending_req:
            yield pending_req, pending_etag, pending_size

    def _requests_to_bytes_iter(self):
        # Take the requests out of self._coalesce_requests, actually make
        # the requests, and generate the bytes from the responses.
        #
        # Yields 2-tuples (segment-name, byte-chunk). The segment name is
        # used for logging.
        for data_or_req, seg_etag, seg_size in self._coalesce_requests():
            if isinstance(data_or_req, bytes):  # ugly, awful overloading
                yield ('data segment', data_or_req)
                continue
            seg_req = data_or_req
            seg_resp = seg_req.get_response(self.app)
            if not is_success(seg_resp.status_int):
                # Error body should be short
                body = seg_resp.body.decode('utf8')
                msg = 'While processing manifest %s, got %d (%s) ' \
                    'while retrieving %s' % (
                        self.name, seg_resp.status_int,
                        body if len(body) <= 60 else body[:57] + '...',
                        seg_req.path)
                if is_server_error(seg_resp.status_int):
                    self.logger.error(msg)
                    raise HTTPServiceUnavailable(
                        request=seg_req, content_type='text/plain')
                raise SegmentError(msg)
            elif ((seg_etag and (seg_resp.etag != seg_etag)) or
                    (seg_size and (seg_resp.content_length != seg_size) and
                     not seg_req.range)):
                # The content-length check is for security reasons. Seems
                # possible that an attacker could upload a >1mb object and
                # then replace it with a much smaller object with same
                # etag. Then create a big nested SLO that calls that
                # object many times which would hammer our obj servers. If
                # this is a range request, don't check content-length
                # because it won't match.
                close_if_possible(seg_resp.app_iter)
                raise SegmentError(
                    'Object segment no longer valid: '
                    '%(path)s etag: %(r_etag)s != %(s_etag)s or '
                    '%(r_size)s != %(s_size)s.' %
                    {'path': seg_req.path, 'r_etag': seg_resp.etag,
                     'r_size': seg_resp.content_length,
                     's_etag': seg_etag,
                     's_size': seg_size})
            else:
                self.current_resp = seg_resp

            resp_len = 0
            seg_hash = None
            if seg_resp.etag and not seg_req.headers.get('Range'):
                # Only calculate the MD5 if it we can use it to validate
                seg_hash = md5(usedforsecurity=False)

            document_iters = maybe_multipart_byteranges_to_document_iters(
                seg_resp.app_iter,
                seg_resp.headers['Content-Type'])

            for chunk in itertools.chain.from_iterable(document_iters):
                if seg_hash:
                    seg_hash.update(chunk)
                    resp_len += len(chunk)
                yield (seg_req.path, chunk)
            close_if_possible(seg_resp.app_iter)

            if seg_hash:
                if resp_len != seg_resp.content_length:
                    raise SegmentError(
                        "Bad response length for %(seg)s as part of %(name)s: "
                        "headers had %(from_headers)s, but response length "
                        "was actually %(actual)s" %
                        {'seg': seg_req.path,
                         'from_headers': seg_resp.content_length,
                         'name': self.name, 'actual': resp_len})
                if seg_hash.hexdigest() != seg_resp.etag:
                    raise SegmentError(
                        "Bad MD5 checksum for %(seg)s as part of %(name)s: "
                        "headers had %(etag)s, but object MD5 was actually "
                        "%(actual)s" %
                        {'seg': seg_req.path, 'etag': seg_resp.etag,
                         'name': self.name, 'actual': seg_hash.hexdigest()})

    def _byte_counting_iter(self):
        # Checks that we give the client the right number of bytes. Raises
        # SegmentError if the number of bytes is wrong.
        bytes_left = self.response_body_length

        for seg_name, chunk in self._requests_to_bytes_iter():
            if bytes_left is None:
                yield chunk
            elif bytes_left >= len(chunk):
                yield chunk
                bytes_left -= len(chunk)
            else:
                yield chunk[:bytes_left]
                bytes_left -= len(chunk)
                raise SegmentError(
                    'Too many bytes for %(name)s; truncating in '
                    '%(seg)s with %(left)d bytes left' %
                    {'name': self.name, 'seg': seg_name,
                     'left': -bytes_left})

        if bytes_left:
            raise SegmentError('Expected another %d bytes for %s; '
                               'closing connection' % (bytes_left, self.name))

    def _time_limited_iter(self):
        # Makes sure a GET response doesn't take more than self.max_get_time
        # seconds to process. Raises an exception if things take too long.
        start_time = time.time()
        for chunk in self._byte_counting_iter():
            now = time.time()
            yield chunk
            if now - start_time > self.max_get_time:
                raise SegmentError(
                    'While processing manifest %s, '
                    'max LO GET time of %ds exceeded' %
                    (self.name, self.max_get_time))

    def _internal_iter(self):
        # Top level of our iterator stack: pass bytes through; catch and
        # handle exceptions.
        try:
            for chunk in self._time_limited_iter():
                yield chunk
        except (ListingIterError, SegmentError) as err:
            self.logger.error(err)
            if not self.validated_first_segment:
                raise
        finally:
            if self.current_resp:
                close_if_possible(self.current_resp.app_iter)

    def app_iter_range(self, *a, **kw):
        """
        swob.Response will only respond with a 206 status in certain cases; one
        of those is if the body iterator responds to .app_iter_range().

        However, this object (or really, its listing iter) is smart enough to
        handle the range stuff internally, so we just no-op this out for swob.
        """
        return self

    def app_iter_ranges(self, ranges, content_type, boundary, content_size):
        """
        This method assumes that iter(self) yields all the data bytes that
        go into the response, but none of the MIME stuff. For example, if
        the response will contain three MIME docs with data "abcd", "efgh",
        and "ijkl", then iter(self) will give out the bytes "abcdefghijkl".

        This method inserts the MIME stuff around the data bytes.
        """
        si = Spliterator(self)
        mri = multi_range_iterator(
            ranges, content_type, boundary, content_size,
            lambda start, end_plus_one: si.take(end_plus_one - start))
        try:
            for x in mri:
                yield x
        finally:
            # Spliterator and multi_range_iterator can't possibly know we've
            # consumed the whole of the app_iter, but we want to read/close the
            # final segment response
            friendly_close(self.app_iter)

    def validate_first_segment(self):
        """
        Start fetching object data to ensure that the first segment (if any) is
        valid. This is to catch cases like "first segment is missing" or
        "first segment's etag doesn't match manifest".

        Note: this does not validate that you have any segments. A
        zero-segment large object is not erroneous; it is just empty.
        """
        if self.validated_first_segment:
            return

        try:
            self.peeked_chunk = next(self.app_iter)
        except StopIteration:
            pass
        finally:
            self.validated_first_segment = True

    def __iter__(self):
        if self.peeked_chunk is not None:
            pc = self.peeked_chunk
            self.peeked_chunk = None
            return CloseableChain([pc], self.app_iter)
        else:
            return self.app_iter

    def close(self):
        """
        Called when the client disconnect. Ensure that the connection to the
        backend server is closed.
        """
        close_if_possible(self.app_iter)


def http_response_to_document_iters(response, read_chunk_size=4096):
    """
    Takes a successful object-GET HTTP response and turns it into an
    iterator of (first-byte, last-byte, length, headers, body-file)
    5-tuples.

    The response must either be a 200 or a 206; if you feed in a 204 or
    something similar, this probably won't work.

    :param response: HTTP response, like from bufferedhttp.http_connect(),
        not a swob.Response.
    """
    chunked = is_chunked(dict(response.getheaders()))

    if response.status == 200:
        if chunked:
            # Single "range" that's the whole object with an unknown length
            return iter([(0, None, None, response.getheaders(),
                          response)])

        # Single "range" that's the whole object
        content_length = int(response.getheader('Content-Length'))
        return iter([(0, content_length - 1, content_length,
                      response.getheaders(), response)])

    content_type, params_list = parse_content_type(
        response.getheader('Content-Type'))
    if content_type != 'multipart/byteranges':
        # Single range; no MIME framing, just the bytes. The start and end
        # byte indices are in the Content-Range header.
        start, end, length = parse_content_range(
            response.getheader('Content-Range'))
        return iter([(start, end, length, response.getheaders(), response)])
    else:
        # Multiple ranges; the response body is a multipart/byteranges MIME
        # document, and we have to parse it using the MIME boundary
        # extracted from the Content-Type header.
        params = dict(params_list)
        return multipart_byteranges_to_document_iters(
            response, wsgi_to_bytes(params['boundary']), read_chunk_size)


def update_etag_is_at_header(req, name):
    """
    Helper function to update an X-Backend-Etag-Is-At header whose value is a
    list of alternative header names at which the actual object etag may be
    found. This informs the object server where to look for the actual object
    etag when processing conditional requests.

    Since the proxy server and/or middleware may set alternative etag header
    names, the value of X-Backend-Etag-Is-At is a comma separated list which
    the object server inspects in order until it finds an etag value.

    :param req: a swob Request
    :param name: name of a sysmeta where alternative etag may be found
    """
    if ',' in name:
        # HTTP header names should not have commas but we'll check anyway
        raise ValueError('Header name must not contain commas')
    existing = req.headers.get("X-Backend-Etag-Is-At")
    req.headers["X-Backend-Etag-Is-At"] = csv_append(
        existing, name)


def resolve_etag_is_at_header(req, metadata):
    """
    Helper function to resolve an alternative etag value that may be stored in
    metadata under an alternate name.

    The value of the request's X-Backend-Etag-Is-At header (if it exists) is a
    comma separated list of alternate names in the metadata at which an
    alternate etag value may be found. This list is processed in order until an
    alternate etag is found.

    The left most value in X-Backend-Etag-Is-At will have been set by the left
    most middleware, or if no middleware, by ECObjectController, if an EC
    policy is in use. The left most middleware is assumed to be the authority
    on what the etag value of the object content is.

    The resolver will work from left to right in the list until it finds a
    value that is a name in the given metadata. So the left most wins, IF it
    exists in the metadata.

    By way of example, assume the encrypter middleware is installed. If an
    object is *not* encrypted then the resolver will not find the encrypter
    middleware's alternate etag sysmeta (X-Object-Sysmeta-Crypto-Etag) but will
    then find the EC alternate etag (if EC policy). But if the object *is*
    encrypted then X-Object-Sysmeta-Crypto-Etag is found and used, which is
    correct because it should be preferred over X-Object-Sysmeta-Ec-Etag.

    :param req: a swob Request
    :param metadata: a dict containing object metadata
    :return: an alternate etag value if any is found, otherwise None
    """
    alternate_etag = None
    metadata = HeaderKeyDict(metadata)
    if "X-Backend-Etag-Is-At" in req.headers:
        names = list_from_csv(req.headers["X-Backend-Etag-Is-At"])
        for name in names:
            if name in metadata:
                alternate_etag = metadata[name]
                break
    return alternate_etag


def update_ignore_range_header(req, name):
    """
    Helper function to update an X-Backend-Ignore-Range-If-Metadata-Present
    header whose value is a list of header names which, if any are present
    on an object, mean the object server should respond with a 200 instead
    of a 206 or 416.

    :param req: a swob Request
    :param name: name of a header which, if found, indicates the proxy will
                 want the whole object
    """
    if ',' in name:
        # HTTP header names should not have commas but we'll check anyway
        raise ValueError('Header name must not contain commas')
    hdr = 'X-Backend-Ignore-Range-If-Metadata-Present'
    req.headers[hdr] = csv_append(req.headers.get(hdr), name)


def resolve_ignore_range_header(req, metadata):
    """
    Helper function to remove Range header from request if metadata matching
    the X-Backend-Ignore-Range-If-Metadata-Present header is found.

    :param req: a swob Request
    :param metadata: dictionary of object metadata
    """
    ignore_range_headers = set(
        h.strip().lower()
        for h in req.headers.get(
            'X-Backend-Ignore-Range-If-Metadata-Present',
            '').split(','))
    if ignore_range_headers.intersection(
            h.lower() for h in metadata):
        req.headers.pop('Range', None)


def is_use_replication_network(headers=None):
    """
    Determine if replication network should be used.

    :param headers: a dict of headers
    :return: the value of the ``x-backend-use-replication-network`` item from
        ``headers``. If no ``headers`` are given or the item is not found then
        False is returned.
    """
    if headers:
        for h, v in headers.items():
            if h.lower() == USE_REPLICATION_NETWORK_HEADER:
                return config_true_value(v)
    return False


def get_ip_port(node, headers):
    """
    Get the ip address and port that should be used for the given ``node``.
    The normal ip address and port are returned unless the ``node`` or
    ``headers`` indicate that the replication ip address and port should be
    used.

    If the ``headers`` dict has an item with key
    ``x-backend-use-replication-network`` and a truthy value then the
    replication ip address and port are returned. Otherwise if the ``node``
    dict has an item with key ``use_replication`` and truthy value then the
    replication ip address and port are returned. Otherwise the normal ip
    address and port are returned.

    :param node: a dict describing a node
    :param headers: a dict of headers
    :return: a tuple of (ip address, port)
    """
    return select_ip_port(
        node, use_replication=is_use_replication_network(headers))


def is_open_expired(app, req):
    """
    Helper function to check if a request with the header 'x-open-expired'
    can access an object that has not yet been reaped by the object-expirer
    based on the allow_open_expired global config.

    :param app: the application instance
    :param req: request object
    """
    return (config_true_value(app.allow_open_expired) and
            config_true_value(req.headers.get('x-open-expired')))


def is_backend_open_expired(request):
    """
    Helper function to check if a request has either the headers
    'x-backend-open-expired' or 'x-backend-replication' for the backend
    to access expired objects.

    :param request: request object
    """
    x_backend_open_expired = config_true_value(request.headers.get(
        'x-backend-open-expired', 'false'))
    x_backend_replication = config_true_value(request.headers.get(
        'x-backend-replication', 'false'))
    return x_backend_open_expired or x_backend_replication


def append_log_info(environ, log_info):
    environ.setdefault('swift.log_info', []).append(log_info)


def get_log_info(environ):
    return ','.join(environ.get('swift.log_info', []))
