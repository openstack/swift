# Copyright (c) 2018 OpenStack Foundation
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

r"""
Middleware that will provide Static Large Object (SLO) support.

This feature is very similar to Dynamic Large Object (DLO) support in that
it allows the user to upload many objects concurrently and afterwards
download them as a single object. It is different in that it does not rely
on eventually consistent container listings to do so. Instead, a user
defined manifest of the object segments is used.

----------------------
Uploading the Manifest
----------------------

After the user has uploaded the objects to be concatenated, a manifest is
uploaded. The request must be a ``PUT`` with the query parameter::

    ?multipart-manifest=put

The body of this request will be an ordered list of segment descriptions in
JSON format. The data to be supplied for each segment is either:

=========== ========================================================
Key         Description
=========== ========================================================
path        the path to the segment object (not including account)
            /container/object_name
etag        (optional) the ETag given back when the segment object
            was PUT
size_bytes  (optional) the size of the complete segment object in
            bytes
range       (optional) the (inclusive) range within the object to
            use as a segment. If omitted, the entire object is used
=========== ========================================================

Or:

=========== ========================================================
Key         Description
=========== ========================================================
data        base64-encoded data to be returned
=========== ========================================================

.. note::
    At least one object-backed segment must be included. If you'd like
    to create a manifest consisting purely of data segments, consider
    uploading a normal object instead.

The format of the list will be::

    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 10485760,
      "range": "1048576-2097151"},
     {"data": base64.b64encode("interstitial data")},
     {"path": "/cont/another-object", ...},
     ...]

The number of object-backed segments is limited to ``max_manifest_segments``
(configurable in proxy-server.conf, default 1000). Each segment must be at
least 1 byte. On upload, the middleware will head every object-backed segment
passed in to verify:

1. the segment exists (i.e. the ``HEAD`` was successful);
2. the segment meets minimum size requirements;
3. if the user provided a non-null ``etag``, the etag matches;
4. if the user provided a non-null ``size_bytes``, the size_bytes matches; and
5. if the user provided a ``range``, it is a singular, syntactically correct
   range that is satisfiable given the size of the object referenced.

For inlined data segments, the middleware verifies each is valid, non-empty
base64-encoded binary data. Note that data segments *do not* count against
``max_manifest_segments``.

Note that the ``etag`` and ``size_bytes`` keys are optional; if omitted, the
verification is not performed. If any of the objects fail to verify (not
found, size/etag mismatch, below minimum size, invalid range) then the user
will receive a 4xx error response. If everything does match, the user will
receive a 2xx response and the SLO object is ready for downloading.

Note that large manifests may take a long time to verify; historically,
clients would need to use a long read timeout for the connection to give
Swift enough time to send a final ``201 Created`` or ``400 Bad Request``
response. Now, clients should use the query parameters::

    ?multipart-manifest=put&heartbeat=on

to request that Swift send an immediate ``202 Accepted`` response and periodic
whitespace to keep the connection alive. A final response code will appear in
the body. The format of the response body defaults to text/plain but can be
either json or xml depending on the ``Accept`` header. An example body is as
follows::

    Response Status: 201 Created
    Response Body:
    Etag: "8f481cede6d2ddc07cb36aa084d9a64d"
    Last Modified: Wed, 25 Oct 2017 17:08:55 GMT
    Errors:

Or, as a json response::

    {"Response Status": "201 Created",
     "Response Body": "",
     "Etag": "\"8f481cede6d2ddc07cb36aa084d9a64d\"",
     "Last Modified": "Wed, 25 Oct 2017 17:08:55 GMT",
     "Errors": []}

Behind the scenes, on success, a JSON manifest generated from the user input is
sent to object servers with an extra ``X-Static-Large-Object: True`` header
and a modified ``Content-Type``. The items in this manifest will include the
``etag`` and ``size_bytes`` for each segment, regardless of whether the client
specified them for verification. The parameter ``swift_bytes=$total_size`` will
be appended to the existing ``Content-Type``, where ``$total_size`` is the sum
of all the included segments' ``size_bytes``. This extra parameter will be
hidden from the user.

Manifest files can reference objects in separate containers, which will improve
concurrent upload speed. Objects can be referenced by multiple manifests. The
segments of a SLO manifest can even be other SLO manifests. Treat them as any
other object i.e., use the ``Etag`` and ``Content-Length`` given on the ``PUT``
of the sub-SLO in the manifest to the parent SLO.

While uploading a manifest, a user can send ``Etag`` for verification. It needs
to be md5 of the segments' etags, if there is no range specified. For example,
if the manifest to be uploaded looks like this::

    [{"path": "/cont/object1",
      "etag": "etagoftheobjectsegment1",
      "size_bytes": 10485760},
     {"path": "/cont/object2",
      "etag": "etagoftheobjectsegment2",
      "size_bytes": 10485760}]

The Etag of the above manifest would be md5 of ``etagoftheobjectsegment1`` and
``etagoftheobjectsegment2``. This could be computed in the following way::

    echo -n 'etagoftheobjectsegment1etagoftheobjectsegment2' | md5sum

If a manifest to be uploaded with a segment range looks like this::

    [{"path": "/cont/object1",
      "etag": "etagoftheobjectsegmentone",
      "size_bytes": 10485760,
      "range": "1-2"},
     {"path": "/cont/object2",
      "etag": "etagoftheobjectsegmenttwo",
      "size_bytes": 10485760,
      "range": "3-4"}]

While computing the Etag of the above manifest, internally each segment's etag
will be taken in the form of ``etagvalue:rangevalue;``. Hence the Etag of the
above manifest would be::

    echo -n 'etagoftheobjectsegmentone:1-2;etagoftheobjectsegmenttwo:3-4;' \
    | md5sum

For the purposes of Etag computations, inlined data segments are considered to
have an etag of the md5 of the raw data (i.e., *not* base64-encoded).


-------------------
Range Specification
-------------------

Users now have the ability to specify ranges for SLO segments.
Users can include an optional ``range`` field in segment descriptions
to specify which bytes from the underlying object should be used for the
segment data. Only one range may be specified per segment.

.. note::

    The ``etag`` and ``size_bytes`` fields still describe the backing object
    as a whole.

If a user uploads this manifest::

    [{"path": "/con/obj_seg_1", "size_bytes": 2097152, "range": "0-1048576"},
     {"path": "/con/obj_seg_2", "size_bytes": 2097152,
      "range": "512-1550000"},
     {"path": "/con/obj_seg_1", "size_bytes": 2097152, "range": "-2048"}]

The segment will consist of the first 1048576 bytes of /con/obj_seg_1,
followed by bytes 513 through 1550000 (inclusive) of /con/obj_seg_2, and
finally bytes 2095104 through 2097152 (i.e., the last 2048 bytes) of
/con/obj_seg_1.

.. note::

    The minimum sized range is 1 byte. This is the same as the minimum
    segment size.


-------------------------
Inline Data Specification
-------------------------

When uploading a manifest, users can include 'data' segments that should
be included along with objects. The data in these segments must be
base64-encoded binary data and will be included in the etag of the
resulting large object exactly as if that data had been uploaded and
referenced as separate objects.

.. note::

    This feature is primarily aimed at reducing the need for storing
    many tiny objects, and as such any supplied data must fit within
    the maximum manifest size (default is 8MiB). This maximum size
    can be configured via ``max_manifest_size`` in proxy-server.conf.


-------------------------
Retrieving a Large Object
-------------------------

A ``GET`` request to the manifest object will return the concatenation of the
objects from the manifest much like DLO. If any of the segments from the
manifest are not found or their ``Etag``/``Content-Length`` have changed since
upload, the connection will drop. In this case a ``409 Conflict`` will be
logged in the proxy logs and the user will receive incomplete results. Note
that this will be enforced regardless of whether the user performed per-segment
validation during upload.

The headers from this ``GET`` or ``HEAD`` request will return the metadata
attached to the manifest object itself with some exceptions:

===================== ==================================================
Header                Value
===================== ==================================================
Content-Length        the total size of the SLO (the sum of the sizes of
                      the segments in the manifest)
X-Static-Large-Object the string "True"
Etag                  the etag of the SLO (generated the same way as DLO)
===================== ==================================================

A ``GET`` request with the query parameter::

    ?multipart-manifest=get

will return a transformed version of the original manifest, containing
additional fields and different key names. For example, the first manifest in
the example above would look like this::

    [{"name": "/cont/object",
      "hash": "etagoftheobjectsegment",
      "bytes": 10485760,
      "range": "1048576-2097151"}, ...]

As you can see, some of the fields are renamed compared to the put request:
*path* is *name*, *etag* is *hash*, *size_bytes* is *bytes*.  The *range* field
remains the same (if present).

A GET request with the query parameters::

    ?multipart-manifest=get&format=raw

will return the contents of the original manifest as it was sent by the client.
The main purpose for both calls is solely debugging.

When the manifest object is uploaded you are more or less guaranteed that
every segment in the manifest exists and matched the specifications.
However, there is nothing that prevents the user from breaking the
SLO download by deleting/replacing a segment referenced in the manifest. It is
left to the user to use caution in handling the segments.

-----------------------
Deleting a Large Object
-----------------------

A ``DELETE`` request will just delete the manifest object itself. The segment
data referenced by the manifest will remain unchanged.

A ``DELETE`` with a query parameter::

    ?multipart-manifest=delete

will delete all the segments referenced in the manifest and then the manifest
itself. The failure response will be similar to the bulk delete middleware.

------------------------
Modifying a Large Object
------------------------

``PUT`` and ``POST`` requests will work as expected; ``PUT``\s will just
overwrite the manifest object for example.

------------------
Container Listings
------------------

In a container listing the size listed for SLO manifest objects will be the
``total_size`` of the concatenated segments in the manifest. The overall
``X-Container-Bytes-Used`` for the container (and subsequently for the account)
will not reflect ``total_size`` of the manifest but the actual size of the JSON
data stored. The reason for this somewhat confusing discrepancy is we want the
container listing to reflect the size of the manifest object when it is
downloaded. We do not, however, want to count the bytes-used twice (for both
the manifest and the segments it's referring to) in the container and account
metadata which can be used for stats and billing purposes.
"""

import base64
from cgi import parse_header
from collections import defaultdict
from datetime import datetime
import json
import mimetypes
import re
import six
import time
from hashlib import md5
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.middleware.listing_formats import \
    MAX_CONTAINER_LISTING_CONTENT_LENGTH
from swift.common.swob import Request, HTTPBadRequest, HTTPServerError, \
    HTTPMethodNotAllowed, HTTPRequestEntityTooLarge, HTTPLengthRequired, \
    HTTPOk, HTTPPreconditionFailed, HTTPException, HTTPNotFound, \
    HTTPUnauthorized, HTTPConflict, HTTPUnprocessableEntity, Response, Range, \
    RESPONSE_REASONS
from swift.common.utils import get_logger, config_true_value, \
    get_valid_utf8_str, override_bytes_from_content_type, split_path, \
    register_swift_info, RateLimitedIterator, quote, close_if_possible, \
    closing_if_possible, LRUCache, StreamingPile, strict_b64decode
from swift.common.request_helpers import SegmentedIterable, \
    get_sys_meta_prefix, update_etag_is_at_header, resolve_etag_is_at_header
from swift.common.constraints import check_utf8, MAX_BUFFERED_SLO_SEGMENTS
from swift.common.http import HTTP_NOT_FOUND, HTTP_UNAUTHORIZED, is_success
from swift.common.wsgi import WSGIContext, make_subrequest
from swift.common.middleware.bulk import get_response_body, \
    ACCEPTABLE_FORMATS, Bulk


DEFAULT_RATE_LIMIT_UNDER_SIZE = 1024 ** 2  # 1 MiB
DEFAULT_MAX_MANIFEST_SEGMENTS = 1000
DEFAULT_MAX_MANIFEST_SIZE = 8 * (1024 ** 2)  # 8 MiB
DEFAULT_YIELD_FREQUENCY = 10


SLO_KEYS = {
    # required: optional
    'data': set(),
    'path': {'range', 'etag', 'size_bytes'},
}

SYSMETA_SLO_ETAG = get_sys_meta_prefix('object') + 'slo-etag'
SYSMETA_SLO_SIZE = get_sys_meta_prefix('object') + 'slo-size'


def parse_and_validate_input(req_body, req_path):
    """
    Given a request body, parses it and returns a list of dictionaries.

    The output structure is nearly the same as the input structure, but it
    is not an exact copy. Given a valid object-backed input dictionary
    ``d_in``, its corresponding output dictionary ``d_out`` will be as follows:

    * d_out['etag'] == d_in['etag']

    * d_out['path'] == d_in['path']

    * d_in['size_bytes'] can be a string ("12") or an integer (12), but
      d_out['size_bytes'] is an integer.

    * (optional) d_in['range'] is a string of the form "M-N", "M-", or
      "-N", where M and N are non-negative integers. d_out['range'] is the
      corresponding swob.Range object. If d_in does not have a key
      'range', neither will d_out.

    Inlined data dictionaries will have any extraneous padding stripped.

    :raises: HTTPException on parse errors or semantic errors (e.g. bogus
             JSON structure, syntactically invalid ranges)

    :returns: a list of dictionaries on success
    """
    try:
        parsed_data = json.loads(req_body)
    except ValueError:
        raise HTTPBadRequest("Manifest must be valid JSON.\n")

    if not isinstance(parsed_data, list):
        raise HTTPBadRequest("Manifest must be a list.\n")

    # If we got here, req_path refers to an object, so this won't ever raise
    # ValueError.
    vrs, account, _junk = split_path(req_path, 3, 3, True)

    errors = []
    for seg_index, seg_dict in enumerate(parsed_data):
        if not isinstance(seg_dict, dict):
            errors.append(b"Index %d: not a JSON object" % seg_index)
            continue

        for required in SLO_KEYS:
            if required in seg_dict:
                segment_type = required
                break
        else:
            errors.append(
                b"Index %d: expected keys to include one of %s"
                % (seg_index,
                   b" or ".join(repr(required) for required in SLO_KEYS)))
            continue

        allowed_keys = SLO_KEYS[segment_type].union([segment_type])
        extraneous_keys = [k for k in seg_dict if k not in allowed_keys]
        if extraneous_keys:
            errors.append(
                b"Index %d: extraneous keys %s"
                % (seg_index,
                   b", ".join(json.dumps(ek).encode('ascii')
                              for ek in sorted(extraneous_keys))))
            continue

        if segment_type == 'path':
            if not isinstance(seg_dict['path'], six.string_types):
                errors.append(b"Index %d: \"path\" must be a string" %
                              seg_index)
                continue
            if not (seg_dict.get('etag') is None or
                    isinstance(seg_dict['etag'], six.string_types)):
                errors.append(b'Index %d: "etag" must be a string or null '
                              b'(if provided)' % seg_index)
                continue

            if '/' not in seg_dict['path'].strip('/'):
                errors.append(
                    b"Index %d: path does not refer to an object. Path must "
                    b"be of the form /container/object." % seg_index)
                continue

            seg_size = seg_dict.get('size_bytes')
            if seg_size is not None:
                try:
                    seg_size = int(seg_size)
                    seg_dict['size_bytes'] = seg_size
                except (TypeError, ValueError):
                    errors.append(b"Index %d: invalid size_bytes" % seg_index)
                    continue
                if seg_size < 1 and seg_index != (len(parsed_data) - 1):
                    errors.append(b"Index %d: too small; each segment must be "
                                  b"at least 1 byte."
                                  % (seg_index,))
                    continue

            obj_path = '/'.join(['', vrs, account,
                                 quote(seg_dict['path'].lstrip('/'))])
            if req_path == obj_path:
                errors.append(
                    b"Index %d: manifest must not include itself as a segment"
                    % seg_index)
                continue

            if seg_dict.get('range'):
                try:
                    seg_dict['range'] = Range('bytes=%s' % seg_dict['range'])
                except ValueError:
                    errors.append(b"Index %d: invalid range" % seg_index)
                    continue

                if len(seg_dict['range'].ranges) > 1:
                    errors.append(b"Index %d: multiple ranges "
                                  b"(only one allowed)" % seg_index)
                    continue

                # If the user *told* us the object's size, we can check range
                # satisfiability right now. If they lied about the size, we'll
                # fail that validation later.
                if (seg_size is not None and 1 != len(
                        seg_dict['range'].ranges_for_length(seg_size))):
                    errors.append(b"Index %d: unsatisfiable range" % seg_index)
                    continue

        elif segment_type == 'data':
            # Validate that the supplied data is non-empty and base64-encoded
            try:
                data = strict_b64decode(seg_dict['data'])
            except ValueError:
                errors.append(
                    b"Index %d: data must be valid base64" % seg_index)
                continue
            if len(data) < 1:
                errors.append(b"Index %d: too small; each segment must be "
                              b"at least 1 byte."
                              % (seg_index,))
                continue
            # re-encode to normalize padding
            seg_dict['data'] = base64.b64encode(data)

    if parsed_data and all('data' in d for d in parsed_data):
        errors.append(b"Inline data segments require at least one "
                      b"object-backed segment.")

    if errors:
        error_message = b"".join(e + b"\n" for e in errors)
        raise HTTPBadRequest(error_message,
                             headers={"Content-Type": "text/plain"})

    return parsed_data


class SloGetContext(WSGIContext):

    max_slo_recursion_depth = 10

    def __init__(self, slo):
        self.slo = slo
        super(SloGetContext, self).__init__(slo.app)

    def _fetch_sub_slo_segments(self, req, version, acc, con, obj):
        """
        Fetch the submanifest, parse it, and return it.
        Raise exception on failures.
        """
        sub_req = make_subrequest(
            req.environ, path=quote('/'.join(['', version, acc, con, obj])),
            method='GET',
            headers={'x-auth-token': req.headers.get('x-auth-token')},
            agent='%(orig)s SLO MultipartGET', swift_source='SLO')
        sub_resp = sub_req.get_response(self.slo.app)

        if not sub_resp.is_success:
            close_if_possible(sub_resp.app_iter)
            raise ListingIterError(
                'while fetching %s, GET of submanifest %s '
                'failed with status %d' % (req.path, sub_req.path,
                                           sub_resp.status_int))

        try:
            with closing_if_possible(sub_resp.app_iter):
                return json.loads(''.join(sub_resp.app_iter))
        except ValueError as err:
            raise ListingIterError(
                'while fetching %s, JSON-decoding of submanifest %s '
                'failed with %s' % (req.path, sub_req.path, err))

    def _segment_path(self, version, account, seg_dict):
        return "/{ver}/{acc}/{conobj}".format(
            ver=version, acc=account,
            conobj=seg_dict['name'].lstrip('/')
        )

    def _segment_length(self, seg_dict):
        """
        Returns the number of bytes that will be fetched from the specified
        segment on a plain GET request for this SLO manifest.
        """
        if 'raw_data' in seg_dict:
            return len(seg_dict['raw_data'])

        seg_range = seg_dict.get('range')
        if seg_range is not None:
            # The range is of the form N-M, where N and M are both positive
            # decimal integers. We know this because this middleware is the
            # only thing that creates the SLO manifests stored in the
            # cluster.
            range_start, range_end = [int(x) for x in seg_range.split('-')]
            return (range_end - range_start) + 1
        else:
            return int(seg_dict['bytes'])

    def _segment_listing_iterator(self, req, version, account, segments,
                                  byteranges):
        for seg_dict in segments:
            if config_true_value(seg_dict.get('sub_slo')):
                override_bytes_from_content_type(seg_dict,
                                                 logger=self.slo.logger)

        # We handle the range stuff here so that we can be smart about
        # skipping unused submanifests. For example, if our first segment is a
        # submanifest referencing 50 MiB total, but start_byte falls in
        # the 51st MiB, then we can avoid fetching the first submanifest.
        #
        # If we were to make SegmentedIterable handle all the range
        # calculations, we would be unable to make this optimization.
        total_length = sum(self._segment_length(seg) for seg in segments)
        if not byteranges:
            byteranges = [(0, total_length - 1)]

        # Cache segments from sub-SLOs in case more than one byterange
        # includes data from a particular sub-SLO. We only cache a few sets
        # of segments so that a malicious user cannot build a giant SLO tree
        # and then GET it to run the proxy out of memory.
        #
        # LRUCache is a little awkward to use this way, but it beats doing
        # things manually.
        #
        # 20 is sort of an arbitrary choice; it's twice our max recursion
        # depth, so we know this won't expand memory requirements by too
        # much.
        cached_fetch_sub_slo_segments = \
            LRUCache(maxsize=20)(self._fetch_sub_slo_segments)

        for first_byte, last_byte in byteranges:
            byterange_listing_iter = self._byterange_listing_iterator(
                req, version, account, segments, first_byte, last_byte,
                cached_fetch_sub_slo_segments)
            for seg_info in byterange_listing_iter:
                yield seg_info

    def _byterange_listing_iterator(self, req, version, account, segments,
                                    first_byte, last_byte,
                                    cached_fetch_sub_slo_segments,
                                    recursion_depth=1):
        last_sub_path = None
        for seg_dict in segments:
            if 'data' in seg_dict:
                seg_dict['raw_data'] = strict_b64decode(seg_dict.pop('data'))

            seg_length = self._segment_length(seg_dict)
            if first_byte >= seg_length:
                # don't need any bytes from this segment
                first_byte -= seg_length
                last_byte -= seg_length
                continue

            if last_byte < 0:
                # no bytes are needed from this or any future segment
                return

            if 'raw_data' in seg_dict:
                yield dict(seg_dict,
                           first_byte=max(0, first_byte),
                           last_byte=min(seg_length - 1, last_byte))
                first_byte -= seg_length
                last_byte -= seg_length
                continue

            seg_range = seg_dict.get('range')
            if seg_range is None:
                range_start, range_end = 0, seg_length - 1
            else:
                # This simple parsing of the range is valid because we already
                # validated and supplied concrete values for the range
                # during SLO manifest creation
                range_start, range_end = map(int, seg_range.split('-'))

            if config_true_value(seg_dict.get('sub_slo')):
                # Do this check here so that we can avoid fetching this last
                # manifest before raising the exception
                if recursion_depth >= self.max_slo_recursion_depth:
                    raise ListingIterError(
                        "While processing manifest %r, "
                        "max recursion depth was exceeded" % req.path)

                sub_path = get_valid_utf8_str(seg_dict['name'])
                sub_cont, sub_obj = split_path(sub_path, 2, 2, True)
                if last_sub_path != sub_path:
                    sub_segments = cached_fetch_sub_slo_segments(
                        req, version, account, sub_cont, sub_obj)
                last_sub_path = sub_path

                # Use the existing machinery to slice into the sub-SLO.
                for sub_seg_dict in self._byterange_listing_iterator(
                        req, version, account, sub_segments,
                        # This adjusts first_byte and last_byte to be
                        # relative to the sub-SLO.
                        range_start + max(0, first_byte),
                        min(range_end, range_start + last_byte),

                        cached_fetch_sub_slo_segments,
                        recursion_depth=recursion_depth + 1):
                    yield sub_seg_dict
            else:
                if isinstance(seg_dict['name'], six.text_type):
                    seg_dict['name'] = seg_dict['name'].encode("utf-8")
                yield dict(seg_dict,
                           first_byte=max(0, first_byte) + range_start,
                           last_byte=min(range_end, range_start + last_byte))

            first_byte -= seg_length
            last_byte -= seg_length

    def _need_to_refetch_manifest(self, req):
        """
        Just because a response shows that an object is a SLO manifest does not
        mean that response's body contains the entire SLO manifest. If it
        doesn't, we need to make a second request to actually get the whole
        thing.

        Note: this assumes that X-Static-Large-Object has already been found.
        """
        if req.method == 'HEAD':
            # We've already looked for SYSMETA_SLO_ETAG/SIZE in the response
            # and didn't find them. We have to fetch the whole manifest and
            # recompute.
            return True

        response_status = int(self._response_status[:3])

        # These are based on etag, and the SLO's etag is almost certainly not
        # the manifest object's etag. Still, it's highly likely that the
        # submitted If-None-Match won't match the manifest object's etag, so
        # we can avoid re-fetching the manifest if we got a successful
        # response.
        if ((req.if_match or req.if_none_match) and
                not is_success(response_status)):
            return True

        if req.range and response_status in (206, 416):
            content_range = ''
            for header, value in self._response_headers:
                if header.lower() == 'content-range':
                    content_range = value
                    break
            # e.g. Content-Range: bytes 0-14289/14290
            match = re.match('bytes (\d+)-(\d+)/(\d+)$', content_range)
            if not match:
                # Malformed or missing, so we don't know what we got.
                return True
            first_byte, last_byte, length = [int(x) for x in match.groups()]
            # If and only if we actually got back the full manifest body, then
            # we can avoid re-fetching the object.
            got_everything = (first_byte == 0 and last_byte == length - 1)
            return not got_everything

        return False

    def handle_slo_get_or_head(self, req, start_response):
        """
        Takes a request and a start_response callable and does the normal WSGI
        thing with them. Returns an iterator suitable for sending up the WSGI
        chain.

        :param req: :class:`~swift.common.swob.Request` object; is a ``GET`` or
                    ``HEAD`` request aimed at what may (or may not) be a static
                    large object manifest.
        :param start_response: WSGI start_response callable
        """
        if req.params.get('multipart-manifest') != 'get':
            # If this object is an SLO manifest, we may have saved off the
            # large object etag during the original PUT. Send an
            # X-Backend-Etag-Is-At header so that, if the SLO etag *was*
            # saved, we can trust the object-server to respond appropriately
            # to If-Match/If-None-Match requests.
            update_etag_is_at_header(req, SYSMETA_SLO_ETAG)
        resp_iter = self._app_call(req.environ)

        # make sure this response is for a static large object manifest
        slo_marker = slo_etag = slo_size = None
        for header, value in self._response_headers:
            header = header.lower()
            if header == SYSMETA_SLO_ETAG:
                slo_etag = value
            elif header == SYSMETA_SLO_SIZE:
                slo_size = value
            elif (header == 'x-static-large-object' and
                  config_true_value(value)):
                slo_marker = value

            if slo_marker and slo_etag and slo_size:
                break

        if not slo_marker:
            # Not a static large object manifest. Just pass it through.
            start_response(self._response_status,
                           self._response_headers,
                           self._response_exc_info)
            return resp_iter

        # Handle pass-through request for the manifest itself
        if req.params.get('multipart-manifest') == 'get':
            if req.params.get('format') == 'raw':
                resp_iter = self.convert_segment_listing(
                    self._response_headers, resp_iter)
            else:
                new_headers = []
                for header, value in self._response_headers:
                    if header.lower() == 'content-type':
                        new_headers.append(('Content-Type',
                                            'application/json; charset=utf-8'))
                    else:
                        new_headers.append((header, value))
                self._response_headers = new_headers
            start_response(self._response_status,
                           self._response_headers,
                           self._response_exc_info)
            return resp_iter

        is_conditional = self._response_status.startswith(('304', '412')) and (
            req.if_match or req.if_none_match)
        if slo_etag and slo_size and (
                req.method == 'HEAD' or is_conditional):
            # Since we have length and etag, we can respond immediately
            resp = Response(
                status=self._response_status,
                headers=self._response_headers,
                app_iter=resp_iter,
                request=req,
                conditional_etag=resolve_etag_is_at_header(
                    req, self._response_headers),
                conditional_response=True)
            resp.headers.update({
                'Etag': '"%s"' % slo_etag,
                'Content-Length': slo_size,
            })
            return resp(req.environ, start_response)

        if self._need_to_refetch_manifest(req):
            req.environ['swift.non_client_disconnect'] = True
            close_if_possible(resp_iter)
            del req.environ['swift.non_client_disconnect']

            get_req = make_subrequest(
                req.environ, method='GET',
                headers={'x-auth-token': req.headers.get('x-auth-token')},
                agent='%(orig)s SLO MultipartGET', swift_source='SLO')
            resp_iter = self._app_call(get_req.environ)

        # Any Content-Range from a manifest is almost certainly wrong for the
        # full large object.
        resp_headers = [(h, v) for h, v in self._response_headers
                        if not h.lower() == 'content-range']

        response = self.get_or_head_response(
            req, resp_headers, resp_iter)
        return response(req.environ, start_response)

    def convert_segment_listing(self, resp_headers, resp_iter):
        """
        Converts the manifest data to match with the format
        that was put in through ?multipart-manifest=put

        :param resp_headers: response headers
        :param resp_iter: a response iterable
        """
        segments = self._get_manifest_read(resp_iter)

        for seg_dict in segments:
            if 'data' in seg_dict:
                continue
            seg_dict.pop('content_type', None)
            seg_dict.pop('last_modified', None)
            seg_dict.pop('sub_slo', None)
            seg_dict['path'] = seg_dict.pop('name', None)
            seg_dict['size_bytes'] = seg_dict.pop('bytes', None)
            seg_dict['etag'] = seg_dict.pop('hash', None)

        json_data = json.dumps(segments)  # convert to string
        if six.PY3:
            json_data = json_data.encode('utf-8')

        new_headers = []
        for header, value in resp_headers:
            if header.lower() == 'content-length':
                new_headers.append(('Content-Length', len(json_data)))
            else:
                new_headers.append((header, value))
        self._response_headers = new_headers

        return [json_data]

    def _get_manifest_read(self, resp_iter):
        with closing_if_possible(resp_iter):
            resp_body = ''.join(resp_iter)
        try:
            segments = json.loads(resp_body)
        except ValueError:
            segments = []

        return segments

    def get_or_head_response(self, req, resp_headers, resp_iter):
        segments = self._get_manifest_read(resp_iter)
        slo_etag = None
        content_length = None
        response_headers = []
        for header, value in resp_headers:
            lheader = header.lower()
            if lheader not in ('etag', 'content-length'):
                response_headers.append((header, value))

            if lheader == SYSMETA_SLO_ETAG:
                slo_etag = value
            elif lheader == SYSMETA_SLO_SIZE:
                # it's from sysmeta, so we don't worry about non-integer
                # values here
                content_length = int(value)

        # Prep to calculate content_length & etag if necessary
        if slo_etag is None:
            calculated_etag = md5()
        if content_length is None:
            calculated_content_length = 0

        for seg_dict in segments:
            # Decode any inlined data; it's important that we do this *before*
            # calculating the segment length and etag
            if 'data' in seg_dict:
                seg_dict['raw_data'] = base64.b64decode(seg_dict.pop('data'))

            if slo_etag is None:
                if 'raw_data' in seg_dict:
                    calculated_etag.update(
                        md5(seg_dict['raw_data']).hexdigest())
                elif seg_dict.get('range'):
                    calculated_etag.update(
                        '%s:%s;' % (seg_dict['hash'], seg_dict['range']))
                else:
                    calculated_etag.update(seg_dict['hash'])

            if content_length is None:
                if config_true_value(seg_dict.get('sub_slo')):
                    override_bytes_from_content_type(
                        seg_dict, logger=self.slo.logger)
                calculated_content_length += self._segment_length(seg_dict)

        if slo_etag is None:
            slo_etag = calculated_etag.hexdigest()
        if content_length is None:
            content_length = calculated_content_length

        response_headers.append(('Content-Length', str(content_length)))
        response_headers.append(('Etag', '"%s"' % slo_etag))

        if req.method == 'HEAD':
            return self._manifest_head_response(req, response_headers)
        else:
            return self._manifest_get_response(
                req, content_length, response_headers, segments)

    def _manifest_head_response(self, req, response_headers):
        conditional_etag = resolve_etag_is_at_header(req, response_headers)
        return HTTPOk(request=req, headers=response_headers, body='',
                      conditional_etag=conditional_etag,
                      conditional_response=True)

    def _manifest_get_response(self, req, content_length, response_headers,
                               segments):
        if req.range:
            byteranges = [
                # For some reason, swob.Range.ranges_for_length adds 1 to the
                # last byte's position.
                (start, end - 1) for start, end
                in req.range.ranges_for_length(content_length)]
        else:
            byteranges = []

        ver, account, _junk = req.split_path(3, 3, rest_with_last=True)
        plain_listing_iter = self._segment_listing_iterator(
            req, ver, account, segments, byteranges)

        def ratelimit_predicate(seg_dict):
            if 'raw_data' in seg_dict:
                return False  # it's already in memory anyway
            start = seg_dict.get('start_byte') or 0
            end = seg_dict.get('end_byte')
            if end is None:
                end = int(seg_dict['bytes']) - 1
            is_small = (end - start + 1) < self.slo.rate_limit_under_size
            return is_small

        ratelimited_listing_iter = RateLimitedIterator(
            plain_listing_iter,
            self.slo.rate_limit_segments_per_sec,
            limit_after=self.slo.rate_limit_after_segment,
            ratelimit_if=ratelimit_predicate)

        # data segments are already in the correct format, but object-backed
        # segments need a path key added
        segment_listing_iter = (
            seg_dict if 'raw_data' in seg_dict else
            dict(seg_dict, path=self._segment_path(ver, account, seg_dict))
            for seg_dict in ratelimited_listing_iter)

        segmented_iter = SegmentedIterable(
            req, self.slo.app, segment_listing_iter,
            name=req.path, logger=self.slo.logger,
            ua_suffix="SLO MultipartGET",
            swift_source="SLO",
            max_get_time=self.slo.max_get_time)

        try:
            segmented_iter.validate_first_segment()
        except (ListingIterError, SegmentError):
            # Copy from the SLO explanation in top of this file.
            # If any of the segments from the manifest are not found or
            # their Etag/Content Length no longer match the connection
            # will drop. In this case a 409 Conflict will be logged in
            # the proxy logs and the user will receive incomplete results.
            return HTTPConflict(request=req)

        conditional_etag = resolve_etag_is_at_header(req, response_headers)
        response = Response(request=req, content_length=content_length,
                            headers=response_headers,
                            conditional_response=True,
                            conditional_etag=conditional_etag,
                            app_iter=segmented_iter)
        return response


class StaticLargeObject(object):
    """
    StaticLargeObject Middleware

    See above for a full description.

    The proxy logs created for any subrequests made will have swift.source set
    to "SLO".

    :param app: The next WSGI filter or app in the paste.deploy chain.
    :param conf: The configuration dict for the middleware.
    :param max_manifest_segments: The maximum number of segments allowed in
                                  newly-created static large objects.
    :param max_manifest_size: The maximum size (in bytes) of newly-created
                              static-large-object manifests.
    :param yield_frequency: If the client included ``heartbeat=on`` in the
                            query parameters when creating a new static large
                            object, the period of time to wait between sending
                            whitespace to keep the connection alive.
    """

    def __init__(self, app, conf,
                 max_manifest_segments=DEFAULT_MAX_MANIFEST_SEGMENTS,
                 max_manifest_size=DEFAULT_MAX_MANIFEST_SIZE,
                 yield_frequency=DEFAULT_YIELD_FREQUENCY):
        self.conf = conf
        self.app = app
        self.logger = get_logger(conf, log_route='slo')
        self.max_manifest_segments = max_manifest_segments
        self.max_manifest_size = max_manifest_size
        self.yield_frequency = yield_frequency
        self.max_get_time = int(self.conf.get('max_get_time', 86400))
        self.rate_limit_under_size = int(self.conf.get(
            'rate_limit_under_size', DEFAULT_RATE_LIMIT_UNDER_SIZE))
        self.rate_limit_after_segment = int(self.conf.get(
            'rate_limit_after_segment', '10'))
        self.rate_limit_segments_per_sec = int(self.conf.get(
            'rate_limit_segments_per_sec', '1'))
        self.concurrency = min(1000, max(0, int(self.conf.get(
            'concurrency', '2'))))
        delete_concurrency = int(self.conf.get(
            'delete_concurrency', self.concurrency))
        self.bulk_deleter = Bulk(
            app, {}, delete_concurrency=delete_concurrency, logger=self.logger)

    def handle_multipart_get_or_head(self, req, start_response):
        """
        Handles the GET or HEAD of a SLO manifest.

        The response body (only on GET, of course) will consist of the
        concatenation of the segments.

        :param req: a :class:`~swift.common.swob.Request` with a path
                    referencing an object
        :param start_response: WSGI start_response callable
        :raises HttpException: on errors
        """
        return SloGetContext(self).handle_slo_get_or_head(req, start_response)

    def handle_multipart_put(self, req, start_response):
        """
        Will handle the PUT of a SLO manifest.
        Heads every object in manifest to check if is valid and if so will
        save a manifest generated from the user input. Uses WSGIContext to
        call self and start_response and returns a WSGI iterator.

        :param req: a :class:`~swift.common.swob.Request` with an obj in path
        :param start_response: WSGI start_response callable
        :raises HttpException: on errors
        """
        vrs, account, container, obj = req.split_path(4, rest_with_last=True)
        if req.content_length > self.max_manifest_size:
            raise HTTPRequestEntityTooLarge(
                "Manifest File > %d bytes" % self.max_manifest_size)
        if req.headers.get('X-Copy-From'):
            raise HTTPMethodNotAllowed(
                'Multipart Manifest PUTs cannot be COPY requests')
        if req.content_length is None and \
                req.headers.get('transfer-encoding', '').lower() != 'chunked':
            raise HTTPLengthRequired(request=req)
        parsed_data = parse_and_validate_input(
            req.body_file.read(self.max_manifest_size),
            req.path)
        problem_segments = []

        object_segments = [seg for seg in parsed_data if 'path' in seg]
        if len(object_segments) > self.max_manifest_segments:
            raise HTTPRequestEntityTooLarge(
                'Number of object-backed segments must be <= %d' %
                self.max_manifest_segments)
        try:
            out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
        except ValueError:
            out_content_type = 'text/plain'  # Ignore invalid header
        if not out_content_type:
            out_content_type = 'text/plain'
        data_for_storage = [None] * len(parsed_data)
        total_size = 0
        path2indices = defaultdict(list)
        for index, seg_dict in enumerate(parsed_data):
            if 'data' in seg_dict:
                data_for_storage[index] = seg_dict
                total_size += len(base64.b64decode(seg_dict['data']))
            else:
                path2indices[seg_dict['path']].append(index)

        def do_head(obj_name):
            obj_path = quote('/'.join([
                '', vrs, account, get_valid_utf8_str(obj_name).lstrip('/')]))

            sub_req = make_subrequest(
                req.environ, path=obj_path + '?',  # kill the query string
                method='HEAD',
                headers={'x-auth-token': req.headers.get('x-auth-token')},
                agent='%(orig)s SLO MultipartPUT', swift_source='SLO')
            return obj_name, sub_req.get_response(self)

        def validate_seg_dict(seg_dict, head_seg_resp, allow_empty_segment):
            obj_name = seg_dict['path']
            if not head_seg_resp.is_success:
                problem_segments.append([quote(obj_name),
                                         head_seg_resp.status])
                return 0, None

            segment_length = head_seg_resp.content_length
            if seg_dict.get('range'):
                # Since we now know the length, we can normalize the
                # range. We know that there is exactly one range
                # requested since we checked that earlier in
                # parse_and_validate_input().
                ranges = seg_dict['range'].ranges_for_length(
                    head_seg_resp.content_length)

                if not ranges:
                    problem_segments.append([quote(obj_name),
                                             'Unsatisfiable Range'])
                elif ranges == [(0, head_seg_resp.content_length)]:
                    # Just one range, and it exactly matches the object.
                    # Why'd we do this again?
                    del seg_dict['range']
                    segment_length = head_seg_resp.content_length
                else:
                    rng = ranges[0]
                    seg_dict['range'] = '%d-%d' % (rng[0], rng[1] - 1)
                    segment_length = rng[1] - rng[0]

            if segment_length < 1 and not allow_empty_segment:
                problem_segments.append(
                    [quote(obj_name),
                     'Too small; each segment must be at least 1 byte.'])

            _size_bytes = seg_dict.get('size_bytes')
            size_mismatch = (
                _size_bytes is not None and
                _size_bytes != head_seg_resp.content_length
            )
            if size_mismatch:
                problem_segments.append([quote(obj_name), 'Size Mismatch'])

            _etag = seg_dict.get('etag')
            etag_mismatch = (
                _etag is not None and
                _etag != head_seg_resp.etag
            )
            if etag_mismatch:
                problem_segments.append([quote(obj_name), 'Etag Mismatch'])

            if head_seg_resp.last_modified:
                last_modified = head_seg_resp.last_modified
            else:
                # shouldn't happen
                last_modified = datetime.now()

            last_modified_formatted = last_modified.strftime(
                '%Y-%m-%dT%H:%M:%S.%f'
            )
            seg_data = {
                'name': '/' + seg_dict['path'].lstrip('/'),
                'bytes': head_seg_resp.content_length,
                'hash': head_seg_resp.etag,
                'content_type': head_seg_resp.content_type,
                'last_modified': last_modified_formatted
            }
            if seg_dict.get('range'):
                seg_data['range'] = seg_dict['range']
            if config_true_value(
                    head_seg_resp.headers.get('X-Static-Large-Object')):
                seg_data['sub_slo'] = True

            return segment_length, seg_data

        heartbeat = config_true_value(req.params.get('heartbeat'))
        separator = ''
        if heartbeat:
            # Apparently some ways of deploying require that this to happens
            # *before* the return? Not sure why.
            req.environ['eventlet.minimum_write_chunk_size'] = 0
            start_response('202 Accepted', [  # NB: not 201 !
                ('Content-Type', out_content_type),
            ])
            separator = '\r\n\r\n'

        def resp_iter(total_size=total_size):
            # wsgi won't propagate start_response calls until some data has
            # been yielded so make sure first heartbeat is sent immediately
            if heartbeat:
                yield ' '
            last_yield_time = time.time()
            with StreamingPile(self.concurrency) as pile:
                for obj_name, resp in pile.asyncstarmap(do_head, (
                        (path, ) for path in path2indices)):
                    now = time.time()
                    if heartbeat and (now - last_yield_time >
                                      self.yield_frequency):
                        # Make sure we've called start_response before
                        # sending data
                        yield ' '
                        last_yield_time = now
                    for i in path2indices[obj_name]:
                        segment_length, seg_data = validate_seg_dict(
                            parsed_data[i], resp,
                            allow_empty_segment=(i == len(parsed_data) - 1))
                        data_for_storage[i] = seg_data
                        total_size += segment_length

            # Middleware left of SLO can add a callback to the WSGI
            # environment to perform additional validation and/or
            # manipulation on the manifest that will be written.
            hook = req.environ.get('swift.callback.slo_manifest_hook')
            if hook:
                more_problems = hook(data_for_storage)
                if more_problems:
                    problem_segments.extend(more_problems)

            if problem_segments:
                err = HTTPBadRequest(content_type=out_content_type)
                resp_dict = {}
                if heartbeat:
                    resp_dict['Response Status'] = err.status
                    resp_dict['Response Body'] = err.body or '\n'.join(
                        RESPONSE_REASONS.get(err.status_int, ['']))
                else:
                    start_response(err.status,
                                   [(h, v) for h, v in err.headers.items()
                                    if h.lower() != 'content-length'])
                yield separator + get_response_body(
                    out_content_type, resp_dict, problem_segments, 'upload')
                return

            slo_etag = md5()
            for seg_data in data_for_storage:
                if 'data' in seg_data:
                    raw_data = base64.b64decode(seg_data['data'])
                    slo_etag.update(md5(raw_data).hexdigest())
                elif seg_data.get('range'):
                    slo_etag.update('%s:%s;' % (seg_data['hash'],
                                                seg_data['range']))
                else:
                    slo_etag.update(seg_data['hash'])

            slo_etag = slo_etag.hexdigest()
            client_etag = req.headers.get('Etag')
            if client_etag and client_etag.strip('"') != slo_etag:
                err = HTTPUnprocessableEntity(request=req)
                if heartbeat:
                    yield separator + get_response_body(out_content_type, {
                        'Response Status': err.status,
                        'Response Body': err.body or '\n'.join(
                            RESPONSE_REASONS.get(err.status_int, [''])),
                    }, problem_segments, 'upload')
                else:
                    for chunk in err(req.environ, start_response):
                        yield chunk
                return

            json_data = json.dumps(data_for_storage)
            if six.PY3:
                json_data = json_data.encode('utf-8')
            req.body = json_data
            req.headers.update({
                SYSMETA_SLO_ETAG: slo_etag,
                SYSMETA_SLO_SIZE: total_size,
                'X-Static-Large-Object': 'True',
                'Etag': md5(json_data).hexdigest(),
            })

            # Ensure container listings have both etags. However, if any
            # middleware to the left of us touched the base value, trust them.
            override_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
            val, sep, params = req.headers.get(
                override_header, '').partition(';')
            req.headers[override_header] = '%s; slo_etag=%s' % (
                (val or req.headers['Etag']) + sep + params, slo_etag)

            env = req.environ
            if not env.get('CONTENT_TYPE'):
                guessed_type, _junk = mimetypes.guess_type(req.path_info)
                env['CONTENT_TYPE'] = (guessed_type or
                                       'application/octet-stream')
            env['swift.content_type_overridden'] = True
            env['CONTENT_TYPE'] += ";swift_bytes=%d" % total_size

            resp = req.get_response(self.app)
            resp_dict = {'Response Status': resp.status}
            if resp.is_success:
                resp.etag = slo_etag
                resp_dict['Etag'] = resp.headers['Etag']
                resp_dict['Last Modified'] = resp.headers['Last-Modified']

            if heartbeat:
                resp_dict['Response Body'] = resp.body
                yield separator + get_response_body(
                    out_content_type, resp_dict, [], 'upload')
            else:
                for chunk in resp(req.environ, start_response):
                    yield chunk

        return resp_iter()

    def get_segments_to_delete_iter(self, req):
        """
        A generator function to be used to delete all the segments and
        sub-segments referenced in a manifest.

        :param req: a :class:`~swift.common.swob.Request` with an SLO manifest
                    in path
        :raises HTTPPreconditionFailed: on invalid UTF8 in request path
        :raises HTTPBadRequest: on too many buffered sub segments and
                                on invalid SLO manifest path
        """
        if not check_utf8(req.path_info):
            raise HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
        vrs, account, container, obj = req.split_path(4, 4, True)

        segments = [{
            'sub_slo': True,
            'name': ('/%s/%s' % (container, obj)).decode('utf-8')}]
        while segments:
            if len(segments) > MAX_BUFFERED_SLO_SEGMENTS:
                raise HTTPBadRequest(
                    'Too many buffered slo segments to delete.')
            seg_data = segments.pop(0)
            if 'data' in seg_data:
                continue
            if seg_data.get('sub_slo'):
                try:
                    segments.extend(
                        self.get_slo_segments(seg_data['name'], req))
                except HTTPException as err:
                    # allow bulk delete response to report errors
                    seg_data['error'] = {'code': err.status_int,
                                         'message': err.body}

                # add manifest back to be deleted after segments
                seg_data['sub_slo'] = False
                segments.append(seg_data)
            else:
                seg_data['name'] = seg_data['name'].encode('utf-8')
                yield seg_data

    def get_slo_segments(self, obj_name, req):
        """
        Performs a :class:`~swift.common.swob.Request` and returns the SLO
        manifest's segments.

        :param obj_name: the name of the object being deleted,
                         as ``/container/object``
        :param req: the base :class:`~swift.common.swob.Request`
        :raises HTTPServerError: on unable to load obj_name or
                                 on unable to load the SLO manifest data.
        :raises HTTPBadRequest: on not an SLO manifest
        :raises HTTPNotFound: on SLO manifest not found
        :returns: SLO manifest's segments
        """
        vrs, account, _junk = req.split_path(2, 3, True)
        new_env = req.environ.copy()
        new_env['REQUEST_METHOD'] = 'GET'
        del(new_env['wsgi.input'])
        new_env['QUERY_STRING'] = 'multipart-manifest=get'
        new_env['CONTENT_LENGTH'] = 0
        new_env['HTTP_USER_AGENT'] = \
            '%s MultipartDELETE' % new_env.get('HTTP_USER_AGENT')
        new_env['swift.source'] = 'SLO'
        new_env['PATH_INFO'] = (
            '/%s/%s/%s' % (vrs, account, obj_name.lstrip('/').encode('utf-8'))
        )
        resp = Request.blank('', new_env).get_response(self.app)

        if resp.is_success:
            if config_true_value(resp.headers.get('X-Static-Large-Object')):
                try:
                    return json.loads(resp.body)
                except ValueError:
                    raise HTTPServerError('Unable to load SLO manifest')
            else:
                raise HTTPBadRequest('Not an SLO manifest')
        elif resp.status_int == HTTP_NOT_FOUND:
            raise HTTPNotFound('SLO manifest not found')
        elif resp.status_int == HTTP_UNAUTHORIZED:
            raise HTTPUnauthorized('401 Unauthorized')
        else:
            raise HTTPServerError('Unable to load SLO manifest or segment.')

    def handle_multipart_delete(self, req):
        """
        Will delete all the segments in the SLO manifest and then, if
        successful, will delete the manifest file.

        :param req: a :class:`~swift.common.swob.Request` with an obj in path
        :returns: swob.Response whose app_iter set to Bulk.handle_delete_iter
        """
        req.headers['Content-Type'] = None  # Ignore content-type from client
        resp = HTTPOk(request=req)
        try:
            out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
        except ValueError:
            out_content_type = None  # Ignore invalid header
        if out_content_type:
            resp.content_type = out_content_type
        resp.app_iter = self.bulk_deleter.handle_delete_iter(
            req, objs_to_delete=self.get_segments_to_delete_iter(req),
            user_agent='MultipartDELETE', swift_source='SLO',
            out_content_type=out_content_type)
        return resp

    def handle_container_listing(self, req, start_response):
        resp = req.get_response(self.app)
        if not resp.is_success or resp.content_type != 'application/json':
            return resp(req.environ, start_response)
        if resp.content_length is None or \
                resp.content_length > MAX_CONTAINER_LISTING_CONTENT_LENGTH:
            return resp(req.environ, start_response)
        try:
            listing = json.loads(resp.body)
        except ValueError:
            return resp(req.environ, start_response)

        for item in listing:
            if 'subdir' in item:
                continue
            etag, params = parse_header(item['hash'])
            if 'slo_etag' in params:
                item['slo_etag'] = '"%s"' % params.pop('slo_etag')
                item['hash'] = etag + ''.join(
                    '; %s=%s' % kv for kv in params.items())

        resp.body = json.dumps(listing).encode('ascii')
        return resp(req.environ, start_response)

    def __call__(self, env, start_response):
        """
        WSGI entry point
        """
        if env.get('swift.slo_override'):
            return self.app(env, start_response)

        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if not obj:
            if req.method == 'GET':
                return self.handle_container_listing(req, start_response)
            return self.app(env, start_response)

        try:
            if req.method == 'PUT' and \
                    req.params.get('multipart-manifest') == 'put':
                return self.handle_multipart_put(req, start_response)
            if req.method == 'DELETE' and \
                    req.params.get('multipart-manifest') == 'delete':
                return self.handle_multipart_delete(req)(env, start_response)
            if req.method == 'GET' or req.method == 'HEAD':
                return self.handle_multipart_get_or_head(req, start_response)
            if 'X-Static-Large-Object' in req.headers:
                raise HTTPBadRequest(
                    request=req,
                    body='X-Static-Large-Object is a reserved header. '
                    'To create a static large object add query param '
                    'multipart-manifest=put.')
        except HTTPException as err_resp:
            return err_resp(env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    max_manifest_segments = int(conf.get('max_manifest_segments',
                                         DEFAULT_MAX_MANIFEST_SEGMENTS))
    max_manifest_size = int(conf.get('max_manifest_size',
                                     DEFAULT_MAX_MANIFEST_SIZE))
    yield_frequency = int(conf.get('yield_frequency',
                                   DEFAULT_YIELD_FREQUENCY))

    register_swift_info('slo',
                        max_manifest_segments=max_manifest_segments,
                        max_manifest_size=max_manifest_size,
                        yield_frequency=yield_frequency,
                        # this used to be configurable; report it as 1 for
                        # clients that might still care
                        min_segment_size=1)

    def slo_filter(app):
        return StaticLargeObject(
            app, conf,
            max_manifest_segments=max_manifest_segments,
            max_manifest_size=max_manifest_size,
            yield_frequency=yield_frequency)
    return slo_filter
