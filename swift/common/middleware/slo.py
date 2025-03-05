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

A GET request to a manifest object with the query parameter::

    ?part-number=<n>

will return the contents of the ``nth`` segment. Segments are indexed from 1,
so ``n`` must be an integer between 1 and the total number of segments in the
manifest. The response status will be ``206 Partial Content`` and its headers
will include: an ``X-Parts-Count`` header equal to the total number of
segments; a ``Content-Length`` header equal to the length of the specified
segment; a ``Content-Range`` header describing the byte range of the specified
part within the SLO. A HEAD request with a ``part-number`` parameter will also
return a response with status ``206 Partial Content`` and the same headers.

.. note::

    When the manifest object is uploaded you are more or less guaranteed that
    every segment in the manifest exists and matched the specifications.
    However, there is nothing that prevents the user from breaking the SLO
    download by deleting/replacing a segment referenced in the manifest. It is
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

A ``DELETE`` with the query parameters::

    ?multipart-manifest=delete&async=yes

will schedule all the segments referenced in the manifest to be deleted
asynchronously and then delete the manifest itself. Note that segments will
continue to appear in listings and be counted for quotas until they are
cleaned up by the object-expirer. This option is only available when all
segments are in the same container and none of them are nested SLOs.

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
from collections import defaultdict
from datetime import datetime
import json
import mimetypes
import re
import time

from swift.cli.container_deleter import make_delete_jobs
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.middleware.listing_formats import \
    MAX_CONTAINER_LISTING_CONTENT_LENGTH
from swift.common.swob import Request, HTTPBadRequest, HTTPServerError, \
    HTTPMethodNotAllowed, HTTPRequestEntityTooLarge, HTTPLengthRequired, \
    HTTPOk, HTTPPreconditionFailed, HTTPException, HTTPNotFound, \
    HTTPUnauthorized, HTTPConflict, HTTPUnprocessableEntity, \
    HTTPServiceUnavailable, Response, Range, normalize_etag, \
    RESPONSE_REASONS, str_to_wsgi, bytes_to_wsgi, wsgi_to_str, wsgi_quote
from swift.common.utils import get_logger, config_true_value, \
    override_bytes_from_content_type, split_path, \
    RateLimitedIterator, quote, closing_if_possible, \
    LRUCache, StreamingPile, strict_b64decode, Timestamp, friendly_close, \
    md5, parse_header
from swift.common.registry import register_swift_info
from swift.common.request_helpers import SegmentedIterable, \
    get_sys_meta_prefix, update_etag_is_at_header, resolve_etag_is_at_header, \
    get_container_update_override_key, update_ignore_range_header, \
    get_param, get_valid_part_num
from swift.common.constraints import check_utf8
from swift.common.http import HTTP_NOT_FOUND, HTTP_UNAUTHORIZED
from swift.common.wsgi import WSGIContext, make_subrequest, make_env, \
    make_pre_authed_request
from swift.common.middleware.bulk import get_response_body, \
    ACCEPTABLE_FORMATS, Bulk
from swift.obj import expirer
from swift.proxy.controllers.base import get_container_info


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
            if not isinstance(seg_dict['path'], str):
                errors.append(b"Index %d: \"path\" must be a string" %
                              seg_index)
                continue
            if not (seg_dict.get('etag') is None or
                    isinstance(seg_dict['etag'], str)):
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
            seg_dict['data'] = base64.b64encode(data).decode('ascii')

    if parsed_data and all('data' in d for d in parsed_data):
        errors.append(b"Inline data segments require at least one "
                      b"object-backed segment.")

    if errors:
        error_message = b"".join(e + b"\n" for e in errors)
        raise HTTPBadRequest(error_message,
                             headers={"Content-Type": "text/plain"})

    return parsed_data


def _annotate_segments(segments, logger=None):
    """
    Decode any inlined data and update sub_slo segments bytes from content-type
    when available; then annotate segment dicts in segments list with
    'segment_length'.

    N.B. raw_data segments don't have a bytes key and range-segments need to
    calculate their length from their range key but afterwards all segments
    dicts will have 'segment_length' representing the length of the segment.
    """
    for seg_dict in segments:
        if 'data' in seg_dict:
            seg_dict['raw_data'] = base64.b64decode(seg_dict.pop('data'))
            segment_length = len(seg_dict['raw_data'])
        else:
            if config_true_value(seg_dict.get('sub_slo')):
                override_bytes_from_content_type(
                    seg_dict, logger=logger)
            seg_range = seg_dict.get('range')
            if seg_range is not None:
                # The range is of the form N-M, where N and M are both
                # positive decimal integers. We know this because this
                # middleware is the only thing that creates the SLO
                # manifests stored in the cluster.
                range_start, range_end = [
                    int(x) for x in seg_range.split('-')]
                segment_length = (range_end - range_start) + 1
            else:
                segment_length = int(seg_dict['bytes'])
        seg_dict['segment_length'] = segment_length


def calculate_byterange_for_part_num(req, segments, part_num):
    """
    Helper function to calculate the byterange for a part_num response.

    N.B. as a side-effect of calculating the single tuple representing the
    byterange required for a part_num response this function will also mutate
    the request's Range header so that swob knows to return 206.

    :param req: the request object
    :param segments: the list of seg_dicts
    :param part_num: the part number of the object to return

    :returns: a tuple representing the byterange
    """
    start = 0
    for seg in segments[:part_num - 1]:
        start += seg['segment_length']
    last = start + segments[part_num - 1]['segment_length']
    # We need to mutate the request's Range header so that swob knows to
    # handle these partial content requests correctly.
    req.range = "bytes=%d-%d" % (start, last - 1)
    return start, last - 1


def calculate_byteranges(req, segments, resp_attrs, part_num):
    """
    Calculate the byteranges based on the request, segments, and part number.

    N.B. as a side-effect of calculating the single tuple representing the
    byterange required for a part_num response this function will also mutate
    the request's Range header so that swob knows to return 206.

    :param req: the request object
    :param segments: the list of seg_dicts
    :param resp_attrs: the slo response attributes
    :param part_num: the part number of the object to return

    :returns: a list of tuples representing byteranges
    """
    if req.range:
        byteranges = [
            # For some reason, swob.Range.ranges_for_length adds 1 to the
            # last byte's position.
            (start, end - 1) for start, end
            in req.range.ranges_for_length(resp_attrs.slo_size)]
    elif part_num:
        byteranges = [
            calculate_byterange_for_part_num(req, segments, part_num)]
    else:
        byteranges = [(0, resp_attrs.slo_size - 1)]

    return byteranges


class RespAttrs(object):
    """
    Encapsulate properties of a GET or HEAD response that are pertinent to
    handling a potential SLO response.

    Instances of this class are typically constructed using the
    ``from_headers`` method.

    :param is_slo: True if the response appears to be an SLO manifest, False
        otherwise.
    :param timestamp: an instance of :class:`~swift.common.utils.Timestamp`.
    :param manifest_etag: the Etag of the manifest object, or None if
        ``is_slo`` is False.
    :param slo_etag: the Etag of the SLO.
    :param slo_size: the size of the SLO.
    """
    def __init__(self, is_slo, timestamp, manifest_etag, slo_etag, slo_size):
        self.is_slo = bool(is_slo)
        self.timestamp = Timestamp(timestamp or 0)
        # manifest_etag is unambiguous, but json_md5 is even more explicit
        self.json_md5 = manifest_etag or ''
        self.slo_etag = slo_etag or ''
        try:
            # even though it's from sysmeta, we have to worry about empty
            # values - see test_get_invalid_sysmeta_passthrough
            self.slo_size = int(slo_size)
        except (ValueError, TypeError):
            self.slo_size = -1
        self.is_legacy = not self._has_size_and_etag()

    def _has_size_and_etag(self):
        return self.slo_size >= 0 and self.slo_etag

    @classmethod
    def from_headers(cls, response_headers):
        """
        Inspect response headers and extract any resp_attrs we can find.

        :param response_headers: list of tuples from a object response
        :returns: an instance of RespAttrs to represent the response headers
        """
        is_slo = False
        timestamp = None
        found_etag = None
        slo_etag = None
        slo_size = None
        for header, value in response_headers:
            header = header.lower()
            if header == 'x-static-large-object':
                is_slo = config_true_value(value)
            elif header == 'x-backend-timestamp':
                timestamp = value
            elif header == 'etag':
                found_etag = value
            elif header == SYSMETA_SLO_ETAG:
                slo_etag = value
            elif header == SYSMETA_SLO_SIZE:
                slo_size = value
        manifest_etag = found_etag if is_slo else None
        return cls(is_slo, timestamp, manifest_etag, slo_etag, slo_size)

    def update_from_segments(self, segments):
        """
        Always called if SLO has fetched the manifest response body, for
        legacy manifests we'll calculate size/etag values we wouldn't have
        gotten from sys-meta headers.
        """
        # we only have to set size/etag once; it doesn't matter if we got the
        # values from sysmeta headers or segments
        if self._has_size_and_etag():
            return

        calculated_size = 0
        calculated_etag = md5(usedforsecurity=False)

        for seg_dict in segments:
            calculated_size += seg_dict['segment_length']

            if 'raw_data' in seg_dict:
                r = md5(seg_dict['raw_data'],
                        usedforsecurity=False).hexdigest()
            elif seg_dict.get('range'):
                r = '%s:%s;' % (seg_dict['hash'], seg_dict['range'])
            else:
                r = seg_dict['hash']
            calculated_etag.update(r.encode('ascii'))

        self.slo_size = calculated_size
        self.slo_etag = calculated_etag.hexdigest()


class SloGetContext(WSGIContext):

    max_slo_recursion_depth = 10

    def __init__(self, slo):
        self.slo = slo
        super(SloGetContext, self).__init__(slo.app)
        # we'll know more after we look at the response metadata
        self.segment_listing_needed = False

    def _fetch_sub_slo_segments(self, req, version, acc, con, obj):
        """
        Fetch the submanifest, parse it, and return it.
        Raise exception on failures.

        :param req: the upstream request
        :param version: whatever
        :param acc: native
        :param con: native
        :param obj: native
        """
        sub_req = make_subrequest(
            req.environ,
            path=wsgi_quote('/'.join([
                '', str_to_wsgi(version),
                str_to_wsgi(acc), str_to_wsgi(con), str_to_wsgi(obj)])),
            method='GET',
            headers={'x-auth-token': req.headers.get('x-auth-token')},
            agent='%(orig)s SLO MultipartGET', swift_source='SLO')
        params_copy = dict(req.params)
        params_copy.pop('part-number', None)
        sub_req.params = params_copy
        sub_resp = sub_req.get_response(self.slo.app)

        if not sub_resp.is_success:
            # Error message should be short
            body = sub_resp.body.decode('utf-8')
            msg = ('while fetching %s, GET of submanifest %s '
                   'failed with status %d (%s)')
            raise ListingIterError(msg % (
                req.path, sub_req.path, sub_resp.status_int,
                body if len(body) <= 60 else body[:57] + '...'))

        try:
            return self._parse_segments(sub_resp.app_iter)
        except HTTPException as err:
            raise ListingIterError(
                'while fetching %s, JSON-decoding of submanifest %s '
                'failed with %s' % (req.path, sub_req.path, err))

    def _segment_path(self, version, account, seg_dict):
        return "/{ver}/{acc}/{conobj}".format(
            ver=version, acc=account,
            conobj=seg_dict['name'].lstrip('/')
        )

    def _segment_listing_iterator(self, req, version, account, segments,
                                  byteranges):
        # We handle the range stuff here so that we can be smart about
        # skipping unused submanifests. For example, if our first segment is a
        # submanifest referencing 50 MiB total, but start_byte falls in
        # the 51st MiB, then we can avoid fetching the first submanifest.
        #
        # If we were to make SegmentedIterable handle all the range
        # calculations, we would be unable to make this optimization.

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
        """
        Iterable that generates a filtered and annotated stream of segment
        dicts describing the sub-segment ranges that would be used by the
        SegmentedIterable to construct the bytes for a ranged response.

        :param req: original request object
        :param version: version
        :param account: account
        :param segments: segments dictionary
        :param first_byte: offset into the large object for the first byte
          that is returned to the client
        :param last_byte: offset into the large object for the last byte
          that is returned to the client
        :param cached_fetch_sub_slo_segments: LRU cache used for fetching
          sub-segments
        :param recursion_depth: max number of recursive sub_slo calls
        """
        last_sub_path = None
        for seg_dict in segments:
            seg_length = seg_dict['segment_length']
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

                sub_path = seg_dict['name']
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
                yield dict(seg_dict,
                           first_byte=max(0, first_byte) + range_start,
                           last_byte=min(range_end, range_start + last_byte))

            first_byte -= seg_length
            last_byte -= seg_length

    def _is_body_complete(self):
        content_range = ''
        for header, value in self._response_headers:
            if header.lower() == 'content-range':
                content_range = value
                break
        # e.g. Content-Range: bytes 0-14289/14290
        match = re.match(r'bytes (\d+)-(\d+)/(\d+)$', content_range)
        if not match:
            # Malformed or missing, so we don't know what we got.
            return False
        first_byte, last_byte, length = [int(x) for x in match.groups()]
        # If and only if we actually got back the full manifest body, then
        # we can avoid re-fetching the object.
        return first_byte == 0 and last_byte == length - 1

    def _need_to_refetch_manifest(self, req, resp_attrs, is_part_num_request):
        """
        Check if the segments will be needed to service the request and update
        the segment_listing_needed attribute.

        :return: boolean indicating if we need to refetch, only if the segments
                 ARE needed we MAY need to refetch them!
        """
        if req.method == 'HEAD':
            # There may be some cases in the future where a HEAD resp on even a
            # modern manifest should refetch, e.g. lp bug #2029174
            self.segment_listing_needed = (resp_attrs.is_legacy or
                                           is_part_num_request)
            # it will always be the case that a HEAD must re-fetch iff
            # segment_listing_needed
            return self.segment_listing_needed

        last_resp_status_int = self._get_status_int()
        # These are based on etag (or last-modified), but the SLO's etag is
        # almost certainly not the manifest object's etag. Still, it's highly
        # likely that the submitted If-None-Match won't match the manifest
        # object's etag, so we can avoid re-fetching the manifest if we got a
        # successful response.
        if last_resp_status_int in (412, 304):
            # a conditional response from a modern manifest would have an
            # accurate SLO etag, AND comparison with the etag-is-at header, but
            # for legacy manifests responses (who always need to calculate the
            # correct etag, even for if-[un]modified-since errors) we can't say
            # what the etag is or if it matches unless we calculate it from
            # segments - so we always need them
            self.segment_listing_needed = resp_attrs.is_legacy
            # if we need them; we can't get them from the error
            return self.segment_listing_needed

        # This is GET request for an SLO object, if we're going to return a
        # successful response we're going to need the segments, but this
        # resp_iter may not contain the entire SLO manifest.
        self.segment_listing_needed = True

        # modern swift object-servers should ignore Range headers on manifests,
        # but during upgrade if we get a range response we'll probably have to
        # refetch
        if last_resp_status_int == 416:
            # if the range wasn't satisfiable we need to refetch
            return True
        elif last_resp_status_int == 206:
            # a partial response might included the whole content-range?!
            return not self._is_body_complete()
        else:
            # a good number of error responses would have returned earlier for
            # lacking is_slo sys-meta, at this point we've filtered all the
            # other response codes, so this is a prefectly normal 200 response,
            # no need to refetch
            return False

    def _refetch_manifest(self, req, resp_iter, orig_resp_attrs):
        req.environ['swift.non_client_disconnect'] = True
        friendly_close(resp_iter)
        del req.environ['swift.non_client_disconnect']

        headers_subset = ['x-auth-token', 'x-open-expired']
        get_req = make_subrequest(
            req.environ, method='GET',
            headers={k: req.headers.get(k)
                     for k in headers_subset if k in req.headers},
            agent='%(orig)s SLO MultipartGET', swift_source='SLO')
        resp_iter = self._app_call(get_req.environ)
        new_resp_attrs = RespAttrs.from_headers(self._response_headers)
        if new_resp_attrs.timestamp < orig_resp_attrs.timestamp and \
                not new_resp_attrs.is_slo:
            # Our *orig_resp_attrs* saw *newer* data that indicated it was an
            # SLO, but on refetch it's an older object or error; 503 seems
            # reasonable?
            friendly_close(resp_iter)
            raise HTTPServiceUnavailable(request=req)
        # else, the caller will know how to return this response
        return new_resp_attrs, resp_iter

    def _parse_segments(self, resp_iter):
        """
        Read the manifest body and parse segments.

        :returns: segments
        :raises: HTTPServerError
        """
        segments = self._get_manifest_read(resp_iter)
        _annotate_segments(segments, logger=self.slo.logger)
        return segments

    def _return_manifest_response(self, req, start_response, resp_iter,
                                  is_format_raw):
        if is_format_raw:
            json_data = self.convert_segment_listing(resp_iter)
            # we've created a new response body
            resp_iter = [json_data]
            replace_headers = {
                # Note that we have to return the large object's content-type
                # (not application/json) so it's like what the client sent on
                # PUT. Otherwise, server-side copy won't work.
                'Content-Length': len(json_data),
                'Etag': md5(json_data, usedforsecurity=False).hexdigest(),
            }
        else:
            # we're going to return the manifest resp_iter as-is
            replace_headers = {
                'Content-Type': 'application/json; charset=utf-8',
            }
        return self._return_response(req, start_response, resp_iter,
                                     replace_headers)

    def _return_slo_response(self, req, start_response, resp_iter, resp_attrs):
        headers = {
            'Etag': '"%s"' % resp_attrs.slo_etag,
            'X-Manifest-Etag': resp_attrs.json_md5,
            # swob will fix this for a GET with Range
            'Content-Length': str(resp_attrs.slo_size),
            # ignore bogus content-range, make swob figure it out
            'Content-Range': None,
        }
        if self.segment_listing_needed:
            # consume existing resp_iter; we'll create a new one
            segments = self._parse_segments(resp_iter)
            resp_attrs.update_from_segments(segments)
            headers['Etag'] = '"%s"' % resp_attrs.slo_etag
            headers['Content-Length'] = str(resp_attrs.slo_size)
            part_num = get_valid_part_num(req)
            if part_num:
                headers['X-Parts-Count'] = len(segments)

            if part_num and part_num > len(segments):
                if req.method == 'HEAD':
                    resp_iter = []
                    headers['Content-Length'] = '0'
                else:
                    body = b'The requested part number is not satisfiable'
                    resp_iter = [body]
                    headers['Content-Length'] = len(body)
                headers['Content-Range'] = 'bytes */%d' % resp_attrs.slo_size
                self._response_status = '416 Requested Range Not Satisfiable'
            elif part_num and req.method == 'HEAD':
                resp_iter = []
                headers['Content-Length'] = \
                    segments[part_num - 1].get('segment_length')
                start, end = calculate_byterange_for_part_num(
                    req, segments, part_num)
                headers['Content-Range'] = \
                    'bytes {}-{}/{}'.format(start, end,
                                            resp_attrs.slo_size)
                # The RFC specifies 206 in the context of Range requests, and
                # Range headers MUST be ignored for HEADs [1], so a HEAD will
                # not normally return a 206. However, a part-number HEAD
                # returns Content-Length equal to the part size, rather than
                # the whole object size, so in this case we do return 206.
                # [1] https://www.rfc-editor.org/rfc/rfc9110#name-range
                self._response_status = '206 Partial Content'
            elif req.method == 'HEAD':
                resp_iter = []
            else:
                byteranges = calculate_byteranges(
                    req, segments, resp_attrs, part_num)
                resp_iter = self._build_resp_iter(req, segments, byteranges)
        return self._return_response(req, start_response, resp_iter,
                                     replace_headers=headers)

    def _return_response(self, req, start_response, resp_iter,
                         replace_headers):
        if req.method == 'HEAD' or self._get_status_int() in (412, 304):
            # we should drain HEAD and unmet condition responses since they
            # don't have bodies
            friendly_close(resp_iter)
            resp_iter = b''
        resp_headers = HeaderKeyDict(self._response_headers, **replace_headers)
        resp = Response(
            status=self._response_status,
            headers=resp_headers,
            app_iter=resp_iter,
            request=req,
            conditional_response=True,
            conditional_etag=resolve_etag_is_at_header(req, resp_headers))
        return resp(req.environ, start_response)

    def _return_non_slo_response(self, req, start_response, resp_iter):
        # our "pass-through" response may have been from a manifest refetch w/o
        # range/conditional headers that turned out to be a real object, and
        # now we want out.  But if the original client request included Range
        # or Conditional headers we can trust swob to do the right conversion
        # back into a 206/416/304/412 (as long as the response we have is a
        # normal successful response and we respect any forwarding middleware's
        # etag-is-at header that we stripped off for the refetch!)
        resp = Response(
            status=self._response_status,
            headers=self._response_headers,
            app_iter=resp_iter,
            request=req,
            conditional_response=self._get_status_int() == 200,
            conditional_etag=resolve_etag_is_at_header(
                req, self._response_headers)
        )
        return resp(req.environ, start_response)

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
        is_manifest_get = get_param(req, 'multipart-manifest') == 'get'
        is_format_raw = is_manifest_get and get_param(req, 'format') == 'raw'

        if not is_manifest_get:
            # If this object is an SLO manifest, we may have saved off the
            # large object etag during the original PUT. Send an
            # X-Backend-Etag-Is-At header so that, if the SLO etag *was* saved,
            # we can trust the object-server to respond appropriately to
            # If-Match/If-None-Match requests.
            update_etag_is_at_header(req, SYSMETA_SLO_ETAG)
            # Tell the object server that if it's a manifest,
            # we want the whole thing
            update_ignore_range_header(req, 'X-Static-Large-Object')

        # process original request
        orig_path_info = req.path_info
        resp_iter = self._app_call(req.environ)
        resp_attrs = RespAttrs.from_headers(self._response_headers)
        if resp_attrs.is_slo and not is_manifest_get:
            try:
                # only validate part-number if the request is to an SLO
                part_num = get_valid_part_num(req)
            except HTTPException:
                friendly_close(resp_iter)
                raise
            # the next two calls hide a couple side effects, sorry:
            #
            # 1) regardless of the return value the "need_to_refetch" check
            #    *may* also set self.segment_listing_needed = True (it's
            #    commented to help you wrap your head around that one,
            #    good luck)
            # 2) if we refetch, we overwrite the current resp_iter and
            #    resp_attrs variables, partly because we *might* get back a NOT
            #    resp_attrs.is_slo response (even if we had one to start), but
            #    hopefully they're just the manifest resp we needed to refetch!
            if self._need_to_refetch_manifest(req, resp_attrs, part_num):
                # reset path in case it was modified during original request
                # (e.g. object versioning might re-write the path)
                req.path_info = orig_path_info
                resp_attrs, resp_iter = self._refetch_manifest(
                    req, resp_iter, resp_attrs)

        if not resp_attrs.is_slo:
            # even if the original resp_attrs may have been SLO we may have
            # refetched, this also handles the server error case
            return self._return_non_slo_response(
                req, start_response, resp_iter)

        if is_manifest_get:
            # manifest pass through doesn't require resp_attrs
            return self._return_manifest_response(req, start_response,
                                                  resp_iter, is_format_raw)

        # this a GET/HEAD response for the SLO object (not the manifest)
        return self._return_slo_response(req, start_response, resp_iter,
                                         resp_attrs)

    def convert_segment_listing(self, resp_iter):
        """
        Converts the manifest data to match with the format
        that was put in through ?multipart-manifest=put

        :param resp_iter: a response iterable

        :raises HTTPServerError:
        :returns: the json-serialized raw format (as bytes)
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

        json_data = json.dumps(segments, sort_keys=True)  # convert to string
        return json_data.encode('utf-8')

    def _get_manifest_read(self, resp_iter):
        with closing_if_possible(resp_iter):
            resp_body = b''.join(resp_iter)
        try:
            segments = json.loads(resp_body)
        except ValueError as e:
            msg = 'Unable to load SLO manifest'
            self.slo.logger.error('%s: %s', msg, e)
            raise HTTPServerError(msg)
        return segments

    def _build_resp_iter(self, req, segments, byteranges):
        """
        Build a response iterable for a GET request.

        :param req: the request object
        :param segments: the list of seg_dicts
        :param byteranges: a list of tuples representing byteranges

        :returns: a segmented iterable
        """
        ver, account, _junk = req.split_path(3, 3, rest_with_last=True)
        account = wsgi_to_str(account)
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
            raise HTTPConflict(request=req)
        return segmented_iter


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
                 yield_frequency=DEFAULT_YIELD_FREQUENCY,
                 allow_async_delete=True):
        self.conf = conf
        self.app = app
        self.logger = get_logger(conf, log_route='slo')
        self.max_manifest_segments = max_manifest_segments
        self.max_manifest_size = max_manifest_size
        self.yield_frequency = yield_frequency
        self.allow_async_delete = allow_async_delete
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
            app, {},
            max_deletes_per_request=float('inf'),
            delete_concurrency=delete_concurrency,
            logger=self.logger)

        self.expirer_config = expirer.ExpirerConfig(conf, logger=self.logger)

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
        if req.headers.get('X-Copy-From'):
            raise HTTPMethodNotAllowed(
                'Multipart Manifest PUTs cannot be COPY requests')
        if req.content_length is None:
            if req.headers.get('transfer-encoding', '').lower() != 'chunked':
                raise HTTPLengthRequired(request=req)
        else:
            if req.content_length > self.max_manifest_size:
                raise HTTPRequestEntityTooLarge(
                    "Manifest File > %d bytes" % self.max_manifest_size)
        parsed_data = parse_and_validate_input(
            req.body_file.read(self.max_manifest_size),
            wsgi_to_str(req.path))
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
            obj_path = '/'.join(['', vrs, account,
                                 str_to_wsgi(obj_name.lstrip('/'))])
            obj_path = wsgi_quote(obj_path)

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
        separator = b''
        if heartbeat:
            # Apparently some ways of deploying require that this to happens
            # *before* the return? Not sure why.
            req.environ['eventlet.minimum_write_chunk_size'] = 0
            start_response('202 Accepted', [  # NB: not 201 !
                ('Content-Type', out_content_type),
            ])
            separator = b'\r\n\r\n'

        def resp_iter(total_size=total_size):
            # wsgi won't propagate start_response calls until some data has
            # been yielded so make sure first heartbeat is sent immediately
            if heartbeat:
                yield b' '
            last_yield_time = time.time()
            with StreamingPile(self.concurrency) as pile:
                for obj_name, resp in pile.asyncstarmap(do_head, (
                        (path, ) for path in path2indices)):
                    now = time.time()
                    if heartbeat and (now - last_yield_time >
                                      self.yield_frequency):
                        # Make sure we've called start_response before
                        # sending data
                        yield b' '
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
                    err_body = err.body.decode('utf-8')
                    resp_dict['Response Body'] = err_body or '\n'.join(
                        RESPONSE_REASONS.get(err.status_int, ['']))
                else:
                    start_response(err.status,
                                   [(h, v) for h, v in err.headers.items()
                                    if h.lower() != 'content-length'])
                yield separator + get_response_body(
                    out_content_type, resp_dict, problem_segments, 'upload')
                return

            slo_etag = md5(usedforsecurity=False)
            for seg_data in data_for_storage:
                if 'data' in seg_data:
                    raw_data = base64.b64decode(seg_data['data'])
                    r = md5(raw_data, usedforsecurity=False).hexdigest()
                elif seg_data.get('range'):
                    r = '%s:%s;' % (seg_data['hash'], seg_data['range'])
                else:
                    r = seg_data['hash']
                slo_etag.update(r.encode('ascii'))

            slo_etag = slo_etag.hexdigest()
            client_etag = normalize_etag(req.headers.get('Etag'))
            if client_etag and client_etag != slo_etag:
                err = HTTPUnprocessableEntity(request=req)
                if heartbeat:
                    resp_dict = {}
                    resp_dict['Response Status'] = err.status
                    err_body = err.body
                    if isinstance(err_body, bytes):
                        err_body = err_body.decode('utf-8', errors='replace')
                    resp_dict['Response Body'] = err_body or '\n'.join(
                        RESPONSE_REASONS.get(err.status_int, ['']))
                    yield separator + get_response_body(
                        out_content_type, resp_dict, problem_segments,
                        'upload')
                else:
                    for chunk in err(req.environ, start_response):
                        yield chunk
                return

            json_data = json.dumps(data_for_storage).encode('utf-8')
            req.body = json_data
            req.headers.update({
                SYSMETA_SLO_ETAG: slo_etag,
                SYSMETA_SLO_SIZE: total_size,
                'X-Static-Large-Object': 'True',
                'Etag': md5(json_data, usedforsecurity=False).hexdigest(),
            })

            # Ensure container listings have both etags. However, if any
            # middleware to the left of us touched the base value, trust them.
            override_header = get_container_update_override_key('etag')
            val, sep, params = req.headers.get(
                override_header, '').partition(';')
            req.headers[override_header] = '%s; slo_etag=%s' % (
                (val or req.headers['Etag']) + sep + params, slo_etag)

            env = req.environ
            if not env.get('CONTENT_TYPE'):
                guessed_type, _junk = mimetypes.guess_type(
                    wsgi_to_str(req.path_info))
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
                resp_body = resp.body
                if isinstance(resp_body, bytes):
                    resp_body = resp_body.decode('utf-8')
                resp_dict['Response Body'] = resp_body
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
        if not check_utf8(wsgi_to_str(req.path_info)):
            raise HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
        vrs, account, container, obj = req.split_path(4, 4, True)
        obj_path = '/%s/%s' % (wsgi_to_str(container), wsgi_to_str(obj))

        segments = [{
            'sub_slo': True,
            'name': obj_path}]
        if 'version-id' in req.params:
            segments[0]['version_id'] = req.params['version-id']

        while segments:
            # We chose not to set the limit at max_manifest_segments
            # in the case this value was decreased by operators.
            # Still it is important to set a limit to avoid this list
            # growing too large and causing OOM failures.
            # x10 is a best guess as to how much operators would change
            # the value of max_manifest_segments.
            if len(segments) > self.max_manifest_segments * 10:
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
                    err_body = err.body
                    if isinstance(err_body, bytes):
                        err_body = err_body.decode('utf-8', errors='replace')
                    seg_data['error'] = {'code': err.status_int,
                                         'message': err_body}

                # add manifest back to be deleted after segments
                seg_data['sub_slo'] = False
                segments.append(seg_data)
            else:
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
        del new_env['wsgi.input']
        new_env['QUERY_STRING'] = 'multipart-manifest=get'
        if 'version-id' in req.params:
            new_env['QUERY_STRING'] += \
                '&version-id=' + req.params['version-id']
        new_env['CONTENT_LENGTH'] = 0
        new_env['HTTP_USER_AGENT'] = \
            '%s MultipartDELETE' % new_env.get('HTTP_USER_AGENT')
        new_env['swift.source'] = 'SLO'
        new_env['PATH_INFO'] = (
            '/%s/%s/%s' % (vrs, account, str_to_wsgi(obj_name.lstrip('/')))
        )
        # Just request the last byte of non-SLO objects so we don't waste
        # a resources in friendly_close() below
        manifest_req = Request.blank('', new_env, range='bytes=-1')
        update_ignore_range_header(manifest_req, 'X-Static-Large-Object')
        resp = manifest_req.get_response(self.app)

        if resp.is_success and config_true_value(resp.headers.get(
                'X-Static-Large-Object')) and len(resp.body) == 1:
            # pre-2.24.0 object-server
            manifest_req = Request.blank('', new_env)
            resp = manifest_req.get_response(self.app)

        if resp.is_success:
            if config_true_value(resp.headers.get('X-Static-Large-Object')):
                try:
                    return json.loads(resp.body)
                except ValueError:
                    raise HTTPServerError('Unable to load SLO manifest')
            else:
                # Drain and close GET request (prevents socket leaks)
                friendly_close(resp)
                raise HTTPBadRequest('Not an SLO manifest')
        elif resp.status_int == HTTP_NOT_FOUND:
            raise HTTPNotFound('SLO manifest not found')
        elif resp.status_int == HTTP_UNAUTHORIZED:
            raise HTTPUnauthorized('401 Unauthorized')
        else:
            raise HTTPServerError('Unable to load SLO manifest or segment.')

    def handle_async_delete(self, req):
        if not check_utf8(wsgi_to_str(req.path_info)):
            raise HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
        vrs, account, container, obj = req.split_path(4, 4, True)
        obj_path = '/%s/%s' % (wsgi_to_str(container), wsgi_to_str(obj))
        segments = [seg for seg in self.get_slo_segments(obj_path, req)
                    if 'data' not in seg]
        if not segments:
            # Degenerate case: just delete the manifest
            return self.app

        segment_containers, segment_objects = zip(*(
            split_path(seg['name'], 2, 2, True) for seg in segments))
        segment_containers = set(segment_containers)
        if len(segment_containers) > 1:
            container_csv = ', '.join(
                '"%s"' % quote(c) for c in segment_containers)
            raise HTTPBadRequest('All segments must be in one container. '
                                 'Found segments in %s' % container_csv)
        if any(seg.get('sub_slo') for seg in segments):
            raise HTTPBadRequest('No segments may be large objects.')

        # Auth checks
        segment_container = segment_containers.pop()
        if 'swift.authorize' in req.environ:
            container_info = get_container_info(
                req.environ, self.app, swift_source='SLO')
            req.acl = container_info.get('write_acl')
            aresp = req.environ['swift.authorize'](req)
            req.acl = None
            if aresp:
                return aresp

            if bytes_to_wsgi(segment_container.encode('utf-8')) != container:
                path = '/%s/%s/%s' % (vrs, account, bytes_to_wsgi(
                    segment_container.encode('utf-8')))
                seg_container_info = get_container_info(
                    make_env(req.environ, path=path, swift_source='SLO'),
                    self.app, swift_source='SLO')
                req.acl = seg_container_info.get('write_acl')
                aresp = req.environ['swift.authorize'](req)
                req.acl = None
                if aresp:
                    return aresp

        # Did our sanity checks; schedule segments to be deleted
        ts = req.ensure_x_timestamp()
        expirer_jobs = make_delete_jobs(
            wsgi_to_str(account), segment_container, segment_objects, ts)
        expiring_objects_account, expirer_cont = \
            self.expirer_config.get_expirer_account_and_container(
                ts, wsgi_to_str(account), wsgi_to_str(container),
                wsgi_to_str(obj))
        enqueue_req = make_pre_authed_request(
            req.environ,
            method='UPDATE',
            path="/v1/%s/%s" % (expiring_objects_account, expirer_cont),
            body=json.dumps(expirer_jobs),
            headers={'Content-Type': 'application/json',
                     'X-Backend-Storage-Policy-Index': '0',
                     'X-Backend-Allow-Private-Methods': 'True'},
        )
        resp = enqueue_req.get_response(self.app)
        if not resp.is_success:
            self.logger.error(
                'Failed to enqueue expiration entries: %s\n%s',
                resp.status, resp.body)
            return HTTPServiceUnavailable()
        # consume the response (should be short)
        friendly_close(resp)

        # Finally, delete the manifest
        return self.app

    def handle_multipart_delete(self, req):
        """
        Will delete all the segments in the SLO manifest and then, if
        successful, will delete the manifest file.

        :param req: a :class:`~swift.common.swob.Request` with an obj in path
        :returns: swob.Response whose app_iter set to Bulk.handle_delete_iter
        """
        if self.allow_async_delete and config_true_value(
                req.params.get('async')):
            return self.handle_async_delete(req)

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
            is_cont_or_obj_req = True
        except ValueError:
            is_cont_or_obj_req = False
        if not is_cont_or_obj_req:
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
    allow_async_delete = config_true_value(conf.get('allow_async_delete',
                                                    'true'))

    register_swift_info('slo',
                        max_manifest_segments=max_manifest_segments,
                        max_manifest_size=max_manifest_size,
                        yield_frequency=yield_frequency,
                        # this used to be configurable; report it as 1 for
                        # clients that might still care
                        min_segment_size=1,
                        allow_async_delete=allow_async_delete)

    def slo_filter(app):
        return StaticLargeObject(
            app, conf,
            max_manifest_segments=max_manifest_segments,
            max_manifest_size=max_manifest_size,
            yield_frequency=yield_frequency,
            allow_async_delete=allow_async_delete)
    return slo_filter
