# Copyright (c) 2010-2011 OpenStack, LLC.
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

from webob.exc import HTTPClientError,\
    HTTPInsufficientStorage as BaseHTTPInsufficientStorage


class HTTPClientDisconnect(HTTPClientError):
    """
    subclass of :class:`~HTTPClientError`

    This code is introduced to log the case when the connection is closed by
    client while HTTP server is processing its request

    code: 499, title: Client Disconnect
    """
    code = 499
    title = 'Client Disconnect'
    explanation = (
        'This code is introduced to log the case when the connection '
        'is closed by client while HTTP server is processing its request')


class HTTPInsuffcientStorage(BaseHTTPInsufficientStorage):
    """
    subclass of :class:`~HTTPInsufficientStorage`

    The server is unable to store the representation needed to
    complete the request.

    code: 507, title: Insufficient Storage
    """
    def __init__(self, drive, *args, **kwargs):
        self.explanation = ('%s is not mounted' % drive)
        super(HTTPInsuffcientStorage, self).__init__(*args, **kwargs)


def is_success(status):
    """
    Check if HTTP status code is successful.

    :param status: http status code
    :returns: True if status is successful, else False
    """
    return HTTP_OK <= status  and status < HTTP_MULTIPLE_CHOICES


def is_redirection(status):
    """
    Check if HTTP status code is redirection.

    :param status: http status code
    :returns: True if status is redirection, else False
    """
    return HTTP_MULTIPLE_CHOICES <= status  and status < HTTP_BAD_REQUEST


def is_client_error(status):
    """
    Check if HTTP status code is client error.

    :param status: http status code
    :returns: True if status is client error, else False
    """
    return HTTP_BAD_REQUEST <= status and status < HTTP_INTERNAL_SERVER_ERROR


def is_server_error(status):
    """
    Check if HTTP status code is server error.

    :param status: http status code
    :returns: True if status is server error, else False
    """
    return HTTP_INTERNAL_SERVER_ERROR <= status and status <= 599


# List of HTTP status codes

################################################################################
## 1xx Informational
################################################################################

HTTP_CONTINUE = 100
HTTP_SWITCHING_PROTOCOLS = 101
HTTP_PROCESSING = 102 # WebDAV
HTTP_CHECKPOINT = 103
HTTP_REQUEST_URI_TOO_LONG = 122

################################################################################
## 2xx Success
################################################################################

HTTP_OK = 200
HTTP_CREATED = 201
HTTP_ACCEPTED = 202
HTTP_NON_AUTHORITATIVE_INFORMATION = 203
HTTP_NO_CONTENT = 204
HTTP_RESET_CONTENT = 205
HTTP_PARTIAL_CONTENT = 206
HTTP_MULTI_STATUS = 207 # WebDAV
HTTP_IM_USED = 226

################################################################################
## 3xx Redirection
################################################################################

HTTP_MULTIPLE_CHOICES = 300
HTTP_MOVED_PERMANENTLY = 301
HTTP_FOUND = 302
HTTP_SEE_OTHER = 303
HTTP_NOT_MODIFIED = 304
HTTP_USE_PROXY = 305
HTTP_SWITCH_PROXY = 306
HTTP_TEMPORARY_REDIRECT = 307
HTTP_RESUME_INCOMPLETE = 308

################################################################################
## 4xx Client Error
################################################################################

HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_PAYMENT_REQUIRED = 402
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_METHOD_NOT_ALLOWED = 405
HTTP_NOT_ACCEPTABLE = 406
HTTP_PROXY_AUTHENTICATION_REQUIRED = 407
HTTP_REQUEST_TIMEOUT = 408
HTTP_CONFLICT = 409
HTTP_GONE = 410
HTTP_LENGTH_REQUIRED = 411
HTTP_PRECONDITION_FAILED = 412
HTTP_REQUEST_ENTITY_TOO_LARGE = 413
HTTP_REQUEST_URI_TOO_LONG = 414
HTTP_UNSUPPORTED_MEDIA_TYPE = 415
HTTP_REQUESTED_RANGE_NOT_SATISFIABLE = 416
HTTP_EXPECTATION_FAILED = 417
HTTP_IM_A_TEAPOT = 418
HTTP_UNPROCESSABLE_ENTITY = 422 # WebDAV
HTTP_LOCKED = 423 # WebDAV
HTTP_FAILED_DEPENDENCY = 424 # WebDAV
HTTP_UNORDERED_COLLECTION = 425
HTTP_UPGRADE_REQUIED = 426
HTTP_PRECONDITION_REQUIRED = 428
HTTP_TOO_MANY_REQUESTS = 429
HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE = 431
HTTP_NO_RESPONSE = 444
HTTP_RETRY_WITH = 449
HTTP_BLOCKED_BY_WINDOWS_PARENTAL_CONTROLS = 450
HTTP_CLIENT_CLOSED_REQUEST = 499

################################################################################
## 5xx Server Error
################################################################################

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_NOT_IMPLEMENTED = 501
HTTP_BAD_GATEWAY = 502
HTTP_SERVICE_UNAVAILABLE = 503
HTTP_GATEWAY_TIMEOUT = 504
HTTP_VERSION_NOT_SUPPORTED = 505
HTTP_VARIANT_ALSO_NEGOTIATES = 506
HTTP_INSUFFICIENT_STORAGE = 507 # WebDAV
HTTP_BANDWIDTH_LIMIT_EXCEEDED = 509
HTTP_NOT_EXTENDED = 510
HTTP_NETWORK_AUTHENTICATION_REQUIRED = 511
HTTP_NETWORK_READ_TIMEOUT_ERROR = 598    # not used in RFC
HTTP_NETWORK_CONNECT_TIMEOUT_ERROR = 599 # not used in RFC
