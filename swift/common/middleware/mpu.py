# Copyright (c) 2024 Nvidia
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
import base64
import binascii
import hmac
import json
import urllib
from collections import namedtuple

from swift.common import swob, constraints
from swift.common.constraints import AUTO_CREATE_ACCOUNT_PREFIX
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import HTTP_CONFLICT, is_success, HTTP_NOT_FOUND
from swift.common.middleware.symlink import ALLOW_RESERVED_NAMES
from swift.common.middleware.versioned_writes.object_versioning import \
    is_versioning_enabled, validate_version
from swift.common.object_ref import ObjectRef, HistoryId, UploadId
from swift.common.storage_policy import POLICIES
from swift.common.utils import drain_and_close, \
    config_positive_int_value, reiterate, parse_content_type, \
    decode_timestamps, split_path, quote, param_str_from_dict
from swift.common.swob import Request, normalize_etag, \
    wsgi_to_str, wsgi_quote, HTTPInternalServerError, HTTPOk, \
    HTTPConflict, HTTPBadRequest, HTTPException, HTTPNotFound, HTTPNoContent, \
    HTTPServiceUnavailable, quote_etag, HTTPAccepted, HTTPCreated
from swift.common.utils import get_logger, Timestamp, md5, public
from swift.common.registry import register_swift_info
from swift.common.request_helpers import get_reserved_name, \
    get_valid_part_num, is_reserved_name, is_user_meta, \
    update_etag_override_header, update_etag_is_at_header, \
    validate_part_number, update_content_type, is_sys_meta, \
    get_container_update_override_key
from swift.common.wsgi import make_pre_authed_request
from swift.proxy.controllers.base import get_container_info

DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024
DEFAULT_MAX_PART_NUMBER = 10000
MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024
MPU_SWIFT_SOURCE = 'MPU'

MPU_OBJECT_SYSMETA_PREFIX = 'x-object-sysmeta-mpu-'
MPU_SYSMETA_UPLOAD_ID_KEY = MPU_OBJECT_SYSMETA_PREFIX + 'upload-id'
MPU_SYSMETA_HISTORY_ID_KEY = MPU_OBJECT_SYSMETA_PREFIX + 'history-id'
MPU_SYSMETA_ETAG_KEY = MPU_OBJECT_SYSMETA_PREFIX + 'etag'
MPU_SYSMETA_PARTS_COUNT_KEY = MPU_OBJECT_SYSMETA_PREFIX + 'parts-count'
MPU_SYSMETA_MAX_MANIFEST_PART_KEY = \
    MPU_OBJECT_SYSMETA_PREFIX + 'max-manifest-part'
MPU_SYSMETA_USER_CONTENT_TYPE_KEY = MPU_OBJECT_SYSMETA_PREFIX + 'content-type'
MPU_SYSMETA_USER_PREFIX = MPU_OBJECT_SYSMETA_PREFIX + 'user-'
MPU_CONTAINER_SYSMETA_PREFIX = 'x-container-sysmeta-mpu-'
MPU_SESSION_CREATED_CONTENT_TYPE = 'application/x-mpu-session-created'
MPU_SESSION_ABORTED_CONTENT_TYPE = 'application/x-mpu-session-aborted'
MPU_SESSION_COMPLETING_CONTENT_TYPE = 'application/x-mpu-session-completing'
MPU_SESSION_COMPLETED_CONTENT_TYPE = 'application/x-mpu-session-completed'
MPU_MANIFEST_DEFAULT_CONTENT_TYPE = 'application/x-mpu'
MPU_MARKER_CONTENT_TYPE = 'application/x-mpu-marker'
MPU_PHONY_OBJECT_CONTENT_TYPE = 'application/x-phony;swift_source=mpu'
MPU_GENERIC_MARKER_SUFFIX = 'marker'
MPU_DELETED_MARKER_SUFFIX = MPU_GENERIC_MARKER_SUFFIX + '-deleted'
MPU_ABORTED_MARKER_SUFFIX = MPU_GENERIC_MARKER_SUFFIX + '-aborted'
MPU_UNEXPECTED_UPLOAD_ID_MSG = 'Request does not support upload-id'
MPU_INVALID_UPLOAD_ID_MSG = 'Invalid upload-id'
MPU_NO_SUCH_UPLOAD_ID_MSG = 'No such upload-id'


def _get_mac_digest(signing_key, path, obj_id):
    # drop the version so that tags are valid for v1, v1.0 or any future
    # API version
    v, a, c, o = split_path(path, 4, 4, True)
    aco_path = '/'.join([a, c, o])
    mac = hmac.HMAC(signing_key, b'', 'sha256')
    mac.update(aco_path.encode('utf8'))
    serialized = obj_id.serialize().encode('utf8')
    mac.update(serialized)
    return mac.hexdigest()


def externalize_upload_id(signing_key, path, upload_id):
    """
    Transform an UploadId to an external representation that is signed with the
    object path. The signature can be used for stateless verification that an
    UploadId was ever created for a given object path.

    :param signing_key: key used to sign the external representation.
    :param path: path for the object for which the UploadId was created.
    :param upload_id: an instance of UploadId.
    """
    # TODO: the tag allows us to verify that an upload id provided by a
    #   user was ever created by the middleware, but an hmac may be
    #   overkill. A CRC might suffice, or we may alternatively choose to
    #   encrypt the upload id which would replace the need for a tag.
    tag = _get_mac_digest(signing_key, path, upload_id)
    tagged_upload_id_str = upload_id.serialize() + tag
    return base64.urlsafe_b64encode(
        tagged_upload_id_str.encode('utf-8')).decode('utf-8')


def parse_external_upload_id(upload_id_str):
    decoded = base64.urlsafe_b64decode(
        upload_id_str.encode('utf-8')).decode('utf-8')
    return UploadId.parse(decoded[:-64]), decoded[-64:]


def internalize_upload_id(signing_key, path, upload_id_str):
    upload_id, tag = parse_external_upload_id(upload_id_str)
    expected = _get_mac_digest(signing_key, path, upload_id)
    if not hmac.compare_digest(expected, tag):
        raise ValueError('Invalid tag')
    return upload_id


def normalize_part_number(part_number):
    return '%06d' % int(part_number)


def make_relative_path(*parts):
    return '/'.join(str(p) for p in parts)


def calculate_max_name_length():
    # TODO: ideally we'd allow longer internal names in hidden containers
    max_suffix = ''
    for suffix in (MPU_DELETED_MARKER_SUFFIX, normalize_part_number(0),
                   'PUT', 'DELETE'):
        if len(suffix) > len(max_suffix):
            max_suffix = suffix
    obj_id = HistoryId(Timestamp.now(), null=True)
    ref = ObjectRef('', obj_id=obj_id.serialize(), tail=max_suffix)

    return constraints.MAX_OBJECT_NAME_LENGTH - len(ref.serialize())


MPUParsedManifest = namedtuple('MPUParsedManifest',
                               ['manifest', 'mpu_etag', 'max_manifest_part'])


class MPUEtagHasher:
    def __init__(self):
        self.hasher = md5(usedforsecurity=False)
        self.part_count = 0

    def update(self, part_etag):
        self.hasher.update(binascii.a2b_hex(normalize_etag(part_etag)))
        self.part_count += 1

    @property
    def etag(self):
        return '%s-%d' % (self.hasher.hexdigest(), self.part_count)


class MPUItem:
    def __init__(self, name, meta_timestamp, data_timestamp=None,
                 ctype_timestamp=None,
                 size=0, content_type='', etag='', deleted=0,
                 storage_policy_index=0, systags=None, **kwargs):
        self._name = name
        self.meta_timestamp = meta_timestamp
        self.data_timestamp = data_timestamp or meta_timestamp
        self.ctype_timestamp = ctype_timestamp or meta_timestamp
        self.size = size
        self.content_type = content_type
        self.etag = etag
        self.deleted = deleted
        self.storage_policy_index = storage_policy_index
        self.systags = systags
        self.kwargs = kwargs

    @property
    def name(self):
        return str(self._name)

    def __iter__(self):
        yield 'name', self.name
        yield 'data_timestamp', self.data_timestamp
        yield 'ctype_timestamp', self.ctype_timestamp
        yield 'meta_timestamp', self.meta_timestamp
        yield 'size', self.size
        yield 'etag', self.etag
        yield 'deleted', self.deleted
        yield 'content_type', self.content_type
        yield 'storage_policy_index', self.storage_policy_index
        yield 'systags', self.systags
        for k, v in self.kwargs.items():
            yield k, v

    @classmethod
    def from_db_record(cls, row):
        data_timestamp, ctype_timestamp, meta_timestamp = \
            decode_timestamps(row['created_at'])
        return cls(data_timestamp=data_timestamp,
                   ctype_timestamp=ctype_timestamp,
                   meta_timestamp=meta_timestamp,
                   **row)

    def to_db_record(self):
        """
        Returns a dict representation of the item in the form required by
        ``ContainerBroker.put_record()``.
        """
        return {'name': self.name,
                'created_at': self.data_timestamp.internal,
                'size': self.size,
                'content_type': self.content_type,
                'etag': self.etag,
                'deleted': self.deleted,
                'storage_policy_index': self.storage_policy_index,
                'ctype_timestamp': self.ctype_timestamp.internal,
                'meta_timestamp': self.meta_timestamp.internal,
                'systags': self.systags}


class MPUSession(MPUItem):
    """
    Encapsulates the state of an MPU session as it progresses from being
    created to being either completed or aborted.

    Session state is represented by the session object's content-type so that
    it appears in both the object's metadata and the container listing::

       ---------     ------------     -----------
      | created |-->| completing |-->| completed |
       ---------     ------------     -----------
          |               ^                ^
          |               |                |
          |               v                |
          |           ---------            |
          ---------->| aborted |<-----------
                      ---------

    A new session is in state 'created'.

    The state transitions to 'completing' when a completeUpload request
    handling is started, before a manifest is created.

    The state transitions to 'completed' when a completeUpload request handling
    is finished. The 'completed' state is definitive: a session only
    transitions to the 'completed' state at the end of a completeUpload, after
    the associated user-namespace object has been linked to a manifest object.

    The state transitions to 'aborted' when an abortUpload request is handled.

    The 'aborted' state is tentative. A client abortUpload request may cause a
    session to transition from 'completing' to 'aborted' while a concurrent
    completeUpload is being handled, and the completeUpload will typically
    continue to succeed. Furthermore, concurrent requests may result in a
    session transitioning from 'completed' to 'aborted'. The mpu-auditor is
    responsible for resolving any ambiguity; once a user-namespace object has
    been created, the mpu-auditor will not remove it regardless of the state of
    the session.

    :param name: the name of the session object.
    :param meta_timestamp: the timestamp for the most recent update to the
        session; this is typically the x-timestamp value of the most recent PUT
        or POST to the session object.
    :param data_timestamp: the timestamp at which the session object was PUT.
    :param ctype_timestamp: the timestamp at which the session object's state
        (i.e. its ``content_type``) was most recently updated.
    :param content_type: the session object content-type, which represents the
        current state of the session.
    :param headers: a dict of other session metadata
    """
    def __init__(self, name,
                 meta_timestamp,
                 data_timestamp=None,
                 ctype_timestamp=None,
                 content_type=MPU_SESSION_CREATED_CONTENT_TYPE,
                 headers=None,
                 **kwargs):
        super().__init__(
            name=name,
            meta_timestamp=meta_timestamp,
            data_timestamp=data_timestamp,
            ctype_timestamp=ctype_timestamp,
            content_type=content_type,
            **kwargs)
        self.headers = HeaderKeyDict(headers)

    @classmethod
    def from_user_headers(cls, name, headers):
        """
        Creates an ``MPUSession`` object for a new session from the headers
        provided with a client createUpload request. The content-type and any
        x-object-meta-* headers found in the given ``headers`` are translated
        to session sysmeta when the session is persisted. These will be used
        during completeUpload to set the content-type and user metadata of the
        user-namespace object.

        :param name: the unique name of the session
        :param headers: a dict of headers
        """
        headers = HeaderKeyDict(headers)
        timestamp = Timestamp(headers.get('X-Timestamp', 0))
        backend_headers = {}
        for k, v in headers.items():
            k = k.lower()
            if is_sys_meta('object', k):
                backend_headers[k] = v
            elif is_user_meta('object', k) or k in (
                    'content-disposition',
                    'content-encoding',
                    'content-language',
                    'cache-control',
                    'expires',
            ):
                # User metadata is stored as sysmeta on the session because it
                # must persist with the session object until a manifest is PUT,
                # even when there are subsequent POSTs to the session object.
                backend_headers[MPU_SYSMETA_USER_PREFIX + k] = v
            elif k == 'content-type':
                backend_headers[MPU_SYSMETA_USER_CONTENT_TYPE_KEY] = v
        return cls(name, timestamp, headers=backend_headers)

    @classmethod
    def from_session_headers(cls, name, backend_headers):
        """
        Creates an ``MPUSession`` object for an existing session from the
        headers returned with a backend session HEAD request.

        :param name: the unique name of the session
        :param headers: a dict of headers
        """
        timestamp = Timestamp(backend_headers.get('X-Timestamp', 0))
        data_timestamp = Timestamp(
            backend_headers.get('X-Backend-Data-Timestamp', timestamp))
        content_type = backend_headers.get('content-type')
        return cls(name, timestamp, content_type=content_type,
                   headers=backend_headers, data_timestamp=data_timestamp)

    @property
    def is_active(self):
        # Note: a session is still active when its state is 'completing'. If
        # complete fails the user should still be able to upload and list parts
        # because it is possible that a missing part caused the complete to
        # fail.
        return not (self.is_completed or self.is_aborted)

    @property
    def is_aborted(self):
        return self.content_type == MPU_SESSION_ABORTED_CONTENT_TYPE

    def set_aborted(self, timestamp):
        self.content_type = MPU_SESSION_ABORTED_CONTENT_TYPE
        self.ctype_timestamp = timestamp

    @property
    def is_completing(self):
        return self.content_type == MPU_SESSION_COMPLETING_CONTENT_TYPE

    def set_completing(self, timestamp):
        self.content_type = MPU_SESSION_COMPLETING_CONTENT_TYPE
        self.ctype_timestamp = timestamp

    @property
    def is_completed(self):
        return self.content_type == MPU_SESSION_COMPLETED_CONTENT_TYPE

    def set_completed(self, timestamp):
        self.content_type = MPU_SESSION_COMPLETED_CONTENT_TYPE
        self.ctype_timestamp = timestamp

    def get_put_headers(self):
        headers = HeaderKeyDict({
            'X-Timestamp': self.data_timestamp.internal,
            'Content-Type': self.content_type,
            'Content-Length': '0',
        })
        headers.update(self.headers)
        return headers

    def get_post_headers(self):
        return HeaderKeyDict({
            'X-Timestamp': self.ctype_timestamp.internal,
            'Content-Type': self.content_type,
        })

    def get_manifest_headers(self):
        headers = HeaderKeyDict()
        for key, val in self.headers.items():
            key_lower = key.lower()
            if key_lower.startswith(MPU_SYSMETA_USER_PREFIX):
                headers[key[len(MPU_SYSMETA_USER_PREFIX):]] = val
            elif key_lower == MPU_SYSMETA_USER_CONTENT_TYPE_KEY:
                headers['Content-Type'] = val
            elif key_lower.startswith(MPU_OBJECT_SYSMETA_PREFIX):
                # filter out mpu session sysmeta
                continue
            elif is_sys_meta('object', key_lower):
                headers[key] = val
        return headers

    @property
    def history_id(self):
        return HistoryId.parse(self.headers.get(MPU_SYSMETA_HISTORY_ID_KEY))


class BaseMPUHandler:
    def __init__(self, mw, req):
        self.mw = mw
        self.app = mw.app
        self.logger = mw.logger
        self.req = req
        # native strings (unquoted utf8)
        try:
            path_parts = req.split_path(4, 4, True)
            self.account, self.container, self.obj = (
                wsgi_to_str(path_part) for path_part in path_parts[1:])
        except ValueError:
            path_parts = req.split_path(3, 3, False)
            self.account, self.container = (
                wsgi_to_str(path_part) for path_part in path_parts[1:])
            self.obj = None

        self.sessions_container = get_reserved_name('mpu_sessions',
                                                    self.container)
        self.parts_container = get_reserved_name('mpu_parts', self.container)
        self.history_container = get_reserved_name('history', self.container)
        self.hidden_account = AUTO_CREATE_ACCOUNT_PREFIX + self.account
        self.user_container_info = get_container_info(
            self.req.environ, self.app, swift_source=MPU_SWIFT_SOURCE)

    def _check_user_container_exists(self):
        info = self.user_container_info
        if is_success(info['status']):
            self.user_container_info.setdefault('sysmeta', {})
            return info
        elif info['status'] == HTTP_NOT_FOUND:
            raise HTTPNotFound()
        else:
            raise HTTPServiceUnavailable()

    def _ensure_container_exists(self, account, container, policy_index):
        # TODO: make storage policy specific parts bucket
        policy_name = POLICIES[policy_index].name
        path = '/'.join(['', 'v1', account, container])
        headers = {'X-Storage-Policy': policy_name}
        cont_req = self.make_subrequest(
            path=path, method='PUT', headers=headers)
        info = get_container_info(cont_req.environ, self.app,
                                  swift_source=MPU_SWIFT_SOURCE)

        if not is_success(info['status']):
            resp = cont_req.get_response(self.app)
            drain_and_close(resp)
            if not resp.is_success or resp.status_int == HTTP_CONFLICT:
                raise HTTPInternalServerError(
                    'Error creating MPU resource container', request=self.req)

    def _authorize_request(self, acl):
        # TODO: this pattern appears in may places (e.g. obj.py,
        #   object_versioning - grep for 'req.acl') - should we have a
        #   request_helper function?
        if 'swift.authorize' in self.req.environ:
            self.req.acl = self.user_container_info.get(acl)
            auth_resp = self.req.environ['swift.authorize'](self.req)
            if auth_resp:
                raise auth_resp

    def _authorize_read_request(self):
        self._authorize_request('read_acl')

    def _authorize_write_request(self):
        self._authorize_request('write_acl')

    def make_path(self, *parts):
        return '/'.join(['', 'v1', make_relative_path(*parts)])

    def make_subrequest(self, method=None, path=None, body=None,
                        headers=None, params=None):
        """
        Make a pre-auth'd sub-request based on ``self.req``.

        The sub-request does *not* inherit headers or query-string parameters
        from ``self.req``.

        The sub-request will have a truthy 'X-Backend-Allow-Reserved-Names'
        header and its swift source will be set to 'MPU'.

        :param method: the sub-request method.
        :param path: the sub-request path.
        :param body: the sub-request body.
        :param headers: a dict of headers for the sub-request.
        :param params: a dict of query-string parameters for the sub-request.
        """
        quoted_path = quote(path)
        req_headers = {'X-Backend-Allow-Reserved-Names': 'true'}
        if headers:
            req_headers.update(headers)
        sub_req = make_pre_authed_request(
            self.req.environ,
            path=quoted_path,
            method=method,
            headers=req_headers,
            swift_source=MPU_SWIFT_SOURCE,
            body=body)
        sub_req.params = params or {}
        return sub_req

    def translate_error_response(self, sub_resp):
        if (sub_resp.status_int not in swob.RESPONSE_REASONS or
                sub_resp.status_int == 404):
            client_resp_status_int = 503
        else:
            client_resp_status_int = sub_resp.status_int
        return swob.status_map[client_resp_status_int](request=self.req)

    def _annotate_with_history_update(self, req, history_id, op, systags=None):
        update_headers = {
            # zero-size so that phony entries don't add to container bytes
            'x-size': '0',
            # listings can filter out based on content-type
            'x-content-type': MPU_PHONY_OBJECT_CONTENT_TYPE,
        }
        history_ref = ObjectRef(self.obj, history_id.serialize(), op)
        if systags:
            update_headers['x-systags'] = param_str_from_dict(systags)
        update = {
            'account': self.hidden_account,
            'container': self.history_container,
            # history entries need to sort in reverse chronological order
            'obj': history_ref.serialize(),
            'headers': update_headers
        }
        if 'swift.container_updates' not in req.environ:
            req.environ['swift.container_updates'] = []
        req.environ['swift.container_updates'].append(update)


class MPUSessionsHandler(BaseMPUHandler):
    """
    Handles the following APIs:

    * List Multipart Uploads
    * Initiate Multipart Upload
    """
    def __init__(self, mw, req):
        super().__init__(mw, req)
        self._check_user_container_exists()

    def _extract_list_uploads_marker(self):
        marker_name = self.req.params.get('marker')
        upload_id_marker = self.req.params.get('upload-id-marker')
        if upload_id_marker:
            try:
                marker_id = internalize_upload_id(
                    self.mw.upload_id_signing_key,
                    self.req.path + '/' + wsgi_quote(marker_name),
                    upload_id_marker)
            except ValueError as err:
                raise HTTPBadRequest(
                    MPU_INVALID_UPLOAD_ID_MSG + ' (%s: upload-id-marker: %s)'
                    % (str(err), upload_id_marker))
        else:
            marker_id = UploadId.newest()
        # TODO: why is this given marker not quoted like following
        #    derived markers?...
        #   the func test seems happy test_list_mpu_sessions
        marker_ref = ObjectRef(marker_name, marker_id.serialize())
        # mpu sessions are sorted in chronological order
        return marker_ref.serialize()

    @public
    def list_uploads(self):
        """
        Handles List Multipart Uploads
        """
        self._authorize_read_request()
        path = self.make_path(self.hidden_account, self.sessions_container)

        subreq_params = {}
        if 'marker' in self.req.params:
            subreq_params['marker'] = self._extract_list_uploads_marker()
        if 'prefix' in self.req.params:
            prefix = self.req.params['prefix']
            subreq_params['prefix'] = ObjectRef(prefix).serialize()
        limit = int(self.req.params.get(
            'limit', constraints.CONTAINER_LISTING_LIMIT))

        listing = []
        items = [None]  # dummy value to get us into the while loop
        while items and len(listing) < limit:
            # The listing from the backend includes sessions that are aborted
            # or completed; these are not included in the listing sent to the
            # client, so we may need more than one backend listing to reach the
            # desired limit in the client listing.
            sub_req = self.make_subrequest(
                path=path, method='GET', params=subreq_params)
            sub_resp = sub_req.get_response(self.app)
            if sub_resp.is_success:
                items = json.loads(sub_resp.body)
                for item in items:
                    subreq_params['marker'] = quote(item['name'])
                    if item['content_type'] in (
                            MPU_SESSION_COMPLETED_CONTENT_TYPE,
                            MPU_SESSION_ABORTED_CONTENT_TYPE):
                        continue
                    mpu_ref = ObjectRef.parse(item['name'])
                    item['name'] = mpu_ref.user_name
                    item['upload_id'] = self.mw.externalize_upload_id(
                        self.req.path + '/' + quote(mpu_ref.user_name),
                        UploadId.parse(mpu_ref.obj_id))
                    listing.append(item)
                    if len(listing) >= limit:
                        break
            else:
                return sub_resp

        sub_resp.body = json.dumps(listing).encode('ascii')
        return sub_resp

    def _ensure_resource_containers_in_metadata(self, policy_index):
        parts_container_key = 'mpu-parts-container-%d' % policy_index
        headers = {}
        # TODO: IDK if history container needs to be in sysmeta - this may be
        #   in anticipation of integrating with versioning when we want
        #   object-versioning to know the container exists?
        for key, val in ((parts_container_key, self.parts_container),
                         ('history-container', self.history_container)):
            if key not in self.user_container_info['sysmeta']:
                headers['x-container-sysmeta-' + key] = quote(val)
        if headers:
            cont_req = self.make_subrequest(
                path=self.make_path(self.account, self.container),
                method='POST',
                headers=headers)
            resp = cont_req.get_response(self.app)
            drain_and_close(resp)
            if not resp.is_success or resp.status_int == HTTP_CONFLICT:
                raise HTTPInternalServerError(
                    'Error writing MPU resource metadata', request=self.req)

    @public
    def create_upload(self):
        """
        Handles Initiate Multipart Upload.
        """
        # TODO: support alternative parts container policies
        if len(self.obj) > self.mw.max_name_length:
            raise HTTPBadRequest(
                body='MPU object name length of %d longer than %d' %
                     (len(self.obj), self.mw.max_name_length),
                request=self.req, content_type='text/plain')
        self._authorize_write_request()
        timestamp = self.req.ensure_x_timestamp()
        policy_index = self.user_container_info['storage_policy']
        self._ensure_container_exists(
            self.hidden_account, self.sessions_container, policy_index)
        # TODO: hide the parts container; for now it must in the user account
        #   because SLO doesn't support cross-account segments
        self._ensure_container_exists(
            self.account, self.parts_container, policy_index)
        self._ensure_container_exists(
            self.hidden_account, self.history_container, policy_index)
        self._ensure_resource_containers_in_metadata(policy_index)

        self.req.headers.pop('Etag', None)
        self.req.headers.pop('Content-Md5', None)
        update_content_type(self.req)

        upload_id = UploadId(timestamp)
        session_ref = ObjectRef(self.obj, upload_id.serialize())
        null = not is_versioning_enabled(self.user_container_info)
        history_id = HistoryId(timestamp, null=null)
        self.req.headers[MPU_SYSMETA_HISTORY_ID_KEY] = history_id.serialize()
        session_name = session_ref.serialize()
        session_path = self.make_path(
            self.hidden_account, self.sessions_container, session_name)
        session = MPUSession.from_user_headers(session_name, self.req.headers)
        session_req = self.make_subrequest(
            path=session_path,
            method='PUT',
            headers=session.get_put_headers()
        )

        session_resp = session_req.get_response(self.app)
        if session_resp.is_success:
            self.logger.debug('created mpu session %s' % session_path)
            drain_and_close(session_resp)
            resp_headers = {
                'X-Upload-Id': self.mw.externalize_upload_id(
                    self.req.path, upload_id),
            }
            resp = HTTPAccepted(headers=resp_headers)
        else:
            resp = self.translate_error_response(session_resp)
        return resp


class MPUSloCallbackHandler:
    ERROR_MSG = 'Upload part too small'

    def __init__(self, mw):
        self.total_bytes = 0
        self.mw = mw
        self.too_small_message = (
            self.ERROR_MSG + ': part must be at least %d bytes'
            % self.mw.min_part_size)

    def __call__(self, slo_manifest):
        # Check the size of each segment except the last and make sure
        # they are all more than the minimum upload chunk size.
        # Note that we need to use the *internal* keys, since we're
        # looking at the manifest that's about to be written.
        errors = []
        for index, item in enumerate(slo_manifest):
            if not item:
                continue
            self.total_bytes += item['bytes']
            if (index < len(slo_manifest) - 1 and
                    item['bytes'] < self.mw.min_part_size):
                # TODO: add tests coverage
                errors.append((item['name'], self.too_small_message))
        return errors


class MPUSessionHandler(BaseMPUHandler):
    """
    Handles the following APIs:

    * List Parts
    * Abort Multipart Upload
    * Complete Multipart Upload
    * Upload Part and Upload Part Copy.
    """
    def __init__(self, mw, req, upload_id):
        super().__init__(mw, req)
        self._check_user_container_exists()
        self.upload_id = upload_id
        self.session_ref = ObjectRef(self.obj, self.upload_id.serialize())
        # mpu sessions for the same user_name sort in chronological order...
        self.session_name = self.session_ref.serialize()
        self.session_path = self.make_path(
            self.hidden_account, self.sessions_container, self.session_name)
        self.req_timestamp = Timestamp(
            self.req.headers.setdefault('X-Timestamp',
                                        Timestamp.now().internal))

    def _load_session(self):
        req = self.make_subrequest(method='HEAD', path=self.session_path)
        resp = req.get_response(self.app)
        self.logger.debug('loading mpu session %s %s'
                          % (self.session_path, resp.status_int))
        if resp.status_int == 404:
            raise HTTPNotFound(MPU_NO_SUCH_UPLOAD_ID_MSG)
        elif not resp.is_success:
            raise HTTPInternalServerError()

        session = MPUSession.from_session_headers(self.session_name,
                                                  resp.headers)
        if session.meta_timestamp >= self.req_timestamp:
            raise HTTPConflict()
        return session

    def _post_session(self, session):
        session_req = self.make_subrequest(
            'POST',
            path=self.session_path,
            headers=session.get_post_headers())
        return session_req.get_response(self.app)

    def _get_user_object_metadata(self):
        path = self.make_path(
            self.account, self.container, self.obj)
        req = self.make_subrequest(method='HEAD', path=path)
        resp = req.get_response(self.app)
        if resp.is_success:
            return resp.headers
        else:
            return {}

    def upload_part(self, part_number):
        self._authorize_write_request()
        session = self._load_session()
        if not session.is_active:
            return HTTPNotFound(MPU_NO_SUCH_UPLOAD_ID_MSG)

        part_ref = self.session_ref.clone()
        part_ref.tail = normalize_part_number(part_number)
        part_path = self.make_path(self.account, self.parts_container,
                                   part_ref.serialize())
        self.logger.debug('mpu upload_part %s', part_path)
        headers = {}
        for k, v in self.req.headers.items():
            # note: x-delete-[at | after] headers are ignored
            # TODO: should we return 400 if client sends x-delete-at?
            if k.lower() in ('content-length',
                             'transfer-encoding',
                             'etag',
                             'x-timestamp'):
                headers[k] = v
        part_req = self.make_subrequest(
            path=part_path, method='PUT', body=self.req.body, headers=headers)

        sub_resp = part_req.get_response(self.app)
        drain_and_close(sub_resp)
        if sub_resp.is_success:
            headers = HeaderKeyDict(sub_resp.headers)
            # mpu mw always quotes response header etag for requests it handles
            headers['Etag'] = quote_etag(sub_resp.headers.get('Etag'))
            resp = HTTPCreated(headers=headers)
        else:
            resp = self.translate_error_response(sub_resp)
        return resp

    def list_parts(self):
        """
        Handles List Parts.
        """
        self._authorize_read_request()
        session = self._load_session()
        if not session.is_active:
            return HTTPNotFound(MPU_NO_SUCH_UPLOAD_ID_MSG)

        path = self.make_path(self.account, self.parts_container)
        subreq_params = {
            'prefix': swob.str_to_wsgi(self.session_ref.serialize())
        }
        try:
            part_number_marker = validate_part_number(
                self.req.params.get('part-number-marker'))
        except ValueError:
            raise HTTPBadRequest(
                'part-number-marker must be an integer greater than 0')
        if part_number_marker is not None:
            marker_ref = self.session_ref.clone()
            marker_ref.tail = normalize_part_number(part_number_marker)
            subreq_params['marker'] = swob.str_to_wsgi(marker_ref.serialize())
        if 'limit' in self.req.params:
            subreq_params['limit'] = self.req.params['limit']

        sub_req = self.make_subrequest(
            path=path, method='GET', params=subreq_params)
        sub_resp = sub_req.get_response(self.app)
        if sub_resp.is_success:
            listing = json.loads(sub_resp.body)
            for item in listing:
                part_ref = ObjectRef.parse(item['name'])
                item['name'] = '/'.join(
                    (part_ref.user_name,
                     self.mw.externalize_upload_id(
                         self.req.path, UploadId.parse(part_ref.obj_id)),
                     part_ref.tail))
            headers = {
                'Content-Type': 'application/json; charset=utf-8',
                'X-Storage-Policy': sub_resp.headers.get('X-Storage-Policy')
            }
            resp = HTTPOk(
                body=json.dumps(listing).encode('ascii'),
                headers=headers,
            )
        else:
            drain_and_close(sub_resp)
            resp = self.translate_error_response(sub_resp)
        return resp

    def abort_upload(self):
        """
        Handles Abort Multipart Upload.
        """
        self._authorize_write_request()
        try:
            session = self._load_session()
        except HTTPException as err:
            if err.status_int == 404:
                # the upload-id has already been parsed so it's valid
                return HTTPNoContent()
            else:
                return err

        if self.req.timestamp < session.data_timestamp:
            return HTTPConflict()

        if not session.is_active:
            return HTTPNoContent()

        #  check if the user object has been linked to the manifest
        # TODO: checking if the user object is linked is not essential - the
        #   auditor will check this before taking any abort action
        user_obj_metadata = self._get_user_object_metadata()
        if user_obj_metadata.get(MPU_SYSMETA_UPLOAD_ID_KEY) == \
                self.upload_id:
            return HTTPConflict()

        # Update the session to be marked as aborted. This will prevent any
        # subsequent complete operation from proceeding.
        # Note: the session is not deleted yet, but it will no longer appear in
        # listings of in-progress sessions; the auditor is responsible for
        # cleaning up session resources.
        # Note: this may race with a concurrent completeUpload, and win! That's
        # ok because the auditor will check if a user object is linked to the
        # MPU resources while handling an aborted session.
        session.timestamp = self.req.timestamp
        session.set_aborted(self.req.timestamp)
        sess_resp = self._post_session(session)
        drain_and_close(sess_resp)
        if sess_resp.is_success:
            resp = HTTPNoContent()
        else:
            resp = self.translate_error_response(sess_resp)
        return resp

    def _parse_part_number(self, part_dict, previous_part):
        try:
            part_number = part_dict['part_number']
            if part_number <= 0:
                raise ValueError(
                    "part_number %s must be greater than zero" % part_number)
            if part_number <= previous_part:
                raise ValueError(
                    "part_number %s must be greater than previous %s"
                    % (part_number, previous_part))
        except KeyError:
            raise ValueError("expected keys to include part_number")
        return part_number

    def _parse_etag(self, part_dict):
        try:
            etag = part_dict['etag']
            etag = normalize_etag(etag)
            if (etag is None or len(etag) != 32 or
                    any(c not in '0123456789abcdef' for c in etag)):
                raise ValueError("etag %s is invalid" % etag)
        except KeyError:
            raise ValueError("expected keys to include etag")
        return etag

    def _parse_user_manifest(self, body):
        try:
            user_manifest = json.loads(body)
        except ValueError:
            raise HTTPBadRequest("Manifest must be valid JSON.\n")

        if not isinstance(user_manifest, list):
            raise HTTPBadRequest("Manifest must be a list.\n")

        errors = []
        manifest = []
        mpu_etag_hasher = MPUEtagHasher()
        previous_part = 0
        part_number = 0
        part_ref = self.session_ref.clone()
        for part_index, part_dict in enumerate(user_manifest):
            manifest_part_dict = {}
            if not isinstance(part_dict, dict):
                errors.append("Index %d: not a JSON object." % part_index)
                continue
            try:
                part_number = self._parse_part_number(part_dict, previous_part)
                part_ref.tail = normalize_part_number(part_number)
                manifest_part_dict['path'] = make_relative_path(
                    self.parts_container, part_ref.serialize()
                )
            except ValueError as err:
                errors.append("Index %d: %s." % (part_index, err))
            try:
                etag = self._parse_etag(part_dict)
                manifest_part_dict['etag'] = etag
                mpu_etag_hasher.update(etag)
            except ValueError as err:
                errors.append("Index %d: %s." % (part_index, str(err)))
            if not errors:
                manifest.append(manifest_part_dict)

        if not manifest and not errors:
            errors.append('Manifest must have at least one part.')

        if len(manifest) > self.mw.max_part_number:
            errors.append('Manifest must have at most %s parts.'
                          % self.mw.max_part_number)

        if errors:
            error_message = b"".join(e.encode('utf8') + b"\n" for e in errors)
            raise HTTPBadRequest(error_message,
                                 headers={"Content-Type": "text/plain"})
        return MPUParsedManifest(
            manifest, mpu_etag_hasher.etag, part_number)

    def _parse_slo_errors(self, slo_resp_dict):
        resp_dict = {'Response Status': '400 Bad Request'}
        errors = []
        for path, reason in slo_resp_dict.get('Errors', []):
            part_number = path.rsplit('/')[-1]
            errors.append([part_number, reason])
        resp_dict['Errors'] = errors
        return resp_dict

    def _put_manifest(self, session, parsed_manifest):
        # create manifest in hidden container
        offset = self.req.timestamp.raw - session.data_timestamp.raw
        offset += session.data_timestamp.offset
        ts_complete = Timestamp(session.data_timestamp, offset=offset)
        # note: setting x-timestamp here causes object-versioning to us that
        # timestamp to form a version id, so version ids are coupled to the
        # upload id and history id for the manifest
        manifest_headers = {
            'X-Timestamp': ts_complete.internal,
            'Accept': 'application/json',
            # report size as 0 in container stats
            get_container_update_override_key('size'): '0',
            ALLOW_RESERVED_NAMES: 'true',
            MPU_SYSMETA_UPLOAD_ID_KEY: str(self.upload_id),
            MPU_SYSMETA_ETAG_KEY: parsed_manifest.mpu_etag,
            MPU_SYSMETA_PARTS_COUNT_KEY: str(len(parsed_manifest.manifest)),
            # The max_manifest_part is not currently used but may prove useful.
            # For example, if max_manifest_part == number of mpu parts then we
            # can infer the path to a part object without a GET for the
            # manifest body.
            MPU_SYSMETA_MAX_MANIFEST_PART_KEY:
                str(parsed_manifest.max_manifest_part)
        }
        manifest_headers.update(session.get_manifest_headers())
        # TODO: pass through more conditional request headers? and add tests
        if 'If-None-Match' in self.req.headers:
            manifest_headers['If-None-Match'] = \
                self.req.headers['If-None-Match']
        # set the MPU etag override to be forwarded to the manifest container
        part_prefix_path = quote(make_relative_path(
            self.parts_container, self.session_ref.serialize()))
        update_etag_override_header(
            manifest_headers, parsed_manifest.mpu_etag,
            [('mpu_etag', parsed_manifest.mpu_etag),
             ('mpu_link', part_prefix_path)])
        params = {'multipart-manifest': 'put', 'heartbeat': 'on'}
        manifest_req = self.make_subrequest(
            path=self.make_path(
                self.account, self.container, self.obj),
            method='PUT',
            headers=manifest_headers,
            body=json.dumps(parsed_manifest.manifest),
            params=params)
        self._annotate_with_history_update(
            manifest_req, session.history_id, 'PUT', {'mpu_policy': 0})
        slo_callback_handler = MPUSloCallbackHandler(self.mw)
        manifest_req.environ['swift.callback.slo_manifest_hook'] = \
            slo_callback_handler
        self.logger.debug('mpu manifest PUT %s %s',
                          manifest_req.path, dict(manifest_req.headers))
        return manifest_req.get_response(self.app), slo_callback_handler

    def _post_session_completing(self, session):
        # Set session state to completing; this will cause the auditor to
        # periodically check if the user object has been linked to the mpu
        # resources, in case the later POST to set session state to completed
        # fails.
        session.timestamp = self.req.timestamp
        session.set_completing(self.req.timestamp)
        return self._post_session(session)

    def _post_session_completed(self, session):
        # TODO: ideally use req.timestamp for this POST (but we
        #   already burnt that for the state=completing POST);
        #   figure out timestamp progression
        session.set_completed(Timestamp.now())
        sess_resp = self._post_session(session)
        drain_and_close(sess_resp)

    def _make_complete_upload_resp_iter(self, session, parsed_manifest):
        def response_iter():
            manifest_resp, slo_callback_handler = self._put_manifest(
                session, parsed_manifest)
            if not manifest_resp.is_success:
                yield json.dumps(
                    {'Response Status': '503 Service Unavailable',
                     'Response Body':
                         manifest_resp.body.decode('utf-8', errors='replace')}
                ).encode('ascii')
                return

            body_chunks = []
            manifest_resp.fix_conditional_response()
            for chunk in manifest_resp.response_iter:
                if not chunk.strip():
                    # pass heartbeat bytes on to the client
                    yield chunk
                    continue
                body_chunks.append(chunk)

            try:
                manifest_resp_body = b''.join(body_chunks)
                body_dict = json.loads(manifest_resp_body)
            except ValueError:
                yield json.dumps(
                    {'Response Status': '503 Service Unavailable'}
                ).encode('ascii')
                return

            manifest_resp_status = body_dict.get('Response Status')
            if manifest_resp_status == '201 Created':
                self._post_session_completed(session)
                # report success to the user whatever the result of the
                # session POST; the auditor will detect that the user obj
                # was linked to the manifest
                body_dict['Etag'] = normalize_etag(
                    parsed_manifest.mpu_etag)
                yield json.dumps(body_dict).encode('ascii')
            elif manifest_resp_status == '400 Bad Request':
                resp_dict = self._parse_slo_errors(body_dict)
                yield json.dumps(resp_dict).encode('ascii')
            else:
                yield manifest_resp_body

        return reiterate(response_iter())

    def complete_upload(self):
        """
        Handles Complete Multipart Upload.
        """
        self._authorize_write_request()

        parsed_manifest = self._parse_user_manifest(self.req.body)
        try:
            session = self._load_session()
            if not session.is_active:
                raise HTTPNotFound(MPU_NO_SUCH_UPLOAD_ID_MSG)
        except HTTPException as err:
            if err.status_int != 404:
                return err
            user_obj_metadata = self._get_user_object_metadata()
            if (user_obj_metadata.get(MPU_SYSMETA_ETAG_KEY) ==
                    parsed_manifest.mpu_etag
                    and user_obj_metadata.get(MPU_SYSMETA_UPLOAD_ID_KEY) ==
                    self.upload_id):
                # TODO: for belt-and-braces, check it is a symlink too
                # session was previously completed, tolerate the retry
                body_dict = {
                    'Response Status': '201 Created',
                    'Etag': parsed_manifest.mpu_etag,
                    'Last Modified': user_obj_metadata.get('Last-Modified'),
                    'Response Body': '',
                    'Errors': [],
                }
                return HTTPAccepted(body=json.dumps(body_dict).encode('ascii'))
            else:
                return err

        if self.req.timestamp < session.data_timestamp:
            return HTTPConflict()

        # TODO: check if a manifest already exists with same mpu_etag, if it
        #   does then skip to putting the symlink to the manifest
        # Set session state to completing; this will cause the auditor to
        # periodically check if the user object has been linked to the mpu
        # resources, in case the later POST to set session state to completed
        # fails.
        sess_resp = self._post_session_completing(session)
        drain_and_close(sess_resp)
        if not sess_resp.is_success:
            return self.translate_error_response(sess_resp)

        # return 202 to match SLO response with heartbeat=on
        resp = HTTPAccepted()  # assume we're good for now...
        resp.app_iter = self._make_complete_upload_resp_iter(
            session, parsed_manifest)
        self.logger.debug('mpu complete_upload %s', self.req.path)
        return resp


class MPUObjHandler(BaseMPUHandler):
    def _handle_get_head_request(self):
        # instruct the object server to look for an mpu-etag in sysmeta
        # for evaluating conditional requests
        update_etag_is_at_header(self.req, MPU_SYSMETA_ETAG_KEY)
        resp = self.req.get_response(self.app)
        if MPU_SYSMETA_UPLOAD_ID_KEY not in resp.headers:
            return resp

        new_headers = HeaderKeyDict()
        mpu_etag = None
        for key, val in resp.headers.items():
            key = key.lower()
            if key in ('x-static-large-object',
                       'content-location',
                       'x-manifest-etag'):
                continue
            if key == MPU_SYSMETA_ETAG_KEY:
                mpu_etag = val
            elif key == MPU_SYSMETA_PARTS_COUNT_KEY:
                new_headers['x-parts-count'] = val
            elif key == MPU_SYSMETA_UPLOAD_ID_KEY:
                upload_id = UploadId.parse(val)
                new_headers['x-upload-id'] = self.mw.externalize_upload_id(
                    self.req.path, upload_id)
            elif key.startswith(MPU_OBJECT_SYSMETA_PREFIX):
                continue
            else:
                new_headers[key] = val
        if mpu_etag:
            # mpu mw always quotes response header etag for requests it handles
            new_headers['etag'] = quote_etag(mpu_etag)
        resp.headers = new_headers
        return resp

    def _handle_put_delete_request(self):
        # TODO: ok to always use default policy for history?
        policy_index = POLICIES.default
        self._ensure_container_exists(
            self.hidden_account, self.history_container, policy_index)
        # TODO: the coupling with object-versioning is unfortunate
        version_id = self.req.params.get('version-id')
        is_versioning = is_versioning_enabled(self.user_container_info)
        # Note: we're relying on request method in the op field for correct
        # sorting of null versions in history i.e. DELETE trumps PUT of
        # otherwise same version.
        if version_id == 'null':
            # this is a new event in the null version's history
            history_id = HistoryId(self.req.ensure_x_timestamp(), null=True)
            op = self.req.method
        elif version_id:
            # this is a new event in a specific version's history
            validate_version(version_id)
            history_id = HistoryId(version_id)
            op = self.req.method
        elif is_versioning:
            # object-versioning will transform both PUTs and DELETEs to a
            # PUT in the versions container, so the history event should also
            # be a PUT
            history_id = HistoryId(self.req.ensure_x_timestamp(), null=False)
            op = 'PUT'
        else:
            # this is a new event in the null version's history
            history_id = HistoryId(self.req.ensure_x_timestamp(), null=True)
            op = self.req.method
        self._annotate_with_history_update(self.req, history_id, op)

    def handle_request(self):
        if self.req.method in ('GET', 'HEAD'):
            # instruct the object server to look for an mpu-etag in sysmeta
            # for evaluating conditional requests
            return self._handle_get_head_request()

        if self.req.method in ('PUT', 'DELETE'):
            return self._handle_put_delete_request()

        return self.req.get_response(self.app)


class MPUContainerHandler(BaseMPUHandler):
    def _update_resp_headers(self, resp):
        # TODO: implement similar in base get_container_info()
        bytes_used = resp.headers.get('X-Container-Bytes-Used')
        if not bytes_used:
            return

        parts_bytes = None
        for key, value in resp.headers.items():
            if not key.lower().startswith(
                    'x-container-sysmeta-mpu-parts-container-'):
                continue
            parts_container = urllib.parse.unquote(value)
            path = self.make_path(self.account, parts_container)
            parts_req = self.make_subrequest('HEAD', path)
            parts_resp = parts_req.get_response(self.app)
            if parts_resp.is_success:
                parts_bytes = (parts_bytes or 0) + int(
                    parts_resp.headers.get('X-Container-Bytes-Used', 0))

        if parts_bytes is not None:
            resp.headers['X-Container-Bytes-Used'] = str(
                int(bytes_used) + parts_bytes)
            resp.headers['X-Container-Mpu-Parts-Bytes-Used'] = str(parts_bytes)

    def _process_json_resp(self, resp):
        body_json = json.loads(resp.body)
        for item in body_json:
            if 'hash' not in item:
                continue

            # SLO will already have extracted the size from swift_bytes
            hash_value, params = parse_content_type(item['hash'])
            new_params = []
            mpu_etag = None
            for k, v in params:
                if k == 'mpu_etag':
                    mpu_etag = v
                elif k == 'mpu_link':
                    continue
                else:
                    new_params.append((k, v))

            if mpu_etag is None:
                continue

            # put back any etag params that may be the responsibility of
            # other middlewares...
            item['hash'] = mpu_etag + ''.join('; %s=%s' % kv
                                              for kv in new_params)
            # hide the implementation details from the user
            item.pop('slo_etag', None)
        resp.body = json.dumps(body_json).encode('ascii')

    def handle_request(self):
        resp = self.req.get_response(self.app)
        if not resp.is_success:
            return resp

        if self.req.method == 'GET':
            self._process_json_resp(resp)
            self._update_resp_headers(resp)
        elif self.req.method == 'HEAD':
            self._update_resp_headers(resp)
        elif self.req.method == 'PUT' and resp.is_success:
            # TODO: ok to always use default policy for history?
            policy_index = POLICIES.default
            self._ensure_container_exists(
                self.hidden_account, self.history_container, policy_index)
        elif self.req.method == 'DELETE':
            pass
            # TODO: implement
            #   check these are empty before deleting the user container?
            #   but they may not be until the mpu-auditor runs, and even then
            #   there may be undeleted orphans
            # self._delete_container(self.parts_container)
            # self._delete_container(self.sessions_container)

        return resp


class MPUMiddleware:
    def __init__(self, app, conf, logger=None):
        self.conf = conf
        self.app = app
        self.logger = logger or get_logger(conf, log_route='slo')
        self.upload_id_signing_key = base64.b64decode(
            conf.get('upload_id_key', '').encode('ascii'))
        self.min_part_size = config_positive_int_value(
            conf.get('min_part_size', DEFAULT_MIN_PART_SIZE))
        self.max_part_number = config_positive_int_value(
            conf.get('max_part_number', DEFAULT_MAX_PART_NUMBER))
        self.max_name_length = calculate_max_name_length()
        register_swift_info('mpu',
                            max_part_number=self.max_part_number,
                            min_part_size=self.min_part_size,
                            max_name_length=self.max_name_length)

    def externalize_upload_id(self, path, upload_id):
        return externalize_upload_id(
            self.upload_id_signing_key, path, upload_id)

    def internalize_upload_id(self, path, upload_id):
        return internalize_upload_id(
            self.upload_id_signing_key, path, upload_id)

    def get_valid_upload_id(self, req):
        """
        Try to extract an upload id from request params.

        :param req: an instance of swob.Request
        :raises HTTPBadRequest: if the ``upload-id`` parameter is found but is
            invalid.
        :returns: an instance of UploadId, or None if the ``upload-id``
            parameter is not found.
        """
        if 'upload-id' not in req.params:
            return None

        upload_id_str = req.params['upload-id']
        try:
            upload_id = self.internalize_upload_id(req.path, upload_id_str)
        except ValueError as err:
            raise HTTPBadRequest(MPU_INVALID_UPLOAD_ID_MSG + ' (%s): %s'
                                 % (str(err), upload_id_str))
        return upload_id

    def handle_request(self, req, container, obj):
        # this defines the MPU API
        upload_id = self.get_valid_upload_id(req)
        part_number = get_valid_part_num(req)
        if obj and upload_id:
            if req.method == 'PUT' and part_number is not None:
                resp = MPUSessionHandler(self, req, upload_id).upload_part(
                    part_number)
            elif req.method == 'GET':
                resp = MPUSessionHandler(self, req, upload_id).list_parts()
            elif req.method == 'POST':
                resp = MPUSessionHandler(
                    self, req, upload_id).complete_upload()
            elif req.method == 'DELETE':
                resp = MPUSessionHandler(self, req, upload_id).abort_upload()
            else:
                resp = None
        elif container and 'uploads' in req.params:
            if req.method == 'GET':
                resp = MPUSessionsHandler(self, req).list_uploads()
            elif obj and req.method == 'POST':
                resp = MPUSessionsHandler(self, req).create_upload()
            else:
                resp = None
        elif obj:
            resp = MPUObjHandler(self, req).handle_request()
        elif container:
            resp = MPUContainerHandler(self, req).handle_request()
        else:
            resp = None
        # TODO: should we return 405 for any unsupported container?uploads
        #     method? Swift typically ignores unrecognised headers and
        #     params, but there is a risk that the user thinks that, for
        #     example, DELETE container?uploads will just abort all the MPU
        #     sessions (whereas it might delete the container).
        return resp

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if is_reserved_name(account, container, obj):
            return self.app(env, start_response)

        try:
            resp = self.handle_request(req, container, obj)
        except HTTPException as err:
            resp = err

        resp = resp or self.app
        return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def mpu_filter(app):
        return MPUMiddleware(app, conf,)
    return mpu_filter
