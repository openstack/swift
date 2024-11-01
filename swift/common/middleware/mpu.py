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
import binascii
import json

import six

from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import HTTP_CONFLICT, is_success, HTTP_NOT_FOUND, \
    HTTP_TEMPORARY_REDIRECT
from swift.common.middleware.symlink import TGT_OBJ_SYMLINK_HDR, \
    ALLOW_RESERVED_NAMES
from swift.common.storage_policy import POLICIES
from swift.common.utils import generate_unique_id, drain_and_close, \
    config_positive_int_value, reiterate, parse_content_type, \
    decode_timestamps, hash_path, split_path
from swift.common.swob import Request, normalize_etag, \
    wsgi_to_str, wsgi_quote, HTTPInternalServerError, HTTPOk, \
    HTTPConflict, HTTPBadRequest, HTTPException, HTTPNotFound, HTTPNoContent, \
    HTTPServiceUnavailable, quote_etag, wsgi_unquote, HTTPAccepted, HTTPCreated
from swift.common.utils import get_logger, Timestamp, md5, public
from swift.common.registry import register_swift_info
from swift.common.request_helpers import get_reserved_name, \
    get_valid_part_num, is_reserved_name, split_reserved_name, is_user_meta, \
    update_etag_override_header, update_etag_is_at_header, \
    validate_part_number, update_content_type, is_sys_meta
from swift.common.wsgi import make_pre_authed_request
from swift.proxy.controllers.base import get_container_info

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024
MPU_SWIFT_SOURCE = 'MPU'
MPU_SYSMETA_PREFIX = 'x-object-sysmeta-mpu-'
MPU_SYSMETA_USER_PREFIX = MPU_SYSMETA_PREFIX + 'user-'
MPU_TRANSIENT_SYSMETA_PREFIX = 'x-object-transient-sysmeta-mpu-'
MPU_SESSION_CREATED_CONTENT_TYPE = 'application/x-mpu-session-created'
MPU_SESSION_ABORTED_CONTENT_TYPE = 'application/x-mpu-session-aborted'
MPU_SESSION_COMPLETED_CONTENT_TYPE = 'application/x-mpu-session-completed'
MPU_MANIFEST_DEFAULT_CONTENT_TYPE = 'application/x-mpu'
MPU_MARKER_CONTENT_TYPE = 'application/x-mpu-marker'
MPU_GENERIC_MARKER_SUFFIX = 'marker'
MPU_DELETED_MARKER_SUFFIX = MPU_GENERIC_MARKER_SUFFIX + '-deleted'
MPU_ABORTED_MARKER_SUFFIX = MPU_GENERIC_MARKER_SUFFIX + '-aborted'


def get_mpu_sysmeta_key(key):
    return MPU_SYSMETA_PREFIX + key


def get_mpu_transient_sysmeta_key(key):
    return MPU_TRANSIENT_SYSMETA_PREFIX + key


def strip_mpu_sysmeta_prefix(key):
    return key[len(MPU_SYSMETA_PREFIX):]


MPU_SYSMETA_UPLOAD_ID_KEY = get_mpu_sysmeta_key('upload-id')
MPU_SYSMETA_ETAG_KEY = get_mpu_sysmeta_key('etag')
MPU_SYSMETA_PARTS_COUNT_KEY = get_mpu_sysmeta_key('parts-count')
MPU_SYSMETA_USER_CONTENT_TYPE_KEY = get_mpu_sysmeta_key('content-type')

MPU_INVALID_UPLOAD_ID_MSG = 'Invalid upload-id'
MPU_NO_SUCH_UPLOAD_ID_MSG = 'No such upload-id'


def get_req_upload_id(req):
    """
    Try to extract an upload id from request params.

    :param req: an instance of swob.Request
    :raises HTTPBadRequest: if the ``upload-id`` parameter is found but is
        invalid.
    :returns: an instance of MPUId, or None if the ``upload-id`` parameter is
        not found.
    """
    if 'upload-id' in req.params:
        try:
            return MPUId.parse(req.params['upload-id'], path=req.path)
        except ValueError:
            raise HTTPBadRequest(MPU_INVALID_UPLOAD_ID_MSG)
    else:
        return None


def normalize_part_number(part_number):
    return '%06d' % int(part_number)


def translate_error_response(sub_resp):
    if (sub_resp.status_int not in swob.RESPONSE_REASONS or
            sub_resp.status_int == 404):
        client_resp_status_int = 503
    else:
        client_resp_status_int = sub_resp.status_int
    return swob.status_map[client_resp_status_int]()


class MPUEtagHasher(object):
    def __init__(self):
        self.hasher = md5(usedforsecurity=False)
        self.part_count = 0

    def update(self, part_etag):
        self.hasher.update(binascii.a2b_hex(normalize_etag(part_etag)))
        self.part_count += 1

    @property
    def etag(self):
        return '%s-%d' % (self.hasher.hexdigest(), self.part_count)


class MPUId(object):
    """
    Encapsulates properties of an MPU id.

    An MPU id is composed of three parts:
      * a timestamp
      * a uuid
      * a tag

    The tag is a hash of a combination of the uuid and a path passed in when
    the id is created. This enables a rudimentary check that an MPU id is
    associated with a path.
    """
    # ~ doesn't get url-encoded and isn't in url-safe base64 encoded unique ids
    ID_DELIMITER = '~'

    __slots__ = ('uuid', 'timestamp', 'tag')

    def __init__(self, uuid, timestamp, tag):
        # don't call this: use either parse or create
        self.uuid = uuid
        self.timestamp = timestamp
        self.tag = tag

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        # MPU listing should be sorted by (<object name>, <creation time>)
        # so we put the timestamp before the uuid.
        return self.ID_DELIMITER.join(
            (self.timestamp.internal, self.uuid, self.tag))

    @classmethod
    def create(cls, path, timestamp):
        """
        Create a unique MPUId.

        :param timestamp: a timestamp whose value will be embedded in the
            ``MPUId``.
        :param path: a path to which the ``MPUId`` will be uniquely associated;
            a transformation of the ``path`` is embedded in the string
            representation of the ``MPUId`` so that the ``MPUId`` can be
            checked for association with a path when parsed.
        :returns: an instance of ``MPUId``.
        """
        uid = generate_unique_id()
        _, account, container, obj = split_path(path, 4, 4, True)
        # NB: hash_path is used to obfuscate the actual path; the path that is
        # hashed is not a path to any actual resource
        tag = hash_path(account, container, get_reserved_name(obj, uid))
        return cls(uid, Timestamp(timestamp), tag)

    @classmethod
    def parse(cls, value, path=None):
        """
        Parse an ``MPUId`` from the given value.

        :param value: a string representation of an ``MPUId``.
        :param path: (optional) if ``path`` is given then the parsed ``MPUId``
            is checked for unique association with the ``path``; if the parsed
            ``MPUId`` is not associated with the given ``path`` then a
            ValueError is raised.
        :returns: an instance of `MPUId``.
        """
        parts = value.strip().split(cls.ID_DELIMITER)
        if not all(parts) or len(parts) != 3:
            raise ValueError
        ts, uuid, parsed_tag = parts
        if path:
            _, account, container, obj = split_path(path, 4, 4, True)
            tag = hash_path(account, container, get_reserved_name(obj, uuid))
            if tag != parsed_tag:
                raise ValueError
        return cls(uuid, Timestamp(ts), parsed_tag)


class MPUItem(object):
    def __init__(self, name, meta_timestamp, data_timestamp=None,
                 ctype_timestamp=None,
                 size=0, content_type='', etag='', deleted=0,
                 storage_policy_index=0, **kwargs):
        self._name = name
        self.timestamp = self.meta_timestamp = meta_timestamp
        self.data_timestamp = data_timestamp or meta_timestamp
        self.ctype_timestamp = ctype_timestamp or meta_timestamp
        self.timestamp = self.meta_timestamp  # for clarity
        self.size = size
        self.content_type = content_type
        self.etag = etag
        self.deleted = deleted
        self.storage_policy_index = storage_policy_index
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
                'meta_timestamp': self.meta_timestamp.internal}


class MPUSession(MPUItem):
    """
    Encapsulates the state of an MPU session as it progresses from being
    created to being either completed or aborted.

    Session state is represented by the session object's content-type so that
    it appears in both the object's metadata and the container listing.

    A new session is in state 'created'. The state may transition to either
    'aborted' or 'completed'.

    The 'aborted' state is tentative. A client abortUpload request may cause a
    session to transition to the 'aborted' state while a concurrent
    completeUpload is being handled, and the completeUpload will typically
    continue to succeed. The mpu-auditor is responsible for resolving any
    ambiguity.

    The 'completed' state is definitive: a session only transitions to the
    'completed' state at the end of a completeUpload, after the associated
    user-namespace object has been linked to a manifest object.
    """
    STATE_KEY = get_mpu_transient_sysmeta_key('state')
    CREATED_STATE = 'created'
    COMPLETING_STATE = 'completing'
    STATES = (CREATED_STATE, COMPLETING_STATE)

    def __init__(self, name,
                 meta_timestamp,
                 data_timestamp=None,
                 ctype_timestamp=None,
                 content_type=MPU_SESSION_CREATED_CONTENT_TYPE,
                 state=CREATED_STATE,
                 headers=None,
                 **kwargs):
        super(MPUSession, self).__init__(
            name=name,
            meta_timestamp=meta_timestamp,
            data_timestamp=data_timestamp,
            ctype_timestamp=ctype_timestamp,
            content_type=content_type,
            **kwargs)
        self.headers = HeaderKeyDict(headers)
        # TODO: _state is going to go away in a following patch when the
        #   mpu-auditor learns to deal with sessions
        self._state = state

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
        state = backend_headers.get(cls.STATE_KEY)
        return cls(name, timestamp, content_type=content_type, state=state,
                   headers=backend_headers, data_timestamp=data_timestamp)

    @property
    def is_active(self):
        return not (self.is_completed or self.is_aborted)

    @property
    def is_aborted(self):
        return self.content_type == MPU_SESSION_ABORTED_CONTENT_TYPE

    def set_aborted(self, timestamp):
        self.content_type = MPU_SESSION_ABORTED_CONTENT_TYPE
        self.ctype_timestamp = timestamp

    @property
    def is_completed(self):
        return self.content_type == MPU_SESSION_COMPLETED_CONTENT_TYPE

    def set_completed(self, timestamp):
        self.content_type = MPU_SESSION_COMPLETED_CONTENT_TYPE
        self.ctype_timestamp = timestamp

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        if state not in self.STATES:
            # TODO: test invalid case
            raise ValueError
        self._state = state

    def get_put_headers(self):
        headers = HeaderKeyDict({
            'X-Timestamp': self.data_timestamp.internal,
            'Content-Type': self.content_type,
            'Content-Length': '0',
            self.STATE_KEY: self.state,
        })
        headers.update(self.headers)
        return headers

    def get_post_headers(self):
        return HeaderKeyDict({
            'X-Timestamp': self.timestamp.internal,
            'Content-Type': self.content_type,
            self.STATE_KEY: self.state,
        })

    def get_manifest_headers(self):
        headers = HeaderKeyDict()
        for key, val in self.headers.items():
            key_lower = key.lower()
            if key_lower.startswith(MPU_SYSMETA_USER_PREFIX):
                headers[key[len(MPU_SYSMETA_USER_PREFIX):]] = val
            elif key_lower == MPU_SYSMETA_USER_CONTENT_TYPE_KEY:
                headers['Content-Type'] = val
            elif key_lower.startswith(MPU_SYSMETA_PREFIX):
                continue
            elif is_sys_meta('object', key_lower):
                headers[key] = val
        return headers

    def get_user_content_type(self):
        return self.headers.get(MPU_SYSMETA_USER_CONTENT_TYPE_KEY)


class BaseMPUHandler(object):
    def __init__(self, mw, req):
        self.mw = mw
        self.app = mw.app
        self.logger = mw.logger
        self.req = req
        try:
            _, self.account, self.container, self.obj = req.split_path(
                4, 4, True)
            self.reserved_obj = get_reserved_name(self.obj)
        except ValueError:
            _, self.account, self.container = req.split_path(3, 3, False)
            self.obj = self.reserved_obj = None

        self.sessions_container = get_reserved_name('mpu_sessions',
                                                    self.container)
        # TODO: make this the versions container...
        self.manifests_container = get_reserved_name('mpu_manifests',
                                                     self.container)
        self.parts_container = get_reserved_name('mpu_parts', self.container)

    def _check_user_container_exists(self):
        info = get_container_info(self.req.environ, self.app,
                                  swift_source=MPU_SWIFT_SOURCE)
        if is_success(info['status']):
            return info
        elif info['status'] == HTTP_NOT_FOUND:
            raise HTTPNotFound()
        else:
            raise HTTPServiceUnavailable()

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

    def make_relative_path(self, *parts):
        return '/'.join(str(p) for p in parts)

    def make_path(self, *parts):
        return '/'.join(
            ['', 'v1', self.account, self.make_relative_path(*parts)])

    def make_subrequest(self, method=None, path=None, body=None,
                        headers=None, params=None):
        """
        Make a pre-auth'd sub-request based on ``self.req``.

        The sub-request does *not* inherit headers or query-string parameters
        from ``self.req``.

        The sub-request will have a truthy 'X-Backend-Allow-Reserved-Names'
        header that its swift source will be set to 'MPU'.

        :param method: the sub-request method.
        :param path: the sub-request path.
        :param body: the sub-request body.
        :param headers: a dict of headers for the sub-request.
        :param params: a dict of query-string parameters for the sub-request.
        """
        req_headers = {'X-Backend-Allow-Reserved-Names': 'true'}
        if headers:
            req_headers.update(headers)
        sub_req = make_pre_authed_request(
            self.req.environ, path=wsgi_quote(path), method=method,
            headers=req_headers, swift_source=MPU_SWIFT_SOURCE, body=body)
        sub_req.params = params or {}
        return sub_req

    def _put_delete_marker(self, marker_path):
        headers = {'Content-Type': MPU_MARKER_CONTENT_TYPE,
                   'Content-Length': '0'}
        marker_req = self.make_subrequest(
            'PUT', path=marker_path, headers=headers)
        marker_resp = marker_req.get_response(self.app)
        drain_and_close(marker_resp)
        if not (marker_resp.is_success or marker_resp.status_int == 409):
            raise marker_resp
        else:
            return marker_resp

    def _put_manifest_delete_marker(self, upload_id, marker_type):
        marker_path = self.make_path(
            self.manifests_container, self.reserved_obj, upload_id,
            marker_type)
        self._put_delete_marker(marker_path)

    def _put_parts_delete_marker(self, upload_id):
        marker_path = self.make_path(
            self.parts_container, self.reserved_obj, upload_id,
            MPU_DELETED_MARKER_SUFFIX)
        self._put_delete_marker(marker_path)


class MPUSessionsHandler(BaseMPUHandler):
    """
    Handles the following APIs:

    * List Multipart Uploads
    * Initiate Multipart Upload
    """
    def __init__(self, mw, req):
        super(MPUSessionsHandler, self).__init__(mw, req)
        self.user_container_info = self._check_user_container_exists()

    @public
    def list_uploads(self):
        """
        Handles List Multipart Uploads
        """
        self._authorize_read_request()
        path = self.make_path(self.sessions_container)

        params = {}
        for key in ('prefix', 'marker', 'end_marker'):
            value = self.req.params.get(key)
            if value:
                params[key] = get_reserved_name(value)
        if 'limit' in self.req.params:
            params['limit'] = self.req.params['limit']

        sub_req = self.make_subrequest(
            path=path, method='GET', params=params)
        resp = sub_req.get_response(self.app)
        if resp.is_success:
            listing = []
            for item in json.loads(resp.body):
                if item['content_type'] != MPU_SESSION_CREATED_CONTENT_TYPE:
                    continue
                item['name'] = split_reserved_name(item['name'])[0]
                listing.append(item)
            resp.body = json.dumps(listing).encode('ascii')
        return resp

    def _ensure_container_exists(self, container):
        # TODO: make storage policy specific parts bucket
        policy_name = POLICIES[self.user_container_info['storage_policy']].name

        # container_name = wsgi_unquote(wsgi_quote(container_name))
        path = self.make_path(container)
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
                    'Error creating MPU resource container')

    @public
    def create_upload(self):
        """
        Handles Initiate Multipart Upload.
        """
        self._authorize_write_request()
        # TODO: can we just call constraints.check_object_creation ?
        # if len(req.object_name) > constraints.MAX_OBJECT_NAME_LENGTH:
        #     # Note that we can still run into trouble where the MPU is just
        #     # within the limit, which means the segment names will go over
        #     raise KeyTooLongError()

        self._ensure_container_exists(self.sessions_container)
        self._ensure_container_exists(self.manifests_container)
        self._ensure_container_exists(self.parts_container)

        upload_id = MPUId.create(self.req.path, self.req.ensure_x_timestamp())
        self.req.headers.pop('Etag', None)
        self.req.headers.pop('Content-Md5', None)
        update_content_type(self.req)
        session_name = self.make_relative_path(self.reserved_obj, upload_id)
        session_path = self.make_path(self.sessions_container, session_name)
        session = MPUSession.from_user_headers(session_name, self.req.headers)
        session_req = self.make_subrequest(
            path=session_path,
            method='PUT',
            headers=session.get_put_headers()
        )

        session_resp = session_req.get_response(self.app)
        if session_resp.is_success:
            drain_and_close(session_resp)
            resp_headers = {'X-Upload-Id': str(upload_id)}
            resp = HTTPAccepted(headers=resp_headers)
        else:
            self.logger.warning('MPU %s %s', session_resp.status,
                                session_resp.body)
            resp = HTTPInternalServerError()
        return resp


class MPUSloCallbackHandler(object):
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
    def __init__(self, mw, req):
        super(MPUSessionHandler, self).__init__(mw, req)
        self.user_container_info = self._check_user_container_exists()
        self.upload_id = get_req_upload_id(req)
        self.session_name = self.make_relative_path(
            self.reserved_obj, self.upload_id)
        self.session_path = self.make_path(self.sessions_container,
                                           self.session_name)
        self.req.headers.setdefault('X-Timestamp', Timestamp.now().internal)
        self.manifest_relative_path = self.make_relative_path(
            self.manifests_container, self.reserved_obj, self.upload_id)
        self.manifest_path = self.make_path(self.manifest_relative_path)

    def _load_session(self):
        # TODO: check that if the request has a timestamp then it is later than
        #   the session created time, if not then 409 or 503
        req = self.make_subrequest(method='HEAD', path=self.session_path)
        resp = req.get_response(self.app)
        if resp.status_int == 404:
            raise HTTPNotFound(MPU_NO_SUCH_UPLOAD_ID_MSG)
        elif not resp.is_success:
            raise HTTPInternalServerError()

        session = MPUSession.from_session_headers(self.session_name,
                                                  resp.headers)
        return session

    def _delete_session(self):
        session_req = self.make_subrequest(
            path=self.session_path, method='DELETE')
        # TODO: check session_resp
        session_resp = session_req.get_response(self.app)
        drain_and_close(session_resp)

    def _get_user_object_metadata(self):
        symlink_path = self.make_path(self.container, self.obj)
        req = self.make_subrequest(method='HEAD', path=symlink_path)
        resp = req.get_response(self.app)
        if resp.is_success:
            return resp.headers
        else:
            return {}

    def upload_part(self, part_number):
        self._authorize_write_request()
        session = self._load_session()
        if session.state not in (MPUSession.CREATED_STATE,
                                 MPUSession.COMPLETING_STATE):
            return HTTPNotFound()
        if session.is_aborted:
            return HTTPNotFound()
        part_path = self.make_path(self.parts_container,
                                   self.reserved_obj,
                                   self.upload_id,
                                   normalize_part_number(part_number))
        self.logger.debug('mpu upload_part %s', part_path)
        headers = {}
        for k, v in self.req.headers.items():
            # TODO: should we return 400 is client sends x-delete-at with a
            #   part upload, or just ignore it?
            if k.lower() in ('content-length', 'transfer-encoding', 'etag'):
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
            resp = translate_error_response(sub_resp)
        return resp

    def list_parts(self):
        """
        Handles List Parts.
        """
        self._authorize_read_request()
        session = self._load_session()  # verify existence of the upload-id
        if session.state not in (MPUSession.CREATED_STATE,
                                 MPUSession.COMPLETING_STATE):
            return HTTPNotFound()
        if session.is_aborted:
            return HTTPNotFound()
        path = self.make_path(self.parts_container)

        params = {'prefix': self.session_name}
        try:
            part_number_marker = validate_part_number(
                self.req.params.get('part-number-marker'))
        except ValueError:
            raise HTTPBadRequest(
                'part-number-marker must be an integer greater than 0')
        if part_number_marker is not None:
            params['marker'] = '%s/%s' % (
                self.session_name, normalize_part_number(part_number_marker))
        if 'limit' in self.req.params:
            params['limit'] = self.req.params['limit']

        sub_req = self.make_subrequest(
            path=path, method='GET', params=params)
        sub_resp = sub_req.get_response(self.app)
        if sub_resp.is_success:
            listing = json.loads(sub_resp.body)
            for item in listing:
                item['name'] = split_reserved_name(item['name'])[0]
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
            resp = translate_error_response(sub_resp)
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
                # TODO: further checks on upload-id to verify that it *ever*
                #   existed (e.g. we could sign upload-ids that are issued)
                return HTTPNoContent()
            else:
                return err

        if self.req.timestamp < session.data_timestamp:
            return HTTPConflict()

        user_obj_metadata = self._get_user_object_metadata()
        if user_obj_metadata.get(TGT_OBJ_SYMLINK_HDR) == \
                self.manifest_relative_path:
            return HTTPConflict()

        # Update the session to be marked as aborted. This will prevent any
        # subsequent complete operation from proceeding.
        session.timestamp = self.req.timestamp
        session.set_aborted(self.req.timestamp)
        session_req = self.make_subrequest(
            'POST',
            path=self.session_path,
            headers=session.get_post_headers()
        )
        # TODO: check response
        session_req.get_response(self.app)
        # Write down an audit-marker in the manifests container that will cause
        # the auditor to check the status of the mpu and possibly cleanup the
        # manifest and parts.
        self._put_manifest_delete_marker(self.upload_id,
                                         MPU_ABORTED_MARKER_SUFFIX)
        # delete the session
        self._delete_session()
        return HTTPNoContent()

    def _parse_part_number(self, part_dict, previous_part):
        try:
            part_number = part_dict['part_number']
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
        parsed_manifest = []
        mpu_etag_hasher = MPUEtagHasher()
        previous_part = 0
        for part_index, part_dict in enumerate(user_manifest):
            if not isinstance(part_dict, dict):
                errors.append("Index %d: not a JSON object." % part_index)
                continue
            try:
                part_number = self._parse_part_number(part_dict, previous_part)
            except ValueError as err:
                errors.append("Index %d: %s." % (part_index, err))
            try:
                etag = self._parse_etag(part_dict)
            except ValueError as err:
                errors.append("Index %d: %s." % (part_index, str(err)))
            if not errors:
                part_path = self.make_relative_path(
                    self.parts_container,
                    self.reserved_obj,
                    self.upload_id,
                    normalize_part_number(part_number))
                parsed_manifest.append({
                    'path': wsgi_to_str(part_path),
                    'etag': etag})
                mpu_etag_hasher.update(etag)

        if errors:
            error_message = b"".join(e.encode('utf8') + b"\n" for e in errors)
            raise HTTPBadRequest(error_message,
                                 headers={"Content-Type": "text/plain"})
        return parsed_manifest, mpu_etag_hasher.etag

    def _parse_slo_errors(self, slo_resp_dict):
        resp_dict = {'Response Status': '400 Bad Request'}
        errors = []
        for path, reason in slo_resp_dict.get('Errors', []):
            part_number = path.rsplit('/')[-1]
            errors.append([part_number, reason])
        resp_dict['Errors'] = errors
        return resp_dict

    def _put_manifest(self, session, manifest, mpu_etag):
        # create manifest in hidden container
        manifest_headers = {
            'X-Timestamp': session.data_timestamp.internal,
            'Accept': 'application/json',
            ALLOW_RESERVED_NAMES: 'true',
            MPU_SYSMETA_UPLOAD_ID_KEY: str(self.upload_id),
            MPU_SYSMETA_ETAG_KEY: mpu_etag,
            MPU_SYSMETA_PARTS_COUNT_KEY: str(len(manifest)),
            # TODO: include max part index in sysmeta.
            #   There *may* turn out to be situations where knowing that
            #   max part index is useful/enable optimisation. For example,
            #   if parts_count == max part index then we know that the index of
            #   every part in the manifest is equal to the part number used
            #   when it was uploaded i.e. we can infer the path to a part
            #   without a GET for the manifest body.
        }
        manifest_headers.update(session.get_manifest_headers())
        # set the MPU etag override to be forwarded to the manifest container
        update_etag_override_header(
            manifest_headers, mpu_etag, [('mpu_etag', mpu_etag)])
        params = {'multipart-manifest': 'put', 'heartbeat': 'on'}
        manifest_req = self.make_subrequest(path=self.manifest_path,
                                            method='PUT',
                                            headers=manifest_headers,
                                            body=json.dumps(manifest),
                                            params=params)
        slo_callback_handler = MPUSloCallbackHandler(self.mw)
        manifest_req.environ['swift.callback.slo_manifest_hook'] = \
            slo_callback_handler
        self.logger.debug('mpu manifest PUT %s %s',
                          manifest_req.path, dict(manifest_req.headers))
        return manifest_req.get_response(self.app), slo_callback_handler

    def _put_symlink(self, session, mpu_etag, mpu_bytes):
        # create symlink in user container pointing to manifest
        symlink_path = self.make_path(self.container, self.obj)
        symlink_headers = HeaderKeyDict({
            'X-Timestamp': session.data_timestamp.internal,
            'Content-Length': '0',
            'Content-Type': session.get_user_content_type(),
            ALLOW_RESERVED_NAMES: 'true',
            MPU_SYSMETA_UPLOAD_ID_KEY: str(self.upload_id),
            TGT_OBJ_SYMLINK_HDR: self.manifest_relative_path,
        })
        # set the MPU etag override to be forwarded to the user container
        update_etag_override_header(
            symlink_headers, mpu_etag,
            [('mpu_etag', mpu_etag), ('mpu_bytes', mpu_bytes)])
        mpu_req = self.make_subrequest(
            path=symlink_path, method='PUT', headers=symlink_headers)
        mpu_resp = mpu_req.get_response(self.app)
        return mpu_resp

    def _make_complete_upload_resp_iter(self, session, manifest, mpu_etag):
        def response_iter():
            # TODO: add support for versioning?? copied from multi_upload.py
            manifest_resp, slo_callback_handler = self._put_manifest(
                session, manifest, mpu_etag)
            if not manifest_resp.is_success:
                yield json.dumps(
                    {'Response Status': '503 Service Unavailable',
                     'Response Body':
                         manifest_resp.body.decode('utf-8', errors='replace')
                         if six.PY3 else manifest_resp.body}
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
                # TODO: maybe repeat check that session has not been
                #   aborted? if it has been aborted then stop right here,
                #   return 409, and leave things for the auditor to clean up.
                #   Alternatively, don't check - assume we'll complete the
                #   symlink before the auditor handles the aborted session.
                mpu_bytes = slo_callback_handler.total_bytes
                mpu_resp = self._put_symlink(session, mpu_etag, mpu_bytes)
                drain_and_close(mpu_resp)
                if mpu_resp.status_int == 201:
                    # NB: etag is response body is not quoted
                    body_dict['Etag'] = normalize_etag(mpu_etag)
                    yield json.dumps(body_dict).encode('ascii')
                    # TODO: move _delete_session before the yield??
                    #   pro: we can delete session object before waiting for
                    #   user to read this response iter; we want to delete the
                    #   session ASAP just in case it is pending abort cleanup
                    #   by the auditor.
                    #   con: we delete session and then fail to yield this
                    #   response iter. Client will have received a 201 but
                    #   doesn't get the confirmation of success from the
                    #   response body.
                    # clean up the multipart-upload record
                    self._delete_session()
                else:
                    yield json.dumps(
                        {'Response Status': mpu_resp.status}
                    ).encode('ascii')
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

        manifest, mpu_etag = self._parse_user_manifest(self.req.body)
        try:
            session = self._load_session()
        except HTTPException as err:
            if err.status_int != 404:
                return
            user_obj_metadata = self._get_user_object_metadata()
            if (user_obj_metadata.get(MPU_SYSMETA_ETAG_KEY) == mpu_etag
                    and user_obj_metadata.get(MPU_SYSMETA_UPLOAD_ID_KEY) ==
                    self.upload_id):
                # session was previously completed, tolerate the retry
                body_dict = {
                    'Response Status': '201 Created',
                    'Etag': mpu_etag,
                    'Last Modified': user_obj_metadata.get('Last-Modified'),
                    'Response Body': '',
                    'Errors': [],
                }
                return HTTPAccepted(body=json.dumps(body_dict).encode('ascii'))
            else:
                return err

        if self.req.timestamp < session.data_timestamp:
            return HTTPConflict()

        if session.is_aborted:
            # The session has been previously aborted but not yet successfully
            # deleted. Refuse to complete. The abort may be concurrent or may
            # have failed to delete the session. Either way, we refuse to
            # complete the upload.
            return HTTPConflict()

        # TODO: check if a manifest already exists with same mpu_etag, if it
        #   does then skip to putting the symlink to the manifest
        session.timestamp = self.req.timestamp
        session.state = MPUSession.COMPLETING_STATE
        session_req = self.make_subrequest(
            'POST',
            path=self.session_path,
            headers=session.get_post_headers()
        )
        # TODO: check response
        session_req.get_response(self.app)

        # TODO: replicate the etag handling from s3api
        # Leave base header value blank; SLO will populate
        # c_etag = '; s3_etag=%s' % manifest_etag
        # manifest_headers[get_container_update_override_key('etag')] = c_etag
        resp = HTTPAccepted()  # assume we're good for now...
        resp.app_iter = self._make_complete_upload_resp_iter(
            session, manifest, mpu_etag)
        self.logger.debug('mpu complete_upload %s', self.req.path)
        return resp


class MPUObjHandler(BaseMPUHandler):
    def _maybe_cleanup_mpu(self, resp):
        # NB: do this even for non-success responses in case any of the
        # backend responses may have succeeded
        if 'x-object-version-id' in resp.headers:
            # TODO: unit test early return
            # existing object became a version -> no cleanup
            return

        upload_id_key = get_mpu_sysmeta_key('upload-id')
        deleted_upload_ids = {}
        for backend_resp in self.req.environ.get('swift.backend_responses',
                                                 []):
            if not is_success(backend_resp.status):
                continue
            # TODO: maybe add more conditions so we're sure it was MPU manifest
            #   e.g. check that backend_resp has x-symlink-target and cross
            #   check its value with expected manifest path
            upload_id_val = backend_resp.headers.get(upload_id_key)
            if upload_id_val:
                try:
                    upload_id = MPUId.parse(upload_id_val)
                    deleted_upload_ids[upload_id] = backend_resp
                except ValueError:
                    # TODO: log a warning?
                    pass
        for upload_id, backend_resp in deleted_upload_ids.items():
            # TODO: unit test multiple upload cleanup
            self._put_manifest_delete_marker(upload_id,
                                             MPU_DELETED_MARKER_SUFFIX)

    def _handle_get_head_response(self, resp):
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
                new_headers['x-upload-id'] = val
            elif key.startswith(MPU_SYSMETA_PREFIX):
                continue
            else:
                new_headers[key] = val
        if mpu_etag:
            # mpu mw always quotes response header etag for requests it handles
            new_headers['etag'] = quote_etag(mpu_etag)
        resp.headers = new_headers

    def _handle_post_response(self, resp):
        if resp.status_int != HTTP_TEMPORARY_REDIRECT:
            return resp

        drain_and_close(resp)
        manifest_path = wsgi_unquote(resp.headers.get('location'))
        marker_req = self.make_subrequest(
            'POST', path=manifest_path, headers=self.req.headers)
        sub_resp = marker_req.get_response(self.app)
        drain_and_close(sub_resp)
        if sub_resp.is_success:
            new_resp = HTTPAccepted(request=self.req, headers=sub_resp.headers)
        else:
            new_resp = translate_error_response(sub_resp)
            new_resp.request = self.req

        return new_resp

    def handle_request(self):
        if self.req.method in ('GET', 'HEAD'):
            # instruct the object server to look for an mpu-etag in sysmeta
            # for evaluating conditional requests
            update_etag_is_at_header(self.req, MPU_SYSMETA_ETAG_KEY)

        resp = self.req.get_response(self.app)

        upload_id = resp.headers.get(MPU_SYSMETA_UPLOAD_ID_KEY)
        if self.req.method in ('GET', 'HEAD') and upload_id:
            self._handle_get_head_response(resp)
        elif self.req.method == 'POST' and upload_id:
            resp = self._handle_post_response(resp)
        elif self.req.method in ('PUT', 'DELETE'):
            # TODO: write down a general maybe-deleted marker in manifests
            #  container *before* forwarding request
            self._maybe_cleanup_mpu(resp)

        return resp


class MPUContainerHandler(BaseMPUHandler):
    def _process_json_resp(self, resp):
        body_json = json.loads(resp.body)
        for item in body_json:
            if 'hash' not in item:
                continue

            # When symlink middleware validates the static symlink to the SLO
            # manifest it gets the container update override etag params from
            # the SLO manifest and adds them the symlink's etag params.
            hash_value, params = parse_content_type(item['hash'])
            new_params = []
            mpu_etag = mpu_bytes = None
            for k, v in params:
                if k == 'mpu_etag':
                    mpu_etag = v
                elif k == 'mpu_bytes':
                    mpu_bytes = int(v)
                else:
                    new_params.append((k, v))

            if mpu_etag is None:
                continue

            # put back any etag params that may be the responsibility of
            # other middlwares...
            item['hash'] = mpu_etag + ''.join('; %s=%s' % kv
                                              for kv in new_params)
            item['bytes'] = mpu_bytes
            # hide the implementation details from the user
            item.pop('symlink_path', None)
        resp.body = json.dumps(body_json).encode('ascii')

    def handle_request(self):
        resp = self.req.get_response(self.app)
        if not resp.is_success:
            return resp

        if self.req.method == 'GET':
            self._process_json_resp(resp)
        elif self.req.method == 'DELETE':
            pass
            # TODO: implement
            #   check these are empty before deleting the user container?
            #   but they may not be until the mpu-auditor runs, and even then
            #   there may be undeleted orphans
            # self._delete_container(self.parts_container)
            # self._delete_container(self.manifests_container)
            # self._delete_container(self.sessions_container)

        return resp


class MPUMiddleware(object):
    def __init__(self, app, conf, logger=None):
        self.conf = conf
        self.app = app
        self.logger = logger or get_logger(conf, log_route='slo')

        self.min_part_size = config_positive_int_value(
            conf.get('min_part_size', 5242880))

    def handle_request(self, req, container, obj):
        # this defines the MPU API
        upload_id = get_req_upload_id(req)
        part_number = get_valid_part_num(req)
        if obj and upload_id:
            if req.method == 'PUT' and part_number is not None:
                resp = MPUSessionHandler(self, req).upload_part(part_number)
            elif req.method == 'GET':
                resp = MPUSessionHandler(self, req).list_parts()
            elif req.method == 'POST':
                resp = MPUSessionHandler(self, req).complete_upload()
            elif req.method == 'DELETE':
                resp = MPUSessionHandler(self, req).abort_upload()
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

    register_swift_info('mpu', enabled=True)

    def mpu_filter(app):
        return MPUMiddleware(app, conf,)
    return mpu_filter
