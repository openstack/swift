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

from swift.common.http import HTTP_CONFLICT, is_success
from swift.common.middleware.symlink import TGT_OBJ_SYMLINK_HDR, \
    ALLOW_RESERVED_NAMES
from swift.common.storage_policy import POLICIES
from swift.common.utils import generate_unique_id, drain_and_close, \
    config_positive_int_value, reiterate
from swift.common.swob import Request, normalize_etag, \
    wsgi_to_str, wsgi_quote, HTTPInternalServerError, HTTPOk, \
    HTTPConflict, HTTPBadRequest, HTTPException, HTTPNotFound, HTTPNoContent
from swift.common.utils import get_logger, Timestamp, md5, public
from swift.common.registry import register_swift_info
from swift.common.request_helpers import get_reserved_name, \
    get_valid_part_num, is_reserved_name, split_reserved_name, is_user_meta
from swift.common.wsgi import make_pre_authed_request
from swift.proxy.controllers.base import get_container_info

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024
MPU_SWIFT_SOURCE = 'MPU'
MPU_SYSMETA_PREFIX = 'x-object-sysmeta-mpu-'
MPU_TRANSIENT_SYSMETA_PREFIX = 'x-object-transient-sysmeta-mpu-'
# TODO: application/directory is used in s3api but why?
MPU_CONTENT_TYPE = 'application/x-mpu'
MPU_ABORTED_CONTENT_TYPE = 'application/x-mpu-aborted'
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


def get_upload_id(req):
    # TODO: add some validation
    return req.params.get('upload-id')


MPU_UPLOAD_ID_KEY = get_mpu_sysmeta_key('upload-id')
MPU_MANIFEST_ETAG_KEY = get_mpu_sysmeta_key('manifest-etag')
MPU_PARTS_COUNT_KEY = get_mpu_sysmeta_key('parts-count')


class MPUSession(object):
    CREATED_TIMESTAMP_KEY = 'X-Backend-Data-Timestamp'
    TIMESTAMP_KEY = 'X-Timestamp'
    HAS_USER_CONTENT_TYPE_KEY = get_mpu_sysmeta_key('has-content-type')
    USER_CONTENT_TYPE_KEY = get_mpu_sysmeta_key('content-type')
    STATE_KEY = get_mpu_transient_sysmeta_key('state')
    CREATED_STATE = 'created'
    COMPLETING_STATE = 'completing'
    STATES = (CREATED_STATE, COMPLETING_STATE)

    def __init__(self, session_headers):
        self.headers = session_headers
        # TODO: validate headers
        self.created_timestamp = Timestamp(self.headers.get(
            self.CREATED_TIMESTAMP_KEY, 0))
        self.timestamp = Timestamp(self.headers.get(self.TIMESTAMP_KEY, 0))
        self._state = session_headers.get(self.STATE_KEY, self.CREATED_STATE)
        self.content_type = session_headers.get('content-type',
                                                MPU_CONTENT_TYPE)

    def is_aborted(self):
        return self.content_type == MPU_ABORTED_MARKER_SUFFIX

    def abort(self):
        self.content_type = MPU_ABORTED_MARKER_SUFFIX

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        if state not in self.STATES:
            # TODO: test invalid case
            raise ValueError
        self._state = state

    def get_post_headers(self):
        return {self.TIMESTAMP_KEY: self.timestamp.internal,
                self.STATE_KEY: self.state,
                'Content-Type': self.content_type}

    def get_manifest_headers(self):
        manifest_headers = {}
        for key, val in self.headers.items():
            stripped_key = strip_mpu_sysmeta_prefix(key)
            if is_user_meta('object', stripped_key):
                manifest_headers[stripped_key] = val
            if (key.lower() == self.HAS_USER_CONTENT_TYPE_KEY and
                    val == 'yes'):
                manifest_headers['content-type'] = self.headers.get(
                    self.USER_CONTENT_TYPE_KEY)
        return manifest_headers

    @classmethod
    def create(cls, user_headers):
        # User metadata is stored as sysmeta on the session because it must
        # persist when there are subsequent POSTs to the session.
        session_headers = {
            'Content-Length': '0',
            cls.STATE_KEY: cls.CREATED_STATE,
            cls.HAS_USER_CONTENT_TYPE_KEY: 'no',
            'Content-Type': MPU_CONTENT_TYPE}
        for k, v in user_headers.items():
            if is_user_meta('object', k):
                session_headers[get_mpu_sysmeta_key(k)] = v
            if k.lower() == 'content-type':
                session_headers[cls.HAS_USER_CONTENT_TYPE_KEY] = 'yes'
                session_headers[cls.USER_CONTENT_TYPE_KEY] = v
            if k.lower() == 'x-timestamp':
                session_headers['x-timestamp'] = v
        return MPUSession(session_headers)


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

    def make_path(self, *parts):
        return '/'.join(['', 'v1', self.account] + [p for p in parts])

    def make_subrequest(self, method=None, path=None, body=None, headers=None,
                        params=None):
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


class MPUHandler(BaseMPUHandler):
    """
    Handles the following APIs:

    * List Multipart Uploads
    * Initiate Multipart Upload
    """
    def handle_request(self):
        if self.req.method == 'GET':
            resp = self.list_uploads()
        elif self.req.method == 'POST' and self.obj:
            resp = self.create_upload()
        else:
            # TODO: should we return 405 for any unsupported container?uploads
            #     method? Swift typically ignores unrecognised headers and
            #     params, but there is a risk that the user thinks that, for
            #     example, DELETE container?uploads will just abort all the MPU
            #     sessions (whereas it might delete the container).
            resp = None
        return resp

    @public
    def list_uploads(self):
        """
        Handles List Multipart Uploads
        """
        path = self.make_path(self.sessions_container)
        sub_req = self.make_subrequest(path=path, method='GET')
        resp = sub_req.get_response(self.app)
        if resp.is_success:
            listing = json.loads(resp.body)
            for item in listing:
                item['name'] = split_reserved_name(item['name'])[0]
            resp.body = json.dumps(listing).encode('ascii')
        return resp

    def _ensure_container_exists(self, container):
        # TODO: make storage policy specific parts bucket
        info = get_container_info(self.req.environ, self.app,
                                  swift_source=MPU_SWIFT_SOURCE)
        policy_name = POLICIES[info['storage_policy']].name

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
        # if len(req.object_name) > constraints.MAX_OBJECT_NAME_LENGTH:
        #     # Note that we can still run into trouble where the MPU is just
        #     # within the limit, which means the segment names will go over
        #     raise KeyTooLongError()
        upload_id = generate_unique_id()

        self._ensure_container_exists(self.sessions_container)
        self._ensure_container_exists(self.manifests_container)
        self._ensure_container_exists(self.parts_container)

        self.req.ensure_x_timestamp()
        self.req.headers.pop('Etag', None)
        self.req.headers.pop('Content-Md5', None)
        path = self.make_path(
            self.sessions_container, self.reserved_obj, upload_id)
        session = MPUSession.create(self.req.headers)
        session_req = self.make_subrequest(
            path=path, method='PUT', headers=session.headers, body=b'')

        session_resp = session_req.get_response(self.app)
        if session_resp.is_success:
            drain_and_close(session_resp)
            resp_headers = {'X-Upload-Id': upload_id}
            resp = HTTPOk(headers=resp_headers)
        else:
            self.logger.warning('MPU %s %s', session_resp.status,
                                session_resp.body)
            resp = HTTPInternalServerError()
        return resp


class MPUSessionHandler(BaseMPUHandler):
    """
    Handles the following APIs:

    * List Parts
    * Abort Multipart Upload
    * Complete Multipart Upload
    * Upload Part and Upload Part Copy.
    """
    def __init__(self, mw, req, upload_id):
        super(MPUSessionHandler, self).__init__(mw, req)
        self.upload_id = upload_id
        self.session_name = '/'.join([self.reserved_obj, upload_id])
        self.session_path = self.make_path(self.sessions_container,
                                           self.session_name)
        self.session = None
        self.manifest_relative_path = '/'.join(
            [self.manifests_container, self.reserved_obj, self.upload_id])
        self.manifest_path = self.make_path(self.manifest_relative_path)

    def _load_session(self):
        req = self.make_subrequest(method='HEAD', path=self.session_path)
        resp = req.get_response(self.app)
        if resp.status_int == 404:
            raise HTTPNotFound()
        elif not resp.is_success:
            raise HTTPInternalServerError()

        session = MPUSession(resp.headers)
        return session

    def _delete_session(self):
        session_req = self.make_subrequest(
            path=self.session_path, method='DELETE')
        # TODO: check session_resp
        session_resp = session_req.get_response(self.app)
        drain_and_close(session_resp)

    def _get_user_object_metadata(self):
        req = self.make_subrequest(method='HEAD', path=self.req.path)
        resp = req.get_response(self.app)
        if resp.is_success:
            return resp.headers
        else:
            return {}

    def handle_request(self):
        self.session = self._load_session()
        self.req.headers.setdefault('X-Timestamp', Timestamp.now().internal)
        part_number = get_valid_part_num(self.req)
        if self.req.method == 'PUT' and part_number > 0:
            resp = self.upload_part(part_number)
        elif self.req.method == 'GET':
            resp = self.list_parts()
        # TODO: support HEAD? return basic metadata about the upload (not
        #   required for s3api)
        elif self.req.method == 'POST':
            resp = self.complete_upload()
        elif self.req.method == 'DELETE':
            resp = self.abort_upload()
        else:
            resp = None
        return resp

    def upload_part(self, part_number):
        part_path = self.make_path(self.parts_container, self.reserved_obj,
                                   self.upload_id, str(part_number))
        part_req = self.make_subrequest(
            path=part_path, method='PUT', body=self.req.body)
        # TODO: support copy part

        resp = part_req.get_response(self.app)
        return HTTPOk(headers=resp.headers)

    def list_parts(self):
        """
        Handles List Parts.
        """
        path = self.make_path(self.parts_container)
        sub_req = self.make_subrequest(
            path=path, method='GET', params={'prefix': self.session_name})
        resp = sub_req.get_response(self.app)
        if resp.is_success:
            listing = json.loads(resp.body)
            for item in listing:
                item['name'] = split_reserved_name(item['name'])[0]
            resp.body = json.dumps(listing).encode('ascii')
        return resp

    def abort_upload(self):
        """
        Handles Abort Multipart Upload.
        """
        if self.req.timestamp < self.session.created_timestamp:
            return HTTPConflict()

        user_obj_metadata = self._get_user_object_metadata()
        if user_obj_metadata.get(TGT_OBJ_SYMLINK_HDR) == \
                self.manifest_relative_path:
            return HTTPConflict()

        # Update the session to be marked as aborted. This will prevent any
        # subsequent complete operation from proceeding.
        self.session.timestamp = self.req.timestamp
        self.session.content_type = MPU_ABORTED_CONTENT_TYPE
        session_req = self.make_subrequest(
            'POST', path=self.session_path,
            headers=self.session.get_post_headers())
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
                    b"part_number %s must be greater than previous %s"
                    % (part_number, previous_part))
        except KeyError:
            raise ValueError(b"expected keys to include part_number")
        return part_number

    def _parse_etag(self, part_dict):
        try:
            etag = part_dict['etag']
            etag = normalize_etag(etag)
            if (etag is None or len(etag) != 32 or
                    any(c not in '0123456789abcdef' for c in etag)):
                raise ValueError(b"etag %s is invalid" % etag)
        except KeyError:
            raise ValueError(b"expected keys to include etag")
        return etag

    def _parse_user_manifest(self, body):
        try:
            parsed_data = json.loads(body)
        except ValueError:
            raise HTTPBadRequest("Manifest must be valid JSON.\n")

        if not isinstance(parsed_data, list):
            raise HTTPBadRequest("Manifest must be a list.\n")

        errors = []
        parsed_manifest = []
        manifest_etag_hasher = md5(usedforsecurity=False)
        previous_part = 0
        for part_index, part_dict in enumerate(parsed_data):
            if not isinstance(part_dict, dict):
                errors.append(b"Index %d: not a JSON object" % part_index)
                continue
            try:
                part_number = self._parse_part_number(part_dict, previous_part)
            except ValueError as err:
                errors.append(b"Index %d: %s" % err)
            try:
                etag = self._parse_etag(part_dict)
            except ValueError as err:
                errors.append(b"Index %d: %s" % err)
            if not errors:
                part_path = '/'.join([
                    self.parts_container, self.reserved_obj, self.upload_id,
                    str(part_number)])
                parsed_manifest.append({
                    'path': wsgi_to_str(part_path),
                    'etag': etag})
                manifest_etag_hasher.update(binascii.a2b_hex(etag))

        if errors:
            error_message = b"".join(e + b"\n" for e in errors)
            raise HTTPBadRequest(error_message,
                                 headers={"Content-Type": "text/plain"})
        return parsed_manifest, manifest_etag_hasher.hexdigest()

    def _make_complete_upload_resp_iter(self, manifest_req, mpu_req):
        def response_iter():
            # TODO: add support for versioning?? copied from multi_upload.py
            put_resp = manifest_req.get_response(self.app)
            if put_resp.status_int == 202:
                body = []
                put_resp.fix_conditional_response()
                for chunk in put_resp.response_iter:
                    if not chunk.strip():
                        yield chunk
                        continue
                    body.append(chunk)
                # TODO: translate problem segments to client resp
                # body = json.loads(b''.join(body))

                # TODO: repeat check that session has not been aborted

                # create symlink to manifest
                # TODO: check mpu_resp
                mpu_resp = mpu_req.get_response(self.app)
                drain_and_close(mpu_resp)
                # clean up the multipart-upload record
                self._delete_session()
            else:
                yield put_resp.body

        return reiterate(response_iter())

    def _make_part_size_checker(self):
        too_small_message = ('MPU part must be at least %d bytes'
                             % self.mw.min_part_size)

        def size_checker(slo_manifest):
            # Check the size of each segment except the last and make sure
            # they are all more than the minimum upload chunk size.
            # Note that we need to use the *internal* keys, since we're
            # looking at the manifest that's about to be written.
            return [
                (item['name'], too_small_message)
                for item in slo_manifest[:-1]
                if item and item['bytes'] < self.mw.min_part_size]

        return size_checker

    def complete_upload(self):
        """
        Handles Complete Multipart Upload.
        """
        if self.req.timestamp < self.session.created_timestamp:
            return HTTPConflict()

        if self.session.content_type == MPU_ABORTED_CONTENT_TYPE:
            # The session has been previously aborted but not yet successfully
            # deleted. Refuse to complete. The abort may be concurrent or may
            # have failed to delete the session. Either way, we refuse to
            # complete the upload.
            return HTTPConflict()

        self.session.timestamp = self.req.timestamp
        self.session.state = MPUSession.COMPLETING_STATE
        session_req = self.make_subrequest(
            'POST', path=self.session_path,
            headers=self.session.get_post_headers())
        # TODO: check response
        session_req.get_response(self.app)

        # TODO: replicate the etag handling from s3api
        # Leave base header value blank; SLO will populate
        # c_etag = '; s3_etag=%s' % manifest_etag
        # manifest_headers[get_container_update_override_key('etag')] = c_etag

        manifest, manifest_etag = self._parse_user_manifest(self.req.body)
        manifest_headers = {
            ALLOW_RESERVED_NAMES: 'true',
            MPU_UPLOAD_ID_KEY: self.upload_id,
            'X-Timestamp': self.session.created_timestamp.internal,
            'Accept': 'application/json',
            MPU_MANIFEST_ETAG_KEY: manifest_etag,
            MPU_PARTS_COUNT_KEY: len(manifest),
            # TODO: include max part index in sysmeta to detect "pure" manifest
        }
        manifest_headers.update(self.session.get_manifest_headers())
        manifest_req = self.make_subrequest(
            path=self.manifest_path, method='PUT', headers=manifest_headers,
            body=json.dumps(manifest),
            params={'multipart-manifest': 'put', 'heartbeat': 'on'})
        manifest_req.environ['swift.callback.slo_manifest_hook'] = \
            self._make_part_size_checker()

        mpu_path = self.make_path(self.container, self.obj)
        mpu_headers = {
            ALLOW_RESERVED_NAMES: 'true',
            MPU_UPLOAD_ID_KEY: self.upload_id,
            'X-Timestamp': self.session.created_timestamp.internal,
            TGT_OBJ_SYMLINK_HDR: self.manifest_relative_path,
            'Content-Length': '0',
        }
        mpu_req = self.make_subrequest(
            path=mpu_path, method='PUT', headers=mpu_headers)

        resp = HTTPOk()  # assume we're good for now...
        resp.app_iter = self._make_complete_upload_resp_iter(
            manifest_req, mpu_req)
        return resp


class MPUObjHandler(BaseMPUHandler):
    def _maybe_cleanup_mpu(self, resp):
        # NB: do this even for non-success responses in case any of the
        # backend responses may have succeeded
        if 'x-object-version-id' in resp.headers:
            # TODO: unit test early return
            # existing object became a version -> no cleanup
            return

        self.logger.debug('MPU resp headers %s', dict(resp.headers))
        upload_id_key = get_mpu_sysmeta_key('upload-id')
        deleted_upload_ids = {}
        for backend_resp in self.req.environ.get('swift.backend_responses',
                                                 []):
            if not is_success(backend_resp.status):
                continue
            # TODO: check that backend_resp has x-symlink-target and cross
            #   check its value with expected manifest path

            upload_id = backend_resp.headers.get(upload_id_key)
            if upload_id:
                deleted_upload_ids[upload_id] = backend_resp
        for upload_id, backend_resp in deleted_upload_ids.items():
            # TODO: unit test multiple upload cleanup
            self._put_manifest_delete_marker(upload_id,
                                             MPU_DELETED_MARKER_SUFFIX)

    def handle_request(self):
        if self.req.method not in ('PUT', 'DELETE'):
            return None

        # TODO: write down a general maybe-deleted marker in manifests
        #  container *before* forwarding request
        resp = self.req.get_response(self.app)
        self._maybe_cleanup_mpu(resp)
        return resp


class MPUMiddleware(object):
    def __init__(self, app, conf, logger=None):
        self.conf = conf
        self.app = app
        self.logger = logger or get_logger(conf, log_route='slo')

        self.min_part_size = config_positive_int_value(
            conf.get('min_part_size', 5242880))

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if is_reserved_name(account, container, obj):
            return self.app(env, start_response)

        upload_id = get_upload_id(req)
        if obj and upload_id:
            handler = MPUSessionHandler(self, req, upload_id)
        elif container and 'uploads' in req.params:
            handler = MPUHandler(self, req)
        elif obj:
            handler = MPUObjHandler(self, req)
        else:
            handler = None

        if handler:
            try:
                resp = handler.handle_request()
            except HTTPException as err:
                resp = err
        else:
            resp = None

        resp = resp or self.app
        return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info('mpu', enabled=True)

    def mpu_filter(app):
        return MPUMiddleware(app, conf,)
    return mpu_filter
