# Copyright (c) 2010-2012 OpenStack Foundation
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
import os
import time
import traceback
import math
from swift import gettext_ as _

from eventlet import Timeout

import six

import swift.common.db
from swift.container.sync_store import ContainerSyncStore
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_SHARD, UNSHARDED, SHARDING, SHARDED, SHARD_UPDATE_STATES
from swift.container.replicator import ContainerReplicatorRpc
from swift.common.db import DatabaseAlreadyExists
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.request_helpers import get_param, \
    split_and_validate_path, is_sys_or_user_meta
from swift.common.utils import get_logger, hash_path, public, \
    Timestamp, storage_directory, validate_sync_to, \
    config_true_value, timing_stats, replication, \
    override_bytes_from_content_type, get_log_line, \
    config_fallocate_value, fs_has_free_space, list_from_csv, \
    ShardRange
from swift.common.constraints import valid_timestamp, check_utf8, check_drive
from swift.common import constraints
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.http import HTTP_NO_CONTENT, HTTP_NOT_FOUND, is_success
from swift.common.middleware import listing_formats
from swift.common.storage_policy import POLICIES
from swift.common.base_storage_server import BaseStorageServer
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPConflict, \
    HTTPCreated, HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPException, HTTPMovedPermanently, \
    wsgi_to_str, str_to_wsgi


def gen_resp_headers(info, is_deleted=False):
    """
    Convert container info dict to headers.
    """
    # backend headers are always included
    headers = {
        'X-Backend-Timestamp': Timestamp(info.get('created_at', 0)).internal,
        'X-Backend-PUT-Timestamp': Timestamp(info.get(
            'put_timestamp', 0)).internal,
        'X-Backend-DELETE-Timestamp': Timestamp(
            info.get('delete_timestamp', 0)).internal,
        'X-Backend-Status-Changed-At': Timestamp(
            info.get('status_changed_at', 0)).internal,
        'X-Backend-Storage-Policy-Index': info.get('storage_policy_index', 0),
    }
    if not is_deleted:
        # base container info on deleted containers is not exposed to client
        headers.update({
            'X-Container-Object-Count': info.get('object_count', 0),
            'X-Container-Bytes-Used': info.get('bytes_used', 0),
            'X-Timestamp': Timestamp(info.get('created_at', 0)).normal,
            'X-PUT-Timestamp': Timestamp(
                info.get('put_timestamp', 0)).normal,
            'X-Backend-Sharding-State': info.get('db_state', UNSHARDED),
        })
    return headers


class ContainerController(BaseStorageServer):
    """WSGI Controller for the container server."""

    # Ensure these are all lowercase
    save_headers = ['x-container-read', 'x-container-write',
                    'x-container-sync-key', 'x-container-sync-to']
    server_type = 'container-server'

    def __init__(self, conf, logger=None):
        super(ContainerController, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='container-server')
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = float(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        #: ContainerSyncCluster instance for validating sync-to values.
        self.realms_conf = ContainerSyncRealms(
            os.path.join(
                conf.get('swift_dir', '/etc/swift'),
                'container-sync-realms.conf'),
            self.logger)
        #: The list of hosts we're allowed to send syncs to. This can be
        #: overridden by data in self.realms_conf
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.replicator_rpc = ContainerReplicatorRpc(
            self.root, DATADIR, ContainerBroker, self.mount_check,
            logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')
        if 'allow_versions' in conf:
            self.logger.warning('Option allow_versions is deprecated. '
                                'Configure the versioned_writes middleware in '
                                'the proxy-server instead. This option will '
                                'be ignored in a future release.')
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.sync_store = ContainerSyncStore(self.root,
                                             self.logger,
                                             self.mount_check)
        self.fallocate_reserve, self.fallocate_is_percent = \
            config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    def _get_container_broker(self, drive, part, account, container, **kwargs):
        """
        Get a DB broker for the container.

        :param drive: drive that holds the container
        :param part: partition the container is in
        :param account: account name
        :param container: container name
        :returns: ContainerBroker object
        """
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('container', container)
        kwargs.setdefault('logger', self.logger)
        return ContainerBroker(db_path, **kwargs)

    def get_and_validate_policy_index(self, req):
        """
        Validate that the index supplied maps to a policy.

        :returns: policy index from request, or None if not present
        :raises HTTPBadRequest: if the supplied index is bogus
        """

        policy_index = req.headers.get('X-Backend-Storage-Policy-Index', None)
        if policy_index is None:
            return None

        try:
            policy_index = int(policy_index)
        except ValueError:
            raise HTTPBadRequest(
                request=req, content_type="text/plain",
                body=("Invalid X-Storage-Policy-Index %r" % policy_index))

        policy = POLICIES.get_by_index(policy_index)
        if policy is None:
            raise HTTPBadRequest(
                request=req, content_type="text/plain",
                body=("Invalid X-Storage-Policy-Index %r" % policy_index))
        return int(policy)

    def account_update(self, req, account, container, broker):
        """
        Update the account server(s) with latest container info.

        :param req: swob.Request object
        :param account: account name
        :param container: container name
        :param broker: container DB broker object
        :returns: if all the account requests return a 404 error code,
                  HTTPNotFound response object,
                  if the account cannot be updated due to a malformed header,
                  an HTTPBadRequest response object,
                  otherwise None.
        """
        account_hosts = [h.strip() for h in
                         req.headers.get('X-Account-Host', '').split(',')]
        account_devices = [d.strip() for d in
                           req.headers.get('X-Account-Device', '').split(',')]
        account_partition = req.headers.get('X-Account-Partition', '')

        if len(account_hosts) != len(account_devices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_(
                'ERROR Account update failed: different  '
                'numbers of hosts and devices in request: '
                '"%(hosts)s" vs "%(devices)s"') % {
                    'hosts': req.headers.get('X-Account-Host', ''),
                    'devices': req.headers.get('X-Account-Device', '')})
            return HTTPBadRequest(req=req)

        if account_partition:
            # zip is lazy on py3, but we need a list, so force evaluation.
            # On py2 it's an extra list copy, but the list is so small
            # (one element per replica in account ring, usually 3) that it
            # doesn't matter.
            updates = list(zip(account_hosts, account_devices))
        else:
            updates = []

        account_404s = 0

        for account_host, account_device in updates:
            account_ip, account_port = account_host.rsplit(':', 1)
            new_path = '/' + '/'.join([account, container])
            info = broker.get_info()
            account_headers = HeaderKeyDict({
                'x-put-timestamp': info['put_timestamp'],
                'x-delete-timestamp': info['delete_timestamp'],
                'x-object-count': info['object_count'],
                'x-bytes-used': info['bytes_used'],
                'x-trans-id': req.headers.get('x-trans-id', '-'),
                'X-Backend-Storage-Policy-Index': info['storage_policy_index'],
                'user-agent': 'container-server %s' % os.getpid(),
                'referer': req.as_referer()})
            if req.headers.get('x-account-override-deleted', 'no').lower() == \
                    'yes':
                account_headers['x-account-override-deleted'] = 'yes'
            try:
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(
                        account_ip, account_port, account_device,
                        account_partition, 'PUT', new_path, account_headers)
                with Timeout(self.node_timeout):
                    account_response = conn.getresponse()
                    account_response.read()
                    if account_response.status == HTTP_NOT_FOUND:
                        account_404s += 1
                    elif not is_success(account_response.status):
                        self.logger.error(_(
                            'ERROR Account update failed '
                            'with %(ip)s:%(port)s/%(device)s (will retry '
                            'later): Response %(status)s %(reason)s'),
                            {'ip': account_ip, 'port': account_port,
                             'device': account_device,
                             'status': account_response.status,
                             'reason': account_response.reason})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later)'),
                    {'ip': account_ip, 'port': account_port,
                     'device': account_device})
        if updates and account_404s == len(updates):
            return HTTPNotFound(req=req)
        else:
            return None

    def _update_sync_store(self, broker, method):
        try:
            self.sync_store.update_sync_store(broker)
        except Exception:
            self.logger.exception('Failed to update sync_store %s during %s' %
                                  (broker.db_file, method))

    def _redirect_to_shard(self, req, broker, obj_name):
        """
        If the request indicates that it can accept a redirection, look for a
        shard range that contains ``obj_name`` and if one exists return a
        HTTPMovedPermanently response.

        :param req: an instance of :class:`~swift.common.swob.Request`
        :param broker: a container broker
        :param obj_name: an object name
        :return: an instance of :class:`swift.common.swob.HTTPMovedPermanently`
            if a shard range exists for the given ``obj_name``, otherwise None.
        """
        if not config_true_value(
                req.headers.get('x-backend-accept-redirect', False)):
            return None

        shard_ranges = broker.get_shard_ranges(
            includes=obj_name, states=SHARD_UPDATE_STATES)
        if not shard_ranges:
            return None

        # note: obj_name may be included in both a created sub-shard and its
        # sharding parent. get_shard_ranges will return the created sub-shard
        # in preference to the parent, which is the desired result.
        containing_range = shard_ranges[0]
        location = "/%s/%s" % (containing_range.name, obj_name)
        headers = {'Location': location,
                   'X-Backend-Redirect-Timestamp':
                       containing_range.timestamp.internal}

        # we do not want the host added to the location
        req.environ['swift.leave_relative_location'] = True
        return HTTPMovedPermanently(headers=headers, request=req)

    def check_free_space(self, drive):
        drive_root = os.path.join(self.root, drive)
        return fs_has_free_space(
            drive_root, self.fallocate_reserve, self.fallocate_is_percent)

    @public
    @timing_stats()
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        req_timestamp = valid_timestamp(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        # policy index is only relevant for delete_obj (and transitively for
        # auto create accounts)
        obj_policy_index = self.get_and_validate_policy_index(req) or 0
        broker = self._get_container_broker(drive, part, account, container)
        if account.startswith(self.auto_create_account_prefix) and obj and \
                not os.path.exists(broker.db_file):
            try:
                broker.initialize(req_timestamp.internal, obj_policy_index)
            except DatabaseAlreadyExists:
                pass
        if not os.path.exists(broker.db_file):
            return HTTPNotFound()
        if obj:     # delete object
            # redirect if a shard range exists for the object name
            redirect = self._redirect_to_shard(req, broker, obj)
            if redirect:
                return redirect

            broker.delete_object(obj, req.headers.get('x-timestamp'),
                                 obj_policy_index)
            return HTTPNoContent(request=req)
        else:
            # delete container
            if not broker.empty():
                return HTTPConflict(request=req)
            existed = Timestamp(broker.get_info()['put_timestamp']) and \
                not broker.is_deleted()
            broker.delete_db(req_timestamp.internal)
            if not broker.is_deleted():
                return HTTPConflict(request=req)
            self._update_sync_store(broker, 'DELETE')
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if existed:
                return HTTPNoContent(request=req)
            return HTTPNotFound()

    def _update_or_create(self, req, broker, timestamp, new_container_policy,
                          requested_policy_index):
        """
        Create new database broker or update timestamps for existing database.

        :param req: the swob request object
        :param broker: the broker instance for the container
        :param timestamp: internalized timestamp
        :param new_container_policy: the storage policy index to use
                                     when creating the container
        :param requested_policy_index: the storage policy index sent in the
                                       request, may be None

        :returns: created, a bool, if database did not previously exist
        """
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(timestamp, new_container_policy)
            except DatabaseAlreadyExists:
                pass
            else:
                return True  # created
        recreated = broker.is_deleted()
        if recreated:
            # only set storage policy on deleted containers
            broker.set_storage_policy_index(new_container_policy,
                                            timestamp=timestamp)
        elif requested_policy_index is not None:
            # validate requested policy with existing container
            if requested_policy_index != broker.storage_policy_index:
                raise HTTPConflict(request=req,
                                   headers={'x-backend-storage-policy-index':
                                            broker.storage_policy_index})
        broker.update_put_timestamp(timestamp)
        if broker.is_deleted():
            raise HTTPConflict(request=req)
        if recreated:
            broker.update_status_changed_at(timestamp)
        return recreated

    def _maybe_autocreate(self, broker, req_timestamp, account,
                          policy_index):
        created = False
        if account.startswith(self.auto_create_account_prefix) and \
                not os.path.exists(broker.db_file):
            if policy_index is None:
                raise HTTPBadRequest(
                    'X-Backend-Storage-Policy-Index header is required')
            try:
                broker.initialize(req_timestamp.internal, policy_index)
            except DatabaseAlreadyExists:
                pass
            else:
                created = True
        if not os.path.exists(broker.db_file):
            raise HTTPNotFound()
        return created

    def _update_metadata(self, req, broker, req_timestamp, method):
        metadata = {}
        metadata.update(
            (wsgi_to_str(key), (wsgi_to_str(value), req_timestamp.internal))
            for key, value in req.headers.items()
            if key.lower() in self.save_headers or
            is_sys_or_user_meta('container', key))
        if metadata:
            if 'X-Container-Sync-To' in metadata:
                if 'X-Container-Sync-To' not in broker.metadata or \
                        metadata['X-Container-Sync-To'][0] != \
                        broker.metadata['X-Container-Sync-To'][0]:
                    broker.set_x_container_sync_points(-1, -1)
            broker.update_metadata(metadata, validate_metadata=True)
            self._update_sync_store(broker, method)

    @public
    @timing_stats()
    def PUT(self, req):
        """Handle HTTP PUT request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        req_timestamp = valid_timestamp(req)
        if 'x-container-sync-to' in req.headers:
            err, sync_to, realm, realm_key = validate_sync_to(
                req.headers['x-container-sync-to'], self.allowed_sync_hosts,
                self.realms_conf)
            if err:
                return HTTPBadRequest(err)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        requested_policy_index = self.get_and_validate_policy_index(req)
        broker = self._get_container_broker(drive, part, account, container)
        if obj:     # put container object
            # obj put expects the policy_index header, default is for
            # legacy support during upgrade.
            obj_policy_index = requested_policy_index or 0
            self._maybe_autocreate(broker, req_timestamp, account,
                                   obj_policy_index)
            # redirect if a shard exists for this object name
            response = self._redirect_to_shard(req, broker, obj)
            if response:
                return response

            broker.put_object(obj, req_timestamp.internal,
                              int(req.headers['x-size']),
                              wsgi_to_str(req.headers['x-content-type']),
                              wsgi_to_str(req.headers['x-etag']), 0,
                              obj_policy_index,
                              wsgi_to_str(req.headers.get(
                                  'x-content-type-timestamp')),
                              wsgi_to_str(req.headers.get('x-meta-timestamp')))
            return HTTPCreated(request=req)

        record_type = req.headers.get('x-backend-record-type', '').lower()
        if record_type == RECORD_TYPE_SHARD:
            try:
                # validate incoming data...
                shard_ranges = [ShardRange.from_dict(sr)
                                for sr in json.loads(req.body)]
            except (ValueError, KeyError, TypeError) as err:
                return HTTPBadRequest('Invalid body: %r' % err)
            created = self._maybe_autocreate(broker, req_timestamp, account,
                                             requested_policy_index)
            self._update_metadata(req, broker, req_timestamp, 'PUT')
            if shard_ranges:
                # TODO: consider writing the shard ranges into the pending
                # file, but if so ensure an all-or-none semantic for the write
                broker.merge_shard_ranges(shard_ranges)
        else:   # put container
            if requested_policy_index is None:
                # use the default index sent by the proxy if available
                new_container_policy = req.headers.get(
                    'X-Backend-Storage-Policy-Default', int(POLICIES.default))
            else:
                new_container_policy = requested_policy_index
            created = self._update_or_create(req, broker,
                                             req_timestamp.internal,
                                             new_container_policy,
                                             requested_policy_index)
            self._update_metadata(req, broker, req_timestamp, 'PUT')
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
        if created:
            return HTTPCreated(request=req,
                               headers={'x-backend-storage-policy-index':
                                        broker.storage_policy_index})
        else:
            return HTTPAccepted(request=req,
                                headers={'x-backend-storage-policy-index':
                                         broker.storage_policy_index})

    @public
    @timing_stats(sample_rate=0.1)
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        out_content_type = listing_formats.get_listing_content_type(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        info, is_deleted = broker.get_info_is_deleted()
        headers = gen_resp_headers(info, is_deleted=is_deleted)
        if is_deleted:
            return HTTPNotFound(request=req, headers=headers)
        headers.update(
            (str_to_wsgi(key), str_to_wsgi(value))
            for key, (value, timestamp) in broker.metadata.items()
            if value != '' and (key.lower() in self.save_headers or
                                is_sys_or_user_meta('container', key)))
        headers['Content-Type'] = out_content_type
        resp = HTTPNoContent(request=req, headers=headers, charset='utf-8')
        resp.last_modified = math.ceil(float(headers['X-PUT-Timestamp']))
        return resp

    def update_data_record(self, record):
        """
        Perform any mutations to container listing records that are common to
        all serialization formats, and returns it as a dict.

        Converts created time to iso timestamp.
        Replaces size with 'swift_bytes' content type parameter.

        :params record: object entry record
        :returns: modified record
        """
        if isinstance(record, ShardRange):
            created = record.timestamp
            response = dict(record)
        else:
            (name, created, size, content_type, etag) = record[:5]
            name_ = name.decode('utf8') if six.PY2 else name
            if content_type is None:
                return {'subdir': name_}
            response = {
                'bytes': size, 'hash': etag, 'name': name_,
                'content_type': content_type}
            override_bytes_from_content_type(response, logger=self.logger)
        response['last_modified'] = Timestamp(created).isoformat
        return response

    @public
    @timing_stats()
    def GET(self, req):
        """
        Handle HTTP GET request.

        The body of the response to a successful GET request contains a listing
        of either objects or shard ranges. The exact content of the listing is
        determined by a combination of request headers and query string
        parameters, as follows:

        * The type of the listing is determined by the
          ``X-Backend-Record-Type`` header. If this header has value ``shard``
          then the response body will be a list of shard ranges; if this header
          has value ``auto``, and the container state is ``sharding`` or
          ``sharded``, then the listing will be a list of shard ranges;
          otherwise the response body will be a list of objects.

        * Both shard range and object listings may be constrained to a name
          range by the ``marker`` and ``end_marker`` query string parameters.
          Object listings will only contain objects whose names are greater
          than any ``marker`` value and less than any ``end_marker`` value.
          Shard range listings will only contain shard ranges whose namespace
          is greater than or includes any ``marker`` value and is less than or
          includes any ``end_marker`` value.

        * Shard range listings may also be constrained by an ``includes`` query
          string parameter. If this parameter is present the listing will only
          contain shard ranges whose namespace includes the value of the
          parameter; any ``marker`` or ``end_marker`` parameters are ignored

        * The length of an object listing may be constrained by the ``limit``
          parameter. Object listings may also be constrained by ``prefix``,
          ``delimiter`` and ``path`` query string parameters.

        * Shard range listings will include deleted shard ranges if and only if
          the ``X-Backend-Include-Deleted`` header value is one of
          :attr:`swift.common.utils.TRUE_VALUES`. Object listings never
          include deleted objects.

        * Shard range listings may be constrained to include only shard ranges
          whose state is specified by a query string ``states`` parameter. If
          present, the ``states`` parameter should be a comma separated list of
          either the string or integer representation of
          :data:`~swift.common.utils.ShardRange.STATES`.

          Two alias values may be used in a ``states`` parameter value:
          ``listing`` will cause the listing to include all shard ranges in a
          state suitable for contributing to an object listing; ``updating``
          will cause the listing to include all shard ranges in a state
          suitable to accept an object update.

          If either of these aliases is used then the shard range listing will
          if necessary be extended with a synthesised 'filler' range in order
          to satisfy the requested name range when insufficient actual shard
          ranges are found. Any 'filler' shard range will cover the otherwise
          uncovered tail of the requested name range and will point back to the
          same container.

        * Listings are not normally returned from a deleted container. However,
          the ``X-Backend-Override-Deleted`` header may be used with a value in
          :attr:`swift.common.utils.TRUE_VALUES` to force a shard range
          listing to be returned from a deleted container whose DB file still
          exists.

        :param req: an instance of :class:`swift.common.swob.Request`
        :returns: an instance of :class:`swift.common.swob.Response`
        """
        drive, part, account, container, obj = split_and_validate_path(
            req, 4, 5, True)
        path = get_param(req, 'path')
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')
        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            # delimiters can be made more flexible later
            return HTTPPreconditionFailed(body='Bad delimiter')
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        limit = constraints.CONTAINER_LISTING_LIMIT
        given_limit = get_param(req, 'limit')
        reverse = config_true_value(get_param(req, 'reverse'))
        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > constraints.CONTAINER_LISTING_LIMIT:
                return HTTPPreconditionFailed(
                    request=req,
                    body='Maximum limit is %d'
                    % constraints.CONTAINER_LISTING_LIMIT)
        out_content_type = listing_formats.get_listing_content_type(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container,
                                            pending_timeout=0.1,
                                            stale_reads_ok=True)
        info, is_deleted = broker.get_info_is_deleted()
        record_type = req.headers.get('x-backend-record-type', '').lower()
        if record_type == 'auto' and info.get('db_state') in (SHARDING,
                                                              SHARDED):
            record_type = 'shard'
        if record_type == 'shard':
            override_deleted = info and config_true_value(
                req.headers.get('x-backend-override-deleted', False))
            resp_headers = gen_resp_headers(
                info, is_deleted=is_deleted and not override_deleted)
            if is_deleted and not override_deleted:
                return HTTPNotFound(request=req, headers=resp_headers)
            resp_headers['X-Backend-Record-Type'] = 'shard'
            includes = get_param(req, 'includes')
            states = get_param(req, 'states')
            fill_gaps = False
            if states:
                states = list_from_csv(states)
                fill_gaps = any(('listing' in states, 'updating' in states))
                try:
                    states = broker.resolve_shard_range_states(states)
                except ValueError:
                    return HTTPBadRequest(request=req, body='Bad state')
            include_deleted = config_true_value(
                req.headers.get('x-backend-include-deleted', False))
            container_list = broker.get_shard_ranges(
                marker, end_marker, includes, reverse, states=states,
                include_deleted=include_deleted, fill_gaps=fill_gaps)
        else:
            resp_headers = gen_resp_headers(info, is_deleted=is_deleted)
            if is_deleted:
                return HTTPNotFound(request=req, headers=resp_headers)
            resp_headers['X-Backend-Record-Type'] = 'object'
            # Use the retired db while container is in process of sharding,
            # otherwise use current db
            src_broker = broker.get_brokers()[0]
            container_list = src_broker.list_objects_iter(
                limit, marker, end_marker, prefix, delimiter, path,
                storage_policy_index=info['storage_policy_index'],
                reverse=reverse)
        return self.create_listing(req, out_content_type, info, resp_headers,
                                   broker.metadata, container_list, container)

    def create_listing(self, req, out_content_type, info, resp_headers,
                       metadata, container_list, container):
        for key, (value, timestamp) in metadata.items():
            if value and (key.lower() in self.save_headers or
                          is_sys_or_user_meta('container', key)):
                resp_headers[str_to_wsgi(key)] = str_to_wsgi(value)
        listing = [self.update_data_record(record)
                   for record in container_list]
        if out_content_type.endswith('/xml'):
            body = listing_formats.container_to_xml(listing, container)
        elif out_content_type.endswith('/json'):
            body = json.dumps(listing).encode('ascii')
        else:
            body = listing_formats.listing_to_text(listing)

        ret = Response(request=req, headers=resp_headers, body=body,
                       content_type=out_content_type, charset='utf-8')
        ret.last_modified = math.ceil(float(resp_headers['X-PUT-Timestamp']))
        if not ret.body:
            ret.status_int = HTTP_NO_CONTENT
        return ret

    @public
    @replication
    @timing_stats(sample_rate=0.01)
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request (json-encoded RPC calls for replication.)
        """
        post_args = split_and_validate_path(req, 3)
        drive, partition, hash = post_args
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        try:
            args = json.load(req.environ['wsgi.input'])
        except ValueError as err:
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        ret = self.replicator_rpc.dispatch(post_args, args)
        ret.request = req
        return ret

    @public
    @timing_stats()
    def POST(self, req):
        """Handle HTTP POST request."""
        drive, part, account, container = split_and_validate_path(req, 4)
        req_timestamp = valid_timestamp(req)
        if 'x-container-sync-to' in req.headers:
            err, sync_to, realm, realm_key = validate_sync_to(
                req.headers['x-container-sync-to'], self.allowed_sync_hosts,
                self.realms_conf)
            if err:
                return HTTPBadRequest(err)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_container_broker(drive, part, account, container)
        if broker.is_deleted():
            return HTTPNotFound(request=req)
        broker.update_put_timestamp(req_timestamp.internal)
        self._update_metadata(req, broker, req_timestamp, 'POST')
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                if req.method not in self.allowed_methods:
                    res = HTTPMethodNotAllowed()
                else:
                    res = getattr(self, req.method)(req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s %(path)s '),
                    {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        if self.log_requests:
            trans_time = time.time() - start_time
            log_message = get_log_line(req, res, trans_time, '')
            if req.method.upper() == 'REPLICATE':
                self.logger.debug(log_message)
            else:
                self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI container server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ContainerController(conf)
