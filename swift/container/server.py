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
import sys
import time
import traceback

from eventlet import Timeout

from urllib.parse import quote

import swift.common.db
from swift.container.sync_store import ContainerSyncStore
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_SHARD, UNSHARDED, SHARDING, SHARDED, SHARD_UPDATE_STATES
from swift.container.replicator import ContainerReplicatorRpc
from swift.common.db import DatabaseAlreadyExists
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.request_helpers import split_and_validate_path, \
    is_sys_or_user_meta, validate_internal_container, validate_internal_obj, \
    validate_container_params
from swift.common.utils import get_logger, hash_path, public, \
    Timestamp, storage_directory, validate_sync_to, \
    config_true_value, timing_stats, replication, \
    override_bytes_from_content_type, get_log_line, \
    config_fallocate_value, fs_has_free_space, list_from_csv, \
    ShardRange, parse_options
from swift.common.constraints import valid_timestamp, check_utf8, \
    check_drive, AUTO_CREATE_ACCOUNT_PREFIX
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
from swift.common.wsgi import run_wsgi


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


def get_container_name_and_placement(req):
    """
    Split and validate path for a container.

    :param req: a swob request

    :returns: a tuple of path parts as strings
    """
    drive, part, account, container = split_and_validate_path(req, 4)
    validate_internal_container(account, container)
    return drive, part, account, container


def get_obj_name_and_placement(req):
    """
    Split and validate path for an object.

    :param req: a swob request

    :returns: a tuple of path parts as strings
    """
    drive, part, account, container, obj = split_and_validate_path(
        req, 4, 5, True)
    validate_internal_obj(account, container, obj)
    return drive, part, account, container, obj


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
        self.auto_create_account_prefix = AUTO_CREATE_ACCOUNT_PREFIX
        self.shards_account_prefix = (
            self.auto_create_account_prefix + 'shards_')
        if config_true_value(conf.get('allow_versions', 'f')):
            self.save_headers.append('x-versions-location')
        if 'allow_versions' in conf:
            self.logger.warning('Option allow_versions is deprecated. '
                                'Configure the versioned_writes middleware in '
                                'the proxy-server instead. This option will '
                                'be ignored in a future release.')
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        swift.common.db.QUERY_LOGGING = \
            config_true_value(conf.get('db_query_logging', 'f'))
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
        header = 'X-Backend-Storage-Policy-Index'
        policy_index = req.headers.get(header, None)
        if policy_index is None:
            return None

        try:
            policy_index = int(policy_index)
            policy = POLICIES.get_by_index(policy_index)
            if policy is None:
                raise ValueError
        except ValueError:
            raise HTTPBadRequest(
                request=req, content_type="text/plain",
                body="Invalid %s %r" % (header, policy_index))
        else:
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
            self.logger.error(
                'ERROR Account update failed: different  '
                'numbers of hosts and devices in request: '
                '"%(hosts)s" vs "%(devices)s"', {
                    'hosts': req.headers.get('X-Account-Host', ''),
                    'devices': req.headers.get('X-Account-Device', '')})
            return HTTPBadRequest(req=req)

        if account_partition:
            # zip is lazy, but we need a list, so force evaluation.
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
                        self.logger.error(
                            'ERROR Account update failed '
                            'with %(ip)s:%(port)s/%(device)s (will retry '
                            'later): Response %(status)s %(reason)s',
                            {'ip': account_ip, 'port': account_port,
                             'device': account_device,
                             'status': account_response.status,
                             'reason': account_response.reason})
            except (Exception, Timeout):
                self.logger.exception(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later)',
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
            # We want to avoid fetching shard ranges for the (more
            # time-sensitive) object-server update, so allow some misplaced
            # objects to land between when we've started sharding and when the
            # proxy learns about it. Note that this path is also used by old,
            # pre-sharding updaters during a rolling upgrade.
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
        if location != quote(location) and not config_true_value(
                req.headers.get('x-backend-accept-quoted-location', False)):
            # Sender expects the destination to be unquoted, but it isn't safe
            # to send unquoted. Eat the update for now and let the sharder
            # move it later. Should only come up during rolling upgrades.
            return None

        headers = {'Location': quote(location),
                   'X-Backend-Location-Is-Quoted': 'true',
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
        drive, part, account, container, obj = get_obj_name_and_placement(req)
        req_timestamp = valid_timestamp(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        # policy index is only relevant for delete_obj (and transitively for
        # auto create accounts)
        obj_policy_index = self.get_and_validate_policy_index(req) or 0
        broker = self._get_container_broker(drive, part, account, container)
        if obj:
            self._maybe_autocreate(broker, req_timestamp, account,
                                   obj_policy_index, req)
        elif not os.path.exists(broker.db_file):
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

    def _should_autocreate(self, account, req):
        auto_create_header = req.headers.get('X-Backend-Auto-Create')
        if auto_create_header:
            # If the caller included an explicit X-Backend-Auto-Create header,
            # assume they know the behavior they want
            return config_true_value(auto_create_header)
        if account.startswith(self.shards_account_prefix):
            # we have to specical case this subset of the
            # auto_create_account_prefix because we don't want the updater
            # accidently auto-creating shards; only the sharder creates
            # shards and it will explicitly tell the server to do so
            return False
        return account.startswith(self.auto_create_account_prefix)

    def _maybe_autocreate(self, broker, req_timestamp, account,
                          policy_index, req):
        created = False
        should_autocreate = self._should_autocreate(account, req)
        if should_autocreate and not os.path.exists(broker.db_file):
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
        metadata = {
            wsgi_to_str(key): (wsgi_to_str(value), req_timestamp.internal)
            for key, value in req.headers.items()
            if key.lower() in self.save_headers
            or is_sys_or_user_meta('container', key)}
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
        drive, part, account, container, obj = get_obj_name_and_placement(req)
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
        if obj:
            return self.PUT_object(req, broker, account, obj, req_timestamp)
        record_type = req.headers.get('x-backend-record-type', '').lower()
        if record_type == RECORD_TYPE_SHARD:
            return self.PUT_shard(req, broker, account, req_timestamp)
        else:
            return self.PUT_container(req, broker, account,
                                      container, req_timestamp)

    @timing_stats()
    def PUT_object(self, req, broker, account, obj, req_timestamp):
        """Put object into container."""
        # obj put expects the policy_index header, default is for
        # legacy support during upgrade.
        requested_policy_index = self.get_and_validate_policy_index(req)
        obj_policy_index = requested_policy_index or 0
        self._maybe_autocreate(
            broker, req_timestamp, account, obj_policy_index, req)
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

    def _create_ok_resp(self, req, broker, created):
        if created:
            return HTTPCreated(request=req,
                               headers={'x-backend-storage-policy-index':
                                        broker.storage_policy_index})
        else:
            return HTTPAccepted(request=req,
                                headers={'x-backend-storage-policy-index':
                                         broker.storage_policy_index})

    @timing_stats()
    def PUT_shard(self, req, broker, account, req_timestamp):
        """Put shards into container."""
        requested_policy_index = self.get_and_validate_policy_index(req)
        try:
            # validate incoming data...
            shard_ranges = [ShardRange.from_dict(sr)
                            for sr in json.loads(req.body)]
        except (ValueError, KeyError, TypeError) as err:
            return HTTPBadRequest('Invalid body: %r' % err)
        created = self._maybe_autocreate(
            broker, req_timestamp, account, requested_policy_index, req)
        self._update_metadata(req, broker, req_timestamp, 'PUT')
        if shard_ranges:
            # TODO: consider writing the shard ranges into the pending
            # file, but if so ensure an all-or-none semantic for the write
            broker.merge_shard_ranges(shard_ranges)
        return self._create_ok_resp(req, broker, created)

    @timing_stats()
    def PUT_container(self, req, broker, account, container, req_timestamp):
        """Update or create container."""
        requested_policy_index = self.get_and_validate_policy_index(req)
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
        return self._create_ok_resp(req, broker, created)

    @public
    @timing_stats(sample_rate=0.1)
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        drive, part, account, container, obj = get_obj_name_and_placement(req)
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
        headers['Content-Length'] = 0
        resp = HTTPNoContent(request=req, headers=headers, charset='utf-8')
        resp.last_modified = Timestamp(headers['X-PUT-Timestamp']).ceil()
        return resp

    def update_shard_record(self, record, shard_record_full=True):
        """
        Return the shard_range database record as a dict, the keys will depend
        on the database fields provided in the record.

        :param record: shard entry record, either ShardRange or Namespace.
        :param shard_record_full: boolean, when true the timestamp field is
                                  added as "last_modified" in iso format.
        :returns: dict suitable for listing responses
        """
        response = dict(record)
        if shard_record_full:
            created = record.timestamp
            response['last_modified'] = Timestamp(created).isoformat
        return response

    def update_object_record(self, record):
        """
        Perform mutation to container listing records that are common to all
        serialization formats, and returns it as a dict.

        Converts created time to iso timestamp.
        Replaces size with 'swift_bytes' content type parameter.

        :param record: object entry record
        :returns: modified record
        """
        # record is object info
        (name, created, size, content_type, etag) = record[:5]
        if content_type is None:
            return {'subdir': name}
        response = {
            'bytes': size, 'hash': etag, 'name': name,
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

        * Both shard range and object listings may be filtered according to
          the constraints described below. However, the
          ``X-Backend-Ignore-Shard-Name-Filter`` header may be used to override
          the application of the ``marker``, ``end_marker``, ``includes`` and
          ``reverse`` parameters to shard range listings. These parameters will
          be ignored if the header has the value 'sharded' and the current db
          sharding state is also 'sharded'. Note that this header does not
          override the ``states`` constraint on shard range listings.

        * The order of both shard range and object listings may be reversed by
          using a ``reverse`` query string parameter with a
          value in :attr:`swift.common.utils.TRUE_VALUES`.

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

          Alias values may be used in a ``states`` parameter value. The
          ``listing`` alias will cause the listing to include all shard ranges
          in a state suitable for contributing to an object listing. The
          ``updating`` alias will cause the listing to include all shard ranges
          in a state suitable to accept an object update.

          If either of these aliases is used then the shard range listing will
          if necessary be extended with a synthesised 'filler' range in order
          to satisfy the requested name range when insufficient actual shard
          ranges are found. Any 'filler' shard range will cover the otherwise
          uncovered tail of the requested name range and will point back to the
          same container.

          The ``auditing`` alias will cause the listing to include all shard
          ranges in a state useful to the sharder while auditing a shard
          container. This alias will not cause a 'filler' range to be added,
          but will cause the container's own shard range to be included in the
          listing. For now, ``auditing`` is only supported when
          'X-Backend-Record-Shard-Format' is 'full'.

        * Shard range listings can be simplified to include only Namespace
          only attributes (name, lower and upper) if the caller send the header
          ``X-Backend-Record-Shard-Format`` with value 'namespace' as a hint
          that it would prefer namespaces. If this header doesn't exist or the
          value is 'full', the listings will default to include all attributes
          of shard ranges. But if params has includes/marker/end_marker then
          the response will be full shard ranges, regardless the header of
          ``X-Backend-Record-Shard-Format``. The response header
          ``X-Backend-Record-Type`` will tell the user what type it gets back.

        * Listings are not normally returned from a deleted container. However,
          the ``X-Backend-Override-Deleted`` header may be used with a value in
          :attr:`swift.common.utils.TRUE_VALUES` to force a shard range
          listing to be returned from a deleted container whose DB file still
          exists.

        :param req: an instance of :class:`swift.common.swob.Request`
        :returns: an instance of :class:`swift.common.swob.Response`
        """
        drive, part, account, container, obj = get_obj_name_and_placement(req)
        params = validate_container_params(req)
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
        db_state = info.get('db_state')
        if record_type == 'auto' and db_state in (SHARDING, SHARDED):
            record_type = 'shard'
        if record_type == 'shard':
            return self.GET_shard(req, broker, container, params, info,
                                  is_deleted, out_content_type)
        else:
            return self.GET_object(req, broker, container, params, info,
                                   is_deleted, out_content_type)

    @timing_stats()
    def GET_shard(self, req, broker, container, params, info,
                  is_deleted, out_content_type):
        """
        Returns a list of persisted shard ranges or namespaces in response.

        :param req: swob.Request object
        :param broker: container DB broker object
        :param container: container name
        :param params: the request params.
        :param info: the global info for the container
        :param is_deleted: the is_deleted status for the container.
        :param out_content_type: content type as a string.
        :returns: an instance of :class:`swift.common.swob.Response`
        """
        override_deleted = info and config_true_value(
            req.headers.get('x-backend-override-deleted', False))
        resp_headers = gen_resp_headers(
            info, is_deleted=is_deleted and not override_deleted)

        if is_deleted and not override_deleted:
            return HTTPNotFound(request=req, headers=resp_headers)

        marker = params.get('marker', '')
        end_marker = params.get('end_marker')
        reverse = config_true_value(params.get('reverse'))
        states = params.get('states')
        includes = params.get('includes')
        include_deleted = config_true_value(
            req.headers.get('x-backend-include-deleted', False))

        resp_headers['X-Backend-Record-Type'] = 'shard'
        override_filter_hdr = req.headers.get(
            'x-backend-override-shard-name-filter', '').lower()
        if override_filter_hdr == info.get('db_state') == 'sharded':
            # respect the request to send back *all* ranges if the db is in
            # sharded state
            resp_headers['X-Backend-Override-Shard-Name-Filter'] = 'true'
            marker = end_marker = includes = None
            reverse = False
        fill_gaps = include_own = False
        if states:
            states = list_from_csv(states)
            fill_gaps = any(('listing' in states, 'updating' in states))
            # The 'auditing' state alias is used by the sharder during
            # shard audit; if the shard is shrinking then it needs to get
            # acceptor shard ranges, which may be the root container
            # itself, so use include_own.
            include_own = 'auditing' in states
            try:
                states = broker.resolve_shard_range_states(states)
            except ValueError:
                return HTTPBadRequest(request=req, body='Bad state')

        # For record type of 'shard', user can specify an additional header
        # to ask for list of Namespaces instead of full ShardRanges.
        # This will allow proxy server who is going to retrieve Namespace
        # to talk to older version of container servers who don't support
        # Namespace yet during upgrade.
        shard_format = req.headers.get(
            'x-backend-record-shard-format', 'full').lower()
        if shard_format == 'namespace':
            resp_headers['X-Backend-Record-Shard-Format'] = 'namespace'
            # Namespace GET does not support all the options of Shard Range
            # GET: 'x-backend-include-deleted' cannot be supported because
            # there is no way for a Namespace to indicate the deleted state;
            # the 'auditing' state query parameter is not supported because it
            # is specific to the sharder which only requests full shard ranges.
            if include_deleted:
                return HTTPBadRequest(
                    request=req, body='No include_deleted for namespace GET')
            if include_own:
                return HTTPBadRequest(
                    request=req, body='No auditing state for namespace GET')
            shard_format_full = False
            container_list = broker.get_namespaces(
                marker, end_marker, includes, reverse, states, fill_gaps)
        else:
            resp_headers['X-Backend-Record-Shard-Format'] = 'full'
            shard_format_full = True
            container_list = broker.get_shard_ranges(
                marker, end_marker, includes, reverse, states=states,
                include_deleted=include_deleted, fill_gaps=fill_gaps,
                include_own=include_own)
        listing = [self.update_shard_record(record, shard_format_full)
                   for record in container_list]
        return self._create_GET_response(req, out_content_type, info,
                                         resp_headers, broker.metadata,
                                         container, listing)

    @timing_stats()
    def GET_object(self, req, broker, container, params, info,
                   is_deleted, out_content_type):
        """
        Returns a list of objects in response.

        :param req: swob.Request object
        :param broker: container DB broker object
        :param container: container name
        :param params: the request params.
        :param info: the global info for the container
        :param is_deleted: the is_deleted status for the container.
        :param out_content_type: content type as a string.
        :returns: an instance of :class:`swift.common.swob.Response`
        """
        marker = params.get('marker', '')
        end_marker = params.get('end_marker')
        reverse = config_true_value(params.get('reverse'))
        path = params.get('path')
        prefix = params.get('prefix')
        delimiter = params.get('delimiter')
        limit = params['limit']
        requested_policy_index = self.get_and_validate_policy_index(req)
        resp_headers = gen_resp_headers(info, is_deleted=is_deleted)
        if is_deleted:
            return HTTPNotFound(request=req, headers=resp_headers)
        resp_headers['X-Backend-Record-Type'] = 'object'
        storage_policy_index = (
            requested_policy_index if requested_policy_index is not None
            else info['storage_policy_index'])
        resp_headers['X-Backend-Record-Storage-Policy-Index'] = \
            storage_policy_index
        # Use the retired db while container is in process of sharding,
        # otherwise use current db
        src_broker = broker.get_brokers()[0]
        container_list = src_broker.list_objects_iter(
            limit, marker, end_marker, prefix, delimiter, path,
            storage_policy_index=storage_policy_index,
            reverse=reverse, allow_reserved=req.allow_reserved_names)
        listing = [self.update_object_record(record)
                   for record in container_list]
        return self._create_GET_response(req, out_content_type, info,
                                         resp_headers, broker.metadata,
                                         container, listing)

    def _create_GET_response(self, req, out_content_type, info, resp_headers,
                             metadata, container, listing):
        for key, (value, _timestamp) in metadata.items():
            if value and (key.lower() in self.save_headers or
                          is_sys_or_user_meta('container', key)):
                resp_headers[str_to_wsgi(key)] = str_to_wsgi(value)

        if out_content_type.endswith('/xml'):
            body = listing_formats.container_to_xml(listing, container)
        elif out_content_type.endswith('/json'):
            body = json.dumps(listing).encode('ascii')
        else:
            body = listing_formats.listing_to_text(listing)

        ret = Response(request=req, headers=resp_headers, body=body,
                       content_type=out_content_type, charset='utf-8')
        ret.last_modified = Timestamp(resp_headers['X-PUT-Timestamp']).ceil()
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
    def UPDATE(self, req):
        """
        Handle HTTP UPDATE request (merge_items RPCs coming from the proxy.)
        """
        drive, part, account, container = get_container_name_and_placement(req)
        req_timestamp = valid_timestamp(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)

        requested_policy_index = self.get_and_validate_policy_index(req)
        broker = self._get_container_broker(drive, part, account, container)
        self._maybe_autocreate(broker, req_timestamp, account,
                               requested_policy_index, req)
        try:
            objs = json.load(req.environ['wsgi.input'])
        except ValueError as err:
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        broker.merge_items(objs)
        return HTTPAccepted(request=req)

    @public
    @timing_stats()
    def POST(self, req):
        """
        Handle HTTP POST request.

        A POST request will update the container's ``put_timestamp``, unless
        it has an ``X-Backend-No-Timestamp-Update`` header with a truthy value.

        :param req: an instance of :class:`~swift.common.swob.Request`.
        """
        drive, part, account, container = get_container_name_and_placement(req)
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
        if not config_true_value(
                req.headers.get('x-backend-no-timestamp-update', False)):
            broker.update_put_timestamp(req_timestamp.internal)
        self._update_metadata(req, broker, req_timestamp, 'POST')
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(wsgi_to_str(req.path_info), internal=True):
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
                self.logger.exception(
                    'ERROR __call__ error with %(method)s %(path)s ',
                    {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        if self.log_requests:
            trans_time = time.time() - start_time
            log_message = get_log_line(req, res, trans_time, '',
                                       self.log_format,
                                       self.anonymization_method,
                                       self.anonymization_salt)
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


def main():
    conf_file, options = parse_options(test_config=True)
    sys.exit(run_wsgi(conf_file, 'container-server', **options))


if __name__ == '__main__':
    main()
