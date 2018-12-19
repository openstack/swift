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
from swift import gettext_ as _

from eventlet import Timeout

import six

import swift.common.db
from swift.account.backend import AccountBroker, DATADIR
from swift.account.utils import account_listing_response, get_response_headers
from swift.common.db import DatabaseConnectionError, DatabaseAlreadyExists
from swift.common.request_helpers import get_param, \
    split_and_validate_path
from swift.common.utils import get_logger, hash_path, public, \
    Timestamp, storage_directory, config_true_value, \
    timing_stats, replication, get_log_line, \
    config_fallocate_value, fs_has_free_space
from swift.common.constraints import valid_timestamp, check_utf8, check_drive
from swift.common import constraints
from swift.common.db_replicator import ReplicatorRpc
from swift.common.base_storage_server import BaseStorageServer
from swift.common.middleware import listing_formats
from swift.common.swob import HTTPAccepted, HTTPBadRequest, \
    HTTPCreated, HTTPForbidden, HTTPInternalServerError, \
    HTTPMethodNotAllowed, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPConflict, Request, \
    HTTPInsufficientStorage, HTTPException
from swift.common.request_helpers import is_sys_or_user_meta


class AccountController(BaseStorageServer):
    """WSGI controller for the account server."""

    server_type = 'account-server'

    def __init__(self, conf, logger=None):
        super(AccountController, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='account-server')
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.replicator_rpc = ReplicatorRpc(self.root, DATADIR, AccountBroker,
                                            self.mount_check,
                                            logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.fallocate_reserve, self.fallocate_is_percent = \
            config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    def _get_account_broker(self, drive, part, account, **kwargs):
        hsh = hash_path(account)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        kwargs.setdefault('account', account)
        kwargs.setdefault('logger', self.logger)
        return AccountBroker(db_path, **kwargs)

    def _deleted_response(self, broker, req, resp, body=''):
        # We are here since either the account does not exist or
        # it exists but marked for deletion.
        headers = {}
        # Try to check if account exists and is marked for deletion
        try:
            if broker.is_status_deleted():
                # Account does exist and is marked for deletion
                headers = {'X-Account-Status': 'Deleted'}
        except DatabaseConnectionError:
            # Account does not exist!
            pass
        return resp(request=req, headers=headers, charset='utf-8', body=body)

    def check_free_space(self, drive):
        drive_root = os.path.join(self.root, drive)
        return fs_has_free_space(
            drive_root, self.fallocate_reserve, self.fallocate_is_percent)

    @public
    @timing_stats()
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        drive, part, account = split_and_validate_path(req, 3)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        req_timestamp = valid_timestamp(req)
        broker = self._get_account_broker(drive, part, account)
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        broker.delete_db(req_timestamp.internal)
        return self._deleted_response(broker, req, HTTPNoContent)

    @public
    @timing_stats()
    def PUT(self, req):
        """Handle HTTP PUT request."""
        drive, part, account, container = split_and_validate_path(req, 3, 4)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        if container:   # put account container
            if 'x-timestamp' not in req.headers:
                timestamp = Timestamp.now()
            else:
                timestamp = valid_timestamp(req)
            pending_timeout = None
            container_policy_index = \
                req.headers.get('X-Backend-Storage-Policy-Index', 0)
            if 'x-trans-id' in req.headers:
                pending_timeout = 3
            broker = self._get_account_broker(drive, part, account,
                                              pending_timeout=pending_timeout)
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                try:
                    broker.initialize(timestamp.internal)
                except DatabaseAlreadyExists:
                    pass
            if req.headers.get('x-account-override-deleted', 'no').lower() != \
                    'yes' and broker.is_deleted():
                return HTTPNotFound(request=req)
            broker.put_container(container, req.headers['x-put-timestamp'],
                                 req.headers['x-delete-timestamp'],
                                 req.headers['x-object-count'],
                                 req.headers['x-bytes-used'],
                                 container_policy_index)
            if req.headers['x-delete-timestamp'] > \
                    req.headers['x-put-timestamp']:
                return HTTPNoContent(request=req)
            else:
                return HTTPCreated(request=req)
        else:   # put account
            timestamp = valid_timestamp(req)
            broker = self._get_account_broker(drive, part, account)
            if not os.path.exists(broker.db_file):
                try:
                    broker.initialize(timestamp.internal)
                    created = True
                except DatabaseAlreadyExists:
                    created = False
            elif broker.is_status_deleted():
                return self._deleted_response(broker, req, HTTPForbidden,
                                              body='Recently deleted')
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp.internal)
                if broker.is_deleted():
                    return HTTPConflict(request=req)
            metadata = {}
            if six.PY2:
                metadata.update((key, (value, timestamp.internal))
                                for key, value in req.headers.items()
                                if is_sys_or_user_meta('account', key))
            else:
                for key, value in req.headers.items():
                    if is_sys_or_user_meta('account', key):
                        # Cast to native strings, so that json inside
                        # updata_metadata eats the data.
                        try:
                            value = value.encode('latin-1').decode('utf-8')
                        except UnicodeDecodeError:
                            raise HTTPBadRequest(
                                'Metadata must be valid UTF-8')
                        metadata[key] = (value, timestamp.internal)
            if metadata:
                broker.update_metadata(metadata, validate_metadata=True)
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)

    @public
    @timing_stats()
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        drive, part, account = split_and_validate_path(req, 3)
        out_content_type = listing_formats.get_listing_content_type(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account,
                                          pending_timeout=0.1,
                                          stale_reads_ok=True)
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        headers = get_response_headers(broker)
        headers['Content-Type'] = out_content_type
        return HTTPNoContent(request=req, headers=headers, charset='utf-8')

    @public
    @timing_stats()
    def GET(self, req):
        """Handle HTTP GET request."""
        drive, part, account = split_and_validate_path(req, 3)
        prefix = get_param(req, 'prefix')
        delimiter = get_param(req, 'delimiter')
        if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
            # delimiters can be made more flexible later
            return HTTPPreconditionFailed(body='Bad delimiter')
        limit = constraints.ACCOUNT_LISTING_LIMIT
        given_limit = get_param(req, 'limit')
        reverse = config_true_value(get_param(req, 'reverse'))
        if given_limit and given_limit.isdigit():
            limit = int(given_limit)
            if limit > constraints.ACCOUNT_LISTING_LIMIT:
                return HTTPPreconditionFailed(
                    request=req,
                    body='Maximum limit is %d' %
                    constraints.ACCOUNT_LISTING_LIMIT)
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        out_content_type = listing_formats.get_listing_content_type(req)

        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account,
                                          pending_timeout=0.1,
                                          stale_reads_ok=True)
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        return account_listing_response(account, req, out_content_type, broker,
                                        limit, marker, end_marker, prefix,
                                        delimiter, reverse)

    @public
    @replication
    @timing_stats()
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request.
        Handler for RPC calls for account replication.
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
        drive, part, account = split_and_validate_path(req, 3)
        req_timestamp = valid_timestamp(req)
        try:
            check_drive(self.root, drive, self.mount_check)
        except ValueError:
            return HTTPInsufficientStorage(drive=drive, request=req)
        if not self.check_free_space(drive):
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account)
        if broker.is_deleted():
            return self._deleted_response(broker, req, HTTPNotFound)
        metadata = {}
        metadata.update((key, (value, req_timestamp.internal))
                        for key, value in req.headers.items()
                        if is_sys_or_user_meta('account', key))
        if metadata:
            broker.update_metadata(metadata, validate_metadata=True)
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which are not publicly accessible
                if req.method not in self.allowed_methods:
                    res = HTTPMethodNotAllowed()
                else:
                    res = getattr(self, req.method)(req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_('ERROR __call__ error with %(method)s'
                                        ' %(path)s '),
                                      {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        if self.log_requests:
            trans_time = time.time() - start_time
            additional_info = ''
            if res.headers.get('x-container-timestamp') is not None:
                additional_info += 'x-container-timestamp: %s' % \
                    res.headers['x-container-timestamp']
            log_msg = get_log_line(req, res, trans_time, additional_info)
            if req.method.upper() == 'REPLICATE':
                self.logger.debug(log_msg)
            else:
                self.logger.info(log_msg)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI account server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return AccountController(conf)
