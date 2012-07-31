# Copyright (c) 2010-2012 OpenStack, LLC.
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

from __future__ import with_statement

import os
import time
import traceback
from urllib import unquote
from xml.sax import saxutils

from eventlet import Timeout
from webob import Request, Response
from webob.exc import HTTPAccepted, HTTPBadRequest, \
    HTTPCreated, HTTPForbidden, HTTPInternalServerError, \
    HTTPMethodNotAllowed, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPConflict
import simplejson

import swift.common.db
from swift.common.db import AccountBroker
from swift.common.utils import get_logger, get_param, hash_path, public, \
    normalize_timestamp, split_path, storage_directory, TRUE_VALUES, \
    validate_device_partition
from swift.common.constraints import ACCOUNT_LISTING_LIMIT, \
    check_mount, check_float, check_utf8, FORMAT2CONTENT_TYPE
from swift.common.db_replicator import ReplicatorRpc
from swift.common.http import HTTPInsufficientStorage


DATADIR = 'accounts'


class AccountController(object):
    """WSGI controller for the account server."""

    def __init__(self, conf):
        self.logger = get_logger(conf, log_route='account-server')
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.replicator_rpc = ReplicatorRpc(self.root, DATADIR, AccountBroker,
            self.mount_check, logger=self.logger)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        swift.common.db.DB_PREALLOCATION = \
            conf.get('db_preallocation', 'f').lower() in TRUE_VALUES

    def _get_account_broker(self, drive, part, account):
        hsh = hash_path(account)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, drive, db_dir, hsh + '.db')
        return AccountBroker(db_path, account=account, logger=self.logger)

    @public
    def DELETE(self, req):
        """Handle HTTP DELETE request."""
        start_time = time.time()
        try:
            drive, part, account = split_path(unquote(req.path), 3)
            validate_device_partition(drive, part)
        except ValueError, err:
            self.logger.increment('DELETE.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                                    request=req)
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('DELETE.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        if 'x-timestamp' not in req.headers or \
                    not check_float(req.headers['x-timestamp']):
            self.logger.increment('DELETE.errors')
            return HTTPBadRequest(body='Missing timestamp', request=req,
                        content_type='text/plain')
        broker = self._get_account_broker(drive, part, account)
        if broker.is_deleted():
            self.logger.timing_since('DELETE.timing', start_time)
            return HTTPNotFound(request=req)
        broker.delete_db(req.headers['x-timestamp'])
        self.logger.timing_since('DELETE.timing', start_time)
        return HTTPNoContent(request=req)

    @public
    def PUT(self, req):
        """Handle HTTP PUT request."""
        start_time = time.time()
        try:
            drive, part, account, container = split_path(unquote(req.path),
                                                         3, 4)
            validate_device_partition(drive, part)
        except ValueError, err:
            self.logger.increment('PUT.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('PUT.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account)
        if container:   # put account container
            if 'x-trans-id' in req.headers:
                broker.pending_timeout = 3
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                broker.initialize(normalize_timestamp(
                    req.headers.get('x-timestamp') or time.time()))
            if req.headers.get('x-account-override-deleted', 'no').lower() != \
                    'yes' and broker.is_deleted():
                self.logger.timing_since('PUT.timing', start_time)
                return HTTPNotFound(request=req)
            broker.put_container(container, req.headers['x-put-timestamp'],
                req.headers['x-delete-timestamp'],
                req.headers['x-object-count'],
                req.headers['x-bytes-used'])
            self.logger.timing_since('PUT.timing', start_time)
            if req.headers['x-delete-timestamp'] > \
                    req.headers['x-put-timestamp']:
                return HTTPNoContent(request=req)
            else:
                return HTTPCreated(request=req)
        else:   # put account
            timestamp = normalize_timestamp(req.headers['x-timestamp'])
            if not os.path.exists(broker.db_file):
                broker.initialize(timestamp)
                created = True
            elif broker.is_status_deleted():
                self.logger.timing_since('PUT.timing', start_time)
                return HTTPForbidden(request=req, body='Recently deleted')
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp)
                if broker.is_deleted():
                    self.logger.increment('PUT.errors')
                    return HTTPConflict(request=req)
            metadata = {}
            metadata.update((key, (value, timestamp))
                for key, value in req.headers.iteritems()
                if key.lower().startswith('x-account-meta-'))
            if metadata:
                broker.update_metadata(metadata)
            self.logger.timing_since('PUT.timing', start_time)
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)

    @public
    def HEAD(self, req):
        """Handle HTTP HEAD request."""
        # TODO(refactor): The account server used to provide a 'account and
        # container existence check all-in-one' call by doing a HEAD with a
        # container path. However, container existence is now checked with the
        # container servers directly so this is no longer needed. We should
        # refactor out the container existence check here and retest
        # everything.
        start_time = time.time()
        try:
            drive, part, account, container = split_path(unquote(req.path),
                                                         3, 4)
            validate_device_partition(drive, part)
        except ValueError, err:
            self.logger.increment('HEAD.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                                    request=req)
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('HEAD.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account)
        if not container:
            broker.pending_timeout = 0.1
            broker.stale_reads_ok = True
        if broker.is_deleted():
            self.logger.timing_since('HEAD.timing', start_time)
            return HTTPNotFound(request=req)
        info = broker.get_info()
        headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        if container:
            container_ts = broker.get_container_timestamp(container)
            if container_ts is not None:
                headers['X-Container-Timestamp'] = container_ts
        headers.update((key, value)
            for key, (value, timestamp) in broker.metadata.iteritems()
            if value != '')
        self.logger.timing_since('HEAD.timing', start_time)
        return HTTPNoContent(request=req, headers=headers)

    @public
    def GET(self, req):
        """Handle HTTP GET request."""
        start_time = time.time()
        try:
            drive, part, account = split_path(unquote(req.path), 3)
            validate_device_partition(drive, part)
        except ValueError, err:
            self.logger.increment('GET.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                                    request=req)
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('GET.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account)
        broker.pending_timeout = 0.1
        broker.stale_reads_ok = True
        if broker.is_deleted():
            self.logger.timing_since('GET.timing', start_time)
            return HTTPNotFound(request=req)
        info = broker.get_info()
        resp_headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        resp_headers.update((key, value)
            for key, (value, timestamp) in broker.metadata.iteritems()
            if value != '')
        try:
            prefix = get_param(req, 'prefix')
            delimiter = get_param(req, 'delimiter')
            if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
                # delimiters can be made more flexible later
                self.logger.increment('GET.errors')
                return HTTPPreconditionFailed(body='Bad delimiter')
            limit = ACCOUNT_LISTING_LIMIT
            given_limit = get_param(req, 'limit')
            if given_limit and given_limit.isdigit():
                limit = int(given_limit)
                if limit > ACCOUNT_LISTING_LIMIT:
                    self.logger.increment('GET.errors')
                    return HTTPPreconditionFailed(request=req,
                        body='Maximum limit is %d' % ACCOUNT_LISTING_LIMIT)
            marker = get_param(req, 'marker', '')
            end_marker = get_param(req, 'end_marker')
            query_format = get_param(req, 'format')
        except UnicodeDecodeError, err:
            self.logger.increment('GET.errors')
            return HTTPBadRequest(body='parameters not utf8',
                                  content_type='text/plain', request=req)
        if query_format:
            req.accept = FORMAT2CONTENT_TYPE.get(query_format.lower(),
                                                 FORMAT2CONTENT_TYPE['plain'])
        try:
            out_content_type = req.accept.best_match(
                                    ['text/plain', 'application/json',
                                     'application/xml', 'text/xml'],
                                    default_match='text/plain')
        except AssertionError, err:
            self.logger.increment('GET.errors')
            return HTTPBadRequest(body='bad accept header: %s' % req.accept,
                                  content_type='text/plain', request=req)
        account_list = broker.list_containers_iter(limit, marker, end_marker,
                                                   prefix, delimiter)
        if out_content_type == 'application/json':
            json_pattern = ['"name":%s', '"count":%s', '"bytes":%s']
            json_pattern = '{' + ','.join(json_pattern) + '}'
            json_out = []
            for (name, object_count, bytes_used, is_subdir) in account_list:
                name = simplejson.dumps(name)
                if is_subdir:
                    json_out.append('{"subdir":%s}' % name)
                else:
                    json_out.append(json_pattern %
                        (name, object_count, bytes_used))
            account_list = '[' + ','.join(json_out) + ']'
        elif out_content_type.endswith('/xml'):
            output_list = ['<?xml version="1.0" encoding="UTF-8"?>',
                           '<account name="%s">' % account]
            for (name, object_count, bytes_used, is_subdir) in account_list:
                name = saxutils.escape(name)
                if is_subdir:
                    output_list.append('<subdir name="%s" />' % name)
                else:
                    item = '<container><name>%s</name><count>%s</count>' \
                           '<bytes>%s</bytes></container>' % \
                           (name, object_count, bytes_used)
                    output_list.append(item)
            output_list.append('</account>')
            account_list = '\n'.join(output_list)
        else:
            if not account_list:
                self.logger.timing_since('GET.timing', start_time)
                return HTTPNoContent(request=req, headers=resp_headers)
            account_list = '\n'.join(r[0] for r in account_list) + '\n'
        ret = Response(body=account_list, request=req, headers=resp_headers)
        ret.content_type = out_content_type
        ret.charset = 'utf-8'
        self.logger.timing_since('GET.timing', start_time)
        return ret

    @public
    def REPLICATE(self, req):
        """
        Handle HTTP REPLICATE request.
        Handler for RPC calls for account replication.
        """
        start_time = time.time()
        try:
            post_args = split_path(unquote(req.path), 3)
            drive, partition, hash = post_args
            validate_device_partition(drive, partition)
        except ValueError, err:
            self.logger.increment('REPLICATE.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                                    request=req)
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('REPLICATE.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        try:
            args = simplejson.load(req.environ['wsgi.input'])
        except ValueError, err:
            self.logger.increment('REPLICATE.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain')
        ret = self.replicator_rpc.dispatch(post_args, args)
        ret.request = req
        self.logger.timing_since('REPLICATE.timing', start_time)
        return ret

    @public
    def POST(self, req):
        """Handle HTTP POST request."""
        start_time = time.time()
        try:
            drive, part, account = split_path(unquote(req.path), 3)
            validate_device_partition(drive, part)
        except ValueError, err:
            self.logger.increment('POST.errors')
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        if 'x-timestamp' not in req.headers or \
                not check_float(req.headers['x-timestamp']):
            self.logger.increment('POST.errors')
            return HTTPBadRequest(body='Missing or bad timestamp',
                request=req, content_type='text/plain')
        if self.mount_check and not check_mount(self.root, drive):
            self.logger.increment('POST.errors')
            return HTTPInsufficientStorage(drive=drive, request=req)
        broker = self._get_account_broker(drive, part, account)
        if broker.is_deleted():
            self.logger.timing_since('POST.timing', start_time)
            return HTTPNotFound(request=req)
        timestamp = normalize_timestamp(req.headers['x-timestamp'])
        metadata = {}
        metadata.update((key, (value, timestamp))
            for key, value in req.headers.iteritems()
            if key.lower().startswith('x-account-meta-'))
        if metadata:
            broker.update_metadata(metadata)
        self.logger.timing_since('POST.timing', start_time)
        return HTTPNoContent(request=req)

    def __call__(self, env, start_response):
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8')
        else:
            try:
                # disallow methods which are not publicly accessible
                try:
                    method = getattr(self, req.method)
                    getattr(method, 'publicly_accessible')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    res = method(req)
            except (Exception, Timeout):
                self.logger.exception(_('ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = '%.4f' % (time.time() - start_time)
        additional_info = ''
        if res.headers.get('x-container-timestamp') is not None:
            additional_info += 'x-container-timestamp: %s' % \
                res.headers['x-container-timestamp']
        log_message = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %s "%s"' % (
            req.remote_addr,
            time.strftime('%d/%b/%Y:%H:%M:%S +0000', time.gmtime()),
            req.method, req.path,
            res.status.split()[0], res.content_length or '-',
            req.headers.get('x-trans-id', '-'),
            req.referer or '-', req.user_agent or '-',
            trans_time,
            additional_info)
        if req.method.upper() == 'REPLICATE':
            self.logger.debug(log_message)
        else:
            self.logger.info(log_message)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI account server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return AccountController(conf)
